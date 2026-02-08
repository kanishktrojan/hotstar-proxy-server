require('dotenv').config();
const cluster = require('cluster');
const os = require('os');
const express = require('express');
const path = require('path');
const fetch = require('node-fetch');
const http = require('http');
const https = require('https');
const zlib = require('zlib');

const StreamCache = require('./lib/cache');
const RequestDedup = require('./lib/dedup');
const RateLimiter = require('./lib/rate-limiter');
const StreamManager = require('./lib/stream-manager');

// ═══════════════════════════════════════════════════
//  CLUSTER MODE — use all CPU cores
// ═══════════════════════════════════════════════════

const WORKER_COUNT = Math.min(os.cpus().length, 4); // cap at 4 workers
const USE_CLUSTER = process.env.NO_CLUSTER !== '1' && WORKER_COUNT > 1;

if (USE_CLUSTER && cluster.isPrimary) {
  console.log(`[Cluster] Primary ${process.pid} starting ${WORKER_COUNT} workers...`);
  for (let i = 0; i < WORKER_COUNT; i++) {
    cluster.fork();
  }
  cluster.on('exit', (worker, code) => {
    console.warn(`[Cluster] Worker ${worker.process.pid} exited (code ${code}), restarting...`);
    cluster.fork();
  });
} else {
  // Worker or single-process mode
  startServer();
}

function startServer() {

const app = express();
app.use(express.json());

// ═══════════════════════════════════════════════════
//  INITIALIZE COMPONENTS
// ═══════════════════════════════════════════════════

// HTTP keep-alive agents — reuse TCP connections to CDN
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 100, maxFreeSockets: 20, timeout: 30000 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 100, maxFreeSockets: 20, timeout: 30000 });

const streams = new StreamManager();
const cache = new StreamCache({ maxMemoryMB: 768 });
const dedup = new RequestDedup();
const rateLimiter = new RateLimiter(60); // Max 60 req/sec to CDN per worker

// Cache TTLs
const CACHE_TTL = {
  MASTER_PLAYLIST: 2000,   // 2s
  CHILD_PLAYLIST:  800,    // 800ms — fast segment discovery
  SEGMENT:         600000, // 10m — .ts segments are immutable
};

// ═══════════════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════════════

function getHeaders(cookieValue) {
  // Normalize cookie value — handle all formats:
  //   1. "exp=123~acl=..."                  → just hdntl value, no prefix
  //   2. "hdntl=exp=123~acl=..."            → hdntl with prefix
  //   3. "CloudFront-...; hdntl=exp=..."    → full multi-cookie string
  let cookie;
  if (cookieValue.includes(';')) {
    // Multi-cookie string — send as-is (CloudFront + hdntl)
    cookie = cookieValue;
  } else if (cookieValue.startsWith('hdntl=')) {
    // Already has hdntl= prefix
    cookie = cookieValue;
  } else {
    // Raw hdntl value — add the prefix
    cookie = `hdntl=${cookieValue}`;
  }
  return {
    'Cookie': cookie,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.hotstar.com/',
    'Origin': 'https://www.hotstar.com',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
  };
}

function getAgent(url) {
  return url.startsWith('https') ? httpsAgent : httpAgent;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const agent = getAgent(url);
    const resp = await fetch(url, { ...options, agent, signal: controller.signal });
    clearTimeout(timer);
    return resp;
  } catch (err) {
    clearTimeout(timer);
    if (err.name === 'AbortError') throw new Error(`Timeout after ${timeoutMs}ms`);
    throw err;
  }
}

/**
 * Cached + deduped fetch for text content (playlists)
 */
async function cachedFetchText(url, cacheKey, ttl, hdntl) {
  // 1. Check cache
  const cached = cache.get(cacheKey);
  if (cached) return { text: cached.data, fromCache: true };

  // 2. Dedup — if same URL is in-flight, wait for it
  return dedup.dedupFetch(cacheKey, async () => {
    // 3. Rate limit before hitting CDN (high priority for playlists)
    await rateLimiter.acquire('high');

    const response = await fetchWithTimeout(url, { headers: getHeaders(hdntl) });
    if (!response.ok) {
      let body = '';
      try { body = await response.text(); } catch (_) {}
      console.error(`[CDN ${response.status}] URL: ${url}`);
      if (body) console.error(`[CDN ${response.status}] Body: ${body.slice(0, 500)}`);
      rateLimiter.onCdnError(response.status);
      if (response.status === 403) {
        throw new Error(`CDN 403 Forbidden — your hdntl token is likely expired or invalid. Update the cookie via the dashboard.`);
      }
      throw new Error(`CDN ${response.status}`);
    }

    rateLimiter.onCdnSuccess();
    const text = await response.text();
    cache.set(cacheKey, text, {}, ttl);
    return { text, fromCache: false };
  });
}

/**
 * Look up stream config and validate it.
 * Returns { stream, error } — if error is set, respond with it.
 */
function resolveStream(slug) {
  const stream = streams.getStreamFull(slug);
  if (!stream) {
    return { stream: null, error: { status: 404, message: `Stream "${slug}" not found` } };
  }
  if (!stream.hdntl) {
    return { stream: null, error: { status: 503, message: 'No cookie configured for this stream' } };
  }
  if (stream.tokenExpiry && Date.now() > stream.tokenExpiry) {
    return { stream, error: { status: 503, message: 'Token expired — update the cookie via the dashboard' } };
  }
  // Warn if token expires within 2 minutes
  if (stream.tokenExpiry && (stream.tokenExpiry - Date.now()) < 120000) {
    console.warn(`[${slug}] Token expiring in ${Math.round((stream.tokenExpiry - Date.now()) / 1000)}s — refresh soon!`);
  }
  return { stream, error: null };
}

// ═══════════════════════════════════════════════════
//  CORS
// ═══════════════════════════════════════════════════

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ── Compression for playlist responses ──
app.use((req, res, next) => {
  // Only compress text responses (playlists), not binary segments
  if (req.path.includes('/segment')) return next();
  const acceptEncoding = req.headers['accept-encoding'] || '';
  if (acceptEncoding.includes('gzip')) {
    const origSend = res.send.bind(res);
    res.send = function(body) {
      if (typeof body === 'string' && body.length > 256) {
        res.set('Content-Encoding', 'gzip');
        res.removeHeader('Content-Length');
        zlib.gzip(Buffer.from(body), (err, compressed) => {
          if (err) return origSend(body);
          origSend(compressed);
        });
      } else {
        origSend(body);
      }
    };
  }
  next();
});

// ═══════════════════════════════════════════════════
//  STREAM PROXY ROUTES
//  Each stream is accessed via /stream/:slug/...
// ═══════════════════════════════════════════════════

// ── Master playlist ──
app.get('/stream/:slug/live.m3u8', async (req, res) => {
  const { slug } = req.params;

  try {
    const { stream, error } = resolveStream(slug);
    if (error) return res.status(error.status).json({ error: error.message });

    const url = `${stream.streamBase}/${stream.playlistFile}`;
    const { text: playlist, fromCache } = await cachedFetchText(
      url,
      `${slug}:master:${stream.playlistFile}`,
      CACHE_TTL.MASTER_PLAYLIST,
      stream.hdntl
    );

    if (!playlist.trimStart().startsWith('#EXTM3U')) {
      return res.status(502).json({ error: 'Invalid playlist response from CDN' });
    }

    // Rewrite URLs to proxy through this server
    let rewritten = playlist
      // Absolute http(s) .ts URLs
      .replace(
        /^(?!#)(https?:\/\/[^\s]+\.ts[^\s]*)$/gm,
        (match) => `/stream/${slug}/segment?url=${encodeURIComponent(match)}`
      )
      // Absolute-path .ts URLs (starts with /)
      .replace(
        /^(?!#)(\/[^\s]+\.ts[^\s]*)$/gm,
        (match) => `/stream/${slug}/segment?path=${encodeURIComponent(match)}`
      )
      // Relative .ts URLs (no leading /)
      .replace(
        /^(?!#)(?!\/)((?!https?:\/\/)[^\s]+\.ts[^\s]*)$/gm,
        `/stream/${slug}/segment?path=$1`
      )
      // Absolute http(s) .m3u8 playlist references
      .replace(
        /^(?!#)(https?:\/\/[^\s]+\.m3u8[^\s]*)$/gm,
        (match) => `/stream/${slug}/playlist?url=${encodeURIComponent(match)}`
      )
      // Absolute-path .m3u8 references (starts with /)
      .replace(
        /^(?!#)(\/[^\s]+\.m3u8[^\s]*)$/gm,
        (match) => `/stream/${slug}/playlist?path=${encodeURIComponent(match)}`
      )
      // Relative .m3u8 playlist references
      .replace(
        /^(?!#)(?!\/)((?!https?:\/\/)[^\s]+\.m3u8[^\s]*)$/gm,
        `/stream/${slug}/playlist?path=$1`
      );

    res.set('Content-Type', 'application/vnd.apple.mpegurl');
    res.set('Cache-Control', 'no-cache, no-store');
    res.set('X-Cache', fromCache ? 'HIT' : 'MISS');
    res.send(rewritten);

  } catch (err) {
    console.error(`[${slug}][M3U8] ${err.message}`);
    res.status(502).json({ error: err.message });
  }
});

// ── Child playlists ──
app.get('/stream/:slug/playlist', async (req, res) => {
  const { slug } = req.params;

  try {
    const { stream, error } = resolveStream(slug);
    if (error) return res.status(error.status).json({ error: error.message });

    // Resolve the actual CDN URL for this playlist
    let url;
    let filename;
    if (req.query.url) {
      // Full absolute URL
      url = decodeURIComponent(req.query.url);
      filename = url.split('/').pop().split('?')[0];
    } else if (req.query.path) {
      const pathVal = decodeURIComponent(req.query.path);
      if (pathVal.startsWith('/')) {
        // Absolute path — build from host
        const baseUrl = new URL(stream.streamBase);
        url = `${baseUrl.protocol}//${baseUrl.host}${pathVal}`;
      } else {
        // Relative path
        url = `${stream.streamBase}/${pathVal}`;
      }
      filename = pathVal.split('/').pop().split('?')[0];
    } else {
      return res.status(400).json({ error: 'Missing path or url parameter' });
    }
    const { text: playlist, fromCache } = await cachedFetchText(
      url,
      `${slug}:child:${filename}`,
      CACHE_TTL.CHILD_PLAYLIST,
      stream.hdntl
    );

    // Rewrite .ts references to proxy
    let rewritten = playlist
      // Absolute http(s) .ts URLs
      .replace(
        /^(?!#)(https?:\/\/[^\s]+\.ts[^\s]*)$/gm,
        (match) => `/stream/${slug}/segment?url=${encodeURIComponent(match)}`
      )
      // Absolute-path .ts URLs (starts with /)
      .replace(
        /^(?!#)(\/[^\s]+\.ts[^\s]*)$/gm,
        (match) => `/stream/${slug}/segment?path=${encodeURIComponent(match)}`
      )
      // Relative .ts URLs
      .replace(
        /^(?!#)(?!\/)((?!https?:\/\/)[^\s]+\.ts[^\s]*)$/gm,
        `/stream/${slug}/segment?path=$1`
      );

    // ── Prefetch: warm cache for segments listed in this playlist ──
    const segMatches = playlist.match(/^(?!#)([^\s]+\.ts[^\s]*)$/gm);
    if (segMatches && segMatches.length > 0) {
      const toWarm = segMatches.slice(-3);
      for (const seg of toWarm) {
        let segUrl;
        if (seg.startsWith('http')) {
          segUrl = seg;
        } else if (seg.startsWith('/')) {
          const baseUrl = new URL(stream.streamBase);
          segUrl = `${baseUrl.protocol}//${baseUrl.host}${seg}`;
        } else {
          segUrl = `${stream.streamBase}/${seg}`;
        }
        const segName = seg.split('/').pop().split('?')[0];
        const segKey = `${slug}:seg:${segName}`;
        if (!cache.has(segKey) && !dedup.inflight.has(segKey)) {
          dedup.dedupFetch(segKey, async () => {
            try {
              await rateLimiter.acquire('normal');
              const resp = await fetchWithTimeout(segUrl, { headers: getHeaders(stream.hdntl) }, 8000);
              if (resp.ok) {
                const buf = await resp.buffer();
                cache.set(segKey, buf, { 'content-length': resp.headers.get('content-length') }, CACHE_TTL.SEGMENT);
                rateLimiter.onCdnSuccess();
              }
            } catch (_) { /* prefetch failure is non-critical */ }
          }).catch(() => {});
        }
      }
    }

    res.set('Content-Type', 'application/vnd.apple.mpegurl');
    res.set('Cache-Control', 'no-cache');
    res.set('X-Cache', fromCache ? 'HIT' : 'MISS');
    res.send(rewritten);

  } catch (err) {
    console.error(`[${slug}][PLAYLIST] ${err.message}`);
    res.status(502).send('Playlist proxy error');
  }
});

// ── TS Segments (unified route) ──
app.get('/stream/:slug/segment', async (req, res) => {
  const { slug } = req.params;

  try {
    const { stream, error } = resolveStream(slug);
    if (error) return res.status(error.status).json({ error: error.message });

    // Resolve the actual CDN URL for this segment
    let url;
    let segName;
    if (req.query.url) {
      // Full absolute URL
      url = decodeURIComponent(req.query.url);
      segName = url.split('/').pop().split('?')[0];
    } else if (req.query.path) {
      const pathVal = decodeURIComponent(req.query.path);
      if (pathVal.startsWith('/')) {
        const baseUrl = new URL(stream.streamBase);
        url = `${baseUrl.protocol}//${baseUrl.host}${pathVal}`;
      } else {
        url = `${stream.streamBase}/${pathVal}`;
      }
      segName = pathVal.split('/').pop().split('?')[0];
    } else {
      return res.status(400).json({ error: 'Missing path or url parameter' });
    }

    const cacheKey = `${slug}:seg:${segName}`;

    // Check cache first
    const cached = cache.get(cacheKey);
    if (cached) {
      res.set('Content-Type', 'video/mp2t');
      res.set('Cache-Control', 'max-age=300');
      res.set('X-Cache', 'HIT');
      if (cached.headers['content-length']) {
        res.set('Content-Length', cached.headers['content-length']);
      }
      return res.send(cached.data);
    }

    // Dedup + rate limit
    const result = await dedup.dedupFetch(cacheKey, async () => {
      await rateLimiter.acquire();

      const response = await fetchWithTimeout(url, { headers: getHeaders(stream.hdntl) }, 12000);

      if (!response.ok) {
        rateLimiter.onCdnError(response.status);
        if (response.status === 403) {
          console.error(`[${slug}][TS] CDN 403 for ${segName} — token likely expired`);
          throw new Error(`CDN 403 Forbidden — hdntl token expired or invalid`);
        }
        throw new Error(`Segment CDN ${response.status}`);
      }

      rateLimiter.onCdnSuccess();
      const buffer = await response.buffer();
      const headers = {
        'content-length': response.headers.get('content-length'),
      };

      cache.set(cacheKey, buffer, headers, CACHE_TTL.SEGMENT);
      return { buffer, headers };
    });

    res.set('Content-Type', 'video/mp2t');
    res.set('Cache-Control', 'max-age=300');
    res.set('X-Cache', 'MISS');
    if (result.headers['content-length']) {
      res.set('Content-Length', result.headers['content-length']);
    }
    res.send(result.buffer);

  } catch (err) {
    console.error(`[${slug}][TS] ${err.message}`);
    res.status(502).send('Segment error');
  }
});

// ═══════════════════════════════════════════════════
//  API ROUTES — Stream Management
// ═══════════════════════════════════════════════════

// ── List all streams ──
app.get('/api/streams', (req, res) => {
  res.json(streams.listStreams());
});

// ── Add a new stream ──
app.post('/api/streams', (req, res) => {
  try {
    const { slug, name, streamUrl, hdntl } = req.body;

    if (!streamUrl || !hdntl) {
      return res.status(400).json({ error: 'streamUrl and hdntl are required' });
    }

    const stream = streams.addStream({ slug, name, streamUrl, hdntl });

    res.status(201).json({
      success: true,
      stream: {
        slug: stream.slug,
        name: stream.name,
        proxyUrl: `/stream/${stream.slug}/live.m3u8`,
        tokenExpiry: stream.tokenExpiry,
        remainingMinutes: stream.tokenExpiry
          ? Math.round((stream.tokenExpiry - Date.now()) / 60000)
          : null,
      },
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// ── Update a stream (full update) ──
app.put('/api/streams/:slug', (req, res) => {
  try {
    const { slug } = req.params;
    const existing = streams.getStream(slug);
    if (!existing) return res.status(404).json({ error: 'Stream not found' });

    const { name, streamUrl, hdntl } = req.body;

    const stream = streams.addStream({
      slug,
      name: name || existing.name,
      streamUrl: streamUrl || existing.streamUrl,
      hdntl: hdntl || existing.hdntl,
    });

    // Invalidate cache for this stream so new config takes effect
    cache.invalidateStream(slug);

    res.json({
      success: true,
      stream: {
        slug: stream.slug,
        name: stream.name,
        proxyUrl: `/stream/${stream.slug}/live.m3u8`,
        tokenExpiry: stream.tokenExpiry,
        remainingMinutes: stream.tokenExpiry
          ? Math.round((stream.tokenExpiry - Date.now()) / 60000)
          : null,
      },
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// ── Update only the cookie for a stream ──
app.patch('/api/streams/:slug/cookie', (req, res) => {
  try {
    const { slug } = req.params;
    const { hdntl } = req.body;
    if (!hdntl) return res.status(400).json({ error: 'hdntl is required' });

    const stream = streams.updateCookie(slug, hdntl);

    // Invalidate playlist cache (segments with old token still work until they expire)
    cache.invalidate(`${slug}:master`);
    cache.invalidate(`${slug}:child`);

    res.json({
      success: true,
      tokenExpiry: stream.tokenExpiry,
      remainingMinutes: stream.tokenExpiry
        ? Math.round((stream.tokenExpiry - Date.now()) / 60000)
        : null,
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// ── Delete a stream ──
app.delete('/api/streams/:slug', (req, res) => {
  const { slug } = req.params;
  const removed = streams.removeStream(slug);

  if (!removed) return res.status(404).json({ error: 'Stream not found' });

  // Clean up cache for this stream
  cache.invalidateStream(slug);

  res.json({ success: true });
});

// ── Server status ──
app.get('/api/status', (req, res) => {
  res.json({
    streams: streams.listStreams().length,
    cache: cache.getStats(),
    dedup: { activeRequests: dedup.activeRequests },
    rateLimiter: rateLimiter.getStats(),
    server: {
      uptime: Math.round(process.uptime()),
      memory: Math.round(process.memoryUsage().heapUsed / 1048576) + 'MB',
    },
  });
});

// ═══════════════════════════════════════════════════
//  FRONTEND — Serve static files
// ═══════════════════════════════════════════════════

app.use(express.static(path.join(__dirname, 'public')));

// SPA fallback — serve index.html for non-API, non-stream routes
app.get('*', (req, res) => {
  if (!req.path.startsWith('/api/') && !req.path.startsWith('/stream/')) {
    return res.sendFile(path.join(__dirname, 'public', 'index.html'));
  }
  res.status(404).json({ error: 'Not found' });
});

// ═══════════════════════════════════════════════════
//  START SERVER
// ═══════════════════════════════════════════════════

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  const streamList = streams.listStreams();
  const workerId = USE_CLUSTER ? ` [Worker ${process.pid}]` : '';

  console.log(`
╔════════════════════════════════════════════════════╗
║         Hotstar Restream Proxy v3.0${workerId.padEnd(16)}║
╠════════════════════════════════════════════════════╣
║  Dashboard : http://localhost:${PORT}                    ║
║  API       : http://localhost:${PORT}/api/status         ║
║  Streams   : ${String(streamList.length).padEnd(2)} configured                         ║
║  Cluster   : ${USE_CLUSTER ? WORKER_COUNT + ' workers' : 'disabled'}                            ║
╚════════════════════════════════════════════════════╝
`);

  if (streamList.length > 0) {
    console.log('Active streams:');
    streamList.forEach(s => {
      const status = s.tokenValid === false ? '(EXPIRED)' : s.tokenValid ? `(${s.remainingMinutes}m left)` : '';
      console.log(`  - ${s.name}: http://localhost:${PORT}${s.proxyUrl} ${status}`);
    });
    console.log('');
  } else {
    console.log('No streams configured. Open the dashboard to add one.\n');
  }
});

} // end startServer()
