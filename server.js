require('dotenv').config();
const express = require('express');
const path    = require('path');
const fetch   = require('node-fetch');
const http    = require('http');
const https   = require('https');
const dns     = require('dns');

const StreamCache     = require('./lib/cache');
const RequestDedup    = require('./lib/dedup');
const RateLimiter     = require('./lib/rate-limiter');
const StreamManager   = require('./lib/stream-manager');
const StreamPrefetcher = require('./lib/prefetcher');

// ─── Single process — cache must be shared ───
startServer();

function startServer() {

const app = express();
app.use(express.json());
app.disable('etag');
app.disable('x-powered-by');

// ═══════════════════════════════════════════
//  DNS CACHE — save 50-150ms per new host
// ═══════════════════════════════════════════
const dnsCache = new Map();
const _lookup  = dns.lookup;
dns.lookup = function (hostname, opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = {}; }
  const k = `${hostname}:${opts.family || 0}`;
  const c = dnsCache.get(k);
  if (c && Date.now() - c.ts < 30000)
    return process.nextTick(() => cb(null, c.address, c.family));
  _lookup.call(dns, hostname, opts, (err, addr, fam) => {
    if (!err) dnsCache.set(k, { address: addr, family: fam, ts: Date.now() });
    cb(err, addr, fam);
  });
};

// ═══════════════════════════════════════════
//  KEEP-ALIVE AGENTS — reuse TCP + TLS
// ═══════════════════════════════════════════
const httpAgent  = new http.Agent({  keepAlive: true, maxSockets: 200, maxFreeSockets: 50, timeout: 15000, scheduling: 'fifo' });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 200, maxFreeSockets: 50, timeout: 15000, scheduling: 'fifo' });
function getAgent(url) { return url.startsWith('https') ? httpsAgent : httpAgent; }

// ═══════════════════════════════════════════
//  CORE SERVICES
// ═══════════════════════════════════════════
const streams     = new StreamManager();
const cache       = new StreamCache({ maxMemoryMB: 1536 });
const dedup       = new RequestDedup();
const rateLimiter = new RateLimiter(40); // Conservative — avoid 429

const CACHE_TTL = {
  MASTER_PLAYLIST: 4000,    // 4 s
  CHILD_PLAYLIST:  2000,    // 2 s
  SEGMENT:         600000,  // 10 min — segments are immutable
};

// ═══════════════════════════════════════════
//  PREFETCHER MANAGEMENT
// ═══════════════════════════════════════════
const prefetchers = new Map();

function startPrefetcher(slug, stream) {
  stopPrefetcher(slug);
  const pf = new StreamPrefetcher({
    slug, stream, cache, rateLimiter,
    fetchFn:   (url, opts, timeout) => fetchWithRetry(url, opts, timeout),
    headersFn: getHeaders,
  });
  prefetchers.set(slug, pf);
  pf.start();
}

function stopPrefetcher(slug) {
  const pf = prefetchers.get(slug);
  if (pf) { pf.stop(); prefetchers.delete(slug); }
}

// ═══════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════
function getHeaders(cookieValue) {
  let cookie = cookieValue;
  if (!cookie.includes('=')) cookie = `hdntl=${cookie}`;
  else if (!cookie.startsWith('hdntl=') && !cookie.includes(';')) cookie = `hdntl=${cookie}`;
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

async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { ...options, agent: getAgent(url), signal: controller.signal });
    clearTimeout(timer);
    return resp;
  } catch (err) {
    clearTimeout(timer);
    if (err.name === 'AbortError') throw new Error(`Timeout ${timeoutMs}ms`);
    throw err;
  }
}

async function fetchWithRetry(url, options = {}, timeoutMs = 8000) {
  try {
    return await fetchWithTimeout(url, options, timeoutMs);
  } catch (err) {
    // single retry — may hit a different CDN edge
    return await fetchWithTimeout(url, options, timeoutMs);
  }
}

async function cachedFetchText(url, cacheKey, ttl, hdntl) {
  const cached = cache.get(cacheKey);
  if (cached) return { text: cached.data, fromCache: true };

  return dedup.dedupFetch(cacheKey, async () => {
    await rateLimiter.acquire('high');
    const resp = await fetchWithRetry(url, { headers: getHeaders(hdntl) });
    if (!resp.ok) {
      rateLimiter.onCdnError(resp.status);
      throw new Error(`CDN ${resp.status}`);
    }
    rateLimiter.onCdnSuccess();
    const text = await resp.text();
    cache.set(cacheKey, text, {}, ttl);
    return { text, fromCache: false };
  });
}

function resolveStream(slug) {
  const stream = streams.getStreamFull(slug);
  if (!stream)       return { stream: null, error: { status: 404, message: `Stream "${slug}" not found` } };
  if (!stream.hdntl) return { stream: null, error: { status: 503, message: 'No cookie configured' } };
  return { stream, error: null };
}

// ═══════════════════════════════════════════
//  CORS
// ═══════════════════════════════════════════
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ═══════════════════════════════════════════
//  PLAYLIST REWRITING — the core of the proxy
//
//  RULE: In an HLS media playlist, every line
//  that isn't a tag (#) and isn't empty IS a
//  segment URL.  We don't match by extension.
//
//  For master playlists, we additionally route
//  .m3u8 URLs through /playlist.
//
//  #EXT-X-MAP:URI is rewritten too (init seg).
// ═══════════════════════════════════════════
function rewritePlaylist(playlist, slug, isMaster) {
  return playlist.split('\n').map(line => {
    const t = line.trim();
    if (!t) return line;

    // ── #EXT-X-MAP:URI — fMP4/CMAF init segment (critical) ──
    if (t.startsWith('#EXT-X-MAP:')) {
      return line.replace(/URI="([^"]+)"/, (_, uri) => {
        if (uri.startsWith('http')) return `URI="/stream/${slug}/segment?url=${encodeURIComponent(uri)}"`;
        return `URI="/stream/${slug}/segment?path=${encodeURIComponent(uri)}"`;
      });
    }

    // Skip all other HLS tags
    if (t.startsWith('#')) return line;

    // ── Non-tag, non-empty line = a URL ──

    // In master playlists, .m3u8 lines are child playlists
    if (isMaster && /\.m3u8/i.test(t)) {
      if (t.startsWith('http')) return `/stream/${slug}/playlist?url=${encodeURIComponent(t)}`;
      return `/stream/${slug}/playlist?path=${encodeURIComponent(t)}`;
    }

    // Everything else is a segment — rewrite through proxy
    if (t.startsWith('http')) return `/stream/${slug}/segment?url=${encodeURIComponent(t)}`;
    return `/stream/${slug}/segment?path=${encodeURIComponent(t)}`;
  }).join('\n');
}

function sendPlaylist(res, body, fromCache) {
  res.set('Content-Type', 'application/vnd.apple.mpegurl');
  res.set('Cache-Control', 'no-cache, no-store');
  res.set('X-Cache', fromCache ? 'HIT' : 'MISS');
  res.send(body);
}

// ═══════════════════════════════════════════
//  MASTER PLAYLIST
// ═══════════════════════════════════════════
app.get('/stream/:slug/live.m3u8', async (req, res) => {
  const { slug } = req.params;
  try {
    const { stream, error } = resolveStream(slug);
    if (error) return res.status(error.status).json({ error: error.message });

    const url = `${stream.streamBase}/${stream.playlistFile}`;
    const { text: playlist, fromCache } = await cachedFetchText(
      url, `${slug}:master:${stream.playlistFile}`, CACHE_TTL.MASTER_PLAYLIST, stream.hdntl
    );

    if (!playlist.trimStart().startsWith('#EXTM3U'))
      return res.status(502).json({ error: 'Invalid playlist from CDN' });

    sendPlaylist(res, rewritePlaylist(playlist, slug, true), fromCache);
  } catch (err) {
    console.error(`[${slug}][M3U8] ${err.message}`);
    res.status(502).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════
//  CHILD (MEDIA) PLAYLIST
// ═══════════════════════════════════════════
app.get('/stream/:slug/playlist', async (req, res) => {
  const { slug } = req.params;
  try {
    const { stream, error } = resolveStream(slug);
    if (error) return res.status(error.status).json({ error: error.message });

    let url, filename;
    if (req.query.url) {
      url = decodeURIComponent(req.query.url);
      filename = url.split('/').pop().split('?')[0];
    } else if (req.query.path) {
      const p = decodeURIComponent(req.query.path);
      url = p.startsWith('/')
        ? (() => { const b = new URL(stream.streamBase); return `${b.protocol}//${b.host}${p}`; })()
        : `${stream.streamBase}/${p}`;
      filename = p.split('/').pop().split('?')[0];
    } else {
      return res.status(400).json({ error: 'Missing path or url' });
    }

    const { text: playlist, fromCache } = await cachedFetchText(
      url, `${slug}:child:${filename}`, CACHE_TTL.CHILD_PLAYLIST, stream.hdntl
    );

    // NO DELAY — ultra low latency, prefetcher keeps cache warm
    sendPlaylist(res, rewritePlaylist(playlist, slug, false), fromCache);
  } catch (err) {
    console.error(`[${slug}][PLAYLIST] ${err.message}`);
    res.status(502).send('Playlist proxy error');
  }
});

// ═══════════════════════════════════════════
//  SEGMENT SERVING
// ═══════════════════════════════════════════
function segmentMime(name) {
  const ext = (name.match(/\.([a-z0-9]+)$/i) || [])[1];
  if (!ext) return 'application/octet-stream';
  switch (ext.toLowerCase()) {
    case 'ts':   return 'video/mp2t';
    case 'm4s':  return 'video/iso.segment';
    case 'mp4':  return 'video/mp4';
    case 'm4v':  return 'video/mp4';
    case 'm4a':  return 'audio/mp4';
    case 'aac':  return 'audio/aac';
    case 'fmp4': return 'video/mp4';
    default:     return 'application/octet-stream';
  }
}

app.get('/stream/:slug/segment', async (req, res) => {
  const { slug } = req.params;
  try {
    const { stream, error } = resolveStream(slug);
    if (error) return res.status(error.status).json({ error: error.message });

    let url, segName;
    if (req.query.url) {
      url = decodeURIComponent(req.query.url);
      segName = url.split('/').pop().split('?')[0];
    } else if (req.query.path) {
      const p = decodeURIComponent(req.query.path);
      url = p.startsWith('/')
        ? (() => { const b = new URL(stream.streamBase); return `${b.protocol}//${b.host}${p}`; })()
        : `${stream.streamBase}/${p}`;
      segName = p.split('/').pop().split('?')[0];
    } else {
      return res.status(400).json({ error: 'Missing path or url' });
    }

    const cacheKey = `${slug}:seg:${segName}`;
    const mime = segmentMime(segName);

    // 1. CACHE HIT — instant
    const cached = cache.get(cacheKey);
    if (cached) {
      const data = cached.data;
      const len  = Buffer.isBuffer(data) ? data.length : Buffer.byteLength(data);
      console.log(`[${slug}] HIT  ${segName} (${(len/1024).toFixed(0)}KB)`);
      res.writeHead(200, {
        'Content-Type': mime, 'Content-Length': len,
        'Cache-Control': 'max-age=300', 'X-Cache': 'HIT',
      });
      return res.end(data);
    }

    // 2. DEDUP — another request is already fetching this
    if (dedup.inflight.has(cacheKey)) {
      try {
        await dedup.inflight.get(cacheKey);
        const entry = cache.get(cacheKey);
        if (entry) {
          const d = entry.data;
          const l = Buffer.isBuffer(d) ? d.length : Buffer.byteLength(d);
          console.log(`[${slug}] DDP  ${segName} (${(l/1024).toFixed(0)}KB)`);
          res.writeHead(200, {
            'Content-Type': mime, 'Content-Length': l,
            'Cache-Control': 'max-age=300', 'X-Cache': 'DEDUP',
          });
          return res.end(d);
        }
      } catch (_) { /* fall through to direct fetch */ }
    }

    // 3. MISS — fetch from CDN, stream to client, cache for others
    console.log(`[${slug}] MISS ${segName} — fetching from CDN`);
    await rateLimiter.acquire('high');

    const response = await fetchWithRetry(url, { headers: getHeaders(stream.hdntl) }, 12000);
    if (!response.ok) {
      rateLimiter.onCdnError(response.status);
      throw new Error(`CDN ${response.status}`);
    }
    rateLimiter.onCdnSuccess();

    const cl = response.headers.get('content-length');
    res.set('Content-Type', mime);
    res.set('Cache-Control', 'max-age=300');
    res.set('X-Cache', 'MISS');
    if (cl) res.set('Content-Length', cl);

    const chunks = [];
    const bufferPromise = new Promise((resolve, reject) => {
      response.body.on('data', chunk => {
        chunks.push(chunk);
        if (!res.destroyed) res.write(chunk);
      });
      response.body.on('end', () => {
        const buf = Buffer.concat(chunks);
        cache.set(cacheKey, buf, { 'content-length': cl || String(buf.length) }, CACHE_TTL.SEGMENT);
        console.log(`[${slug}] SAVE ${segName} (${(buf.length/1024).toFixed(0)}KB)`);
        if (!res.destroyed) res.end();
        resolve();
      });
      response.body.on('error', err => {
        if (!res.destroyed) res.end();
        reject(err);
      });
    });

    dedup.inflight.set(cacheKey, bufferPromise);
    bufferPromise.finally(() => dedup.inflight.delete(cacheKey));
    await bufferPromise;

  } catch (err) {
    console.error(`[${slug}] ERR  ${err.message}`);
    if (!res.headersSent) res.status(502).send('Segment error');
    else if (!res.destroyed) res.end();
  }
});

// ═══════════════════════════════════════════
//  API ROUTES
// ═══════════════════════════════════════════
app.get('/api/streams', (req, res) => res.json(streams.listStreams()));

app.post('/api/streams', (req, res) => {
  try {
    const { slug, name, streamUrl, hdntl } = req.body;
    if (!streamUrl || !hdntl) return res.status(400).json({ error: 'streamUrl and hdntl required' });
    const stream = streams.addStream({ slug, name, streamUrl, hdntl });
    startPrefetcher(stream.slug, stream);
    res.status(201).json({
      success: true,
      stream: {
        slug: stream.slug, name: stream.name,
        proxyUrl: `/stream/${stream.slug}/live.m3u8`,
        tokenExpiry: stream.tokenExpiry,
        remainingMinutes: stream.tokenExpiry ? Math.round((stream.tokenExpiry - Date.now()) / 60000) : null,
      },
    });
  } catch (err) { res.status(400).json({ error: err.message }); }
});

app.put('/api/streams/:slug', (req, res) => {
  try {
    const { slug } = req.params;
    const existing = streams.getStream(slug);
    if (!existing) return res.status(404).json({ error: 'Not found' });
    const { name, streamUrl, hdntl } = req.body;
    const stream = streams.addStream({
      slug, name: name || existing.name,
      streamUrl: streamUrl || existing.streamUrl,
      hdntl: hdntl || existing.hdntl,
    });
    if (streamUrl && streamUrl !== existing.streamUrl) {
      cache.invalidateStream(slug);
    } else {
      cache.invalidate(`${slug}:master`);
      cache.invalidate(`${slug}:child`);
    }
    startPrefetcher(stream.slug, stream);
    res.json({ success: true, stream: { slug: stream.slug, name: stream.name, proxyUrl: `/stream/${stream.slug}/live.m3u8` } });
  } catch (err) { res.status(400).json({ error: err.message }); }
});

app.patch('/api/streams/:slug/cookie', (req, res) => {
  try {
    const { slug } = req.params;
    const { hdntl } = req.body;
    if (!hdntl) return res.status(400).json({ error: 'hdntl required' });
    const stream = streams.updateCookie(slug, hdntl);
    cache.invalidate(`${slug}:master`);
    cache.invalidate(`${slug}:child`);
    const pf = prefetchers.get(slug);
    if (pf) pf.swapCookie(streams.getStreamFull(slug));
    else startPrefetcher(slug, streams.getStreamFull(slug));
    res.json({ success: true, tokenExpiry: stream.tokenExpiry });
  } catch (err) { res.status(400).json({ error: err.message }); }
});

app.delete('/api/streams/:slug', (req, res) => {
  const { slug } = req.params;
  if (!streams.removeStream(slug)) return res.status(404).json({ error: 'Not found' });
  stopPrefetcher(slug);
  cache.invalidateStream(slug);
  res.json({ success: true });
});

app.get('/api/status', (req, res) => {
  const pfStatus = {};
  for (const [slug, pf] of prefetchers) pfStatus[slug] = pf.getStats();
  res.json({
    streams: streams.listStreams().length,
    cache: cache.getStats(),
    dedup: { activeRequests: dedup.activeRequests },
    rateLimiter: rateLimiter.getStats(),
    prefetch: pfStatus,
    server: {
      uptime: Math.round(process.uptime()),
      memory: Math.round(process.memoryUsage().heapUsed / 1048576) + 'MB',
      pid: process.pid,
    },
  });
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  if (!req.path.startsWith('/api/') && !req.path.startsWith('/stream/'))
    return res.sendFile(path.join(__dirname, 'public', 'index.html'));
  res.status(404).json({ error: 'Not found' });
});

// ═══════════════════════════════════════════
//  START
// ═══════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════════════╗
║  Hotstar Restream Proxy v5.0 (Ultra-Low-Latency)  ║
║  Port: ${PORT} | PID: ${String(process.pid).padEnd(30)}║
║  Zero delay · Prefetch · Dedup · Rate-limited      ║
╚════════════════════════════════════════════════════╝`);

  const existing = streams.listStreams();
  for (const s of existing) {
    const full = streams.getStreamFull(s.slug);
    if (full && full.hdntl) startPrefetcher(s.slug, full);
  }
  if (existing.length > 0)
    console.log(`[Boot] Auto-started ${existing.length} prefetcher(s)`);
});

} // end startServer
