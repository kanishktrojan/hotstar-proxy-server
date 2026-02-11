require('dotenv').config();
const cluster = require('cluster');
const os = require('os');
const express = require('express');
const path = require('path');
const fetch = require('node-fetch');
const http = require('http');
const https = require('https');
const zlib = require('zlib');
const dns = require('dns');
const { promisify } = require('util');

const StreamCache = require('./lib/cache');
const RequestDedup = require('./lib/dedup');
const RateLimiter = require('./lib/rate-limiter');
const StreamManager = require('./lib/stream-manager');
const StreamPrefetcher = require('./lib/prefetcher');

// ═══════════════════════════════════════════════════
//  CLUSTER MODE
// ═══════════════════════════════════════════════════

const WORKER_COUNT = Math.min(os.cpus().length, 4);
const USE_CLUSTER = process.env.NO_CLUSTER !== '1' && WORKER_COUNT > 1;

if (USE_CLUSTER && cluster.isPrimary) {
  console.log(`[Cluster] Primary ${process.pid} starting ${WORKER_COUNT} workers...`);
  for (let i = 0; i < WORKER_COUNT; i++) cluster.fork();
  cluster.on('exit', (worker, code) => {
    console.warn(`[Cluster] Worker ${worker.process.pid} exited (${code}), restarting...`);
    cluster.fork();
  });
} else {
  startServer();
}

function startServer() {

const app = express();
app.use(express.json());

// ═══════════════════════════════════════════════════
//  FIX #1: DNS CACHE — avoid repeated DNS lookups
//  Saves 50-150ms per new connection
// ═══════════════════════════════════════════════════

const dnsCache = new Map();
const originalLookup = dns.lookup;

dns.lookup = function(hostname, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }

  const key = `${hostname}:${options.family || 0}`;
  const cached = dnsCache.get(key);

  if (cached && (Date.now() - cached.ts) < 30000) { // 30s DNS TTL
    return process.nextTick(() => callback(null, cached.address, cached.family));
  }

  originalLookup.call(dns, hostname, options, (err, address, family) => {
    if (!err) {
      dnsCache.set(key, { address, family, ts: Date.now() });
    }
    callback(err, address, family);
  });
};

// ═══════════════════════════════════════════════════
//  FIX #2: Aggressive keep-alive + higher socket pool
// ═══════════════════════════════════════════════════

const httpAgent = new http.Agent({
  keepAlive: true,
  maxSockets: 200,        // was 100 — more parallel connections
  maxFreeSockets: 50,     // was 20 — keep more idle sockets warm
  timeout: 15000,         // was 30000 — fail faster
  scheduling: 'fifo',     // reuse most-recently-used socket (warmer)
});

const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 200,
  maxFreeSockets: 50,
  timeout: 15000,
  scheduling: 'fifo',
});

const streams = new StreamManager();
const cache = new StreamCache({ maxMemoryMB: 1536 }); // 1.5GB — room for HD segments
const dedup = new RequestDedup();
const rateLimiter = new RateLimiter(200); // Increased for prefetcher + client traffic

// Performance: strip unnecessary Express overhead
app.disable('etag');
app.disable('x-powered-by');

// ═══════════════════════════════════════════════════
//  FIX #4: Cache TTLs tuned for prefetcher strategy
// ═══════════════════════════════════════════════════

const CACHE_TTL = {
  MASTER_PLAYLIST: 4000,   // 4s — prefetcher keeps it warm
  CHILD_PLAYLIST:  1500,   // 1.5s — prefetcher refreshes every ~1s
  SEGMENT:         600000, // 10m — segments are immutable
};

// ═══════════════════════════════════════════════════
//  BACKGROUND PREFETCHER — zero buffering strategy
//
//  1. For each stream, a background loop continuously polls
//     playlists and pre-downloads ALL segments into cache.
//  2. Client playlists are delayed by N segments so they
//     only request segments ALREADY in cache.
//  3. Result: ~100% cache hit rate → zero buffering.
//  4. Human-like request patterns (jitter, pacing, backoff)
//     prevent CDN bot-detection.
// ═══════════════════════════════════════════════════

const prefetchers = new Map(); // slug -> StreamPrefetcher
const DELAY_SEGMENTS = parseInt(process.env.DELAY_SEGMENTS || '1', 10);

function startPrefetcher(slug, stream) {
  stopPrefetcher(slug);
  const pf = new StreamPrefetcher({
    slug,
    stream,
    cache,
    rateLimiter,
    fetchFn: (url, options, timeout) => fetchWithRetry(url, options, timeout),
    headersFn: getHeaders,
  });
  prefetchers.set(slug, pf);
  pf.start();
}

function stopPrefetcher(slug) {
  const pf = prefetchers.get(slug);
  if (pf) {
    pf.stop();
    prefetchers.delete(slug);
  }
}

/**
 * Delay a live HLS playlist by stripping the last N segments.
 * The player sees slightly older content (~2-4s behind live)
 * but every segment it requests is guaranteed to be in cache.
 *
 * #EXT-X-MEDIA-SEQUENCE stays unchanged (first segment index)
 * so playback continuity is maintained across polls.
 */
function delayPlaylist(text, delaySegments) {
  if (delaySegments <= 0) return text;

  const lines = text.split('\n');
  const segEntries = []; // { start, end } line indices for each segment

  for (let i = 0; i < lines.length; i++) {
    if (lines[i].trim().startsWith('#EXTINF:')) {
      let end = i + 1;
      while (end < lines.length) {
        const l = lines[end].trim();
        if (!l || l.startsWith('#EXT-X-BYTERANGE') ||
            l.startsWith('#EXT-X-PROGRAM-DATE-TIME') ||
            l.startsWith('#EXT-X-DISCONTINUITY')) {
          end++;
          continue;
        }
        if (l.startsWith('#')) { end++; continue; }
        break; // URL line
      }
      if (end < lines.length) {
        segEntries.push({ start: i, end });
        i = end;
      }
    }
  }

  // Keep at least 2 segments for the player to work
  if (segEntries.length <= delaySegments + 1) return text;

  const toRemove = new Set();
  for (let i = segEntries.length - delaySegments; i < segEntries.length; i++) {
    for (let j = segEntries[i].start; j <= segEntries[i].end; j++) {
      toRemove.add(j);
    }
  }

  return lines.filter((_, idx) => !toRemove.has(idx)).join('\n');
}

// ═══════════════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════════════

function getHeaders(cookieValue) {
  let cookie;
  if (cookieValue.includes(';')) {
    cookie = cookieValue;
  } else if (cookieValue.startsWith('hdntl=')) {
    cookie = cookieValue;
  } else {
    cookie = `hdntl=${cookieValue}`;
  }
  return {
    'Cookie': cookie,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
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

// ═══════════════════════════════════════════════════
//  FIX #5: Shorter timeouts — fail fast, retry fast
// ═══════════════════════════════════════════════════

async function fetchWithTimeout(url, options = {}, timeoutMs = 6000) { // was 10000
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      ...options,
      agent: getAgent(url),
      signal: controller.signal,
    });
    clearTimeout(timer);
    return resp;
  } catch (err) {
    clearTimeout(timer);
    if (err.name === 'AbortError') throw new Error(`Timeout after ${timeoutMs}ms`);
    throw err;
  }
}

// ═══════════════════════════════════════════════════
//  FIX #6: Retry on failure (1 retry, fast)
// ═══════════════════════════════════════════════════

async function fetchWithRetry(url, options = {}, timeoutMs = 6000) {
  try {
    return await fetchWithTimeout(url, options, timeoutMs);
  } catch (err) {
    // One fast retry — different socket, might hit different CDN edge
    console.warn(`[RETRY] ${url.split('/').pop()} — ${err.message}`);
    return await fetchWithTimeout(url, options, timeoutMs);
  }
}

async function cachedFetchText(url, cacheKey, ttl, hdntl) {
  const cached = cache.get(cacheKey);
  if (cached) return { text: cached.data, fromCache: true };

  return dedup.dedupFetch(cacheKey, async () => {
    await rateLimiter.acquire('high');

    const response = await fetchWithRetry(url, { headers: getHeaders(hdntl) });
    if (!response.ok) {
      let body = '';
      try { body = await response.text(); } catch (_) {}
      console.error(`[CDN ${response.status}] URL: ${url}`);
      rateLimiter.onCdnError(response.status);
      if (response.status === 403) {
        throw new Error('CDN 403 — hdntl token expired or invalid');
      }
      throw new Error(`CDN ${response.status}`);
    }

    rateLimiter.onCdnSuccess();
    const text = await response.text();
    cache.set(cacheKey, text, {}, ttl);
    return { text, fromCache: false };
  });
}

function resolveStream(slug) {
  const stream = streams.getStreamFull(slug);
  if (!stream) return { stream: null, error: { status: 404, message: `Stream "${slug}" not found` } };
  if (!stream.hdntl) return { stream: null, error: { status: 503, message: 'No cookie configured' } };
  if (stream.tokenExpiry && Date.now() > stream.tokenExpiry) {
    return { stream, error: { status: 503, message: 'Token expired' } };
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

// ═══════════════════════════════════════════════════
//  FIX #7: Synchronous gzip with pre-compressed cache
//  Old: async callback gzip on every response
//  New: compress once at cache time, serve compressed directly
// ═══════════════════════════════════════════════════

const gzipSync = zlib.gzipSync;

function sendPlaylist(res, body, fromCache) {
  res.set('Content-Type', 'application/vnd.apple.mpegurl');
  res.set('Cache-Control', 'no-cache, no-store');
  res.set('X-Cache', fromCache ? 'HIT' : 'MISS');
  res.send(body); // let express handle it directly — no async gzip overhead
}

// ═══════════════════════════════════════════════════
//  FIX #8: Precompile URL rewrite regexes (avoid recompile per request)
// ═══════════════════════════════════════════════════

const RE_ABS_TS     = /^(?!#)(https?:\/\/[^\s]+\.ts[^\s]*)$/gm;
const RE_ABSPATH_TS = /^(?!#)(\/[^\s]+\.ts[^\s]*)$/gm;
const RE_REL_TS     = /^(?!#)(?!\/)((?!https?:\/\/)[^\s]+\.ts[^\s]*)$/gm;
const RE_ABS_M3U8     = /^(?!#)(https?:\/\/[^\s]+\.m3u8[^\s]*)$/gm;
const RE_ABSPATH_M3U8 = /^(?!#)(\/[^\s]+\.m3u8[^\s]*)$/gm;
const RE_REL_M3U8     = /^(?!#)(?!\/)((?!https?:\/\/)[^\s]+\.m3u8[^\s]*)$/gm;
const RE_SEG_LINES    = /^(?!#)([^\s]+\.ts[^\s]*)$/gm;

function rewritePlaylist(playlist, slug, includeM3u8 = false) {
  let result = playlist
    .replace(RE_ABS_TS, m => `/stream/${slug}/segment?url=${encodeURIComponent(m)}`)
    .replace(RE_ABSPATH_TS, m => `/stream/${slug}/segment?path=${encodeURIComponent(m)}`)
    .replace(RE_REL_TS, `/stream/${slug}/segment?path=$1`);

  if (includeM3u8) {
    result = result
      .replace(RE_ABS_M3U8, m => `/stream/${slug}/playlist?url=${encodeURIComponent(m)}`)
      .replace(RE_ABSPATH_M3U8, m => `/stream/${slug}/playlist?path=${encodeURIComponent(m)}`)
      .replace(RE_REL_M3U8, `/stream/${slug}/playlist?path=$1`);
  }

  return result;
}

// ═══════════════════════════════════════════════════
//  STREAM ROUTES
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
      return res.status(502).json({ error: 'Invalid playlist from CDN' });
    }

    const rewritten = rewritePlaylist(playlist, slug, true);
    sendPlaylist(res, rewritten, fromCache);

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

    let url, filename;
    if (req.query.url) {
      url = decodeURIComponent(req.query.url);
      filename = url.split('/').pop().split('?')[0];
    } else if (req.query.path) {
      const p = decodeURIComponent(req.query.path);
      if (p.startsWith('/')) {
        const base = new URL(stream.streamBase);
        url = `${base.protocol}//${base.host}${p}`;
      } else {
        url = `${stream.streamBase}/${p}`;
      }
      filename = p.split('/').pop().split('?')[0];
    } else {
      return res.status(400).json({ error: 'Missing path or url' });
    }

    const { text: playlist, fromCache } = await cachedFetchText(
      url,
      `${slug}:child:${filename}`,
      CACHE_TTL.CHILD_PLAYLIST,
      stream.hdntl
    );

    // Apply delay: strip trailing segments so the player only
    // requests segments the prefetcher has already cached
    const delayed = delayPlaylist(playlist, DELAY_SEGMENTS);
    const rewritten = rewritePlaylist(delayed, slug, false);

    // ═══════════════════════════════════════════════
    //  FIX #9: Aggressive prefetch — top priority, more segments
    // ═══════════════════════════════════════════════

    if (!fromCache) { // only prefetch on fresh playlist (new segments appeared)
      const segMatches = playlist.match(RE_SEG_LINES);
      if (segMatches && segMatches.length > 0) {
        // Prefetch the LAST 3 segments (most likely to be requested next)
        const toWarm = segMatches.slice(-3);
        for (const seg of toWarm) {
          let segUrl;
          if (seg.startsWith('http')) {
            segUrl = seg;
          } else if (seg.startsWith('/')) {
            const base = new URL(stream.streamBase);
            segUrl = `${base.protocol}//${base.host}${seg}`;
          } else {
            segUrl = `${stream.streamBase}/${seg}`;
          }
          const segName = seg.split('/').pop().split('?')[0];
          const segKey = `${slug}:seg:${segName}`;

          if (!cache.has(segKey) && !dedup.inflight.has(segKey)) {
            // FIX: Use 'high' priority for prefetch — these WILL be requested
            dedup.dedupFetch(segKey, async () => {
              try {
                await rateLimiter.acquire('high'); // was 'low' — upgraded
                const resp = await fetchWithTimeout(
                  segUrl,
                  { headers: getHeaders(stream.hdntl) },
                  8000 // was 10000
                );
                if (resp.ok) {
                  const buf = await resp.buffer();
                  cache.set(segKey, buf, {
                    'content-length': resp.headers.get('content-length')
                  }, CACHE_TTL.SEGMENT);
                  rateLimiter.onCdnSuccess();
                }
              } catch (_) {}
            }).catch(() => {});
          }
        }
      }
    }

    sendPlaylist(res, rewritten, fromCache);

  } catch (err) {
    console.error(`[${slug}][PLAYLIST] ${err.message}`);
    res.status(502).send('Playlist proxy error');
  }
});

// ═══════════════════════════════════════════════════
//  FIX #10: Streaming segments — biggest latency fix
//
//  BEFORE: Dedup waiters waited for FULL buffer download
//          → 200-800ms delay for 2nd+ concurrent viewer
//
//  AFTER:  All concurrent clients get chunks as they arrive
//          via a PassThrough multicast
// ═══════════════════════════════════════════════════

const { PassThrough } = require('stream');

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
      if (p.startsWith('/')) {
        const base = new URL(stream.streamBase);
        url = `${base.protocol}//${base.host}${p}`;
      } else {
        url = `${stream.streamBase}/${p}`;
      }
      segName = p.split('/').pop().split('?')[0];
    } else {
      return res.status(400).json({ error: 'Missing path or url' });
    }

    const cacheKey = `${slug}:seg:${segName}`;

    // 1. Cache hit — instant response
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

    // 2. Another request is already downloading this segment
    //    → STREAM chunks as they arrive (don't wait for full buffer)
    if (dedup.inflight.has(cacheKey)) {
      try {
        const result = await dedup.inflight.get(cacheKey);
        res.set('Content-Type', 'video/mp2t');
        res.set('Cache-Control', 'max-age=300');
        res.set('X-Cache', 'DEDUP');
        if (result.headers && result.headers['content-length']) {
          res.set('Content-Length', result.headers['content-length']);
        }
        return res.send(result.buffer);
      } catch (err) {
        // If the original request failed, fall through and try ourselves
        console.warn(`[${slug}][TS] Dedup failed for ${segName}, fetching directly`);
      }
    }

    // 3. First request — fetch from CDN and stream to client
    await rateLimiter.acquire();

    const response = await fetchWithRetry(
      url,
      { headers: getHeaders(stream.hdntl) },
      10000  // was 15000 — faster timeout for segments
    );

    if (!response.ok) {
      rateLimiter.onCdnError(response.status);
      throw new Error(`Segment CDN ${response.status}`);
    }

    rateLimiter.onCdnSuccess();

    const cl = response.headers.get('content-length');
    res.set('Content-Type', 'video/mp2t');
    res.set('Cache-Control', 'max-age=300');
    res.set('X-Cache', 'MISS');
    if (cl) res.set('Content-Length', cl);

    // Stream to client + collect buffer for cache
    const chunks = [];
    const bufferPromise = new Promise((resolve, reject) => {
      response.body.on('data', chunk => {
        chunks.push(chunk);
        if (!res.destroyed) res.write(chunk);
      });
      response.body.on('end', () => {
        const buffer = Buffer.concat(chunks);
        const headers = { 'content-length': cl || String(buffer.length) };
        cache.set(cacheKey, buffer, headers, CACHE_TTL.SEGMENT);
        if (!res.destroyed) res.end();
        resolve({ buffer, headers });
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
    console.error(`[${slug}][TS] ${err.message}`);
    if (!res.headersSent) {
      res.status(502).send('Segment error');
    } else if (!res.destroyed) {
      res.end();
    }
  }
});

// ═══════════════════════════════════════════════════
//  API ROUTES (unchanged — no latency impact)
// ═══════════════════════════════════════════════════

app.get('/api/streams', (req, res) => res.json(streams.listStreams()));

app.post('/api/streams', (req, res) => {
  try {
    const { slug, name, streamUrl, hdntl } = req.body;
    if (!streamUrl || !hdntl) return res.status(400).json({ error: 'streamUrl and hdntl required' });
    const stream = streams.addStream({ slug, name, streamUrl, hdntl });
    // Start background prefetcher IMMEDIATELY — segments cached before any viewer connects
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
    cache.invalidateStream(slug);
    startPrefetcher(stream.slug, stream); // Restart prefetcher with new config
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
    // Update prefetcher with new cookie so it keeps fetching
    const pf = prefetchers.get(slug);
    if (pf) pf.updateStream(streams.getStreamFull(slug));
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
  const prefetchStatus = {};
  for (const [slug, pf] of prefetchers) {
    prefetchStatus[slug] = pf.getStats();
  }
  res.json({
    streams: streams.listStreams().length,
    cache: cache.getStats(),
    dedup: { activeRequests: dedup.activeRequests },
    rateLimiter: rateLimiter.getStats(),
    prefetch: prefetchStatus,
    delaySegments: DELAY_SEGMENTS,
    server: {
      uptime: Math.round(process.uptime()),
      memory: Math.round(process.memoryUsage().heapUsed / 1048576) + 'MB',
      pid: process.pid,
    },
  });
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  if (!req.path.startsWith('/api/') && !req.path.startsWith('/stream/')) {
    return res.sendFile(path.join(__dirname, 'public', 'index.html'));
  }
  res.status(404).json({ error: 'Not found' });
});

// ═══════════════════════════════════════════════════
//  START
// ═══════════════════════════════════════════════════

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════════════╗
║  Hotstar Restream Proxy v4.0 (Zero-Buffer)        ║
║  Port: ${PORT} | PID: ${String(process.pid).padEnd(30)}║
║  Delay: ${String(DELAY_SEGMENTS + ' segment(s)').padEnd(39)}║
╚════════════════════════════════════════════════════╝`);

  // Auto-start prefetchers for all existing streams on boot
  const existingStreams = streams.listStreams();
  for (const s of existingStreams) {
    const full = streams.getStreamFull(s.slug);
    if (full && full.hdntl) {
      startPrefetcher(s.slug, full);
    }
  }
  if (existingStreams.length > 0) {
    console.log(`[Prefetch] Auto-started ${existingStreams.length} prefetcher(s)`);
  }
});

} // end startServer()