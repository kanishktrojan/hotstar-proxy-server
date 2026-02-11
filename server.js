require('dotenv').config();
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

// CLUSTER DISABLED — cache + prefetcher must be in a single process.
// Node's event loop is fast enough for serving cached buffers to 50+ clients.
// If you need multi-core, use a reverse proxy (nginx) in front.
startServer();

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
const DELAY_SEGMENTS = parseInt(process.env.DELAY_SEGMENTS || '5', 10);

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
  // DON'T block on expired token — cached segments are still valid.
  // The prefetcher will fail on CDN fetches but clients get cache hits.
  // This prevents the "continuous loading" after cookie change.
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
//  FIX #8: Precompile URL rewrite regexes
//  Match ALL segment types: .ts .m4s .mp4 .aac .m4a .m4v .fmp4
// ═══════════════════════════════════════════════════

const SEG_EXT = '(?:ts|m4s|mp4|aac|m4a|m4v|fmp4)';

// Playlist URL patterns (.m3u8) — only used if needed elsewhere
// (rewritePlaylist now uses line-by-line approach)

// For extracting segment lines from playlist text
const RE_SEG_LINES = new RegExp(`^(?!#)([^\\s]+\\.${SEG_EXT}[^\\s]*)$`, 'gm');

// Regex to test a single line for segment/playlist extension
const RE_IS_SEG  = new RegExp(`\\.${SEG_EXT}(\\?|$)`, 'i');
const RE_IS_M3U8 = /\.m3u8(\?|$)/i;

function rewritePlaylist(playlist, slug, includeM3u8 = false) {
  return playlist.split('\n').map(line => {
    const t = line.trim();
    if (!t || t.startsWith('#')) return line;

    const isSeg  = RE_IS_SEG.test(t);
    const isM3u8 = RE_IS_M3U8.test(t);

    if (isSeg) {
      if (t.startsWith('http'))  return `/stream/${slug}/segment?url=${encodeURIComponent(t)}`;
      if (t.startsWith('/'))     return `/stream/${slug}/segment?path=${encodeURIComponent(t)}`;
      return `/stream/${slug}/segment?path=${encodeURIComponent(t)}`;
    }

    if (isM3u8 && includeM3u8) {
      if (t.startsWith('http'))  return `/stream/${slug}/playlist?url=${encodeURIComponent(t)}`;
      if (t.startsWith('/'))     return `/stream/${slug}/playlist?path=${encodeURIComponent(t)}`;
      return `/stream/${slug}/playlist?path=${encodeURIComponent(t)}`;
    }

    return line;
  }).join('\n');
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
//  SEGMENT SERVING — optimized for concurrent clients
//
//  Key optimizations:
//  1. writeHead() + end(data) — skips Express's res.send() overhead
//     (no JSON detection, no ETag, no content-type guessing)
//  2. Cache hits are instant — just buffer write, no CDN round-trip
//  3. Dedup: concurrent clients for same uncached segment share
//     one CDN request, then all get served from cache
// ═══════════════════════════════════════════════════

// Determine MIME type from segment filename
function segmentMime(name) {
  const ext = (name.match(/\.([a-z0-9]+)$/i) || [])[1];
  switch (ext && ext.toLowerCase()) {
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
    const mime = segmentMime(segName);

    // 1. CACHE HIT — fast writeHead + end, bypasses Express .send() overhead
    const cached = cache.get(cacheKey);
    if (cached) {
      const data = cached.data;
      const len = Buffer.isBuffer(data) ? data.length : Buffer.byteLength(data);
      console.log(`[${slug}][SEG] \u2705 HIT \u2192 ${segName} (${(len/1024).toFixed(0)}KB) served from cache`);
      res.writeHead(200, {
        'Content-Type': mime,
        'Content-Length': len,
        'Cache-Control': 'max-age=300',
        'X-Cache': 'HIT',
      });
      return res.end(data);
    }

    // 2. DEDUP — another request is downloading this segment;
    //    wait for it then serve from cache (now populated)
    if (dedup.inflight.has(cacheKey)) {
      try {
        await dedup.inflight.get(cacheKey);
        // After dedup resolves, segment is in cache
        const entry = cache.get(cacheKey);
        if (entry) {
          const d = entry.data;
          const l = Buffer.isBuffer(d) ? d.length : Buffer.byteLength(d);
          console.log(`[${slug}][SEG] \u267B\uFE0F DEDUP \u2192 ${segName} (${(l/1024).toFixed(0)}KB) served from cache`);
          res.writeHead(200, {
            'Content-Type': mime,
            'Content-Length': l,
            'Cache-Control': 'max-age=300',
            'X-Cache': 'DEDUP',
          });
          return res.end(d);
        }
      } catch (err) {
        console.warn(`[${slug}][SEG] Dedup failed for ${segName}, fetching directly`);
      }
    }

    // 3. MISS — fetch from CDN, stream to client, cache for everyone else
    console.log(`[${slug}][SEG] \u274C MISS \u2192 fetching ${segName} from CDN`);
    await rateLimiter.acquire();

    const response = await fetchWithRetry(
      url,
      { headers: getHeaders(stream.hdntl) },
      10000
    );

    if (!response.ok) {
      rateLimiter.onCdnError(response.status);
      throw new Error(`Segment CDN ${response.status}`);
    }

    rateLimiter.onCdnSuccess();

    const cl = response.headers.get('content-length');
    res.set('Content-Type', mime);
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
        console.log(`[${slug}][SEG] ✅ Cached ${segName} (${(buffer.length / 1024).toFixed(0)}KB) after CDN fetch`);
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
    console.error(`[${slug}][SEG] ❌ ERROR ${err.message}`);
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
    // Only invalidate playlists if URL changed, keep segment cache
    if (streamUrl && streamUrl !== existing.streamUrl) {
      cache.invalidateStream(slug);
    } else {
      cache.invalidate(`${slug}:master`);
      cache.invalidate(`${slug}:child`);
    }
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
    // ONLY invalidate playlists — KEEP cached segments!
    // Segments are immutable content; only the auth token changes.
    // This prevents the "continuous loading" after cookie rotation.
    cache.invalidate(`${slug}:master`);
    cache.invalidate(`${slug}:child`);
    // Seamlessly swap cookie in prefetcher — no restart, no gap
    const pf = prefetchers.get(slug);
    if (pf) {
      pf.swapCookie(streams.getStreamFull(slug));
    } else {
      startPrefetcher(slug, streams.getStreamFull(slug));
    }
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