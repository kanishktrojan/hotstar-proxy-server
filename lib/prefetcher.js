/**
 * Background Stream Prefetcher v2
 *
 * Continuously polls playlists and pre-downloads segments
 * into cache BEFORE clients request them.
 *
 * Human-like behavior:
 * - Random jitter on all poll intervals (no fixed timing)
 * - Sequential segment downloads (not parallel bursts)
 * - Realistic inter-request delays
 * - Backs off on CDN errors (429, 403)
 * - Staggered start for multi-variant streams
 *
 * This is the core of the zero-buffering strategy:
 * By the time a client requests a segment, it's already in cache.
 */

class StreamPrefetcher {
  /**
   * @param {Object} opts
   * @param {string} opts.slug
   * @param {Object} opts.stream       - Stream config from StreamManager
   * @param {StreamCache} opts.cache
   * @param {RateLimiter} opts.rateLimiter
   * @param {Function} opts.fetchFn    - (url, options, timeoutMs) => Promise<Response>
   * @param {Function} opts.headersFn  - (hdntl) => headers object
   */
  constructor({ slug, stream, cache, rateLimiter, fetchFn, headersFn }) {
    this.slug = slug;
    this.stream = stream;
    this.cache = cache;
    this.rateLimiter = rateLimiter;
    this.fetch = fetchFn;
    this.getHeaders = headersFn;

    this.running = false;
    this.warming = true;  // true until first full segment round completes
    this.variants = [];   // { url, bandwidth, filename, dir }
    this.cachedSegKeys = new Set();  // keys that are CONFIRMED in cache
    this.failedSegKeys = new Set();  // keys that failed — retry next poll
    this._timers = new Set();

    this.stats = {
      playlistPolls: 0,
      segmentsPrefetched: 0,
      segmentBytes: 0,
      cacheHits: 0,
      errors: 0,
      lastPoll: null,
      startedAt: null,
    };
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.warming = true;
    this.stats.startedAt = Date.now();
    console.log(`[Prefetch][${this.slug}] ▶ Starting background prefetcher`);
    this._schedule(() => this._pollMaster(), 50);
  }

  stop() {
    this.running = false;
    for (const timer of this._timers) clearTimeout(timer);
    this._timers.clear();
    console.log(`[Prefetch][${this.slug}] ■ Stopped`);
  }

  updateStream(stream) {
    this.stream = stream;
    if (this.running) {
      this.stop();
      this.variants = [];
      this.cachedSegKeys.clear();
      this.failedSegKeys.clear();
      this.start();
    }
  }

  /**
   * Hot-swap the cookie WITHOUT restarting polling or clearing cache.
   * Existing segments stay in cache, only future CDN requests use new cookie.
   */
  swapCookie(stream) {
    console.log(`[Prefetch][${this.slug}] 🔄 Cookie swapped (no restart, cache preserved)`);
    this.stream = stream;
    // Clear failed set so segments that failed with old cookie get retried
    this.failedSegKeys.clear();
  }

  // ── Timer Management ──

  _schedule(fn, delayMs) {
    if (!this.running) return;
    const timer = setTimeout(() => {
      this._timers.delete(timer);
      if (this.running) fn();
    }, delayMs);
    this._timers.add(timer);
  }

  // ══════════════════════════════════════
  //  MASTER PLAYLIST POLLING
  // ══════════════════════════════════════

  async _pollMaster() {
    if (!this.running) return;

    try {
      const url = `${this.stream.streamBase}/${this.stream.playlistFile}`;
      console.log(`[Prefetch][${this.slug}] Polling master: ${this.stream.playlistFile}`);

      const resp = await this.fetch(
        url,
        { headers: this.getHeaders(this.stream.hdntl) },
        6000
      );

      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

      const text = await resp.text();
      const cacheKey = `${this.slug}:master:${this.stream.playlistFile}`;
      this.cache.set(cacheKey, text, {}, 5000);

      // Parse variant (child playlist) URLs
      const newVariants = this._parseVariants(text);

      if (newVariants.length > 0) {
        const changed = newVariants.length !== this.variants.length;
        this.variants = newVariants;
        if (changed) {
          console.log(`[Prefetch][${this.slug}] Found ${this.variants.length} quality variants:`);
          this.variants.forEach((v, i) => {
            console.log(`  [${i}] ${v.filename} (${(v.bandwidth / 1000).toFixed(0)}kbps)`);
          });
          this._startVariantPolling();
        }
      } else {
        // Maybe it's not a multi-variant playlist — treat the master itself as a media playlist
        console.log(`[Prefetch][${this.slug}] No variants found — treating master as media playlist`);
        await this._downloadNewSegments(text, {
          dir: this.stream.streamBase,
          filename: this.stream.playlistFile,
        });
      }
    } catch (err) {
      this.stats.errors++;
      console.warn(`[Prefetch][${this.slug}] ✗ Master poll error: ${err.message}`);
    }

    // Re-poll master every 8-12s
    if (this.running) {
      this._schedule(() => this._pollMaster(), 8000 + Math.random() * 4000);
    }
  }

  _parseVariants(masterText) {
    const lines = masterText.split('\n');
    const variants = [];

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line.startsWith('#EXT-X-STREAM-INF:')) continue;

      let j = i + 1;
      while (j < lines.length) {
        const l = lines[j].trim();
        if (l && !l.startsWith('#')) break;
        j++;
      }
      if (j >= lines.length) continue;

      const variantPath = lines[j].trim();
      let variantUrl, variantDir;

      if (variantPath.startsWith('http')) {
        variantUrl = variantPath;
        variantDir = variantPath.substring(0, variantPath.lastIndexOf('/'));
      } else if (variantPath.startsWith('/')) {
        const base = new URL(this.stream.streamBase);
        variantUrl = `${base.protocol}//${base.host}${variantPath}`;
        variantDir = `${base.protocol}//${base.host}${variantPath.substring(0, variantPath.lastIndexOf('/'))}`;
      } else {
        variantUrl = `${this.stream.streamBase}/${variantPath}`;
        const pathDir = variantPath.substring(0, variantPath.lastIndexOf('/'));
        variantDir = pathDir ? `${this.stream.streamBase}/${pathDir}` : this.stream.streamBase;
      }

      const bwMatch = line.match(/BANDWIDTH=(\d+)/);
      variants.push({
        url: variantUrl,
        bandwidth: bwMatch ? parseInt(bwMatch[1]) : 0,
        filename: variantPath.split('/').pop().split('?')[0],
        dir: variantDir,
      });

      i = j;
    }

    variants.sort((a, b) => b.bandwidth - a.bandwidth);
    return variants;
  }

  // ══════════════════════════════════════
  //  VARIANT (CHILD) PLAYLIST POLLING
  // ══════════════════════════════════════

  _startVariantPolling() {
    this.variants.forEach((variant, idx) => {
      const delay = idx * (200 + Math.random() * 300);
      this._schedule(() => this._pollVariant(variant), delay);
    });
  }

  async _pollVariant(variant) {
    if (!this.running) return;

    try {
      await this.rateLimiter.acquire('normal');

      const resp = await this.fetch(
        variant.url,
        { headers: this.getHeaders(this.stream.hdntl) },
        5000
      );

      if (!resp.ok) {
        if (resp.status === 429 || resp.status === 403) {
          this.rateLimiter.onCdnError(resp.status);
        }
        throw new Error(`HTTP ${resp.status}`);
      }

      const text = await resp.text();
      const cacheKey = `${this.slug}:child:${variant.filename}`;
      this.cache.set(cacheKey, text, {}, 2000);

      this.stats.playlistPolls++;
      this.stats.lastPoll = Date.now();
      this.rateLimiter.onCdnSuccess();

      // Download NEW segments that appeared in this playlist
      await this._downloadNewSegments(text, variant);

    } catch (err) {
      this.stats.errors++;
      console.warn(`[Prefetch][${this.slug}] ✗ Variant poll error (${variant.filename}): ${err.message}`);
    }

    // Re-poll every 800-1200ms (slightly ahead of typical HLS segment duration)
    if (this.running) {
      const jitter = 800 + Math.random() * 400;
      this._schedule(() => this._pollVariant(variant), jitter);
    }
  }

  // ══════════════════════════════════════
  //  SEGMENT PRE-DOWNLOADING
  // ══════════════════════════════════════

  /**
   * Extract a canonical segment name from a URL/path.
   * This MUST match what the server's segment handler does:
   *   segName = url.split('/').pop().split('?')[0]
   */
  _segName(segPath) {
    return segPath.split('/').pop().split('?')[0];
  }

  /**
   * Download and cache an init segment (#EXT-X-MAP:URI).
   * CRITICAL for fMP4/CMAF — player CANNOT decode any media segment without this.
   * Uses high priority because a missing init segment = total playback failure.
   */
  async _prefetchInitSegment(initUri, variant) {
    const segName = this._segName(initUri);
    const cacheKey = `${this.slug}:seg:${segName}`;

    if (this.cache.has(cacheKey)) return; // already cached

    let segUrl;
    if (initUri.startsWith('http')) {
      segUrl = initUri;
    } else if (initUri.startsWith('/')) {
      const base = new URL(this.stream.streamBase);
      segUrl = `${base.protocol}//${base.host}${initUri}`;
    } else {
      segUrl = `${variant.dir}/${initUri}`;
    }

    try {
      await this.rateLimiter.acquire('high'); // init = critical
      const resp = await this.fetch(
        segUrl,
        { headers: this.getHeaders(this.stream.hdntl) },
        8000
      );
      if (resp.ok) {
        const buf = await resp.buffer();
        this.cache.set(cacheKey, buf, {
          'content-length': String(buf.length),
        }, 600000); // 10 min TTL
        this.cachedSegKeys.add(cacheKey);
        this.stats.segmentsPrefetched++;
        this.stats.segmentBytes += buf.length;
        this.rateLimiter.onCdnSuccess();
        console.log(`[Prefetch][${this.slug}] ✅ INIT ${segName} (${(buf.length / 1024).toFixed(0)}KB) [${variant.filename}]`);
      } else {
        console.warn(`[Prefetch][${this.slug}] ❌ INIT FAILED ${segName} → CDN ${resp.status}`);
        this.rateLimiter.onCdnError(resp.status);
      }
    } catch (err) {
      console.warn(`[Prefetch][${this.slug}] ❌ INIT FAILED ${segName} → ${err.message}`);
    }
  }

  async _downloadNewSegments(playlistText, variant) {
    const lines = playlistText.split('\n');
    const segments = [];

    for (const line of lines) {
      const trimmed = line.trim();

      // ── Handle #EXT-X-MAP init segment (critical for fMP4/CMAF) ──
      if (trimmed.startsWith('#EXT-X-MAP:')) {
        const match = trimmed.match(/URI="([^"]+)"/);
        if (match) {
          await this._prefetchInitSegment(match[1], variant);
        }
        continue;
      }

      if (!trimmed || trimmed.startsWith('#')) continue;
      // Accept ALL non-comment, non-empty lines as potential segment URLs.
      // This handles .ts, .m4s, .mp4, .aac, .fmp4 and any other format.
      segments.push(trimmed);
    }

    if (segments.length === 0) return;

    let newCount = 0;
    let skipCount = 0;

    for (const seg of segments) {
      if (!this.running) break;

      const segName = this._segName(seg);
      const cacheKey = `${this.slug}:seg:${segName}`;

      // Already confirmed in cache — skip
      if (this.cache.has(cacheKey)) {
        this.stats.cacheHits++;
        skipCount++;
        continue;
      }

      // If we previously cached it but it expired, remove from our tracking
      this.cachedSegKeys.delete(cacheKey);

      // Was in failed set — allow retry (clear it)
      this.failedSegKeys.delete(cacheKey);

      // Resolve full segment URL
      let segUrl;
      if (seg.startsWith('http')) {
        segUrl = seg;
      } else if (seg.startsWith('/')) {
        const base = new URL(this.stream.streamBase);
        segUrl = `${base.protocol}//${base.host}${seg}`;
      } else {
        segUrl = `${variant.dir}/${seg}`;
      }

      // Human-like delay between downloads
      if (newCount > 0) {
        const delay = this.warming
          ? 5 + Math.random() * 15    // ultra-fast during warm-up
          : 15 + Math.random() * 35;  // fast normal pace
        await this._sleep(delay);
      }

      try {
        await this.rateLimiter.acquire('normal');

        const resp = await this.fetch(
          segUrl,
          { headers: this.getHeaders(this.stream.hdntl) },
          15000  // generous timeout for large 1080p segments
        );

        if (resp.ok) {
          const buf = await resp.buffer();
          const sizeKB = (buf.length / 1024).toFixed(0);
          this.cache.set(cacheKey, buf, {
            'content-length': resp.headers.get('content-length') || String(buf.length),
          }, 600000); // 10 min TTL

          this.cachedSegKeys.add(cacheKey);
          this.stats.segmentsPrefetched++;
          this.stats.segmentBytes += buf.length;
          this.rateLimiter.onCdnSuccess();
          newCount++;
          console.log(`[Prefetch][${this.slug}] ✅ CACHED ${segName} (${sizeKB}KB) [${variant.filename}] | total: ${this.stats.segmentsPrefetched}`);
        } else {
          this.failedSegKeys.add(cacheKey);
          this.rateLimiter.onCdnError(resp.status);
          this.stats.errors++;
          console.warn(`[Prefetch][${this.slug}] ❌ FAILED ${segName} → CDN ${resp.status} [${variant.filename}]`);
        }
      } catch (err) {
        this.failedSegKeys.add(cacheKey);
        this.stats.errors++;
        console.warn(`[Prefetch][${this.slug}] ❌ FAILED ${segName} → ${err.message} [${variant.filename}]`);
      }
    }

    if (newCount > 0 || skipCount > 0) {
      console.log(`[Prefetch][${this.slug}] 📊 ${variant.filename}: ${newCount} new cached, ${skipCount} already cached, ${segments.length} total in playlist`);
    }

    // First successful round = warm-up complete
    if (newCount > 0 && this.warming) {
      this.warming = false;
      console.log(`[Prefetch][${this.slug}] 🔥 WARM-UP COMPLETE — ${newCount} segments pre-cached, ready for clients!`);
    }

    // Trim tracking sets to prevent memory leak
    if (this.cachedSegKeys.size > 1000) {
      const arr = [...this.cachedSegKeys];
      this.cachedSegKeys = new Set(arr.slice(-500));
    }
    if (this.failedSegKeys.size > 200) {
      this.failedSegKeys.clear(); // just reset, they'll be retried
    }
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // ══════════════════════════════════════
  //  STATUS
  // ══════════════════════════════════════

  getStats() {
    return {
      running: this.running,
      warming: this.warming,
      variants: this.variants.length,
      cachedSegments: this.cachedSegKeys.size,
      failedSegments: this.failedSegKeys.size,
      playlistPolls: this.stats.playlistPolls,
      segmentsPrefetched: this.stats.segmentsPrefetched,
      segmentsMB: (this.stats.segmentBytes / 1048576).toFixed(1),
      cacheHits: this.stats.cacheHits,
      errors: this.stats.errors,
      lastPoll: this.stats.lastPoll,
      uptimeSeconds: this.stats.startedAt
        ? Math.round((Date.now() - this.stats.startedAt) / 1000)
        : 0,
    };
  }
}

module.exports = StreamPrefetcher;
