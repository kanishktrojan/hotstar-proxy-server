/**
 * Background Stream Prefetcher
 *
 * Continuously polls playlists and pre-downloads segments
 * into cache BEFORE clients request them.
 *
 * Human-like behavior:
 * - Random jitter on all poll intervals (no fixed timing)
 * - Sequential segment downloads (not parallel bursts)
 * - Realistic inter-request delays (80-200ms)
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
    this.knownSegments = new Set();
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
    this._schedule(() => this._pollMaster(), 50); // start almost immediately
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
      this.knownSegments.clear();
      this.start();
    }
  }

  /**
   * Hot-swap the cookie WITHOUT restarting polling or clearing cache.
   * Existing segments stay in cache, only future CDN requests use new cookie.
   * This is the key to zero-buffering on cookie rotation.
   */
  swapCookie(stream) {
    console.log(`[Prefetch][${this.slug}] Cookie swapped (no restart)`);
    this.stream = stream;
    // Don't clear knownSegments — those segments are still valid in cache.
    // Don't stop/restart polling — the next poll cycle will use the new cookie automatically.
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
          console.log(`[Prefetch][${this.slug}] Found ${this.variants.length} quality variants`);
          this._startVariantPolling();
        }
      }
    } catch (err) {
      this.stats.errors++;
      console.warn(`[Prefetch][${this.slug}] Master poll error: ${err.message}`);
    }

    // Re-poll master every 8-12s (master rarely changes)
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

      // Find next non-empty, non-comment line (the variant URL)
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

    // Sort by bandwidth descending (highest quality first for priority downloading)
    variants.sort((a, b) => b.bandwidth - a.bandwidth);
    return variants;
  }

  // ══════════════════════════════════════
  //  VARIANT (CHILD) PLAYLIST POLLING
  // ══════════════════════════════════════

  _startVariantPolling() {
    // Stagger variant polls — looks like different viewers starting at different times
    this.variants.forEach((variant, idx) => {
      const delay = idx * (150 + Math.random() * 250);
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
    }

    // Re-poll every 800-1200ms (human-like jitter around typical 1s HLS refresh)
    if (this.running) {
      const jitter = 800 + Math.random() * 400;
      this._schedule(() => this._pollVariant(variant), jitter);
    }
  }

  // ══════════════════════════════════════
  //  SEGMENT PRE-DOWNLOADING
  // ══════════════════════════════════════

  async _downloadNewSegments(playlistText, variant) {
    const lines = playlistText.split('\n');
    const segments = [];

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      // Match any media segment type
      if (/\.(ts|aac|mp4|m4s|m4a|m4v|fmp4)(\?|$)/i.test(trimmed)) {
        segments.push(trimmed);
      }
    }

    let newCount = 0;

    for (const seg of segments) {
      if (!this.running) break;

      const segName = seg.split('/').pop().split('?')[0];
      const cacheKey = `${this.slug}:seg:${segName}`;

      // Already cached or known — skip
      if (this.cache.has(cacheKey)) {
        this.stats.cacheHits++;
        continue;
      }
      if (this.knownSegments.has(cacheKey)) continue;
      this.knownSegments.add(cacheKey);

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

      // Human-like delay between segment downloads (50-150ms)
      // But during warm-up, go faster (30-80ms) to fill cache quickly
      if (newCount > 0) {
        const delay = this.warming
          ? 30 + Math.random() * 50
          : 50 + Math.random() * 100;
        await this._sleep(delay);
      }

      try {
        await this.rateLimiter.acquire('normal');

        const resp = await this.fetch(
          segUrl,
          { headers: this.getHeaders(this.stream.hdntl) },
          12000  // generous timeout for large 1080p segments (2-4MB)
        );

        if (resp.ok) {
          const buf = await resp.buffer();
          this.cache.set(cacheKey, buf, {
            'content-length': resp.headers.get('content-length') || String(buf.length),
          }, 600000); // 10 min TTL — segments are immutable

          this.stats.segmentsPrefetched++;
          this.stats.segmentBytes += buf.length;
          this.rateLimiter.onCdnSuccess();
          newCount++;
        } else {
          this.rateLimiter.onCdnError(resp.status);
          this.stats.errors++;
        }
      } catch (err) {
        this.stats.errors++;
      }
    }

    // First successful round = warm-up complete
    if (newCount > 0 && this.warming) {
      this.warming = false;
      console.log(`[Prefetch][${this.slug}] ✓ Warm-up complete (${newCount} segments pre-cached)`);
    }

    // Trim known segments set to prevent memory leak
    if (this.knownSegments.size > 500) {
      const arr = [...this.knownSegments];
      this.knownSegments = new Set(arr.slice(-250));
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
      knownSegments: this.knownSegments.size,
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
