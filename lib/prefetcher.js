/**
 * Background Stream Prefetcher v3 — Ultra-Low-Latency
 *
 * Continuously polls playlists and pre-downloads segments
 * into cache BEFORE clients request them.
 *
 * Key design:
 * - Polls child playlists every ~1s (matches HLS segment duration)
 * - Downloads ALL segments found in playlist (no extension matching)
 * - Uses 'normal' priority so client requests always go first
 * - Human-like jitter on poll intervals to avoid CDN detection
 * - Handles both TS and fMP4/CMAF (init segments via #EXT-X-MAP)
 */
class StreamPrefetcher {
  constructor({ slug, stream, cache, rateLimiter, fetchFn, headersFn }) {
    this.slug        = slug;
    this.stream      = stream;
    this.cache       = cache;
    this.rateLimiter = rateLimiter;
    this.fetch       = fetchFn;
    this.getHeaders  = headersFn;

    this.running  = false;
    this.variants = [];
    this._timers  = new Set();
    this._knownSegs = new Set(); // segment names we've already attempted

    this.stats = {
      polls: 0, prefetched: 0, bytes: 0,
      hits: 0, errors: 0, lastPoll: null, startedAt: null,
    };
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.stats.startedAt = Date.now();
    console.log(`[PF][${this.slug}] ▶ Started`);
    this._schedule(() => this._pollMaster(), 50);
  }

  stop() {
    this.running = false;
    for (const t of this._timers) clearTimeout(t);
    this._timers.clear();
    console.log(`[PF][${this.slug}] ■ Stopped`);
  }

  swapCookie(stream) {
    this.stream = stream;
    this._knownSegs.clear(); // retry everything with new cookie
    console.log(`[PF][${this.slug}] 🔄 Cookie swapped`);
  }

  _schedule(fn, ms) {
    if (!this.running) return;
    const t = setTimeout(() => { this._timers.delete(t); if (this.running) fn(); }, ms);
    this._timers.add(t);
  }

  // ═══════════════════════════════════════
  //  MASTER PLAYLIST
  // ═══════════════════════════════════════
  async _pollMaster() {
    if (!this.running) return;
    try {
      const url  = `${this.stream.streamBase}/${this.stream.playlistFile}`;
      const resp = await this.fetch(url, { headers: this.getHeaders(this.stream.hdntl) }, 6000);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const text = await resp.text();

      // Cache the master playlist
      this.cache.set(`${this.slug}:master:${this.stream.playlistFile}`, text, {}, 5000);

      const variants = this._parseVariants(text);
      if (variants.length > 0) {
        const changed = variants.length !== this.variants.length;
        this.variants = variants;
        if (changed) {
          console.log(`[PF][${this.slug}] ${variants.length} variants found`);
          this._launchVariantPollers();
        }
      } else {
        // Single-variant / media playlist
        await this._processMediaPlaylist(text, this.stream.streamBase);
      }
    } catch (err) {
      this.stats.errors++;
      console.warn(`[PF][${this.slug}] master error: ${err.message}`);
    }
    if (this.running) this._schedule(() => this._pollMaster(), 8000 + Math.random() * 4000);
  }

  _parseVariants(text) {
    const lines = text.split('\n');
    const out = [];
    for (let i = 0; i < lines.length; i++) {
      if (!lines[i].trim().startsWith('#EXT-X-STREAM-INF:')) continue;
      let j = i + 1;
      while (j < lines.length && (!lines[j].trim() || lines[j].trim().startsWith('#'))) j++;
      if (j >= lines.length) continue;

      const p = lines[j].trim();
      let url, dir;
      if (p.startsWith('http')) {
        url = p; dir = p.substring(0, p.lastIndexOf('/'));
      } else if (p.startsWith('/')) {
        const b = new URL(this.stream.streamBase);
        url = `${b.protocol}//${b.host}${p}`;
        dir = `${b.protocol}//${b.host}${p.substring(0, p.lastIndexOf('/'))}`;
      } else {
        url = `${this.stream.streamBase}/${p}`;
        const pd = p.substring(0, p.lastIndexOf('/'));
        dir = pd ? `${this.stream.streamBase}/${pd}` : this.stream.streamBase;
      }

      const bw = (lines[i].match(/BANDWIDTH=(\d+)/) || [])[1];
      out.push({ url, dir, bandwidth: bw ? +bw : 0, filename: p.split('/').pop().split('?')[0] });
      i = j;
    }
    out.sort((a, b) => b.bandwidth - a.bandwidth);
    return out;
  }

  // ═══════════════════════════════════════
  //  VARIANT POLLING
  // ═══════════════════════════════════════
  _launchVariantPollers() {
    // Only prefetch the top 2 variants (highest bandwidth + one fallback).
    // Prefetching ALL variants wastes CDN requests and triggers 429.
    const toPoll = this.variants.slice(0, 2);
    console.log(`[PF][${this.slug}] Polling ${toPoll.length} of ${this.variants.length} variants (top quality)`);
    toPoll.forEach((v, i) => {
      this._schedule(() => this._pollVariant(v), i * 500 + Math.random() * 500);
    });
  }

  async _pollVariant(variant) {
    if (!this.running) return;

    // Skip polling entirely if rate limiter is under heavy pressure
    if (this.rateLimiter.backoffMultiplier > 2) {
      console.log(`[PF][${this.slug}] ⏸ Skipping poll — CDN pressure (backoff ${this.rateLimiter.backoffMultiplier.toFixed(1)}x)`);
      if (this.running) this._schedule(() => this._pollVariant(variant), 5000 + Math.random() * 3000);
      return;
    }

    try {
      await this.rateLimiter.acquire('normal');
      const resp = await this.fetch(variant.url, { headers: this.getHeaders(this.stream.hdntl) }, 5000);
      if (!resp.ok) {
        if (resp.status === 429 || resp.status === 403) this.rateLimiter.onCdnError(resp.status);
        throw new Error(`HTTP ${resp.status}`);
      }
      const text = await resp.text();
      this.cache.set(`${this.slug}:child:${variant.filename}`, text, {}, 2500);
      this.rateLimiter.onCdnSuccess();
      this.stats.polls++;
      this.stats.lastPoll = Date.now();
      await this._processMediaPlaylist(text, variant.dir);
    } catch (err) {
      this.stats.errors++;
    }
    // Poll every 2-3s — segments are typically 4-6s so this is well ahead
    if (this.running) this._schedule(() => this._pollVariant(variant), 2000 + Math.random() * 1000);
  }

  // ═══════════════════════════════════════
  //  SEGMENT DOWNLOADING
  // ═══════════════════════════════════════

  /** Canonical segment name — MUST match server.js logic exactly */
  _segName(raw) { return raw.split('/').pop().split('?')[0]; }

  /** Resolve a path/URL from a playlist line into a full URL */
  _resolveUrl(raw, baseDir) {
    if (raw.startsWith('http')) return raw;
    if (raw.startsWith('/')) {
      const b = new URL(this.stream.streamBase);
      return `${b.protocol}//${b.host}${raw}`;
    }
    return `${baseDir}/${raw}`;
  }

  async _processMediaPlaylist(text, baseDir) {
    const lines = text.split('\n');
    const toFetch = []; // { url, segName, cacheKey, isInit }

    for (const line of lines) {
      const t = line.trim();

      // Init segment
      if (t.startsWith('#EXT-X-MAP:')) {
        const m = t.match(/URI="([^"]+)"/);
        if (m) {
          const sn = this._segName(m[1]);
          const ck = `${this.slug}:seg:${sn}`;
          if (!this.cache.has(ck)) {
            toFetch.push({ url: this._resolveUrl(m[1], baseDir), segName: sn, cacheKey: ck, isInit: true });
          }
        }
        continue;
      }

      if (!t || t.startsWith('#')) continue;

      // Regular segment — every non-tag non-empty line
      const sn = this._segName(t);
      const ck = `${this.slug}:seg:${sn}`;

      if (this._knownSegs.has(sn) && this.cache.has(ck)) {
        this.stats.hits++;
        continue;
      }

      if (!this.cache.has(ck)) {
        toFetch.push({ url: this._resolveUrl(t, baseDir), segName: sn, cacheKey: ck, isInit: false });
      }
      this._knownSegs.add(sn);
    }

    if (toFetch.length === 0) return;

    let ok = 0, fail = 0;
    for (const seg of toFetch) {
      if (!this.running) break;
      try {
        await this.rateLimiter.acquire(seg.isInit ? 'high' : 'normal');
        const resp = await this.fetch(seg.url, { headers: this.getHeaders(this.stream.hdntl) }, 12000);
        if (resp.ok) {
          const buf = await resp.buffer();
          this.cache.set(seg.cacheKey, buf, { 'content-length': String(buf.length) }, 600000);
          this.stats.prefetched++;
          this.stats.bytes += buf.length;
          this.rateLimiter.onCdnSuccess();
          ok++;
          console.log(`[PF][${this.slug}] ✅ ${seg.isInit ? 'INIT ' : ''}${seg.segName} (${(buf.length/1024).toFixed(0)}KB)`);
        } else {
          this.rateLimiter.onCdnError(resp.status);
          fail++;
          console.warn(`[PF][${this.slug}] ✗ ${seg.segName} CDN ${resp.status}`);
          // Remove from known so it gets retried next poll
          this._knownSegs.delete(seg.segName);
        }
      } catch (err) {
        fail++;
        this._knownSegs.delete(seg.segName);
      }

      // Human-like delay between fetches (80-200ms)
      if (toFetch.indexOf(seg) < toFetch.length - 1) {
        await new Promise(r => setTimeout(r, 80 + Math.random() * 120));
      }
    }

    if (ok > 0) console.log(`[PF][${this.slug}] 📊 ${ok} cached, ${fail} failed, total prefetched: ${this.stats.prefetched}`);
  }

  // ═══════════════════════════════════════
  //  HOUSEKEEPING
  // ═══════════════════════════════════════
  getStats() {
    return {
      running: this.running,
      variants: this.variants.length,
      knownSegments: this._knownSegs.size,
      polls: this.stats.polls,
      prefetched: this.stats.prefetched,
      megabytes: (this.stats.bytes / 1048576).toFixed(1),
      hits: this.stats.hits,
      errors: this.stats.errors,
      lastPoll: this.stats.lastPoll,
      uptimeSeconds: this.stats.startedAt ? Math.round((Date.now() - this.stats.startedAt) / 1000) : 0,
    };
  }
}

module.exports = StreamPrefetcher;
