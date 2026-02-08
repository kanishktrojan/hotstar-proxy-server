/**
 * Adaptive Token-Bucket Rate Limiter
 *
 * - Token bucket allows controlled bursts while maintaining average rate
 * - Automatically backs off when CDN returns 429/403
 * - High-priority lane for playlists so they never stall
 * - Smooth request pacing instead of bursty sliding window
 */
class RateLimiter {
  /**
   * @param {number} maxPerSecond - Maximum CDN requests per second
   */
  constructor(maxPerSecond = 50) {
    this.maxPerSecond = maxPerSecond;
    this.tokens = maxPerSecond; // start full
    this.maxTokens = maxPerSecond;
    this.lastRefill = Date.now();

    // Adaptive backoff state
    this.backoffMultiplier = 1;
    this.consecutiveErrors = 0;
    this.lastError = 0;

    // Refill tokens smoothly every 50ms
    this._refillInterval = setInterval(() => this._refill(), 50);

    // Stats
    this.totalAcquired = 0;
    this.totalWaited = 0;
  }

  _refill() {
    const now = Date.now();
    const elapsed = now - this.lastRefill;
    const rate = this.maxPerSecond / this.backoffMultiplier;
    const newTokens = (elapsed / 1000) * rate;
    this.tokens = Math.min(this.maxTokens, this.tokens + newTokens);
    this.lastRefill = now;

    // Decay backoff over time (recover after 10s without errors)
    if (this.backoffMultiplier > 1 && (now - this.lastError) > 10000) {
      this.backoffMultiplier = Math.max(1, this.backoffMultiplier * 0.9);
      if (this.backoffMultiplier < 1.05) this.backoffMultiplier = 1;
    }
  }

  /**
   * Wait until a request slot is available.
   * @param {string} [priority='normal'] - 'high' for playlists, 'normal' for segments
   */
  async acquire(priority = 'normal') {
    const needed = priority === 'high' ? 0.5 : 1;

    if (this.tokens >= needed) {
      this.tokens -= needed;
      this.totalAcquired++;
      return;
    }

    const rate = this.maxPerSecond / this.backoffMultiplier;
    const waitMs = Math.ceil((needed - this.tokens) / rate * 1000) + 5;
    this.totalWaited++;

    await new Promise(resolve => setTimeout(resolve, Math.min(waitMs, 2000)));
    this._refill();
    this.tokens = Math.max(0, this.tokens - needed);
    this.totalAcquired++;
  }

  /**
   * Signal that CDN returned an error — trigger backoff
   */
  onCdnError(statusCode) {
    this.lastError = Date.now();
    this.consecutiveErrors++;

    if (statusCode === 429) {
      this.backoffMultiplier = Math.min(8, this.backoffMultiplier * 2);
      this.tokens = 0;
      console.warn(`[RateLimiter] 429 — backing off ${this.backoffMultiplier.toFixed(1)}x`);
    } else if (statusCode === 403) {
      this.backoffMultiplier = Math.min(4, this.backoffMultiplier * 1.5);
      console.warn(`[RateLimiter] 403 — backing off ${this.backoffMultiplier.toFixed(1)}x`);
    }
  }

  /**
   * Signal successful request — reduce backoff
   */
  onCdnSuccess() {
    this.consecutiveErrors = 0;
    if (this.backoffMultiplier > 1) {
      this.backoffMultiplier = Math.max(1, this.backoffMultiplier * 0.95);
    }
  }

  get currentRate() {
    return Math.round(this.maxPerSecond / this.backoffMultiplier);
  }

  getStats() {
    return {
      tokensAvailable: Math.round(this.tokens),
      effectiveRate: this.currentRate,
      backoffMultiplier: this.backoffMultiplier.toFixed(2),
      totalAcquired: this.totalAcquired,
      totalWaited: this.totalWaited,
    };
  }

  destroy() {
    clearInterval(this._refillInterval);
  }
}

module.exports = RateLimiter;
