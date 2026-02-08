/**
 * Request Deduplication
 * 
 * When multiple viewers request the same resource simultaneously,
 * only ONE actual CDN request is made. All other requests wait
 * for that single result.
 * 
 * This prevents the "thundering herd" problem where 10 viewers
 * requesting the same segment would cause 10 CDN requests.
 */
class RequestDedup {
  constructor() {
    this.inflight = new Map();
  }

  /**
   * Execute fetchFn only if no identical request is already in-flight.
   * Otherwise, piggyback on the existing request.
   * 
   * @param {string} key   - Unique key for the request (e.g., cache key)
   * @param {Function} fetchFn - Async function that performs the actual fetch
   * @returns {Promise<*>}
   */
  async dedupFetch(key, fetchFn) {
    if (this.inflight.has(key)) {
      return this.inflight.get(key);
    }

    const promise = fetchFn().finally(() => {
      this.inflight.delete(key);
    });

    this.inflight.set(key, promise);
    return promise;
  }

  get activeRequests() {
    return this.inflight.size;
  }
}

module.exports = RequestDedup;
