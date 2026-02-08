/**
 * LRU In-Memory Stream Cache
 *
 * - Doubly-linked list for O(1) LRU eviction
 * - Separate hot/cold tracking: recently accessed entries survive longer
 * - Memory-bounded with configurable max
 */
class StreamCache {
  constructor({ maxMemoryMB = 512 } = {}) {
    this.store = new Map(); // key -> { data, headers, size, expiresAt, prev, next }
    this.maxMemoryBytes = maxMemoryMB * 1024 * 1024;
    this.currentBytes = 0;
    this.stats = { hits: 0, misses: 0, evictions: 0 };

    // LRU doubly-linked list sentinels
    this._head = { key: '__HEAD__' };
    this._tail = { key: '__TAIL__' };
    this._head.next = this._tail;
    this._tail.prev = this._head;

    // Cleanup expired entries every 15s
    this._cleanupTimer = setInterval(() => this._cleanup(), 15000);
  }

  /**
   * Get a cached entry by key
   */
  get(key) {
    const entry = this.store.get(key);
    if (!entry) {
      this.stats.misses++;
      return null;
    }
    if (Date.now() > entry.expiresAt) {
      this._delete(key);
      this.stats.misses++;
      return null;
    }
    // Move to front (most recently used)
    this._moveToFront(entry);
    this.stats.hits++;
    return entry;
  }

  /**
   * Store data in cache with a TTL
   */
  set(key, data, headers, ttlMs) {
    if (this.store.has(key)) {
      this._delete(key);
    }

    const size = this._estimateSize(data);

    // Evict LRU entries until we're under budget
    while (this.currentBytes + size > this.maxMemoryBytes && this.store.size > 0) {
      this._evictLRU();
    }

    const entry = {
      key,
      data,
      headers: headers || {},
      size,
      expiresAt: Date.now() + ttlMs,
      createdAt: Date.now(),
      prev: null,
      next: null,
    };

    this.store.set(key, entry);
    this._addToFront(entry);
    this.currentBytes += size;
  }

  /**
   * Check if key exists and is not expired (without counting as access)
   */
  has(key) {
    const entry = this.store.get(key);
    if (!entry) return false;
    if (Date.now() > entry.expiresAt) {
      this._delete(key);
      return false;
    }
    return true;
  }

  invalidate(pattern) {
    let count = 0;
    for (const key of this.store.keys()) {
      if (key.includes(pattern)) {
        this._delete(key);
        count++;
      }
    }
    return count;
  }

  invalidateStream(slug) {
    return this.invalidate(`${slug}:`);
  }

  clear() {
    this.store.clear();
    this._head.next = this._tail;
    this._tail.prev = this._head;
    this.currentBytes = 0;
  }

  getStats() {
    const total = this.stats.hits + this.stats.misses;
    return {
      entries: this.store.size,
      hits: this.stats.hits,
      misses: this.stats.misses,
      evictions: this.stats.evictions,
      hitRate: total > 0 ? ((this.stats.hits / total) * 100).toFixed(1) + '%' : 'N/A',
      memoryMB: (this.currentBytes / (1024 * 1024)).toFixed(2),
      maxMemoryMB: (this.maxMemoryBytes / (1024 * 1024)).toFixed(0),
    };
  }

  // ── LRU Linked List Operations ──

  _addToFront(entry) {
    entry.next = this._head.next;
    entry.prev = this._head;
    this._head.next.prev = entry;
    this._head.next = entry;
  }

  _removeNode(entry) {
    if (entry.prev) entry.prev.next = entry.next;
    if (entry.next) entry.next.prev = entry.prev;
    entry.prev = null;
    entry.next = null;
  }

  _moveToFront(entry) {
    this._removeNode(entry);
    this._addToFront(entry);
  }

  _evictLRU() {
    const lru = this._tail.prev;
    if (lru === this._head) return; // empty
    this._delete(lru.key);
    this.stats.evictions++;
  }

  _delete(key) {
    const entry = this.store.get(key);
    if (entry) {
      this._removeNode(entry);
      this.currentBytes -= entry.size;
      this.store.delete(key);
    }
  }

  _cleanup() {
    const now = Date.now();
    let cleaned = 0;
    for (const [key, entry] of this.store) {
      if (now > entry.expiresAt) {
        this._delete(key);
        cleaned++;
      }
    }
    if (cleaned > 0) {
      console.log(`[Cache] Cleaned ${cleaned} expired | ${this.store.size} active | ${(this.currentBytes / 1048576).toFixed(1)}MB`);
    }
  }

  _estimateSize(data) {
    if (Buffer.isBuffer(data)) return data.length;
    if (typeof data === 'string') return Buffer.byteLength(data, 'utf8');
    return JSON.stringify(data).length;
  }

  destroy() {
    clearInterval(this._cleanupTimer);
    this.clear();
  }
}

module.exports = StreamCache;
