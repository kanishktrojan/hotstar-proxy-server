/**
 * Stream Manager
 * 
 * Manages multiple stream configurations. Each stream has:
 * - A unique slug (used in proxy URLs)
 * - A name (for display in UI)
 * - Stream base URL + playlist file
 * - hdntl cookie value
 * - Token expiry timestamp
 * 
 * Streams are persisted to data/streams.json so they
 * survive server restarts.
 */

const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const DATA_DIR = path.join(__dirname, '..', 'data');
const STREAMS_FILE = path.join(DATA_DIR, 'streams.json');

class StreamManager {
  constructor() {
    this.streams = new Map(); // slug -> stream config
    this._ensureDataDir();
    this._load();
  }

  /**
   * Add or update a stream
   * @param {object} config
   * @param {string} [config.slug]        - Custom slug (auto-generated if empty)
   * @param {string} config.name          - Display name
   * @param {string} config.streamUrl     - Full stream URL (parsed into base + file)
   * @param {string} config.hdntl         - hdntl cookie value
   * @returns {object} The saved stream config
   */
  addStream({ slug, name, streamUrl, hdntl }) {
    if (!streamUrl || !hdntl) {
      throw new Error('streamUrl and hdntl are required');
    }

    // Normalize cookie — strip 'hdntl=' prefix if someone pastes the full cookie header
    // But keep multi-cookie strings (CloudFront + hdntl) as-is
    if (!hdntl.includes(';') && hdntl.startsWith('hdntl=')) {
      hdntl = hdntl.slice(6); // strip 'hdntl=' prefix
    }

    // Parse the stream URL into base + playlist file
    const { base, file } = this._parseStreamUrl(streamUrl);

    // Extract token expiry from hdntl if present
    const expMatch = hdntl.match(/exp=(\d+)/);
    const tokenExpiry = expMatch ? parseInt(expMatch[1]) * 1000 : null;

    // Generate or sanitize slug
    const finalSlug = slug
      ? this._sanitizeSlug(slug)
      : this._generateSlug(name);

    const stream = {
      slug: finalSlug,
      name: name || `Stream ${this.streams.size + 1}`,
      streamBase: base,
      playlistFile: file,
      streamUrl,
      hdntl,
      tokenExpiry,
      createdAt: this.streams.has(finalSlug)
        ? this.streams.get(finalSlug).createdAt
        : Date.now(),
      updatedAt: Date.now(),
      active: true,
    };

    this.streams.set(finalSlug, stream);
    this._save();

    return stream;
  }

  /**
   * Update only the cookie (hdntl) for an existing stream
   */
  updateCookie(slug, hdntl) {
    const stream = this.streams.get(slug);
    if (!stream) throw new Error(`Stream "${slug}" not found`);

    const expMatch = hdntl.match(/exp=(\d+)/);
    stream.hdntl = hdntl;
    stream.tokenExpiry = expMatch ? parseInt(expMatch[1]) * 1000 : null;
    stream.updatedAt = Date.now();

    this._save();
    return stream;
  }

  /**
   * Update stream URL for an existing stream
   */
  updateStreamUrl(slug, streamUrl) {
    const stream = this.streams.get(slug);
    if (!stream) throw new Error(`Stream "${slug}" not found`);

    const { base, file } = this._parseStreamUrl(streamUrl);
    stream.streamBase = base;
    stream.playlistFile = file;
    stream.streamUrl = streamUrl;
    stream.updatedAt = Date.now();

    this._save();
    return stream;
  }

  /**
   * Remove a stream
   */
  removeStream(slug) {
    const existed = this.streams.delete(slug);
    if (existed) this._save();
    return existed;
  }

  /**
   * Get stream config by slug
   */
  getStream(slug) {
    return this.streams.get(slug) || null;
  }

  /**
   * List all streams (safe for API response — no full hdntl)
   */
  listStreams() {
    const list = [];
    for (const [slug, stream] of this.streams) {
      list.push({
        slug: stream.slug,
        name: stream.name,
        streamBase: stream.streamBase,
        playlistFile: stream.playlistFile,
        streamUrl: stream.streamUrl,
        hasToken: !!stream.hdntl,
        tokenExpiry: stream.tokenExpiry,
        tokenValid: stream.tokenExpiry ? Date.now() < stream.tokenExpiry : null,
        remainingMinutes: stream.tokenExpiry
          ? Math.round((stream.tokenExpiry - Date.now()) / 60000)
          : null,
        active: stream.active,
        createdAt: stream.createdAt,
        updatedAt: stream.updatedAt,
        proxyUrl: `/stream/${stream.slug}/live.m3u8`,
      });
    }
    return list;
  }

  /**
   * Get full stream details (including hdntl) — internal use only
   */
  getStreamFull(slug) {
    return this.streams.get(slug) || null;
  }

  // ── Internal helpers ──

  _parseStreamUrl(url) {
    // Remove query string for path parsing
    const cleanUrl = url.split('?')[0];
    const parts = cleanUrl.split('/');
    const file = parts.pop(); // Last segment is the playlist file
    const base = parts.join('/');
    return { base, file };
  }

  _sanitizeSlug(slug) {
    return slug
      .toLowerCase()
      .replace(/[^a-z0-9-]/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '')
      .substring(0, 32);
  }

  _generateSlug(name) {
    if (name) {
      const base = this._sanitizeSlug(name);
      if (base && !this.streams.has(base)) return base;
    }
    // Fallback: short uuid
    return uuidv4().split('-')[0];
  }

  _ensureDataDir() {
    if (!fs.existsSync(DATA_DIR)) {
      fs.mkdirSync(DATA_DIR, { recursive: true });
    }
  }

  _save() {
    try {
      const data = {};
      for (const [slug, stream] of this.streams) {
        data[slug] = stream;
      }
      fs.writeFileSync(STREAMS_FILE, JSON.stringify(data, null, 2), 'utf8');
    } catch (err) {
      console.error(`[StreamManager] Save failed: ${err.message}`);
    }
  }

  _load() {
    try {
      if (!fs.existsSync(STREAMS_FILE)) return;
      const raw = fs.readFileSync(STREAMS_FILE, 'utf8');
      const data = JSON.parse(raw);
      for (const [slug, stream] of Object.entries(data)) {
        this.streams.set(slug, stream);
      }
      console.log(`[StreamManager] Loaded ${this.streams.size} stream(s) from disk`);
    } catch (err) {
      console.error(`[StreamManager] Load failed: ${err.message}`);
    }
  }
}

module.exports = StreamManager;
