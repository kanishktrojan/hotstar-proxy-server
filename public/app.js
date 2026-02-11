/**
 * Restream Proxy — Dashboard Frontend
 */

const API = {
  async getStreams() {
    const res = await fetch('/api/streams');
    return res.json();
  },

  async addStream(data) {
    const res = await fetch('/api/streams', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    return res.json();
  },

  async updateStream(slug, data) {
    const res = await fetch(`/api/streams/${slug}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    return res.json();
  },

  async deleteStream(slug) {
    const res = await fetch(`/api/streams/${slug}`, { method: 'DELETE' });
    return res.json();
  },

  async getStatus() {
    const res = await fetch('/api/status');
    return res.json();
  },
};

// ═══════════════════════════════════════
//  STATE
// ═══════════════════════════════════════

let streams = [];

// ═══════════════════════════════════════
//  DOM REFS
// ═══════════════════════════════════════

const $streamsList = document.getElementById('streamsList');
const $emptyState = document.getElementById('emptyState');
const $addForm = document.getElementById('addStreamForm');
const $addCard = document.querySelector('.add-stream-card');
const $addToggle = document.getElementById('addStreamToggle');
const $toggleIcon = document.getElementById('toggleIcon');
const $refreshBtn = document.getElementById('refreshBtn');
const $editModal = document.getElementById('editModal');
const $editForm = document.getElementById('editForm');
const $editModalClose = document.getElementById('editModalClose');
const $editCancelBtn = document.getElementById('editCancelBtn');
const $editModalTitle = document.getElementById('editModalTitle');
const $toastContainer = document.getElementById('toastContainer');

// Status pills
const $pillUptime = document.getElementById('pillUptime');
const $pillMemory = document.getElementById('pillMemory');
const $pillCache = document.getElementById('pillCache');

// ═══════════════════════════════════════
//  TOAST
// ═══════════════════════════════════════

function showToast(message, type = 'info') {
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  $toastContainer.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateX(30px)';
    toast.style.transition = 'all 0.25s';
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}

// ═══════════════════════════════════════
//  RENDER STREAMS
// ═══════════════════════════════════════

function getFullProxyUrl(proxyPath) {
  return `${window.location.origin}${proxyPath}`;
}

function renderBadge(stream) {
  let badges = '';

  // Token badge
  if (stream.tokenValid === true) {
    badges += `<span class="badge badge-valid">${stream.remainingMinutes}m left</span>`;
  } else if (stream.tokenValid === false) {
    badges += `<span class="badge badge-expired">EXPIRED</span>`;
  } else {
    badges += `<span class="badge badge-unknown">Unknown Expiry</span>`;
  }

  // Prefetch badge (updated dynamically by loadStatus)
  badges += ` <span class="badge badge-prefetch" id="pf-badge-${stream.slug}">Prefetching...</span>`;

  return badges;
}

function renderStreams() {
  if (streams.length === 0) {
    $streamsList.innerHTML = `
      <div class="empty-state">
        <p>No streams configured yet.</p>
        <p class="hint">Add a stream above to get started.</p>
      </div>`;
    return;
  }

  const fullProxyBase = window.location.origin;

  $streamsList.innerHTML = streams.map(stream => `
    <div class="stream-item" data-slug="${stream.slug}">
      <div class="stream-top">
        <div>
          <span class="stream-name">${escapeHtml(stream.name)}</span>
          ${renderBadge(stream)}
        </div>
        <div class="stream-actions">
          <button class="btn-icon" onclick="editStream('${stream.slug}')" title="Edit">&#9998;</button>
          <button class="btn-icon danger" onclick="deleteStream('${stream.slug}')" title="Delete">&#128465;</button>
        </div>
      </div>
      <div class="stream-meta">
        <div class="meta-item">
          <span class="meta-label">Slug</span>
          <span class="meta-value">${stream.slug}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">Playlist</span>
          <span class="meta-value">${stream.playlistFile || '—'}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">Token</span>
          <span class="meta-value">${stream.hasToken ? 'Set' : 'Missing'}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">Expires</span>
          <span class="meta-value">${stream.tokenExpiry ? new Date(stream.tokenExpiry).toLocaleString() : '—'}</span>
        </div>
      </div>
      <div class="stream-url-row">
        <span class="proxy-url" id="url-${stream.slug}">${fullProxyBase}${stream.proxyUrl}</span>
        <button class="btn-copy" onclick="copyUrl('${stream.slug}')">Copy URL</button>
      </div>
    </div>
  `).join('');
}

// ═══════════════════════════════════════
//  ACTIONS
// ═══════════════════════════════════════

async function loadStreams() {
  try {
    streams = await API.getStreams();
    renderStreams();
  } catch (err) {
    showToast('Failed to load streams', 'error');
  }
}

async function loadStatus() {
  try {
    const status = await API.getStatus();
    $pillUptime.textContent = `Up: ${formatUptime(status.server.uptime)}`;
    $pillMemory.textContent = status.server.memory;
    $pillCache.textContent = `Cache: ${status.cache.hitRate}`;

    // Update prefetch badges on stream cards
    if (status.prefetch) {
      for (const [slug, pf] of Object.entries(status.prefetch)) {
        const badge = document.getElementById(`pf-badge-${slug}`);
        if (badge) {
          if (pf.warming) {
            badge.className = 'badge badge-warning';
            badge.textContent = 'Warming...';
          } else if (pf.running) {
            badge.className = 'badge badge-prefetch';
            badge.textContent = `${pf.segmentsPrefetched} segs cached`;
          } else {
            badge.className = 'badge badge-unknown';
            badge.textContent = 'Prefetch OFF';
          }
        }
      }
    }
  } catch {
    // Silently fail for status updates
  }
}

function formatUptime(seconds) {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}

window.copyUrl = function (slug) {
  const urlEl = document.getElementById(`url-${slug}`);
  const url = urlEl.textContent;
  navigator.clipboard.writeText(url).then(() => {
    const btn = urlEl.parentElement.querySelector('.btn-copy');
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(() => {
      btn.textContent = 'Copy URL';
      btn.classList.remove('copied');
    }, 2000);
  });
};

window.editStream = function (slug) {
  const stream = streams.find(s => s.slug === slug);
  if (!stream) return;

  $editModalTitle.textContent = `Edit: ${stream.name}`;
  document.getElementById('editSlug').value = slug;
  document.getElementById('editName').value = stream.name;
  document.getElementById('editUrl').value = stream.streamUrl || '';
  document.getElementById('editHdntl').value = ''; // Don't pre-fill for security
  document.getElementById('editHdntl').placeholder = 'Leave blank to keep existing cookie';

  $editModal.classList.add('active');
};

window.deleteStream = async function (slug) {
  const stream = streams.find(s => s.slug === slug);
  if (!stream) return;

  if (!confirm(`Delete stream "${stream.name}"?`)) return;

  try {
    const result = await API.deleteStream(slug);
    if (result.success) {
      showToast(`Deleted "${stream.name}"`, 'success');
      await loadStreams();
    } else {
      showToast(result.error || 'Failed to delete', 'error');
    }
  } catch (err) {
    showToast('Failed to delete stream', 'error');
  }
};

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

// ═══════════════════════════════════════
//  EVENT LISTENERS
// ═══════════════════════════════════════

// Toggle add-stream form
$addToggle.addEventListener('click', () => {
  $addCard.classList.toggle('collapsed');
});

// Add stream
$addForm.addEventListener('submit', async (e) => {
  e.preventDefault();

  const data = {
    name: document.getElementById('streamName').value.trim(),
    slug: document.getElementById('streamSlug').value.trim() || undefined,
    streamUrl: document.getElementById('streamUrl').value.trim(),
    hdntl: document.getElementById('streamHdntl').value.trim(),
  };

  if (!data.streamUrl || !data.hdntl) {
    showToast('Stream URL and cookie are required', 'error');
    return;
  }

  const btn = document.getElementById('addStreamBtn');
  btn.disabled = true;
  btn.textContent = 'Adding...';

  try {
    const result = await API.addStream(data);

    if (result.success) {
      showToast(`Stream "${result.stream.name}" added!`, 'success');
      $addForm.reset();
      $addCard.classList.add('collapsed');
      await loadStreams();
    } else {
      showToast(result.error || 'Failed to add stream', 'error');
    }
  } catch (err) {
    showToast('Failed to add stream', 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Add Stream';
  }
});

// Edit form submit
$editForm.addEventListener('submit', async (e) => {
  e.preventDefault();

  const slug = document.getElementById('editSlug').value;
  const data = {};

  const name = document.getElementById('editName').value.trim();
  const url = document.getElementById('editUrl').value.trim();
  const hdntl = document.getElementById('editHdntl').value.trim();

  if (name) data.name = name;
  if (url) data.streamUrl = url;
  if (hdntl) data.hdntl = hdntl;

  try {
    const result = await API.updateStream(slug, data);
    if (result.success) {
      showToast(`Stream updated!`, 'success');
      $editModal.classList.remove('active');
      await loadStreams();
    } else {
      showToast(result.error || 'Failed to update', 'error');
    }
  } catch (err) {
    showToast('Failed to update stream', 'error');
  }
});

// Close modal
$editModalClose.addEventListener('click', () => {
  $editModal.classList.remove('active');
});

$editCancelBtn.addEventListener('click', () => {
  $editModal.classList.remove('active');
});

$editModal.addEventListener('click', (e) => {
  if (e.target === $editModal) {
    $editModal.classList.remove('active');
  }
});

// Refresh button
$refreshBtn.addEventListener('click', async () => {
  $refreshBtn.textContent = 'Loading...';
  await loadStreams();
  await loadStatus();
  $refreshBtn.textContent = 'Refresh';
});

// Keyboard shortcut: Escape closes modal
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && $editModal.classList.contains('active')) {
    $editModal.classList.remove('active');
  }
});

// ═══════════════════════════════════════
//  INIT
// ═══════════════════════════════════════

(async function init() {
  await loadStreams();
  await loadStatus();

  // Poll status every 5s (prefetch badges need frequent updates)
  setInterval(loadStatus, 5000);

  // Refresh stream list every 60s (to update expiry times)
  setInterval(loadStreams, 60000);
})();
