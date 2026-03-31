// Mammoth Admin — SPA Router, API Client, State Management

const API = '/api/v1';

// --- API Client ---
async function api(path, opts = {}) {
  const res = await fetch(API + path, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
  });
  const data = await res.json();
  if (!data.ok) throw new Error(data.error || 'API error');
  return data.data;
}

// --- SPA Router ---
const routes = {};

function route(path, handler) {
  routes[path] = handler;
}

async function navigate() {
  const hash = location.hash.slice(1) || '/';
  const content = document.getElementById('content');

  // Update active nav link
  document.querySelectorAll('#nav a').forEach(a => {
    const href = a.getAttribute('href').slice(1);
    a.classList.toggle('active', href === hash || (hash.startsWith(href) && href !== '/'));
  });

  // Find matching route
  let handler = routes[hash];
  if (!handler) {
    // Try prefix match for parameterized routes
    for (const [pattern, fn] of Object.entries(routes)) {
      if (hash.startsWith(pattern) && pattern !== '/') {
        handler = fn;
        break;
      }
    }
  }

  if (handler) {
    try {
      await handler(content, hash);
    } catch (e) {
      content.innerHTML = `<div class="card"><p style="color:var(--danger)">Error: ${esc(e.message)}</p></div>`;
    }
  } else {
    content.innerHTML = '<div class="card"><p>Page not found</p></div>';
  }
}

window.addEventListener('hashchange', navigate);

// --- Utilities ---
function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatNumber(n) {
  return new Intl.NumberFormat().format(n);
}

function closeModal() {
  document.getElementById('modal').classList.add('hidden');
}

function openModal(title, bodyHtml) {
  document.getElementById('modal-title').textContent = title;
  document.getElementById('modal-body').innerHTML = bodyHtml;
  document.getElementById('modal').classList.remove('hidden');
}

// --- State ---
let serverInfo = null;

async function loadServerInfo() {
  if (!serverInfo) {
    serverInfo = await api('/status');
  }
  return serverInfo;
}

// --- Init ---
document.addEventListener('DOMContentLoaded', async () => {
  try {
    const info = await loadServerInfo();
    document.getElementById('version').textContent = 'v' + info.version;
  } catch (e) {
    document.getElementById('version').textContent = 'offline';
  }
  navigate();
});
