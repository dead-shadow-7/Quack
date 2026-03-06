const API = "http://localhost:3000";

// ── View switching ─────────────────────────────────────────────────────────────
function showView(name) {
  document
    .querySelectorAll(".view")
    .forEach((v) => v.classList.remove("active"));
  document.getElementById(name + "-view").classList.add("active");
}

// ── Sync both inputs ───────────────────────────────────────────────────────────
function syncInputs(value) {
  document.getElementById("home-input").value = value;
  document.getElementById("results-input").value = value;
}

// ── Main search ────────────────────────────────────────────────────────────────
async function performSearch(query) {
  query = (query || "").trim();
  if (!query) return;

  syncInputs(query);
  showView("results");

  const metaEl = document.getElementById("results-meta");
  const listEl = document.getElementById("results-list");

  metaEl.textContent = "";
  listEl.innerHTML = '<div class="loading">Searching…</div>';

  try {
    const t0 = Date.now();
    const res = await fetch(`${API}/search?q=${encodeURIComponent(query)}`);

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || `Server error ${res.status}`);
    }

    // ── FIX: backend wraps results in { query, count, results } ──────────────
    const data = await res.json();
    const results = data.results ?? []; // safely unwrap
    const elapsed = ((Date.now() - t0) / 1000).toFixed(2);

    metaEl.textContent = `About ${data.count ?? results.length} results (${elapsed}s)`;
    listEl.innerHTML = "";

    if (results.length === 0) {
      listEl.innerHTML = `
        <div class="no-results">
          <p>No results found for <strong>"${escHtml(query)}"</strong></p>
          <p class="hint">Try different keywords or check your spelling.</p>
        </div>`;
      return;
    }

    results.forEach((result) => {
      const card = document.createElement("div");
      card.className = "result-card";
      card.innerHTML = `
        <div class="result-url">${escHtml(friendlyUrl(result.url))}</div>
        <a class="result-title" href="${escHtml(result.url)}" target="_blank" rel="noopener">
          ${escHtml(result.title || result.url)}
        </a>
        <p class="result-snippet">${escHtml(result.snippet || "")}</p>`;
      listEl.appendChild(card);
    });
  } catch (err) {
    listEl.innerHTML = `
      <div class="error">
        <strong>Something went wrong.</strong>
        <p>${escHtml(err.message)}</p>
      </div>`;
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────────
function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function friendlyUrl(url) {
  try {
    const p = new URL(url);
    return p.hostname + p.pathname.replace(/\/$/, "");
  } catch {
    return url;
  }
}

// ── Event wiring ───────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  // Home search
  document.getElementById("home-btn").addEventListener("click", () => {
    performSearch(document.getElementById("home-input").value);
  });
  document.getElementById("home-input").addEventListener("keydown", (e) => {
    if (e.key === "Enter") performSearch(e.target.value);
  });

  // Results bar search
  document.getElementById("results-btn").addEventListener("click", () => {
    performSearch(document.getElementById("results-input").value);
  });
  document.getElementById("results-input").addEventListener("keydown", (e) => {
    if (e.key === "Enter") performSearch(e.target.value);
  });

  // Logo → back to home
  document.getElementById("logo-back").addEventListener("click", (e) => {
    e.preventDefault();
    syncInputs("");
    showView("home");
  });
});
