// static/js/core/api.js

// API-обёртки и полезные хелперы
window.apiGet = async function (path, params = {}) {
  const url = new URL(path, window.location.origin);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);

  const r = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!r.ok) throw new Error(`API error ${ r.status }`);
  return await r.json();
}

// Кэширование простых GET-ов
window.cachedGet = async function (path, params = {}) {
  window._apiCache = window._apiCache || new Map();
  const cache = window._apiCache;
  const key = path + JSON.stringify(params);
  if (cache.has(key)) return cache.get(key);
  const data = await window.apiGet(path, params);
  cache.set(key, data);
  return data;
}
