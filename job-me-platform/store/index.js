const fs = require('fs');
const path = require('path');

const DATA_DIR = path.resolve('./data');

function ensureDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function filePath(name) {
  return path.join(DATA_DIR, name);
}

module.exports = {
  get(key, defaultValue = null) {
    ensureDir();
    try {
      const data = JSON.parse(fs.readFileSync(filePath('store.json'), 'utf-8'));
      return data[key] !== undefined ? data[key] : defaultValue;
    } catch {
      return defaultValue;
    }
  },

  set(key, value) {
    ensureDir();
    let data = {};
    try {
      data = JSON.parse(fs.readFileSync(filePath('store.json'), 'utf-8'));
    } catch {}
    data[key] = value;
    fs.writeFileSync(filePath('store.json'), JSON.stringify(data, null, 2));
  },

  getAll() {
    ensureDir();
    try {
      return JSON.parse(fs.readFileSync(filePath('store.json'), 'utf-8'));
    } catch {
      return {};
    }
  },

  clear() {
    ensureDir();
    fs.writeFileSync(filePath('store.json'), '{}');
  },

  saveCookies(platform, cookies) {
    ensureDir();
    const cookieDir = path.join(DATA_DIR, 'cookies');
    if (!fs.existsSync(cookieDir)) fs.mkdirSync(cookieDir, { recursive: true });
    fs.writeFileSync(path.join(cookieDir, `${platform}.json`), JSON.stringify(cookies, null, 2));
  },

  loadCookies(platform) {
    try {
      return JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'cookies', `${platform}.json`)));
    } catch {
      return null;
    }
  },

  addDelivery(entry) {
    ensureDir();
    let history = [];
    try {
      history = JSON.parse(fs.readFileSync(filePath('deliveries.json'), 'utf-8'));
    } catch {}
    history.unshift({ ...entry, time: Date.now() });
    if (history.length > 500) history = history.slice(0, 500);
    fs.writeFileSync(filePath('deliveries.json'), JSON.stringify(history, null, 2));
  },

  getDeliveries(limit = 100) {
    try {
      const history = JSON.parse(fs.readFileSync(filePath('deliveries.json'), 'utf-8'));
      return history.slice(0, limit);
    } catch {
      return [];
    }
  },
};