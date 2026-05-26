import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import config from '../config.js';

let db = null;

export function initDB() {
  const dbPath = path.resolve(config.db.path);
  const dbDir = path.dirname(dbPath);

  if (!fs.existsSync(dbDir)) {
    fs.mkdirSync(dbDir, { recursive: true });
  }

  db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      platform TEXT NOT NULL,
      title TEXT NOT NULL,
      company TEXT,
      salary TEXT,
      salary_min INTEGER DEFAULT 0,
      salary_max INTEGER DEFAULT 0,
      location TEXT,
      welfare TEXT,
      experience TEXT,
      education TEXT,
      tags TEXT,
      link TEXT,
      is_headhunter INTEGER DEFAULT 0,
      is_outsourcing INTEGER DEFAULT 0,
      score REAL DEFAULT 0,
      raw_data TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS delivery_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT,
      platform TEXT NOT NULL,
      title TEXT,
      company TEXT,
      location TEXT,
      status TEXT DEFAULT 'sent',
      message TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS chat_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      platform TEXT NOT NULL,
      hr_name TEXT,
      company_name TEXT,
      chat_id TEXT,
      last_message TEXT,
      reply TEXT,
      reply_source TEXT DEFAULT 'manual',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS hr_profiles (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      platform TEXT NOT NULL,
      hr_name TEXT,
      company_name TEXT,
      chat_id TEXT UNIQUE,
      processed INTEGER DEFAULT 0,
      greeted INTEGER DEFAULT 0,
      resume_sent INTEGER DEFAULT 0,
      notes TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT
    );

    CREATE TABLE IF NOT EXISTS blacklist (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_name TEXT NOT NULL UNIQUE,
      reason TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_delivery_platform ON delivery_history(platform);
    CREATE INDEX IF NOT EXISTS idx_delivery_created ON delivery_history(created_at);
    CREATE INDEX IF NOT EXISTS idx_hr_chat_id ON hr_profiles(chat_id);
  `);

  console.log('[DB] 数据库已初始化');
  return db;
}

export function getDB() {
  if (!db) initDB();
  return db;
}

export const dbAPI = {
  saveJob(job) {
    const stmt = getDB().prepare(`
      INSERT OR REPLACE INTO jobs (id, platform, title, company, salary, salary_min, salary_max,
        location, welfare, tags, link, is_headhunter, score, raw_data)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      job.id, job.platform, job.title, job.company, job.salary,
      job.salaryMin || 0, job.salaryMax || 0,
      job.location, job.welfare,
      JSON.stringify(job.tags || []), job.link || '',
      job.isHeadhunter ? 1 : 0, job.score || 0,
      JSON.stringify(job)
    );
  },

  getTodayDeliveries() {
    const today = new Date().toISOString().split('T')[0];
    return getDB().prepare(
      'SELECT * FROM delivery_history WHERE date(created_at) = ? ORDER BY created_at DESC'
    ).all(today);
  },

  addDelivery(job) {
    const stmt = getDB().prepare(`
      INSERT INTO delivery_history (job_id, platform, title, company, location, status)
      VALUES (?, ?, ?, ?, ?, 'sent')
    `);
    stmt.run(job.id || '', job.platform, job.title, job.company, job.location);
  },

  getDeliveryStats() {
    const today = new Date().toISOString().split('T')[0];
    const total = getDB().prepare('SELECT COUNT(*) as count FROM delivery_history').get();
    const todayCount = getDB().prepare(
      'SELECT COUNT(*) as count FROM delivery_history WHERE date(created_at) = ?'
    ).get(today);
    return { total: total.count, today: todayCount.count };
  },

  saveChatRecord(platform, hrName, companyName, chatId, lastMessage, reply, source) {
    const stmt = getDB().prepare(`
      INSERT INTO chat_history (platform, hr_name, company_name, chat_id, last_message, reply, reply_source)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(platform, hrName, companyName, chatId, lastMessage, reply, source);
  },

  upsertHRProfile(platform, hrName, companyName, chatId) {
    const stmt = getDB().prepare(`
      INSERT INTO hr_profiles (platform, hr_name, company_name, chat_id, processed, greeted)
      VALUES (?, ?, ?, ?, 1, 1)
      ON CONFLICT(chat_id) DO UPDATE SET
        processed = 1, greeted = 1, updated_at = CURRENT_TIMESTAMP
    `);
    stmt.run(platform, hrName, companyName, chatId);
  },

  getSetting(key, defaultValue = null) {
    const row = getDB().prepare('SELECT value FROM settings WHERE key = ?').get(key);
    return row ? JSON.parse(row.value) : defaultValue;
  },

  setSetting(key, value) {
    const stmt = getDB().prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)');
    stmt.run(key, JSON.stringify(value));
  },

  isBlacklisted(companyName) {
    const row = getDB().prepare('SELECT id FROM blacklist WHERE company_name = ?').get(companyName);
    return !!row;
  },

  addBlacklist(companyName, reason = '') {
    const stmt = getDB().prepare('INSERT OR IGNORE INTO blacklist (company_name, reason) VALUES (?, ?)');
    stmt.run(companyName, reason);
  },

  getRecentJobs(limit = 50) {
    return getDB().prepare(
      'SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?'
    ).all(limit);
  },

  getRecentDeliveries(limit = 100) {
    return getDB().prepare(
      'SELECT * FROM delivery_history ORDER BY created_at DESC LIMIT ?'
    ).all(limit);
  },
};