const path = require('path');
const fs = require('fs');

const STRATEGIES = {};

// ────────────────────────────────────────────
// Playwright 策略
// ────────────────────────────────────────────
async function createPlaywright(options = {}) {
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({
    headless: options.headless !== false,
    args: [
      '--no-sandbox', '--disable-setuid-sandbox',
      '--disable-dev-shm-usage', '--disable-gpu',
      ...(options.proxy ? [`--proxy-server=${options.proxy}`] : []),
    ],
  });

  const context = await browser.newContext({
    viewport: options.viewport || { width: 1366, height: 768 },
    userAgent: options.userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale: 'zh-CN',
    timezoneId: 'Asia/Shanghai',
  });

  if (options.cookies) {
    await context.addCookies(options.cookies);
  }

  const page = await context.newPage();
  page.setDefaultTimeout(options.timeout || 30000);

  return {
    _browser: browser,
    _context: context,
    _page: page,
    strategy: 'playwright',

    async navigate(url) {
      await page.goto(url, { waitUntil: 'networkidle' });
    },

    async screenshot() {
      return await page.screenshot({ type: 'jpeg', quality: 65, fullPage: false });
    },

    async evaluate(fn) {
      return await page.evaluate(fn);
    },

    async evaluateHandle(sel, fn) {
      return await page.$eval(sel, fn);
    },

    async waitForSelector(sel, timeout) {
      return await page.waitForSelector(sel, { timeout: timeout || 10000 });
    },

    async $$(sel) {
      return await page.$$(sel);
    },

    async $(sel) {
      return await page.$(sel);
    },

    async click(sel) {
      await page.click(sel);
    },

    async fill(sel, text) {
      await page.fill(sel, text);
    },

    async textContent(sel) {
      return await page.textContent(sel);
    },

    async url() {
      return page.url();
    },

    async title() {
      return page.title();
    },

    async waitForTimeout(ms) {
      await page.waitForTimeout(ms);
    },

    async waitForUrl(urlPattern) {
      await page.waitForURL(urlPattern, { timeout: 120000 });
    },

    async cookies() {
      return await context.cookies();
    },

    async scrollToBottom() {
      await page.evaluate(() => window.scrollTo({
        top: document.documentElement.scrollHeight,
        behavior: 'smooth',
      }));
      await this.waitForTimeout(1500);
    },

    async addCookie(c) {
      await context.addCookies([c]);
    },

    async close() {
      await browser.close();
    },
  };
}

// ────────────────────────────────────────────
// Puppeteer 策略
// ────────────────────────────────────────────
async function createPuppeteer(options = {}) {
  let puppeteer;
  try {
    puppeteer = require('puppeteer');
  } catch {
    throw new Error('Puppeteer 未安装。请运行: npm install puppeteer 安装后即可使用');
  }
  const browser = await puppeteer.launch({
    headless: options.headless !== false,
    args: [
      '--no-sandbox', '--disable-setuid-sandbox',
      '--disable-dev-shm-usage', '--disable-gpu',
      ...(options.proxy ? [`--proxy-server=${options.proxy}`] : []),
    ],
    defaultViewport: options.viewport || { width: 1366, height: 768 },
  });

  const pages = await browser.pages();
  const page = pages[0] || await browser.newPage();

  if (options.cookies) {
    await page.setCookie(...options.cookies);
  }

  await page.setUserAgent(options.userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');

  page.setDefaultTimeout(options.timeout || 30000);

  return {
    _browser: browser,
    _page: page,
    strategy: 'puppeteer',

    async navigate(url) {
      await page.goto(url, { waitUntil: 'networkidle2' });
    },

    async screenshot() {
      return await page.screenshot({ type: 'jpeg', quality: 65, fullPage: false });
    },

    async evaluate(fn) {
      return await page.evaluate(fn);
    },

    async evaluateHandle(sel, fn) {
      const el = await page.$(sel);
      if (!el) return null;
      return await page.evaluate(fn, el);
    },

    async waitForSelector(sel, timeout) {
      await page.waitForSelector(sel, { timeout: timeout || 10000 });
    },

    async $$(sel) {
      return await page.$$(sel);
    },

    async $(sel) {
      return await page.$(sel);
    },

    async click(sel) {
      await page.click(sel);
    },

    async fill(sel, text) {
      await page.click(sel, { clickCount: 3 });
      await page.keyboard.press('Backspace');
      await page.type(sel, text);
    },

    async textContent(sel) {
      const el = await page.$(sel);
      if (!el) return null;
      return await page.evaluate(el => el.textContent.trim(), el);
    },

    async url() {
      return page.url();
    },

    async title() {
      return page.title();
    },

    async waitForTimeout(ms) {
      await page.waitForTimeout(ms);
    },

    async waitForUrl(urlPattern) {
      await page.waitForFunction(
        (pattern) => window.location.href.includes(pattern),
        urlPattern,
        { timeout: 120000 }
      );
    },

    async cookies() {
      return await page.cookies();
    },

    async scrollToBottom() {
      await page.evaluate(() => window.scrollTo({
        top: document.documentElement.scrollHeight,
        behavior: 'smooth',
      }));
      await this.waitForTimeout(1500);
    },

    async addCookie(c) {
      await page.setCookie(c);
    },

    async close() {
      await browser.close();
    },
  };
}

// ────────────────────────────────────────────
// 工厂函数
// ────────────────────────────────────────────
STRATEGIES.playwright = createPlaywright;
STRATEGIES.puppeteer = createPuppeteer;

async function createAutomation(strategy = 'playwright', options = {}) {
  const factory = STRATEGIES[strategy];
  if (!factory) throw new Error(`不支持的自动化策略: ${strategy}，可选: ${Object.keys(STRATEGIES).join(', ')}`);

  global.sendLog?.({
    time: Date.now(),
    emoji: '🌐',
    text: `使用 ${strategy} 引擎启动浏览器...`,
    type: 'info',
  });

  const inst = await factory(options);
  return inst;
}

module.exports = { createAutomation, STRATEGIES };