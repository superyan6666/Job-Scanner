import { chromium } from 'playwright';
import config from '../config.js';

class BrowserManager {
  constructor() {
    this.browser = null;
    this.contexts = new Map();
  }

  async launch() {
    if (this.browser) return this.browser;

    this.browser = await chromium.launch({
      headless: config.browser.headless,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
      ],
    });

    console.log('[Browser] 浏览器已启动');
    return this.browser;
  }

  async createContext(name, cookiePath = null) {
    await this.launch();
    const context = await this.browser.newContext({
      viewport: config.browser.viewport,
      userAgent: config.browser.userAgent,
      locale: 'zh-CN',
      timezoneId: 'Asia/Shanghai',
    });

    if (cookiePath) {
      try {
        const cookies = JSON.parse(require('fs').readFileSync(cookiePath, 'utf-8'));
        await context.addCookies(cookies);
        console.log(`[Browser] 已加载 Cookie: ${cookiePath}`);
      } catch (e) {
        console.log(`[Browser] 未找到 Cookie 文件: ${cookiePath}，需手动登录`);
      }
    }

    const page = await context.newPage();
    page.setDefaultTimeout(config.browser.timeout);

    this.contexts.set(name, { context, page });
    return { context, page };
  }

  async getPage(name) {
    const entry = this.contexts.get(name);
    if (!entry) return null;
    return entry.page;
  }

  async closeContext(name) {
    const entry = this.contexts.get(name);
    if (entry) {
      await entry.context.close();
      this.contexts.delete(name);
    }
  }

  async closeAll() {
    for (const [name] of this.contexts) {
      await this.closeContext(name);
    }
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
    }
    console.log('[Browser] 浏览器已关闭');
  }

  async saveCookies(name, cookiePath) {
    const entry = this.contexts.get(name);
    if (!entry) return;
    const cookies = await entry.context.cookies();
    require('fs').writeFileSync(cookiePath, JSON.stringify(cookies, null, 2));
    console.log(`[Browser] Cookie 已保存: ${cookiePath}`);
  }
}

export default new BrowserManager();