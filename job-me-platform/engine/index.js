import config from '../config.js';
import { filterJobs } from '../core/filter.js';
import { createAIClient, buildSystemPrompt } from '../ai/client.js';
import { dbAPI } from '../db/index.js';
import BrowserManager from './browser.js';
import BossAdapter from './adapters/boss.js';
import fs from 'fs';
import path from 'path';

const ADAPTERS = {
  boss: BossAdapter,
};

const PLATFORM_NAMES = {
  boss: 'BOSS直聘',
  zhilian: '智联招聘',
  job51: '前程无忧',
  liepin: '猎聘',
};

export class JobEngine {
  constructor() {
    this.running = false;
    this.deliveryCount = 0;
    this.aiClient = null;
    this.currentPlatform = null;
    this.adapters = {};
    this.sessionConfig = {};
    this._listeners = [];
    this.screenshotDir = null;
    this.currentStep = '';
    this.currentJob = null;
    this.lastScreenshot = null;
  }

  onProgress(cb) {
    this._listeners.push(cb);
    return () => { this._listeners = this._listeners.filter(l => l !== cb); };
  }

  emit(type, data = {}) {
    const payload = { type, time: Date.now(), ...data, step: this.currentStep };
    for (const cb of this._listeners) cb(payload);
  }

  async takeScreenshot(name) {
    try {
      const entry = BrowserManager.contexts.get(this.currentPlatform);
      if (!entry) return null;
      const page = entry.page;
      const buf = await page.screenshot({ type: 'jpeg', quality: 60, fullPage: false });
      const b64 = buf.toString('base64');
      this.lastScreenshot = b64;
      this.emit('screenshot', { image: b64, name });
      return b64;
    } catch { return null; }
  }

  async logStep(emoji, title, detail = '') {
    this.emit('step', {
      emoji, title, detail,
      deliveryCount: this.deliveryCount,
      maxDelivery: this.sessionConfig.maxPerSession,
    });
    await this.takeScreenshot(title.replace(/[^a-zA-Z0-9\u4e00-\u9fa5]/g, '_').slice(0, 30));
  }

  initAI() {
    if (!this.aiClient && config.ai.apiKey) {
      this.aiClient = createAIClient(config.ai);
    }
  }

  getCookiePath(platform) {
    const dir = path.resolve('./data/cookies');
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    return path.join(dir, `${platform}_cookies.json`);
  }

  getScreenshotDir() {
    if (!this.screenshotDir) {
      this.screenshotDir = path.resolve('./data/screenshots');
      if (!fs.existsSync(this.screenshotDir)) fs.mkdirSync(this.screenshotDir, { recursive: true });
    }
    return this.screenshotDir;
  }

  async startDelivery(platform, filters = {}) {
    this.running = true;
    this.deliveryCount = 0;
    this.currentPlatform = platform;
    this.sessionConfig = { ...config.delivery, ...filters };

    const platformName = PLATFORM_NAMES[platform] || platform;
    const AdapterClass = ADAPTERS[platform];
    if (!AdapterClass) {
      this.emit('error', { message: `不支持的平台: ${platform}` });
      this.running = false;
      return;
    }

    this.emit('start', { platform, platformName, maxDelivery: this.sessionConfig.maxPerSession });
    this.emit('status', { message: `🚀 ${platformName} 自动化引擎启动中...` });

    try {
      this.currentStep = '启动浏览器';
      await this.logStep('🌐', '启动浏览器', '正在打开 Playwright 浏览器...');
      const context = await BrowserManager.createContext(platform, this.getCookiePath(platform));
      const page = context.page;

      const adapter = new AdapterClass();
      adapter.setPage(page);
      this.adapters[platform] = adapter;

      this.currentStep = '登录验证';
      await this.logStep('🔑', '检查登录状态', `正在导航至 ${platformName}...`);
      const loggedIn = await adapter.login();
      if (!loggedIn) {
        await BrowserManager.saveCookies(platform, this.getCookiePath(platform));
      }
      await this.logStep('✅', '登录完成', loggedIn ? '已保持登录状态' : '扫码登录成功');

      this.currentStep = '搜索岗位';
      const keywords = filters.includeKeywords || ['财务', '会计'];
      await this.logStep('🔍', '搜索岗位', `关键词: ${keywords.join(', ')}`);
      await adapter.navigateToList(keywords, filters.cityCode || '');
      await this.randomDelay(2000);
      await this.logStep('📋', '页面已加载', '等待岗位列表渲染...');

      let noJobRounds = 0;
      while (this.running && noJobRounds < 5) {
        if (this.deliveryCount >= this.sessionConfig.maxPerSession) {
          await this.logStep('🎯', '达到投递上限', `已完成 ${this.deliveryCount}/${this.sessionConfig.maxPerSession} 次投递`);
          break;
        }

        this.currentStep = '扫描岗位列表';
        const result = await this.processJobList(adapter, platform, filters);

        if (result === 'NO_MORE_JOBS' || result === 'ALL_SKIPPED') {
          noJobRounds++;
          if (result === 'NO_MORE_JOBS') {
            await this.logStep('📭', '暂无更多岗位', `第 ${noJobRounds} 轮尝试加载更多...`);
            const loaded = await this.tryLoadMore(adapter);
            if (!loaded) {
              await this.logStep('🏁', '没有更多岗位了', `共投递 ${this.deliveryCount} 个岗位`);
              break;
            }
          } else {
            await this.logStep('⏭️', '本轮岗位已全部过滤跳过', '尝试加载新岗位...');
            const loaded = await this.tryLoadMore(adapter);
            if (!loaded) {
              await this.logStep('🏁', '没有更多可投递岗位', `共投递 ${this.deliveryCount} 个岗位`);
              break;
            }
          }
        } else {
          noJobRounds = 0;
        }

        await this.randomDelay(1000);
      }

      if (noJobRounds >= 5) {
        await this.logStep('⏹️', '多次尝试无新岗位，自动停止', `共投递 ${this.deliveryCount} 个岗位`);
      }

      this.emit('complete', {
        deliveryCount: this.deliveryCount,
        message: `🏆 ${platformName} 自动化完成，共投递 ${this.deliveryCount} 个岗位`,
      });

    } catch (err) {
      this.emit('error', { message: `❌ 自动化异常: ${err.message}` });
      await this.takeScreenshot('error_state');
    } finally {
      this.running = false;
      this.currentStep = '';
      this.emit('stop', {});
      try { await BrowserManager.saveCookies(platform, this.getCookiePath(platform)); } catch {}
    }
  }

  async processJobList(adapter, platform, filters) {
    const cards = await adapter.getJobCards();
    if (!cards.length) return 'NO_MORE_JOBS';

    let processed = 0;
    let skipped = 0;

    for (const card of cards) {
      if (!this.running) break;
      if (this.deliveryCount >= this.sessionConfig.maxPerSession) break;

      try {
        this.currentStep = '解析岗位卡片';
        const parsed = await adapter.parseJobCard(card);
        if (!parsed || !parsed.title) { skipped++; continue; }

        this.currentJob = parsed;
        this.emit('scan', {
          title: parsed.title,
          company: parsed.company,
          salary: parsed.salary,
          location: parsed.location,
        });

        if (this.isBlacklisted(parsed)) {
          this.emit('skip', { reason: '黑名单', title: parsed.title, company: parsed.company });
          skipped++;
          continue;
        }

        const filtered = filterJobs([parsed], filters);
        if (!filtered.length) {
          this.emit('skip', { reason: '不匹配过滤条件', title: parsed.title, company: parsed.company });
          skipped++;
          continue;
        }

        const job = filtered[0];
        await this.logStep('🎯', `正在投递: ${job.title}`, `${job.company} | ${job.salary || '薪资面议'} | ${job.location || ''}`);

        this.currentStep = '点击沟通按钮';
        await adapter.focusJobCard?.(card);
        await this.randomDelay(1000, 500);
        await this.logStep('👆', '点击"立即沟通"', '等待按钮响应...');

        const clicked = await adapter.clickCommunicate(card);

        if (clicked) {
          this.currentStep = '处理弹窗';
          await this.logStep('🔄', '投递中', '处理平台弹窗...');
          const modalResult = await adapter.handleModal();

          if (modalResult === 'LIMIT_REACHED') {
            await this.logStep('⛔', '今日沟通已达上限！', '平台限制了今日沟通次数');
            this.emit('limit', {});
            this.running = false;
            break;
          }

          this.deliveryCount++;
          await this.logStep('✅', `投递成功 (第 ${this.deliveryCount} 次)`, `${job.title} @ ${job.company}`);

          dbAPI.saveJob({ ...job, platform, score: 0 });
          dbAPI.addDelivery(job);

          this.emit('delivered', {
            count: this.deliveryCount,
            max: this.sessionConfig.maxPerSession,
            title: job.title,
            company: job.company,
          });
        } else {
          await this.logStep('⏭️', '无投递按钮，跳过', `${job.title} - 可能已投递或岗位不支持立即沟通`);
          this.emit('no_button', { title: job.title, company: job.company });
        }

        this.currentJob = null;
        this.currentStep = '等待延迟';
        await this.randomDelay(this.sessionConfig.delayBetweenJobs, this.sessionConfig.delayJitter);

      } catch (err) {
        this.emit('error', { message: `处理岗位出错: ${err.message}` });
        await this.randomDelay(2000);
      }
    }

    if (skipped > 0 && processed === 0) return 'ALL_SKIPPED';
    return 'PROCESSED';
  }

  async tryLoadMore(adapter) {
    const name = PLATFORM_NAMES[this.currentPlatform] || this.currentPlatform;
    const currentCount = (await adapter.getJobCards()).length;
    this.emit('status', { message: `正在 ${name} 上滚动加载更多岗位...` });

    for (let i = 0; i < 3; i++) {
      await adapter.page.evaluate(() => {
        window.scrollTo({ top: document.documentElement.scrollHeight, behavior: 'smooth' });
      });
      await this.randomDelay(2000, 1000);

      const newCount = (await adapter.getJobCards()).length;
      if (newCount > currentCount) {
        await this.logStep('📥', `加载到 ${newCount - currentCount} 个新岗位`, `总数: ${currentCount} → ${newCount}`);
        return true;
      }
    }

    return false;
  }

  isBlacklisted(job) {
    return dbAPI.isBlacklisted(job.company);
  }

  randomDelay(base, jitter = 1000) {
    return new Promise(r => setTimeout(r, base + Math.random() * jitter));
  }

  stop() {
    this.running = false;
    this.emit('status', { message: '🛑 用户手动停止' });
  }

  async cleanup() {
    this.stop();
    await BrowserManager.closeAll();
  }

  async chatAutoReply(platform, hrName, companyName, chatId, lastMessage) {
    if (!this.aiClient) { this.initAI(); if (!this.aiClient) return null; }
    const systemPrompt = buildSystemPrompt(config.ai.prompt || '');
    const reply = await this.aiClient.generateReply(lastMessage, systemPrompt);
    if (reply) dbAPI.saveChatRecord(platform, hrName, companyName, chatId, lastMessage, reply, 'ai');
    return reply;
  }

  async analyzeJob(jd) {
    if (!this.aiClient) this.initAI();
    if (!this.aiClient) return null;
    return await this.aiClient.analyzeJob(jd);
  }
}

export default new JobEngine();