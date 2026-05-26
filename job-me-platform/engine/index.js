import config from '../config.js';
import { filterJobs } from '../core/filter.js';
import { scoreJobs, parseSalary, extractExperienceEducation } from '../core/scorer.js';
import { generateGreeting, generateTextResume } from '../core/template.js';
import { createAIClient, buildSystemPrompt } from '../ai/client.js';
import { dbAPI } from '../db/index.js';
import BrowserManager from './browser.js';
import BossAdapter from './adapters/boss.js';
import fs from 'fs';
import path from 'path';

const ADAPTERS = {
  boss: BossAdapter,
};

export class JobEngine {
  constructor() {
    this.running = false;
    this.deliveryCount = 0;
    this.aiClient = null;
    this.currentPlatform = null;
    this.adapters = {};
    this.sessionConfig = {};
    this.onProgress = null;
  }

  setProgressCallback(cb) {
    this.onProgress = cb;
  }

  emitProgress(data) {
    if (this.onProgress) this.onProgress(data);
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

  async startDelivery(platform, filters = {}) {
    this.running = true;
    this.deliveryCount = 0;
    this.currentPlatform = platform;
    this.sessionConfig = { ...config.delivery, ...filters };
    this.initAI();

    const AdapterClass = ADAPTERS[platform];
    if (!AdapterClass) throw new Error(`不支持的平台: ${platform}`);

    this.emitProgress({ type: 'status', message: `正在启动 ${platform} 自动化...` });

    const context = await BrowserManager.createContext(platform, this.getCookiePath(platform));
    const page = context.page;

    const adapter = new AdapterClass();
    adapter.setPage(page);
    this.adapters[platform] = adapter;

    try {
      const loggedIn = await adapter.login();
      if (!loggedIn) {
        await BrowserManager.saveCookies(platform, this.getCookiePath(platform));
      }

      const keywords = filters.includeKeywords || ['财务', '会计'];
      await adapter.navigateToList(keywords, filters.cityCode || '');
      await this.randomDelay(2000);

      while (this.running) {
        if (this.deliveryCount >= this.sessionConfig.maxPerSession) {
          this.emitProgress({ type: 'complete', message: `达到投递上限 (${this.sessionConfig.maxPerSession})` });
          break;
        }

        const result = await this.processJobList(adapter, platform, filters);
        if (result === 'NO_MORE_JOBS') {
          this.emitProgress({ type: 'status', message: '暂无更多岗位，尝试加载...' });
          const loaded = await this.tryLoadMore(adapter);
          if (!loaded) {
            this.emitProgress({ type: 'complete', message: '所有岗位已处理完毕' });
            break;
          }
        }

        await this.randomDelay(1000);
      }
    } catch (err) {
      this.emitProgress({ type: 'error', message: `自动化出错: ${err.message}` });
      console.error('[Engine]', err);
    } finally {
      this.running = false;
      await BrowserManager.saveCookies(platform, this.getCookiePath(platform));
    }
  }

  async processJobList(adapter, platform, filters) {
    const cards = await adapter.getJobCards();
    if (!cards.length) return 'NO_MORE_JOBS';

    for (const card of cards) {
      if (!this.running) break;
      if (this.deliveryCount >= this.sessionConfig.maxPerSession) break;

      try {
        const parsed = await adapter.parseJobCard(card);
        if (!parsed || !parsed.title) continue;

        if (this.isBlacklisted(parsed)) {
          this.emitProgress({ type: 'skip', message: `跳过黑名单: ${parsed.company}`, job: parsed });
          continue;
        }

        const filtered = filterJobs([parsed], filters);
        if (!filtered.length) {
          this.emitProgress({ type: 'skip', message: `跳过(不匹配): ${parsed.title}`, job: parsed });
          continue;
        }

        const job = filtered[0];
        this.emitProgress({ type: 'processing', message: `正在投递: ${job.title} @ ${job.company}`, job });

        await adapter.focusJobCard?.(card);
        await this.randomDelay(1000, 500);

        const clicked = await adapter.clickCommunicate(card);
        if (clicked) {
          const modalResult = await adapter.handleModal();
          if (modalResult === 'LIMIT_REACHED') {
            this.emitProgress({ type: 'limit', message: '今日沟通已达上限' });
            this.running = false;
            break;
          }

          this.deliveryCount++;
          this.emitProgress({ type: 'delivered', message: `✅ 已投递 (${this.deliveryCount}): ${job.title}`, job, count: this.deliveryCount });

          dbAPI.saveJob({ ...job, platform, score: 0 });
          dbAPI.addDelivery(job);
        } else {
          this.emitProgress({ type: 'no_button', message: `无投递按钮: ${job.title}`, job });
        }

        await this.randomDelay(this.sessionConfig.delayBetweenJobs, this.sessionConfig.delayJitter);
      } catch (err) {
        console.error('[Engine] 处理卡片失败:', err.message);
      }
    }

    return 'PROCESSED';
  }

  async tryLoadMore(adapter) {
    const currentCount = (await adapter.getJobCards()).length;

    for (let i = 0; i < 3; i++) {
      await adapter.page.evaluate(() => {
        window.scrollTo({ top: document.documentElement.scrollHeight, behavior: 'smooth' });
      });
      await this.randomDelay(2000, 1000);

      const newCount = (await adapter.getJobCards()).length;
      if (newCount > currentCount) {
        this.emitProgress({ type: 'status', message: `加载到更多岗位: ${currentCount} → ${newCount}` });
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
    this.emitProgress({ type: 'status', message: '正在停止...' });
  }

  async cleanup() {
    this.stop();
    await BrowserManager.closeAll();
  }

  async chatAutoReply(platform, hrName, companyName, chatId, lastMessage) {
    if (!this.aiClient) {
      this.initAI();
      if (!this.aiClient) return null;
    }

    const systemPrompt = buildSystemPrompt(config.ai.prompt || '');
    const reply = await this.aiClient.generateReply(lastMessage, systemPrompt);

    if (reply) {
      dbAPI.saveChatRecord(platform, hrName, companyName, chatId, lastMessage, reply, 'ai');
    }

    return reply;
  }

  async analyzeJob(jd) {
    if (!this.aiClient) this.initAI();
    if (!this.aiClient) return null;
    return await this.aiClient.analyzeJob(jd);
  }
}

export default new JobEngine();