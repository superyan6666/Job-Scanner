const { createAutomation } = require('./automation.js');
const BossAdapter = require('./adapters/boss.js');
const { filterJobs } = require('../core/filter.js');
const store = require('../store/index.js');

const ADAPTERS = { boss: BossAdapter };
const PLATFORM_NAMES = {
  boss: 'BOSS直聘', zhilian: '智联招聘', job51: '前程无忧', liepin: '猎聘',
};

class JobEngine {
  constructor() {
    this.running = false;
    this.bot = null;
    this.adapter = null;
    this.deliveryCount = 0;
    this.maxDelivery = 50;
    this.currentStep = '';
    this.currentPlatform = '';
    this._mainWindow = null;

    this._onScreenshot = null;
    this._onLog = null;
    this._onStatus = null;
  }

  setMainWindow(win) {
    this._mainWindow = win;
  }

  emit(type, data) {
    const payload = { type, time: Date.now(), ...data, deliveryCount: this.deliveryCount };
    if (this._mainWindow && !this._mainWindow.isDestroyed()) {
      this._mainWindow.webContents.send('engine:event', payload);
    }
  }

  async logStep(emoji, title, detail = '') {
    const entry = { time: Date.now(), emoji, title, detail, deliveryCount: this.deliveryCount };
    if (this._mainWindow && !this._mainWindow.isDestroyed()) {
      this._mainWindow.webContents.send('engine:log', entry);
    }
    this.currentStep = title;
    this.emit('statusUpdate', {
      running: this.running,
      step: this.currentStep,
      deliveryCount: this.deliveryCount,
      maxDelivery: this.maxDelivery,
    });
  }

  async takeScreenshot() {
    if (!this.bot) return;
    try {
      const buf = await this.bot.screenshot();
      const b64 = buf.toString('base64');
      if (this._mainWindow && !this._mainWindow.isDestroyed()) {
        this._mainWindow.webContents.send('engine:screenshot', b64);
      }
    } catch {}
  }

  randomDelay(base, jitter = 1500) {
    return new Promise(r => setTimeout(r, base + Math.random() * jitter));
  }

  async startDelivery(config) {
    const {
      platform = 'boss',
      strategy = 'playwright',
      keywords = ['财务', '会计'],
      excludeKeywords = ['外包', '培训'],
      cityCode = '',
      headless = true,
      proxy = '',
      maxPerSession = 50,
    } = config;

    this.running = true;
    this.deliveryCount = 0;
    this.maxDelivery = maxPerSession;
    this.currentPlatform = platform;
    this.currentStep = '启动';

    const AdapterClass = ADAPTERS[platform];
    if (!AdapterClass) {
      await this.logStep('❌', `不支持的平台: ${platform}`);
      this.running = false;
      return;
    }

    this.emit('start', { platform, platformName: PLATFORM_NAMES[platform] || platform });

    try {
      // 1. 启动浏览器
      await this.logStep('🌐', '启动浏览器', `引擎: ${strategy}`);
      const cookies = store.loadCookies(platform);
      this.bot = await createAutomation(strategy, { headless, proxy, cookies });

      this.adapter = new AdapterClass();
      this.adapter.setBot(this.bot);
      await this.takeScreenshot();

      // 2. 登录
      await this.logStep('🔑', '登录平台', PLATFORM_NAMES[platform]);
      const loggedIn = await this.adapter.login(this.bot);
      if (!loggedIn) {
        store.saveCookies(platform, await this.bot.cookies());
      }
      await this.takeScreenshot();

      // 3. 搜索岗位
      await this.logStep('🔍', '搜索岗位', `关键词: ${keywords.join(', ')}`);
      await this.adapter.search(this.bot, keywords, cityCode);
      await this.takeScreenshot();
      await this.bot.waitForTimeout(2000);

      // 4. 循环投递
      let emptyRounds = 0;
      while (this.running && emptyRounds < 5) {
        if (this.deliveryCount >= this.maxDelivery) {
          await this.logStep('🎯', '达到投递上限', `已完成 ${this.deliveryCount}/${this.maxDelivery}`);
          break;
        }

        const result = await this.processJobs(config);

        if (result === 'EMPTY' || result === 'ALL_SKIPPED') {
          emptyRounds++;
          await this.logStep('📭', `暂无更多岗位 (第${emptyRounds}轮)`, '尝试滚动加载...');
          await this.bot.scrollToBottom();
          await this.bot.waitForTimeout(1500);
        } else {
          emptyRounds = 0;
        }
      }

      await this.logStep('🏁', '自动化完成', `共投递 ${this.deliveryCount} 个岗位`);
      this.emit('complete', { deliveryCount: this.deliveryCount });

    } catch (err) {
      await this.logStep('❌', '自动化出错', err.message);
    } finally {
      this.running = false;
      await this.takeScreenshot();
      if (this.bot) await this.bot.close();
      this.emit('stop', {});
    }
  }

  async processJobs(config) {
    const { excludeKeywords = [], includeKeywords = [] } = config;
    const cards = await this.adapter.getCards(this.bot);
    if (!cards || !cards.length) return 'EMPTY';

    let processed = 0;
    for (const card of cards) {
      if (!this.running || this.deliveryCount >= this.maxDelivery) break;

      const job = await this.adapter.parseCard(this.bot, card);
      if (!job || !job.title) continue;

      this.emit('scan', job);

      const filtered = filterJobs([job], {
        includeKeywords,
        excludeKeywords,
        excludeHeadhunters: true,
      });
      if (!filtered.length) {
        await this.logStep('⏭️', `跳过: ${job.title}`, `${job.company} | 不匹配过滤条件`);
        continue;
      }

      const match = filtered[0];
      await this.logStep('🎯', `正在投递: ${match.title}`, `${match.company} | ${match.salary || '薪资面议'}`);

      const ok = await this.adapter.communicate(this.bot, card);
      if (ok) {
        this.deliveryCount++;
        await this.logStep('✅', `投递成功 #${this.deliveryCount}`, `${match.title} @ ${match.company}`);
        store.addDelivery(match);
        this.emit('delivered', { count: this.deliveryCount, title: match.title, company: match.company });
      } else {
        const btnText = await card.evaluate(el => el.textContent).catch(() => '');
        if (btnText.includes('上限')) {
          await this.logStep('⛔', '今日沟通已达上限！', '平台限制了沟通次数');
          this.running = false;
          break;
        }
        await this.logStep('⏭️', '无法投递', `${match.title} - 可能已投递过`);
      }

      await this.randomDelay(3000, 2000);
      await this.takeScreenshot();
      processed++;
    }

    return processed === 0 ? 'ALL_SKIPPED' : 'OK';
  }

  stop() {
    this.running = false;
    if (this._mainWindow && !this._mainWindow.isDestroyed()) {
      this._mainWindow.webContents.send('engine:statusUpdate', { running: false, step: '已停止', deliveryCount: this.deliveryCount });
    }
  }
}

module.exports = { JobEngine };