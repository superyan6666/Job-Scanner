import PlatformAdapter from './base.js';

export default class BossAdapter extends PlatformAdapter {
  constructor() {
    super('boss');
  }

  async login() {
    await this.page.goto('https://www.zhipin.com/web/geek/job', { waitUntil: 'networkidle' });
    const currentUrl = this.page.url();

    if (currentUrl.includes('passport')) {
      console.log('[BOSS] 需要登录，请在浏览器中手动扫码...');
      await this.page.waitForURL('**/web/geek/**', { timeout: 120000 });
      console.log('[BOSS] 登录成功');
      return false;
    }
    console.log('[BOSS] 已登录状态');
    return true;
  }

  async navigateToList(keywords, cityCode = '') {
    const query = encodeURIComponent(keywords.join(' '));
    const city = cityCode ? `&city=${cityCode}` : '';
    const url = `https://www.zhipin.com/web/geek/job?query=${query}${city}`;

    await this.page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });
    await this.randomDelay(2000, 1000);
  }

  async getJobCards() {
    try {
      await this.page.waitForSelector('li.job-card-box', { timeout: 8000 });
    } catch {
      return [];
    }
    return await this.page.$$('li.job-card-box');
  }

  async parseJobCard(card) {
    return await card.evaluate(el => {
      const getText = (sel) => el.querySelector(sel)?.textContent?.trim() || '';

      const title = getText('.job-name');
      const company = getText('.company-name') || getText('.company-text');
      const salary = getText('.salary');
      const location = getText('.job-area') || getText('.job-address-desc');
      const welfare = getText('.info-desc');
      const hrInfo = getText('.info-public');
      const jobId = el.getAttribute('data-jobid') || '';

      const btnText = el.querySelector('button, .op-btn-chat, a.op-btn-chat')?.textContent?.trim() || '';

      const tags = Array.from(el.querySelectorAll('.job-tags li')).map(t => t.textContent.trim());

      const isHeadhunter = (el.querySelector('.job-tag-icon')?.alt || '').includes('猎头');

      return {
        id: jobId,
        title,
        company,
        salary,
        salaryParsed: parseSalary(salary),
        location,
        welfare,
        hrInfo,
        btnText,
        tags,
        isHeadhunter,
        cardText: el.textContent || '',
      };

      function parseSalary(str) {
        if (!str) return { min: 0, max: 0, avg: 0 };
        let bonusMonth = 12;
        let mainPart = str;

        if (str.includes('·')) {
          const parts = str.split('·');
          mainPart = parts[0].trim();
          const bm = parts[1]?.match(/(\d+)\s*薪/);
          if (bm) bonusMonth = parseInt(bm[1]);
        }

        const rm = mainPart.match(/^(\d+\.?\d*)\s*[-~至]\s*(\d+\.?\d*)\s*([Kk万])/);
        if (rm) {
          const m = rm[3].toLowerCase() === 'k' ? 1000 : 10000;
          return {
            min: parseFloat(rm[1]) * m,
            max: parseFloat(rm[2]) * m,
            avg: Math.round((parseFloat(rm[1]) + parseFloat(rm[2])) / 2 * m * bonusMonth / 12),
          };
        }
        return { min: 0, max: 0, avg: 0 };
      }
    });
  }

  async clickCommunicate(card) {
    const btn = await card.$('a.op-btn-chat');
    if (!btn) return false;

    const text = await btn.textContent();
    if (!text.trim().includes('立即沟通')) return false;

    await btn.click();
    await this.randomDelay(2000, 1500);
    return true;
  }

  async handleModal() {
    try {
      const cancelBtn = await this.page.locator('.default-btn.cancel-btn', { hasText: '留在此页' });
      if (await cancelBtn.isVisible({ timeout: 2000 })) {
        await cancelBtn.click();
        await this.randomDelay(1500, 500);
      }

      const bodyText = await this.page.textContent('body');
      if (bodyText.includes('沟通人数已达上限') || bodyText.includes('今日沟通已达上限')) {
        console.log('[BOSS] ⛔ 今日沟通已达上限');
        return 'LIMIT_REACHED';
      }
      return true;
    } catch {
      return true;
    }
  }

  async sendGreeting() {
    try {
      const btnDict = await this.page.$('.btn-dict');
      if (!btnDict) return false;

      await btnDict.click();
      await this.randomDelay(500, 300);

      const items = await this.page.$$('ul[data-v-f115c50c=""] li');
      for (const item of items) {
        await item.click();
        await this.randomDelay(300, 200);
      }
      return true;
    } catch {
      return false;
    }
  }

  async sendMessage(text) {
    try {
      const input = await this.page.$('#chat-input');
      if (!input) return false;

      await input.fill(text);
      await this.randomDelay(300, 200);

      const sendBtn = await this.page.$('.btn-send');
      if (sendBtn) {
        await sendBtn.click();
      } else {
        await input.press('Enter');
      }
      return true;
    } catch {
      return false;
    }
  }

  async getLastMessage() {
    try {
      const messages = await this.page.$$('.chat-message .im-list li.message-item.item-friend');
      if (!messages.length) return null;
      const last = messages[messages.length - 1];
      const text = await last.$eval('.text span', el => el.textContent.trim());
      return text;
    } catch {
      return null;
    }
  }

  async hasOnlySelfMessage() {
    const friends = await this.page.$$('.chat-message .im-list li.message-item.item-friend');
    return friends.length === 0;
  }

  async getCurrentChatInfo() {
    try {
      const nameText = await this.page.$eval('.name-text', el => el.textContent.trim());
      const companyText = await this.page.$eval('.company-text', el => el.textContent.trim());
      const cleanName = nameText.replace(/刚刚活跃|今日活跃|本月活跃|本周活跃|3日内活跃|在线/g, '').trim();
      return { hrName: cleanName, companyName: companyText };
    } catch {
      return { hrName: '未知', companyName: '未知' };
    }
  }

  async getChatList() {
    return await this.page.$$('ul[role="group"] li[role="listitem"]:has(.friend-content-warp)');
  }

  async openChat(chatItem) {
    const figure = await chatItem.$('.figure');
    if (!figure) return false;
    await figure.click();
    await this.randomDelay(1500, 1000);
    return true;
  }

  async hasCardMessage() {
    const messages = await this.page.$$('.chat-message .im-list li.message-item.item-friend');
    if (!messages.length) return false;
    const last = messages[messages.length - 1];
    return await last.$('.message-card-wrap') !== null;
  }

  async acceptCardMessage() {
    const messages = await this.page.$$('.chat-message .im-list li.message-item.item-friend');
    if (!messages.length) return false;
    const last = messages[messages.length - 1];
    const btn = await last.$('.card-btn, .btn-agree');
    if (!btn) return false;
    const text = await btn.textContent();
    if (text.trim() === '同意' || text.includes('交换')) {
      await btn.click();
      return true;
    }
    return false;
  }
}