const BaseAdapter = require('./base.js');

module.exports = class BossAdapter extends BaseAdapter {
  constructor() {
    super('boss');
  }

  async login(bot) {
    await bot.navigate('https://www.zhipin.com/web/geek/job');
    const url = await bot.url();

    if (url.includes('passport')) {
      await bot.waitForUrl('web/geek/');
      return false;
    }
    return true;
  }

  async search(bot, keywords, cityCode) {
    const query = encodeURIComponent(keywords.join(' '));
    const city = cityCode ? `&city=${cityCode}` : '';
    await bot.navigate(`https://www.zhipin.com/web/geek/job?query=${query}${city}`);
  }

  async getCards(bot) {
    try {
      await bot.waitForSelector('li.job-card-box', 8000);
    } catch {
      return [];
    }
    return await bot.$$('li.job-card-box');
  }

  async parseCard(bot, card) {
    return await bot.evaluateHandle('li.job-card-box', (el) => {
      const getText = (sel) => el.querySelector(sel)?.textContent?.trim() || '';
      const title = getText('.job-name');
      const company = getText('.company-name') || getText('.company-text');
      const salary = getText('.salary');
      const location = getText('.job-area') || getText('.job-address-desc');
      const tags = Array.from(el.querySelectorAll('.job-tags li')).map(t => t.textContent.trim());
      return { id: el.getAttribute('data-jobid') || '', title, company, salary, location, platform: 'boss', tags, cardText: el.textContent || '' };
    });
  }

  async communicate(bot, card) {
    const btn = await card.$('a.op-btn-chat');
    if (!btn) return false;
    await btn.click();
    return true;
  }

  async focusCard(bot, card) {
    // 在 Playwright 中自动聚焦，无需额外操作
  }
};