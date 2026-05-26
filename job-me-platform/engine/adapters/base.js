module.exports = class BaseAdapter {
  constructor(name) {
    this.name = name;
    this.bot = null;
  }

  setBot(bot) {
    this.bot = bot;
  }

  async login(bot) { throw new Error('login() 必须由子类实现'); }
  async search(bot, keywords, cityCode) { throw new Error('search() 必须由子类实现'); }
  async getCards(bot) { throw new Error('getCards() 必须由子类实现'); }
  async parseCard(bot, card) { throw new Error('parseCard() 必须由子类实现'); }
  async communicate(bot, card) { throw new Error('communicate() 必须由子类实现'); }
};