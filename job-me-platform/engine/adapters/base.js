export default class PlatformAdapter {
  constructor(name) {
    this.name = name;
    this.page = null;
  }

  setPage(page) {
    this.page = page;
  }

  async login() {
    throw new Error('login() 必须由子类实现');
  }

  async navigateToList(keywords, city) {
    throw new Error('navigateToList() 必须由子类实现');
  }

  async getJobCards() {
    throw new Error('getJobCards() 必须由子类实现');
  }

  async parseJobCard(card) {
    throw new Error('parseJobCard() 必须由子类实现');
  }

  async clickCommunicate(card) {
    throw new Error('clickCommunicate() 必须由子类实现');
  }

  async sendGreeting(page) {
    throw new Error('sendGreeting() 必须由子类实现');
  }

  async sendMessage(text) {
    throw new Error('sendMessage() 必须由子类实现');
  }

  async getLastMessage() {
    throw new Error('getLastMessage() 必须由子类实现');
  }

  async handleModal() {
    return true;
  }

  randomDelay(base, jitter = 1000) {
    return new Promise(r => setTimeout(r, base + Math.random() * jitter));
  }
}