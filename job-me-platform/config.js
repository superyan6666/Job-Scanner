import dotenv from 'dotenv';
dotenv.config();

export default {
  port: parseInt(process.env.PORT || '3000'),
  host: process.env.HOST || '0.0.0.0',

  browser: {
    headless: process.env.BROWSER_HEADLESS !== 'false',
    viewport: { width: 1920, height: 1080 },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    timeout: 30000,
  },

  ai: {
    provider: process.env.AI_PROVIDER || 'deepseek',
    endpoint: process.env.AI_ENDPOINT || 'https://api.deepseek.com/v1/chat/completions',
    apiKey: process.env.AI_API_KEY || '',
    model: process.env.AI_MODEL || 'deepseek-chat',
    temperature: 0.7,
  },

  delivery: {
    maxPerSession: parseInt(process.env.MAX_DELIVERY || '50'),
    delayBetweenJobs: parseInt(process.env.DELAY_JOBS || '4000'),
    delayJitter: parseInt(process.env.DELAY_JITTER || '2000'),
  },

  db: {
    path: process.env.DB_PATH || './data/jobme.db',
  },

  platforms: {
    boss: { enabled: true },
    zhilian: { enabled: true },
    job51: { enabled: true },
    liepin: { enabled: true },
  },
};