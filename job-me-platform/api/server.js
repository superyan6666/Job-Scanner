import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import config from '../config.js';
import engine from '../engine/index.js';
import { dbAPI, initDB } from '../db/index.js';
import { scoreJobs } from '../core/scorer.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export function createServer() {
  const app = express();
  app.use(cors());
  app.use(express.json());

  const webPath = path.join(__dirname, '..', 'web');
  app.use(express.static(webPath));

  app.get('/api/status', (req, res) => {
    res.json({
      running: engine.running,
      platform: engine.currentPlatform,
      deliveryCount: engine.deliveryCount,
      maxDelivery: engine.sessionConfig.maxPerSession || config.delivery.maxPerSession,
    });
  });

  app.get('/api/stats', (req, res) => {
    res.json(dbAPI.getDeliveryStats());
  });

  app.post('/api/start', (req, res) => {
    if (engine.running) {
      return res.status(400).json({ error: '引擎已在运行中' });
    }

    const { platform = 'boss', filters = {} } = req.body;

    engine.setProgressCallback((data) => {
      console.log('[Progress]', data.message);
    });

    engine.startDelivery(platform, {
      ...filters,
      maxPerSession: filters.maxPerSession || config.delivery.maxPerSession,
    }).catch(err => {
      console.error('[API] 启动失败:', err);
    });

    res.json({ success: true, message: '自动化已启动' });
  });

  app.post('/api/stop', (req, res) => {
    engine.stop();
    res.json({ success: true, message: '正在停止...' });
  });

  app.get('/api/deliveries', (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    res.json(dbAPI.getRecentDeliveries(limit));
  });

  app.get('/api/jobs', (req, res) => {
    const limit = parseInt(req.query.limit) || 50;
    res.json(dbAPI.getRecentJobs(limit));
  });

  app.get('/api/ai/test', async (req, res) => {
    if (!config.ai.apiKey) {
      return res.status(400).json({ error: '未配置 AI API Key，请检查 .env 文件' });
    }
    try {
      const { createAIClient } = await import('../ai/client.js');
      const client = createAIClient(config.ai);
      const reply = await client.chat([
        { role: 'user', content: '用一句话介绍你自己（测试用，请简短回复）' },
      ], { maxTokens: 100 });
      res.json({ success: true, reply, provider: config.ai.provider });
    } catch (err) {
      res.status(500).json({ error: `AI 连接失败: ${err.message}` });
    }
  });

  app.post('/api/ai/reply', async (req, res) => {
    const { message, resumeText } = req.body;
    if (!message) return res.status(400).json({ error: '缺少消息内容' });

    try {
      const { createAIClient, buildSystemPrompt } = await import('../ai/client.js');
      const client = createAIClient(config.ai);
      const systemPrompt = buildSystemPrompt(resumeText || '');
      const reply = await client.generateReply(message, systemPrompt);
      res.json({ success: true, reply });
    } catch (err) {
      res.status(500).json({ error: `AI 回复失败: ${err.message}` });
    }
  });

  app.post('/api/blacklist', (req, res) => {
    const { company, reason } = req.body;
    if (!company) return res.status(400).json({ error: '缺少公司名' });
    dbAPI.addBlacklist(company, reason);
    res.json({ success: true });
  });

  app.get('/api/blacklist', (req, res) => {
    const db = initDB();
    const list = db.prepare('SELECT * FROM blacklist ORDER BY created_at DESC').all();
    res.json(list);
  });

  app.delete('/api/blacklist/:id', (req, res) => {
    const db = initDB();
    db.prepare('DELETE FROM blacklist WHERE id = ?').run(req.params.id);
    res.json({ success: true });
  });

  app.post('/api/settings', (req, res) => {
    const { key, value } = req.body;
    if (!key) return res.status(400).json({ error: '缺少 key' });
    dbAPI.setSetting(key, value);
    res.json({ success: true });
  });

  app.get('/api/settings/:key', (req, res) => {
    res.json({ value: dbAPI.getSetting(req.params.key) });
  });

  app.get('*', (req, res) => {
    res.sendFile(path.join(webPath, 'index.html'));
  });

  return app;
}

export function startServer(port = config.port) {
  const app = createServer();
  return new Promise((resolve) => {
    app.listen(port, config.host, () => {
      console.log(`\n========================================`);
      console.log(`  🌐 Job Me 平台已启动`);
      console.log(`  📡 控制面板: http://localhost:${port}`);
      console.log(`  🔌 API 地址:  http://localhost:${port}/api`);
      console.log(`========================================\n`);
      resolve(app);
    });
  });
}