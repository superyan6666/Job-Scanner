import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import config from '../config.js';
import engine from '../engine/index.js';
import { dbAPI } from '../db/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export function createServer() {
  const app = express();
  app.use(cors());
  app.use(express.json());
  app.use(express.static(path.join(__dirname, '..', 'web')));

  // ── SSE 实时推送 ──
  app.get('/api/events', (req, res) => {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });

    const sendEvent = (type, data) => {
      res.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`);
    };

    sendEvent('connected', { message: 'SSE 连接已建立' });

    const unsubscribe = engine.onProgress((payload) => {
      try { sendEvent(payload.type, payload); } catch {}
    });

    const keepAlive = setInterval(() => {
      try { res.write(': keepalive\n\n'); } catch { clearInterval(keepAlive); }
    }, 15000);

    req.on('close', () => {
      unsubscribe();
      clearInterval(keepAlive);
    });
  });

  // ── 状态查询 ──
  app.get('/api/status', (req, res) => {
    res.json({
      running: engine.running,
      platform: engine.currentPlatform,
      deliveryCount: engine.deliveryCount,
      maxDelivery: engine.sessionConfig.maxPerSession || config.delivery.maxPerSession,
      currentStep: engine.currentStep,
      currentJob: engine.currentJob,
      screenshot: engine.lastScreenshot || null,
    });
  });

  app.get('/api/stats', (req, res) => {
    res.json(dbAPI.getDeliveryStats());
  });

  // ── 控制 ──
  app.post('/api/start', (req, res) => {
    if (engine.running) {
      return res.status(400).json({ error: '引擎已在运行中', running: true });
    }
    const { platform = 'boss', filters = {} } = req.body;
    engine.startDelivery(platform, {
      ...filters,
      maxPerSession: filters.maxPerSession || config.delivery.maxPerSession,
    }).catch(err => console.error('[API] 启动失败:', err));

    res.json({
      success: true,
      message: `🚀 ${platform} 自动化已启动`,
      platform,
      maxDelivery: filters.maxPerSession || config.delivery.maxPerSession,
    });
  });

  app.post('/api/stop', (req, res) => {
    engine.stop();
    res.json({ success: true, message: '🛑 正在停止...' });
  });

  // ── 数据查询 ──
  app.get('/api/deliveries', (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    res.json(dbAPI.getRecentDeliveries(limit));
  });

  app.get('/api/jobs', (req, res) => {
    const limit = parseInt(req.query.limit) || 50;
    res.json(dbAPI.getRecentJobs(limit));
  });

  // ── AI ──
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

  app.get('/api/ai/test', async (req, res) => {
    if (!config.ai.apiKey) {
      return res.status(400).json({ error: '未配置 AI API Key，请在 .env 文件中填写', keyMissing: true });
    }
    try {
      const { createAIClient } = await import('../ai/client.js');
      const client = createAIClient(config.ai);
      const reply = await client.chat([
        { role: 'user', content: '用一句话回复：你好，我是HR，请问你对我们公司的职位感兴趣吗？' },
      ], { maxTokens: 100 });
      res.json({ success: true, reply, provider: config.ai.provider });
    } catch (err) {
      res.status(500).json({ error: `AI 连接失败: ${err.message}` });
    }
  });

  // ── 黑名单管理 ──
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
    initDB().prepare('DELETE FROM blacklist WHERE id = ?').run(req.params.id);
    res.json({ success: true });
  });

  // ── Catch-all ──
  app.get('/api/*', (req, res) => {
    res.status(404).json({ error: '未知接口' });
  });

  app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'web', 'index.html'));
  });

  return app;
}

export function startServer(port = config.port) {
  const app = createServer();
  return new Promise((resolve) => {
    app.listen(port, config.host, () => {
      console.log(`\n========================================`);
      console.log(`  🚀 Job Me 平台已启动`);
      console.log(`  📡 控制面板: http://localhost:${port}`);
      console.log(`  🔌 API:      http://localhost:${port}/api/events（SSE实时推送）`);
      console.log(`========================================`);
      console.log(`  ⚡ 使用说明:`);
      console.log(`  1. 打开浏览器访问 http://localhost:${port}`);
      console.log(`  2. 选择招聘平台，配置关键词`);
      console.log(`  3. 点击「启动自动化」`);
      console.log(`  4. 观察实时截图和步骤日志`);
      console.log(`========================================\n`);
      resolve(app);
    });
  });
}