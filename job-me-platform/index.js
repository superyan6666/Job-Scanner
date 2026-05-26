import dotenv from 'dotenv';
dotenv.config();

import { initDB } from './db/index.js';
import { startServer } from './api/server.js';
import { fileURLToPath } from 'url';
import path from 'path';
import fs from 'fs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function main() {
  console.log(`
  ╔═══════════════════════════════════════════╗
  ║     🚀 Job Me Platform v1.0              ║
  ║     智能简历投放 · 前后端完整系统          ║
  ╚═══════════════════════════════════════════╝
  `);

  const dataDir = path.resolve('./data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  initDB();

  const app = await startServer();

  process.on('SIGINT', async () => {
    console.log('\n正在关闭服务...');
    const { default: engine } = await import('./engine/index.js');
    await engine.cleanup();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    const { default: engine } = await import('./engine/index.js');
    await engine.cleanup();
    process.exit(0);
  });
}

main().catch(err => {
  console.error('启动失败:', err);
  process.exit(1);
});