const { execSync } = require('child_process');
const os = require('os');

console.log('🔄 正在安装自动化引擎浏览器...\n');

// 安装 Playwright 浏览器
console.log('[1/2] 安装 Playwright Chromium...');
try {
  execSync('npx playwright install chromium', { stdio: 'inherit' });
  console.log('✅ Playwright Chromium 安装完成');
} catch (e) {
  console.log('⚠️  Playwright 安装失败，可手动运行: npx playwright install chromium');
}

console.log('');
console.log('✅ 安装完成！');
console.log('');
console.log('运行: npm start');