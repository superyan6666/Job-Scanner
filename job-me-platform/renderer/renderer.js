// ─── DOM refs ───
const $ = id => document.getElementById(id);
const dom = {
  engineStatus: $('engineStatus'),
  screenshotImg: $('screenshotImg'),
  screenshotPlaceholder: $('screenshotPlaceholder'),
  screenshotStatus: $('screenshotStatus'),
  logContainer: $('logContainer'),
  deliveryCount: $('deliveryCount'),
  deliveryList: $('deliveryList'),
  footerEngine: $('footerEngine'),
  footerStep: $('footerStep'),
  footerCount: $('footerCount'),
  footerMax: $('footerMax'),

  btnStart: $('btnStart'),
  btnStop: $('btnStop'),
  btnClear: $('btnClearDeliveries'),

  selStrategy: $('selStrategy'),
  selHeadless: $('selHeadless'),
  inpProxy: $('inpProxy'),
  inpKeywords: $('inpKeywords'),
  inpExclude: $('inpExclude'),
  inpCity: $('inpCity'),
  inpMax: $('inpMax'),
  inpAiKey: $('inpAiKey'),
  inpAiEndpoint: $('inpAiEndpoint'),
  inpAiModel: $('inpAiModel'),

  pbtns: document.querySelectorAll('.pbtn'),
};

let selectedPlatform = 'boss';

// ─── 平台选择 ───
dom.pbtns.forEach(btn => {
  btn.addEventListener('click', () => {
    dom.pbtns.forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    selectedPlatform = btn.dataset.platform;
  });
});

// ─── IPC 监听 ───

// 截图
window.api.engine.onScreenshot((base64) => {
  dom.screenshotImg.src = 'data:image/jpeg;base64,' + base64;
  dom.screenshotImg.style.display = 'block';
  dom.screenshotPlaceholder.style.display = 'none';
  dom.screenshotStatus.textContent = '📸 实时截图';
});

// 日志
window.api.engine.onLog((entry) => {
  const placeholder = dom.logContainer.querySelector('.log-placeholder');
  if (placeholder) placeholder.remove();

  const time = new Date(entry.time).toLocaleTimeString();
  const item = document.createElement('div');
  item.className = 'log-item';
  item.innerHTML = `
    <span class="log-emoji">${entry.emoji || '•'}</span>
    <div class="log-body">
      <div class="log-title">${entry.title}</div>
      ${entry.detail ? `<div class="log-detail">${entry.detail}</div>` : ''}
    </div>
    <span class="log-time">${time}</span>
  `;
  dom.logContainer.appendChild(item);
  dom.logContainer.scrollTop = dom.logContainer.scrollHeight;
  dom.footerStep.textContent = entry.title;

  if (entry.deliveryCount > 0) {
    dom.deliveryCount.textContent = `已投递: ${entry.deliveryCount}`;
    dom.footerCount.textContent = entry.deliveryCount;
  }
});

// 状态
window.api.engine.onStatusUpdate((status) => {
  if (status.running) {
    dom.engineStatus.textContent = '▶️ 运行中';
    dom.engineStatus.className = 'status-badge running';
    dom.btnStart.disabled = true;
    dom.btnStop.disabled = false;
  } else {
    dom.engineStatus.textContent = '⏸️ 待命';
    dom.engineStatus.className = 'status-badge';
    dom.btnStart.disabled = false;
    dom.btnStop.disabled = true;
  }

  dom.footerStep.textContent = status.step || '—';
  dom.footerCount.textContent = status.deliveryCount || 0;

  if (status.step?.includes('完成')) {
    dom.engineStatus.textContent = '✅ 已完成';
    dom.engineStatus.className = 'status-badge done';
  }
  if (status.step?.includes('出错') || status.step?.includes('错误')) {
    dom.engineStatus.textContent = '❌ 错误';
    dom.engineStatus.className = 'status-badge error';
  }
  if (status.step?.includes('上限')) {
    dom.engineStatus.textContent = '⛔ 达上限';
    dom.engineStatus.className = 'status-badge error';
  }
});

// 通用事件
window.api.engine.onEvent = (event) => {
  // 处理投递完成
  if (event.type === 'delivered' && event.count) {
    addDeliveryItem(event.count, event.title, event.company);
  }
  if (event.type === 'complete') {
    dom.engineStatus.textContent = '✅ 已完成';
    dom.engineStatus.className = 'status-badge done';
    dom.btnStart.disabled = false;
    dom.btnStop.disabled = true;
  }
};

// ─── 投递记录 ───
function addDeliveryItem(index, title, company) {
  const placeholder = dom.deliveryList.querySelector('.dl-placeholder');
  if (placeholder) placeholder.remove();

  const item = document.createElement('div');
  item.className = 'dl-item';
  item.innerHTML = `
    <span class="idx">#${index}</span>
    <span class="title">${title || '-'}</span>
    <span class="company">${company || '-'}</span>
  `;
  dom.deliveryList.insertBefore(item, dom.deliveryList.firstChild);
  if (dom.deliveryList.children.length > 100) {
    dom.deliveryList.removeChild(dom.deliveryList.lastChild);
  }
}

// ─── 启动 ───
dom.btnStart.addEventListener('click', () => {
  const config = {
    platform: selectedPlatform,
    strategy: dom.selStrategy.value,
    headless: dom.selHeadless.value === 'true',
    proxy: dom.inpProxy.value.trim(),
    keywords: dom.inpKeywords.value.split(/[,，]/).map(s => s.trim()).filter(Boolean),
    excludeKeywords: dom.inpExclude.value.split(/[,，]/).map(s => s.trim()).filter(Boolean),
    cityCode: dom.inpCity.value.trim(),
    maxPerSession: parseInt(dom.inpMax.value) || 50,
  };

  // 保存配置
  window.api.config.save(config);

  // 清空日志
  dom.logContainer.innerHTML = '<div class="log-placeholder">启动中...</div>';
  dom.deliveryList.innerHTML = '<div class="dl-placeholder">暂无记录</div>';
  dom.deliveryCount.textContent = '已投递: 0';
  dom.footerEngine.textContent = config.strategy;
  dom.footerMax.textContent = config.maxPerSession;
  dom.footerCount.textContent = '0';
  dom.screenshotImg.style.display = 'none';
  dom.screenshotPlaceholder.style.display = 'flex';

  // 启动引擎
  window.api.engine.start(config);
});

// ─── 停止 ───
dom.btnStop.addEventListener('click', () => {
  window.api.engine.stop();
});

// ─── 清空投递记录 ───
dom.btnClear.addEventListener('click', () => {
  dom.deliveryList.innerHTML = '<div class="dl-placeholder">暂无记录</div>';
});

// ─── 截图点击放大 ───
dom.screenshotImg.addEventListener('click', () => {
  if (dom.screenshotImg.src) {
    window.open(dom.screenshotImg.src);
  }
});

// ─── 加载保存的配置 ───
async function loadConfig() {
  const cfg = await window.api.config.load();
  if (!cfg) return;
  if (cfg.keywords) dom.inpKeywords.value = cfg.keywords.join(',');
  if (cfg.excludeKeywords) dom.inpExclude.value = cfg.excludeKeywords.join(',');
  if (cfg.cityCode) dom.inpCity.value = cfg.cityCode;
  if (cfg.maxPerSession) dom.inpMax.value = cfg.maxPerSession;
  if (cfg.strategy) dom.selStrategy.value = cfg.strategy;
  if (cfg.proxy) dom.inpProxy.value = cfg.proxy;
}

// ─── 初始化 ───
document.addEventListener('DOMContentLoaded', () => {
  loadConfig();
  dom.footerEngine.textContent = dom.selStrategy.value;

  // 当策略切换时更新页脚
  dom.selStrategy.addEventListener('change', () => {
    dom.footerEngine.textContent = dom.selStrategy.value;
  });
});