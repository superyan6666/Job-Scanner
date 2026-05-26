const API_BASE = '';

// ─── DOM refs ───
const $ = id => document.getElementById(id);
const dom = {
  // SSE
  sseDot: $('sseDot'),
  sseText: $('sseText'),

  // Workflow
  wfSteps: {
    browser: $('wf-browser'),
    login: $('wf-login'),
    search: $('wf-search'),
    deliver: $('wf-deliver'),
  },
  wfCount: $('wfCount'),

  // Status card
  statusIndicator: $('statusIndicator'),
  currentPlatformDisplay: $('currentPlatformDisplay'),
  siStep: $('siStep'),
  siJob: $('siJob'),
  siProgress: $('siProgress'),
  progressFill: $('progressFill'),

  // Preview
  previewContainer: $('previewContainer'),
  previewPlaceholder: $('previewPlaceholder'),
  previewImage: $('previewImage'),
  previewStatus: $('previewStatus'),

  // Controls
  platformBtns: document.querySelectorAll('.platform-card'),
  filterKeywords: $('filterKeywords'),
  filterExclude: $('filterExclude'),
  cityCode: $('cityCode'),
  maxDelivery: $('maxDelivery'),
  btnStart: $('btnStart'),
  btnStop: $('btnStop'),
  btnTestAI: $('btnTestAI'),

  // Step flow
  stepFlow: $('stepFlow'),
  deliveryCountLabel: $('deliveryCountLabel'),

  // Scan info
  scanCard: $('scanCard'),
  scanTitle: $('scanTitle'),
  scanCompany: $('scanCompany'),
  scanSalary: $('scanSalary'),
  scanLocation: $('scanLocation'),

  // Delivery list
  deliveryList: $('deliveryList'),

  // AI
  aiStatus: $('aiStatus'),
  aiTestInput: $('aiTestInput'),
  btnAiTest: $('btnAiTest'),
  aiProvider: $('aiProvider'),
  aiReply: $('aiReply'),
};

// ─── State ───
let selectedPlatform = 'boss';
let isRunning = false;

// ─── SSE 连接 ───
function connectSSE() {
  const evtSource = new EventSource(API_BASE + '/api/events');

  evtSource.addEventListener('connected', () => {
    dom.sseDot.className = 'dot connected';
    dom.sseText.textContent = '实时连接';
    addStepItem('🔗', 'SSE 实时连接已建立', '等待操作指令', 0);
  });

  evtSource.addEventListener('start', (e) => {
    const data = JSON.parse(e.data);
    isRunning = true;
    dom.statusIndicator.textContent = '▶️ 运行中';
    dom.statusIndicator.className = 'status-indicator running';
    dom.currentPlatformDisplay.textContent = data.platformName || data.platform;
    dom.btnStart.disabled = true;
    dom.btnStop.disabled = false;
    dom.previewStatus.textContent = '运行中...';
    dom.stepFlow.innerHTML = '';
    dom.deliveryList.innerHTML = '<div class="dl-placeholder">等待投递...</div>';
    dom.deliveryCountLabel.textContent = '已投递: 0';
    dom.wfCount.textContent = '0';
    resetWorkflow();
  });

  evtSource.addEventListener('step', (e) => {
    const data = JSON.parse(e.data);
    dom.siStep.textContent = data.title || '';
    addStepItem(data.emoji || '•', data.title || '', data.detail || '', data.deliveryCount || 0);
    dom.siProgress.textContent = `${data.deliveryCount || 0} / ${data.maxDelivery || '?'}`;
    if (data.maxDelivery > 0) {
      const pct = Math.min((data.deliveryCount / data.maxDelivery) * 100, 100);
      dom.progressFill.style.width = pct + '%';
    }
  });

  evtSource.addEventListener('scan', (e) => {
    const data = JSON.parse(e.data);
    dom.scanCard.style.display = 'block';
    dom.scanTitle.textContent = data.title || '—';
    dom.scanCompany.textContent = data.company || '—';
    dom.scanSalary.textContent = data.salary || '—';
    dom.scanLocation.textContent = data.location || '—';
    dom.siJob.textContent = `${data.title} @ ${data.company}`;
  });

  evtSource.addEventListener('delivered', (e) => {
    const data = JSON.parse(e.data);
    dom.wfCount.textContent = data.count || 0;
    dom.deliveryCountLabel.textContent = `已投递: ${data.count}`;
    addDeliveryItem(data.count, data.title, data.company);
    dom.scanCard.style.display = 'none';
  });

  evtSource.addEventListener('skip', (e) => {
    const data = JSON.parse(e.data);
    addStepItem('⏭️', `跳过: ${data.title}`, `原因: ${data.reason} · ${data.company}`, 0);
    dom.siJob.textContent = `⏭️ ${data.title}`;
  });

  evtSource.addEventListener('screenshot', (e) => {
    const data = JSON.parse(e.data);
    if (data.image) {
      dom.previewImage.src = 'data:image/jpeg;base64,' + data.image;
      dom.previewImage.style.display = 'block';
      dom.previewPlaceholder.style.display = 'none';
      dom.previewStatus.textContent = `📸 ${data.name || '截图'}`;
    }
  });

  evtSource.addEventListener('status', (e) => {
    const data = JSON.parse(e.data);
    addStepItem('ℹ️', data.message || '', '', 0);
  });

  evtSource.addEventListener('limit', () => {
    addStepItem('⛔', '⚠️ 今日沟通已达上限！', '平台限制了今日沟通次数，建议明天再试', 0);
    dom.statusIndicator.textContent = '⛔ 已达上限';
    dom.statusIndicator.style.color = 'var(--warning)';
  });

  evtSource.addEventListener('complete', (e) => {
    const data = JSON.parse(e.data);
    addStepItem('🏆', '自动化完成！', `共投递 ${data.deliveryCount} 个岗位`, data.deliveryCount);
    dom.statusIndicator.textContent = '✅ 已完成';
    dom.btnStart.disabled = false;
    dom.btnStop.disabled = true;
    dom.previewStatus.textContent = '已完成';
    setWorkflowDone();
  });

  evtSource.addEventListener('error', (e) => {
    const data = JSON.parse(e.data);
    addStepItem('❌', '错误', data.message || '未知错误', 0);
    dom.statusIndicator.textContent = '❌ 错误';
    dom.btnStart.disabled = false;
    dom.btnStop.disabled = true;
  });

  evtSource.addEventListener('stop', () => {
    isRunning = false;
    dom.btnStart.disabled = false;
    dom.btnStop.disabled = true;
    dom.previewStatus.textContent = '已停止';
    addStepItem('⏹️', '自动化已停止', '用户手动停止或任务结束', 0);
  });

  evtSource.onerror = () => {
    dom.sseDot.className = 'dot disconnected';
    dom.sseText.textContent = '连接断开';
    addStepItem('⚠️', 'SSE 连接断开', '尝试重新连接...', 0);
  };
}

// ─── Workflow visual ───
function resetWorkflow() {
  Object.values(dom.wfSteps).forEach(el => {
    el.classList.remove('active', 'done');
  });
}

function setWorkflowDone() {
  Object.values(dom.wfSteps).forEach(el => {
    el.classList.remove('active');
    el.classList.add('done');
  });
}

function updateWorkflow(stepIndex) {
  resetWorkflow();
  const steps = Object.values(dom.wfSteps);
  steps.forEach((el, i) => {
    if (i + 1 < stepIndex) el.classList.add('done');
    else if (i + 1 === stepIndex) el.classList.add('active');
  });
}

// ─── Step flow items ───
function addStepItem(emoji, title, detail, count) {
  const placeholder = dom.stepFlow.querySelector('.sf-placeholder');
  if (placeholder) placeholder.remove();

  const item = document.createElement('div');
  item.className = 'sf-item done';
  const time = new Date().toLocaleTimeString();
  item.innerHTML = `
    <span class="sf-emoji">${emoji}</span>
    <div class="sf-content">
      <div class="sf-title">${title}</div>
      ${detail ? `<div class="sf-detail">${detail}</div>` : ''}
    </div>
    <span class="sf-time">${time}</span>
  `;
  dom.stepFlow.appendChild(item);
  dom.stepFlow.scrollTop = dom.stepFlow.scrollHeight;

  // Update workflow based on title keywords
  if (title.includes('启动浏览器')) updateWorkflow(1);
  if (title.includes('登录') || title.includes('检查登录')) updateWorkflow(2);
  if (title.includes('搜索') || title.includes('页面已加载')) updateWorkflow(3);
  if (title.includes('投递') || title.includes('已投递')) updateWorkflow(4);

  // Update count
  if (count > 0) {
    dom.wfCount.textContent = count;
    dom.deliveryCountLabel.textContent = `已投递: ${count}`;
  }
}

// ─── Delivery list item ───
function addDeliveryItem(index, title, company) {
  const placeholder = dom.deliveryList.querySelector('.dl-placeholder');
  if (placeholder) placeholder.remove();

  const item = document.createElement('div');
  item.className = 'dl-item';
  const time = new Date().toLocaleTimeString();
  item.innerHTML = `
    <span class="dl-index">#${index}</span>
    <span class="dl-title">${title || '-'}</span>
    <span class="dl-company">${company || '-'}</span>
    <span style="font-size:11px;color:var(--text-muted)">${time}</span>
  `;
  dom.deliveryList.insertBefore(item, dom.deliveryList.firstChild);

  if (dom.deliveryList.children.length > 50) {
    dom.deliveryList.removeChild(dom.deliveryList.lastChild);
  }
}

// ─── Platform selection ───
dom.platformBtns.forEach(btn => {
  btn.addEventListener('click', () => {
    dom.platformBtns.forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    selectedPlatform = btn.dataset.platform;
    dom.currentPlatformDisplay.textContent = btn.querySelector('.pc-name').textContent;
    addStepItem('📌', `已选择平台: ${btn.querySelector('.pc-name').textContent}`, '', 0);
  });
});

// ─── Start ───
dom.btnStart.addEventListener('click', async () => {
  const keywords = dom.filterKeywords.value;
  const exclude = dom.filterExclude.value;
  const cityCode = dom.cityCode.value;
  const maxDelivery = parseInt(dom.maxDelivery.value) || 50;

  addStepItem('🚀', `启动 ${selectedPlatform.toUpperCase()} 自动化投递`, `关键词: ${keywords || '无'} | 上限: ${maxDelivery}`, 0);

  try {
    const resp = await fetch(API_BASE + '/api/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        platform: selectedPlatform,
        filters: {
          includeKeywords: keywords ? keywords.split(/[,，]/).map(s => s.trim()).filter(Boolean) : [],
          excludeKeywords: exclude ? exclude.split(/[,，]/).map(s => s.trim()).filter(Boolean) : [],
          cityCode: cityCode || undefined,
          maxPerSession: maxDelivery,
        },
      }),
    });
    const data = await resp.json();
    if (data.error) {
      addStepItem('⚠️', data.error, '', 0);
    }
  } catch (err) {
    addStepItem('❌', '启动失败', err.message, 0);
  }
});

// ─── Stop ───
dom.btnStop.addEventListener('click', async () => {
  addStepItem('🛑', '正在停止自动化...', '', 0);
  await fetch(API_BASE + '/api/stop', { method: 'POST' });
});

// ─── Test AI ───
dom.btnTestAI.addEventListener('click', async () => {
  dom.btnTestAI.disabled = true;
  dom.btnTestAI.innerHTML = '<span class="btn-icon">⏳</span><span class="btn-text">测试中...</span>';
  dom.aiStatus.textContent = '🤖 正在测试 AI 连接...';
  dom.aiStatus.className = 'ai-status';

  try {
    const resp = await fetch(API_BASE + '/api/ai/test');
    const data = await resp.json();
    if (data.success) {
      dom.aiStatus.textContent = `✅ AI 已就绪 (${data.provider})`;
      dom.aiStatus.className = 'ai-status ok';
      dom.aiProvider.textContent = `✓ ${data.provider}`;
      addStepItem('🤖', '✅ AI 连接测试成功', `回复: ${data.reply}`, 0);
    } else {
      dom.aiStatus.textContent = `❌ ${data.error}`;
      dom.aiStatus.className = 'ai-status err';
    }
  } catch (err) {
    dom.aiStatus.textContent = `❌ 连接失败: ${err.message}`;
    dom.aiStatus.className = 'ai-status err';
  }

  dom.btnTestAI.disabled = false;
  dom.btnTestAI.innerHTML = '<span class="btn-icon">🤖</span><span class="btn-text">测试 AI</span>';
});

// ─── AI Reply generator ───
dom.btnAiTest.addEventListener('click', async () => {
  const text = dom.aiTestInput.value.trim();
  if (!text) return;
  dom.aiReply.style.display = 'none';
  dom.aiReply.textContent = '';

  try {
    const resp = await fetch(API_BASE + '/api/ai/reply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text }),
    });
    const data = await resp.json();
    if (data.success) {
      dom.aiReply.textContent = data.reply;
      dom.aiReply.className = 'ai-reply show';
    } else {
      dom.aiReply.textContent = `❌ ${data.error}`;
      dom.aiReply.className = 'ai-reply show';
    }
  } catch (err) {
    dom.aiReply.textContent = `❌ ${err.message}`;
    dom.aiReply.className = 'ai-reply show';
  }
});

// ─── Click screenshot to enlarge ───
dom.previewImage.addEventListener('click', () => {
  if (dom.previewImage.src && dom.previewImage.src.includes('base64')) {
    window.open(dom.previewImage.src);
  }
});

// ─── Init ───
document.addEventListener('DOMContentLoaded', () => {
  connectSSE();
  dom.currentPlatformDisplay.textContent = 'BOSS直聘';
  addStepItem('👋', 'Job Me 平台已启动', '选择平台和关键词，点击「启动自动化」开始投递', 0);

  // Auto test AI on start (delay to not overwhelm)
  setTimeout(() => {
    dom.btnTestAI.click();
  }, 3000);
});