const API = {
  base: '',
  async get(path) { const r = await fetch(this.base + path); return r.json(); },
  async post(path, data) {
    const r = await fetch(this.base + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data),
    });
    return r.json();
  },
  async del(path) { const r = await fetch(this.base + path, { method: 'DELETE' }); return r.json(); },
};

const state = {
  running: false,
  platform: 'boss',
  polling: null,
  logPaused: false,
};

// ---- Status updates ----
async function updateStatus() {
  try {
    const status = await API.get('/api/status');
    state.running = status.running;
    document.getElementById('sessionCount').textContent = status.deliveryCount || 0;
    const pct = status.maxDelivery > 0 ? Math.round((status.deliveryCount / status.maxDelivery) * 100) : 0;
    document.getElementById('progressPct').textContent = pct + '%';

    updateBtnStates();
    updateConnection(true);
  } catch {
    updateConnection(false);
  }
}

async function updateStats() {
  try {
    const stats = await API.get('/api/stats');
    document.getElementById('todayCount').textContent = stats.today || 0;
    document.getElementById('totalCount').textContent = stats.total || 0;
  } catch {}
}

function updateConnection(connected) {
  const dot = document.querySelector('.dot');
  const text = document.getElementById('statusText');
  if (connected) {
    dot.className = 'dot connected';
    text.textContent = '已连接';
  } else {
    dot.className = 'dot error';
    text.textContent = '连接断开';
  }
}

function updateBtnStates() {
  document.getElementById('btnStart').disabled = state.running;
  document.getElementById('btnStop').disabled = !state.running;
}

// ---- Log ----
function addLog(message, type = 'status') {
  const container = document.getElementById('logContainer');
  const placeholder = container.querySelector('.log-placeholder');
  if (placeholder) placeholder.remove();

  const time = new Date().toLocaleTimeString();
  const entry = document.createElement('div');
  entry.className = `log-entry ${type}`;
  entry.textContent = `[${time}] ${message}`;
  container.appendChild(entry);
  container.scrollTop = container.scrollHeight;

  if (type === 'delivered') {
    const countEl = document.getElementById('deliveryCountLabel');
    const current = parseInt(countEl.textContent.match(/\d+/)?.[0] || 0);
    countEl.textContent = `已投递: ${current + 1}`;
  }
}

// ---- History ----
async function loadHistory() {
  try {
    const deliveries = await API.get('/api/deliveries?limit=30');
    const tbody = document.getElementById('historyBody');
    if (!deliveries.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="empty-row">暂无投递记录</td></tr>';
      return;
    }
    tbody.innerHTML = deliveries.map(d => `
      <tr>
        <td>${new Date(d.created_at).toLocaleString()}</td>
        <td>${d.title || '-'}</td>
        <td>${d.company || '-'}</td>
        <td>${d.platform || '-'}</td>
        <td><span class="status-badge">${d.status}</span></td>
      </tr>
    `).join('');
  } catch {}
}

// ---- Actions ----
async function startAutomation() {
  const keywords = document.getElementById('filterKeywords').value;
  const exclude = document.getElementById('filterExclude').value;
  const maxDelivery = parseInt(document.getElementById('maxDelivery').value) || 50;

  addLog(`🚀 启动 ${state.platform} 自动化投递...`, 'status');
  addLog(`   关键词: ${keywords}`, 'status');
  addLog(`   排除: ${exclude || '无'}`, 'status');
  addLog(`   上限: ${maxDelivery}`, 'status');

  const result = await API.post('/api/start', {
    platform: state.platform,
    filters: {
      includeKeywords: keywords ? keywords.split(/[,，]/).map(s => s.trim()).filter(Boolean) : [],
      excludeKeywords: exclude ? exclude.split(/[,，]/).map(s => s.trim()).filter(Boolean) : [],
      maxPerSession: maxDelivery,
    },
  });

  if (result.success) {
    addLog('✅ 自动化任务已启动，正在监控进度...', 'status');
    state.running = true;
    updateBtnStates();

    if (state.polling) clearInterval(state.polling);
    state.polling = setInterval(pollProgress, 2000);
  }
}

async function stopAutomation() {
  addLog('🛑 正在停止自动化...', 'status');
  await API.post('/api/stop');
  state.running = false;
  updateBtnStates();
  if (state.polling) {
    clearInterval(state.polling);
    state.polling = null;
  }
  setTimeout(() => addLog('⏸️ 已手动停止', 'status'), 1000);
}

async function pollProgress() {
  try {
    const status = await API.get('/api/status');
    document.getElementById('sessionCount').textContent = status.deliveryCount || 0;
    const max = status.maxDelivery || 50;
    const pct = max > 0 ? Math.round((status.deliveryCount / max) * 100) : 0;
    document.getElementById('progressPct').textContent = pct + '%';

    if (!status.running && state.running) {
      state.running = false;
      updateBtnStates();
      if (state.polling) { clearInterval(state.polling); state.polling = null; }
      addLog('🏁 自动化任务已完成', 'status');
      updateStats();
      loadHistory();
    }
  } catch {}
}

async function testAI() {
  const btn = document.getElementById('btnTestAI');
  btn.disabled = true;
  btn.textContent = '测试中...';

  addLog('🤖 正在测试 AI 连接...', 'status');

  try {
    const result = await API.get('/api/ai/test');
    if (result.success) {
      addLog(`✅ AI 连接成功 (${result.provider}): ${result.reply}`, 'status');
      document.getElementById('aiProvider').textContent = `✓ ${result.provider}`;
      document.getElementById('aiConfigInfo').textContent = `✅ AI 已就绪 (${result.provider})`;
      document.getElementById('aiConfigInfo').className = 'ai-config-info ok';
    } else {
      addLog(`❌ AI 测试失败`, 'error');
      document.getElementById('aiConfigInfo').textContent = `❌ ${result.error}`;
      document.getElementById('aiConfigInfo').className = 'ai-config-info err';
    }
  } catch (err) {
    addLog(`❌ AI 连接错误: ${err.message}`, 'error');
    document.getElementById('aiConfigInfo').textContent = `❌ 连接失败: ${err.message}`;
    document.getElementById('aiConfigInfo').className = 'ai-config-info err';
  }

  btn.disabled = false;
  btn.textContent = '🤖 测试 AI';
}

async function generateAIReply() {
  const input = document.getElementById('aiTestInput');
  const replyDiv = document.getElementById('aiReply');
  const text = input.value.trim();
  if (!text) return;

  replyDiv.style.display = 'none';
  replyDiv.textContent = '正在思考...';

  try {
    const result = await API.post('/api/ai/reply', { message: text });
    if (result.success) {
      replyDiv.textContent = result.reply;
      replyDiv.className = 'ai-reply show';
    } else {
      replyDiv.textContent = `❌ ${result.error}`;
      replyDiv.className = 'ai-reply show';
      replyDiv.style.borderColor = 'var(--danger)';
    }
  } catch (err) {
    replyDiv.textContent = `❌ ${err.message}`;
    replyDiv.className = 'ai-reply show';
    replyDiv.style.borderColor = 'var(--danger)';
  }
}

// ---- Init ----
document.addEventListener('DOMContentLoaded', async () => {
  document.querySelectorAll('.platform-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.platform-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.platform = btn.dataset.platform;
      addLog(`📌 切换到: ${btn.textContent}`, 'status');
    });
  });

  document.getElementById('btnStart').addEventListener('click', startAutomation);
  document.getElementById('btnStop').addEventListener('click', stopAutomation);
  document.getElementById('btnTestAI').addEventListener('click', testAI);
  document.getElementById('btnAiTest').addEventListener('click', generateAIReply);
  document.getElementById('btnRefresh').addEventListener('click', loadHistory);

  // Initial load
  await Promise.all([updateStatus(), updateStats(), loadHistory()]);
  setInterval(updateStatus, 5000);
  setInterval(updateStats, 10000);

  // Check AI config on startup
  setTimeout(testAI, 2000);

  addLog('📊 Job Me 平台已加载，等待操作...', 'status');

  // Listen for SSE or polling-based log updates
  Object.keys(state).forEach(k => {
    if (k.startsWith('_')) return;
  });
});