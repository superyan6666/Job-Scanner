const { app, BrowserWindow, ipcMain, dialog, shell } = require('electron');
const path = require('path');
const fs = require('fs');

let mainWindow = null;
let engine = null;

// ─── 窗口创建 ───
function createWindow() {
  mainWindow = new BrowserWindow({
    width: 960,
    height: 720,
    minWidth: 780,
    minHeight: 600,
    resizable: true,
    title: 'Job Me 简历投放助手',
    icon: path.join(__dirname, 'assets', 'icon.png'),
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
    show: false,
    backgroundColor: '#0b1120',
  });

  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'));

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
    engine = null;
  });
}

app.whenReady().then(createWindow);
app.on('window-all-closed', () => { if (process.platform !== 'darwin') app.quit(); });
app.on('activate', () => { if (!mainWindow) createWindow(); });

// ─── IPC 处理 ───

// 加载自动化引擎 (延迟加载)
function getEngine() {
  if (engine) return engine;
  const { JobEngine } = require('./engine/index.js');
  engine = new JobEngine();
  engine.setMainWindow(mainWindow);
  return engine;
}

ipcMain.handle('engine:start', async (event, config) => {
  try {
    const eng = getEngine();
    await eng.startDelivery(config);
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
});

ipcMain.handle('engine:stop', async () => {
  if (engine) engine.stop();
  return { success: true };
});

ipcMain.handle('engine:status', () => {
  if (!engine) return { running: false, currentStep: '', deliveryCount: 0 };
  return {
    running: engine.running,
    currentStep: engine.currentStep,
    deliveryCount: engine.deliveryCount,
    maxDelivery: engine.maxDelivery,
  };
});

ipcMain.handle('config:load', () => {
  const configPath = path.join(__dirname, 'data', 'config.json');
  try {
    return JSON.parse(fs.readFileSync(configPath, 'utf-8'));
  } catch {
    return {};
  }
});

ipcMain.handle('config:save', (event, config) => {
  const configPath = path.join(__dirname, 'data', 'config.json');
  const dir = path.dirname(configPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
  return { success: true };
});

ipcMain.handle('app:selectFile', async () => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openFile'],
    filters: [{ name: '文本文件', extensions: ['txt', 'md'] }],
  });
  if (result.canceled) return null;
  const content = fs.readFileSync(result.filePaths[0], 'utf-8');
  return { path: result.filePaths[0], content };
});

ipcMain.handle('app:openExternal', (event, url) => {
  shell.openExternal(url);
});

// 引擎推送截图到渲染进程
global.sendScreenshot = (base64) => {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('engine:screenshot', base64);
  }
};

// 引擎推送日志到渲染进程
global.sendLog = (logEntry) => {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('engine:log', logEntry);
  }
};

// 引擎推送状态更新
global.sendStatus = (status) => {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('engine:statusUpdate', status);
  }
};