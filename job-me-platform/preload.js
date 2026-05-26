const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('api', {
  engine: {
    start: (config) => ipcRenderer.invoke('engine:start', config),
    stop: () => ipcRenderer.invoke('engine:stop'),
    getStatus: () => ipcRenderer.invoke('engine:status'),
    onScreenshot: (cb) => {
      ipcRenderer.on('engine:screenshot', (e, data) => cb(data));
    },
    onLog: (cb) => {
      ipcRenderer.on('engine:log', (e, data) => cb(data));
    },
    onStatusUpdate: (cb) => {
      ipcRenderer.on('engine:statusUpdate', (e, data) => cb(data));
    },
  },
  config: {
    load: () => ipcRenderer.invoke('config:load'),
    save: (cfg) => ipcRenderer.invoke('config:save', cfg),
  },
  app: {
    selectFile: () => ipcRenderer.invoke('app:selectFile'),
    openExternal: (url) => ipcRenderer.invoke('app:openExternal', url),
  },
});