const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
// 加载 .env 环境变量
require('dotenv').config();
const SileroVAD = require(path.join(__dirname, 'scripts', 'silero_vad_onnx'));
const ffmpeg = require(path.join(__dirname, 'lib', 'ffmpeg'));
const { md5File, getOutputAudioPathFromVideo, getSubtitlePathFromAudio, resolveDirSafe, isVideoFileName, formatBytes, formatDate, walkVideoFiles } = require('./lib/fileUtils');
const { createTaskService } = require(path.join(__dirname, 'lib', 'tasks'));

// 模块化引入配置、数据库与 AI 客户端
const { MOUNT_DIR } = require('./lib/config');
const { prisma } = require('./lib/db');
const { getSnClient, isAuthError } = require('./lib/ai');


async function initApp() {
  // 使用 Prisma 保证默认用户存在
  try {

  } catch (e) {
    console.error('Prisma init failed:', e);
  }
}

const app = express();
const PORT = process.env.PORT || 3000;

// 新增：内存中的运行任务跟踪（仅用于实时 UI 展示）
const activeTasks = new Map();
function setTaskState(id, patch) {
  console.log("tasklog", patch)
  const old = activeTasks.get(id) || {};
  const next = { ...old, ...patch, id, updatedAt: Date.now() };
  activeTasks.set(id, next);
}
function getCurrentActiveTask() {
  let cur = null;
  for (const t of activeTasks.values()) {
    if (t && (t.status === 'processing' || t.status === 'queued')) {
      // 不再按扩展名过滤，避免设置中排除了某些视频扩展导致顶部不显示任务
      if (!cur || (t.updatedAt || 0) > (cur.updatedAt || 0)) cur = t;
    }
  }
  return cur;
}
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
// 视图引擎
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.set('view cache', false);

// 可识别的视频/字幕扩展名（可通过设置动态配置）
let VIDEO_EXTS = new Set(['.mp4', '.mkv', '.mov', '.avi', '.m4v', '.webm']);
let SUB_EXTS = new Set(['srt', 'ass', 'vtt']);
// 解析数据库中的视频/字幕后缀字段（兼容数组、JSON 字符串、逗号分隔字符串）
function parseVideoExts(value) {
  try {
    if (Array.isArray(value)) return value;
    if (typeof value === 'string') {
      let arr = null;
      try { const parsed = JSON.parse(value); if (Array.isArray(parsed)) arr = parsed; } catch (_) { }
      if (!arr) arr = value.split(',');
      const set = new Set(
        arr
          .map(x => String(x || '').trim().toLowerCase())
          .filter(x => x.length > 0)
          .map(x => (x.startsWith('.') ? x : ('.' + x)))
          .map(x => x.replace(/\s+/g, ''))
      );
      return Array.from(set).slice(0, 32);
    }
  } catch (_) { }
  return null;
}
function parseSubExts(value) {
  try {
    if (Array.isArray(value)) {
      return value
        .map(x => String(x || '').trim().toLowerCase())
        .filter(x => x.length > 0)
        .map(x => x.replace(/^\./, ''))
        .map(x => x.replace(/\s+/g, ''))
        .map(x => x.split('.').pop()); // 只保留最后一段，确保是纯后缀
    }
    if (typeof value === 'string') {
      let arr = null;
      try { const parsed = JSON.parse(value); if (Array.isArray(parsed)) arr = parsed; } catch (_) { }
      if (!arr) arr = value.split(',');
      const set = new Set(
        arr
          .map(x => String(x || '').trim().toLowerCase())
          .filter(x => x.length > 0)
          .map(x => x.replace(/^\./, ''))
          .map(x => x.replace(/\s+/g, ''))
          .map(x => x.split('.').pop()) // 只保留最后一段，确保是纯后缀
      );
      return Array.from(set).slice(0, 32);
    }
  } catch (_) { }
  return null;
}
async function loadExtSettings() {
  try {
    const s = await prisma.settings.findUnique({ where: { key: 'singleton' }, select: { videoExts: true, subExts: true } });
    const v = parseVideoExts(s && s.videoExts);
    const sub = parseSubExts(s && s.subExts);
    const vset = new Set(
      (v && v.length ? v : ['.mp4', '.mkv', '.mov', '.avi', '.m4v', '.webm'])
        .map(x => String(x || '').trim().toLowerCase())
        .filter(x => x)
        .map(x => x.startsWith('.') ? x : ('.' + x))
    );
    const sset = new Set(
      (sub && sub.length ? sub : ['srt', 'ass', 'vtt'])
        .map(x => String(x || '').trim().toLowerCase().replace(/^\./, ''))
        .filter(x => x)
    );
    if (vset.size) VIDEO_EXTS = vset;
    if (sset.size) SUB_EXTS = sset;
  } catch (_) { }
}
loadExtSettings().catch(() => { });


// 列出挂载目录下的一层视频文件
function listVideos() {
  try {
    const items = fs.readdirSync(MOUNT_DIR, { withFileTypes: true });
    return items
      .filter((it) => it.isFile() && isVideoFileName(it.name, VIDEO_EXTS))
      .map((it) => it.name)
      .sort((a, b) => a.localeCompare(b, 'zh-CN'));
  } catch (e) {
    return [];
  }
}

// 将前端传入的文件名解析为容器内绝对路径（限制在 MOUNT_DIR 下）
function resolveVideo(basename) {
  const safe = path.basename(basename || ''); // 防止目录穿越
  const full = path.join(MOUNT_DIR, safe);
  if (!fs.existsSync(full)) return null;
  return full;
}


// 新增：单并发任务队列与调度器（确保一次只处理一个任务）
let processing = false;
const taskQueue = [];

// 新增：任务控制（用于“停止/暂停”当前正在运行的任务）
const taskControls = new Map(); // taskId => { canceled: boolean, procs: Set<ChildProcess>, controllers: Set<AbortController>, reason?: string }
function getOrCreateTaskControl(taskId) {
  let ctl = taskControls.get(taskId);
  if (!ctl) {
    ctl = { canceled: false, procs: new Set(), controllers: new Set(), reason: '' };
    taskControls.set(taskId, ctl);
  }
  return ctl;
}
function registerProc(taskId, proc) {
  try {
    const ctl = getOrCreateTaskControl(taskId);
    ctl.procs.add(proc);
    const cleanup = () => { try { ctl.procs.delete(proc); } catch (_) { } };
    proc.on('close', cleanup);
    proc.on('exit', cleanup);
    proc.on('error', cleanup);
  } catch (_) { }
}
function registerAbort(taskId, controller) {
  try {
    const ctl = getOrCreateTaskControl(taskId);
    ctl.controllers.add(controller);
    const cleanup = () => { try { ctl.controllers.delete(controller); } catch (_) { } };
    // 当被动结束时移除
    try { controller.signal?.addEventListener?.('abort', cleanup, { once: true }); } catch (_) { }
  } catch (_) { }
}

// 基于依赖注入的任务服务
const taskService = createTaskService({
  fs,
  prisma,
  MOUNT_DIR,
  SileroVAD,
  ffmpeg,
  getSnClient,
  getOutputAudioPathFromVideo,
  taskControls,
  activeTasks,
  taskQueue,
  setTaskState,
  detectSubtitles,
  isVideoFileName,
  VIDEO_EXTS,
  registerProc,
  registerAbort,
  isAuthError,
  getProcessing: () => processing,
  setProcessing: (v) => { processing = !!v; },
});

async function cancelTask(taskId, reason) { return taskService.cancelTask(taskId, reason); }
async function cancelAllTasks(reason, source) { return taskService.cancelAllTasks(reason, source); }

// 自动入队：定时器逻辑迁移到任务服务内部
async function maybeAutoEnqueueNext() { return taskService.maybeAutoEnqueueNext(); }
function ensureAutoTimer() { try { taskService.ensureAutoTimer(); } catch (_) { } }
ensureAutoTimer();

async function startNextTask() { return taskService.startNextTask(); }

// 生成字幕：提取音频 -> VAD 分段 -> 逐段识别 -> 写入 SRT
async function processGenerate(job) { return taskService.processGenerate(job); }

// 为语音识别准备较小体积的音频：改用 lib/ffmpeg 实现
const prepareAudioForTranscription = ffmpeg.prepareAudioForTranscription;

// removed duplicate home route (kept the later unified implementation)


// legacy routes removed: /operate, /audio, /list, /subtitle, /extract-audio

// removed duplicate /transcribe-subtitle-vad route (kept the later unified implementation)


// removed duplicate /vad-segments route (kept the later unified implementation)



// 新增：字幕检测（同目录，基于视频文件名）
// 使用工具模块中的 detectSubtitles：保持签名一致，通过 SUB_EXTS 传参
function detectSubtitles(dir, videoFilename) {
  const { detectSubtitles: detectUtil } = require('./lib/fileUtils');
  return detectUtil(dir, videoFilename, SUB_EXTS);
}

// removed duplicate /api/dirs route (kept the later unified implementation)


// legacy route removed: /api/debug/detect

app.get('/api/files', async (req, res) => {
  try {
    const rel = (req.query.dir || '').toString();
    const baseDir = resolveDirSafe(rel) || MOUNT_DIR;

    // 使用通用的 formatBytes 与 formatDate（已从 fileUtils 引入）

    // 任务映射：按 dirRel/name 匹配
    const taskByKey = new Map();
    try {
      for (const t of activeTasks.values()) {
        if (!t) continue;
        if (t.status === 'queued' || t.status === 'processing') {
          const key = `${t.dirRel || ''}/${t.name}`;
          taskByKey.set(key, t);
        }
      }
    } catch (_) { }

    // 递归扫描子文件夹中的视频 -> 使用工具函数封装
    const recursive = req.query.recursive !== '0';
    const fileEntries = walkVideoFiles(baseDir, VIDEO_EXTS, recursive);

    let totalBytes = 0;
    let items = fileEntries.map((fe) => {
      let size = 0;
      let mtime = new Date();
      try {
        const st = fs.statSync(fe.abs);
        size = st.size;
        mtime = st.mtime;
      } catch { }
      totalBytes += size;
      const key = `${fe.dirRel}/${fe.name}`;
      const t = taskByKey.get(key);
      return {
        name: fe.name,
        dirRel: fe.dirRel,
        sizeLabel: formatBytes(size),
        dateLabel: formatDate(mtime),
        type: 'F',
        subs: detectSubtitles(fe.dirAbs, fe.name), // 同目录字幕
        task: t ? { status: t.status, stage: t.stage || '', progress: Math.max(0, Math.min(100, Number(t.progress) || 0)) } : null,
        __meta: { size, mtime },
      };
    });

    // 按全局维护排序 sequence（首次发现的新文件或序号<=0 的文件，赋值为全局最大+1）
    try {
      if (items.length) {
        // 计算全局 currentMax（跨所有目录）
        const globalMaxRow = await prisma.mediaFile.findFirst({ orderBy: { sequence: 'desc' }, select: { sequence: true } });
        let currentMax = ((globalMaxRow && (globalMaxRow.sequence || 0)) || 0);
        // 分组：dirRel -> items[]
        const groups = new Map();
        for (const it of items) {
          const arr = groups.get(it.dirRel) || [];
          arr.push(it);
          groups.set(it.dirRel, arr);
        }
        // 逐组写入/更新
        for (const [gRel, list] of groups.entries()) {
          const names = list.map((it) => it.name);
          const metaByName = new Map(list.map((it) => [it.name, it.__meta || {}]));

          const existing = await prisma.mediaFile.findMany({
            where: { dirRel: gRel, name: { in: names } },
            select: { name: true, sequence: true },
          });
          const existingMap = new Map(existing.map((r) => [r.name, r.sequence || 0]));

          // 目录内最大值逻辑已废弃，统一使用外层的全局 currentMax
          const tx = [];
          for (const it of list) {
            const meta = metaByName.get(it.name) || {};
            const seq = existingMap.has(it.name) ? (existingMap.get(it.name) || 0) : undefined;
            if (typeof seq === 'undefined') {
              currentMax += 1;
              tx.push(
                prisma.mediaFile.create({
                  data: { dirRel: gRel, name: it.name, sequence: currentMax, size: meta.size || 0, mtime: meta.mtime || new Date() },
                })
              );
            } else if (seq <= 0) {
              currentMax += 1;
              tx.push(
                prisma.mediaFile.update({
                  where: { dirRel_name: { dirRel: gRel, name: it.name } },
                  data: { sequence: currentMax, size: meta.size || 0, mtime: meta.mtime || new Date() },
                })
              );
            } else {
              tx.push(
                prisma.mediaFile.update({
                  where: { dirRel_name: { dirRel: gRel, name: it.name } },
                  data: { size: meta.size || 0, mtime: meta.mtime || new Date() },
                })
              );
            }
          }
          if (tx.length) await prisma.$transaction(tx);
        }

        // 回读全部 sequence
        const orConds = [];
        for (const [gRel, list] of groups.entries()) {
          orConds.push({ dirRel: gRel, name: { in: list.map((it) => it.name) } });
        }
        if (orConds.length) {
          const recs = await prisma.mediaFile.findMany({ where: { OR: orConds }, select: { dirRel: true, name: true, sequence: true } });
          const seqMap = new Map(recs.map((r) => [`${r.dirRel}/${r.name}`, r.sequence || 0]));
          items = items.map((it) => ({ ...it, sequence: seqMap.get(`${it.dirRel}/${it.name}`) || 0 }));
        }
      } else {
        items = [];
      }
    } catch (e) {
      items = items.map((it) => ({ ...it, sequence: 0 }));
    }

    // 排序：sequence desc -> 目录 -> 文件名
    items.sort((a, b) => {
      const sa = Number(a.sequence) || 0;
      const sb = Number(b.sequence) || 0;
      if (sa !== sb) return sb - sa;
      if ((a.dirRel || '') !== (b.dirRel || '')) return (a.dirRel || '').localeCompare(b.dirRel || '', 'zh-CN');
      return a.name.localeCompare(b.name, 'zh-CN');
    });

    const page = Math.max(1, parseInt(req.query.page || '1', 10));
    const pageSize = Math.max(1, Math.min(100, parseInt(req.query.pageSize || '50', 10)));
    const total = items.length;
    const start = (page - 1) * pageSize;
    const end = start + pageSize;
    const pageItems = items.slice(start, end).map(({ __meta, ...rest }) => rest);

    const totalLabel = formatBytes(totalBytes);
    res.json({ items: pageItems, totalLabel, total, page, pageSize });
  } catch (e) {
    res.json({ items: [], totalLabel: '0 B', total: 0, page: 1, pageSize: 50 });
  }
});

app.get('/blank', (req, res) => {
  try {
    const rel = (req.query.dir || '').toString();
    const baseDir = resolveDirSafe(rel) || MOUNT_DIR;

    const mapType = (name) => {
      const ext = path.extname(name).toLowerCase();
      if (ext === '.pdf') return 'P';
      if (ext === '.sketch') return 'S';
      if (ext === '.xd') return 'X';
      return 'F';
    };

    // 使用通用的 formatBytes 与 formatDate（已从 fileUtils 引入）

    const entries = fs.readdirSync(baseDir, { withFileTypes: true });
    const files = entries.filter((e) => e.isFile() && isVideoFileName(e.name, VIDEO_EXTS));

    let totalBytes = 0;
    const items = files
      .map((f) => {
        const full = path.join(baseDir, f.name);
        let size = 0;
        let mtime = new Date();
        try {
          const st = fs.statSync(full);
          size = st.size;
          mtime = st.mtime;
        } catch { }
        totalBytes += size;
        return {
          name: f.name,
          dirRel: path.relative(MOUNT_DIR, baseDir).split(path.sep).join('/'),
          sizeLabel: formatBytes(size),
          dateLabel: formatDate(mtime),
          type: mapType(f.name),
          subs: detectSubtitles(baseDir, f.name),
        };
      })
      .sort((a, b) => a.name.localeCompare(b.name, 'zh-CN'));

    const totalLabel = formatBytes(totalBytes);
    const currentDirRel = path.relative(MOUNT_DIR, baseDir).split(path.sep).join('/');
    const displayDir = '/' + (currentDirRel ? `data/${currentDirRel}` : 'data');
    const hostMountDir = String(process.env.HOST_MOUNT_DIR || process.env.MOUNT_DIR || '');
    return res.render('blank', { items, totalLabel, navActive: 'files', currentDirRel, displayDir, hostMountDir });
  } catch (e) {
    const hostMountDir = String(process.env.HOST_MOUNT_DIR || process.env.MOUNT_DIR || '');
    const Mount_DIR = process.env.MOUNT_DIR || '/data';
    return res.render('blank', { items: [], totalLabel: '0 B', navActive: 'files', currentDirRel: '', displayDir: Mount_DIR, hostMountDir });
  }
});

// 自定义首页
app.get('/home', (req, res) => {
  try {
    const baseDir = MOUNT_DIR;
    // 递归扫描所有视频文件
    const files = [];
    (function visit(dirAbs) {
      let ents = [];
      try { ents = fs.readdirSync(dirAbs, { withFileTypes: true }); } catch { return; }
      for (const e of ents) {
        const abs = path.join(dirAbs, e.name);
        if (e.isDirectory()) { visit(abs); continue; }
        if (e.isFile() && isVideoFileName(e.name, VIDEO_EXTS)) {
          files.push({ dirAbs, name: e.name });
        }
      }
    })(baseDir);

    let totalVideos = files.length;
    let withSubs = 0;
    let translatedSubs = 0;
    for (const f of files) {
      const subs = detectSubtitles(f.dirAbs, f.name);
      if (subs && subs.has) withSubs++;
      if (subs && subs.cn) translatedSubs++;
    }

    const stats = { totalVideos, withSubs, translatedSubs, pendingTasks: 0 };
    res.render('home', { navActive: 'home', stats });
  } catch (e) {
    res.render('home', { navActive: 'home', stats: { totalVideos: 0, withSubs: 0, translatedSubs: 0, pendingTasks: 0 } });
  }
});

// 新增：任务列表页面
app.get('/tasks', (req, res) => {
  try {
    const files = listVideos();
    const tasks = files.slice(0, 5).map((name, i) => {
      const full = resolveVideo(name);
      let size = 0; let mtime = new Date();
      try {
        const st = fs.statSync(full);
        size = st.size; mtime = st.mtime;
      } catch { }
      const progress = i % 3 === 0 ? 100 : (35 + i * 12) % 95; // 100 或者 进度中
      const status = progress >= 100 ? '已完成' : '处理中';
      return {
        name,
        sizeLabel: formatBytes(size),
        dateLabel: formatDate(mtime),
        status,
        progress
      };
    });

    const recent = files.slice(0, 8).map((name) => {
      const full = resolveVideo(name);
      let size = 0; let mtime = new Date();
      try { const st = fs.statSync(full); size = st.size; mtime = st.mtime; } catch { }
      return { name, sizeLabel: formatBytes(size), dateLabel: formatDate(mtime) };
    });

    res.render('tasks', { navActive: 'tasks', tasks, recent });
  } catch (e) {
    res.render('tasks', { navActive: 'tasks', tasks: [], recent: [] });
  }
});

// 根首页统一为 Home
app.get('/', (req, res) => {
  res.render('home', { navActive: 'home' });
});

// legacy route removed: /video

// legacy route removed: /transcribe-subtitle

// 启动服务（在完成初始化后监听端口）
initApp()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Server is running on port ${PORT}`);
    });
  })
  .catch((e) => {
    console.error('Init failed, continue starting server:', e);
    // If init fails we still start the server once; avoid duplicate listen calls
    if (!app.listening) {
      app.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
      });
    }
  });
// 设置页
app.get('/settings', (req, res) => {
  res.render('settings', { navActive: 'settings' });
});

// 新增：OpenAI 诊断接口（检查 Key、连接与 whisper-1 是否可用）
app.get('/api/diagnose/openai', async (req, res) => {
  try {
    const testKey = req.query.key;
    let client;
    const baseURL = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
    
    if (testKey) {
      const OpenAI = require('openai');
      client = new OpenAI({ 
        apiKey: testKey.trim() || 'sk-dummy-key',
        baseURL 
      });
    } else {
      client = await getSnClient();
    }
    
    //const client = await getSnClient();
    // 列出可用模型以验证连接与授权
    const models = await client.models.list();
    const list = Array.isArray(models.data) ? models.data : [];
    const hasWhisper = list.some(m => {
      try { return (m && (m.id === 'whisper-1' || String(m.id || '').toLowerCase().includes('whisper'))); } catch (_) { return false; }
    });
    res.json({ ok: true, baseURL: (process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1'), modelsCount: list.length, hasWhisper });
  } catch (e) {
    const auth = isAuthError(e);
    res.status(auth ? 401 : 500).json({ ok: false, message: (e && (e.message || String(e))) || 'diagnose failed', code: e && e.code });
  }
});

// 设置 API：读取
app.get('/api/settings', async (req, res) => {
  try {
    const s = await prisma.settings.findUnique({ where: { key: 'singleton' } });
    if (!s) return res.json({ ok: true, data: null });
    const includeSecret = String(req.query.debug || '') === '1';
    const { openaiKey, videoExts, subExts, auto, ...rest } = s; // 默认不回传密钥且忽略 auto
    const hasOpenaiKey = typeof openaiKey === 'string' && openaiKey.trim().length > 0;
    const payload = { ...rest, hasOpenaiKey };
    const vArr = parseVideoExts(videoExts);
    const sArr = parseSubExts(subExts);
    // 附带扩展名设置（若未设置则回传默认）
    payload.videoExts = Array.isArray(vArr) && vArr.length ? vArr : ['.mp4', '.mkv', '.mov', '.avi', '.m4v', '.webm'];
    payload.subExts = Array.isArray(sArr) && sArr.length ? sArr : ['srt', 'ass', 'vtt'];
    // 登录状态占位：当前未接入鉴权，默认 false；未来接入后可改为基于 cookie/session 判断
    payload.isLoggedIn = false;
    if (includeSecret) payload.openaiKey = openaiKey || '';
    res.json({ ok: true, data: payload });
  } catch (e) {
    res.status(500).json({ ok: false, message: '读取设置失败' });
  }
});

// 设置 API：保存
app.post('/api/settings', async (req, res) => {
  try {
    const body = req.body || {};
    // 是否显式提交了 openaiKey 字段（用于区分“未改动”与“清空”）
    const wantsToUpdateKey = Object.prototype.hasOwnProperty.call(body, 'openaiKey');
    const normalizedKey = wantsToUpdateKey
      ? (typeof body.openaiKey === 'string' ? body.openaiKey.trim() : null)
      : undefined; // 未提交该字段表示不更新

    // 规范化扩展名设置（若前端未传则保持 undefined，不更新到 DB）
    let normalizedVideoExts;
    let normalizedSubExts;
    try {
      if (Object.prototype.hasOwnProperty.call(body, 'videoExts')) {
        const raw = Array.isArray(body.videoExts)
          ? body.videoExts
          : (typeof body.videoExts === 'string' ? body.videoExts.split(',') : []);
        const arr = raw
          .map(x => String(x || '').trim().toLowerCase())
          .filter(x => x.length > 0)
          .map(x => (x.startsWith('.') ? x : ('.' + x)))
          .map(x => x.replace(/\s+/g, ''));
        const set = new Set(arr);
        normalizedVideoExts = Array.from(set).slice(0, 32);
      }
      if (Object.prototype.hasOwnProperty.call(body, 'subExts')) {
        const raw = Array.isArray(body.subExts)
          ? body.subExts
          : (typeof body.subExts === 'string' ? body.subExts.split(',') : []);
        const arr = raw
          .map(x => String(x || '').trim().toLowerCase())
          .filter(x => x.length > 0)
          .map(x => x.replace(/^\./, ''))
          .map(x => x.replace(/\s+/g, ''));
        const set = new Set(arr);
        normalizedSubExts = Array.from(set).slice(0, 32);
      }
    } catch (_) { }

    const baseData = {
      key: 'singleton',
      svc: typeof body.svc === 'string' ? body.svc : 'openai',
      autoGenerate: !!body.autoGenerate,
      autoTranslate: !!body.autoTranslate,
      srcLang: body.srcLang ? String(body.srcLang) : null,
      tgtLang: body.tgtLang ? String(body.tgtLang) : null,
      model: body.model ? String(body.model) : null,
      profile: body.profile ? String(body.profile) : null,
      prompt: body.prompt ? String(body.prompt) : null,
      polish: !!body.polish,
    };

    const createData = {
      ...baseData,
      ...(normalizedVideoExts !== undefined ? { videoExts: JSON.stringify(normalizedVideoExts) } : {}),
      ...(normalizedSubExts !== undefined ? { subExts: JSON.stringify(normalizedSubExts) } : {}),
      ...(wantsToUpdateKey ? { openaiKey: (normalizedKey || null) } : {}),
    };

    const updateData = {
      svc: baseData.svc,
      autoGenerate: baseData.autoGenerate,
      autoTranslate: baseData.autoTranslate,
      srcLang: baseData.srcLang,
      tgtLang: baseData.tgtLang,
      model: baseData.model,
      profile: baseData.profile,
      prompt: baseData.prompt,
      polish: baseData.polish,
      ...(normalizedVideoExts !== undefined ? { videoExts: JSON.stringify(normalizedVideoExts) } : {}),
      ...(normalizedSubExts !== undefined ? { subExts: JSON.stringify(normalizedSubExts) } : {}),
      ...(wantsToUpdateKey ? { openaiKey: (normalizedKey || null) } : {}),
    };

    const prevSettings = await prisma.settings.findUnique({ where: { key: 'singleton' }, select: { autoGenerate: true } });

    await prisma.settings.upsert({
      where: { key: 'singleton' },
      create: createData,
      update: updateData,
    });

    // 刷新内存中的扩展名配置
    setImmediate(() => { try { loadExtSettings(); } catch (_) { } });

    // 设置更新后，尝试触发一次自动入队检查
    setImmediate(() => { try { maybeAutoEnqueueNext(); } catch (_) { } });

    // 若关闭了自动任务（仅在 true->false 变化时），则立即尝试停止当前正在运行的任务并清空队列
    try {
      const wasAuto = !!(prevSettings && prevSettings.autoGenerate);
      const nowAuto = !!baseData.autoGenerate;
      try { console.info('[settings] autoGenerate change', { wasAuto, nowAuto }); } catch (_) { }
      if (wasAuto && !nowAuto) setImmediate(() => { try { console.info('[tasks] stopping due to settings autoGenerate turned off'); cancelAllTasks('已关闭自动任务，停止当前任务', 'settings:auto-off'); } catch (_) { } });
    } catch (_) { }

    res.json({ ok: true });
  } catch (e) {
    try {
      console.error('Save settings error:', e && e.message, e && e.code, e && e.meta);
      if (e && e.stack) console.error(e.stack);
    } catch (_) {
      console.error('Save settings error (fallback)', e);
    }
    res.status(500).json({ ok: false, message: '保存设置失败' });
  }
});

// 查询当前正在运行的任务（用于顶部导航显示）
app.get('/api/current-task', (req, res) => {
  const t = getCurrentActiveTask();
  if (!t) return res.json({ ok: true, task: null });
  const payload = {
    id: t.id,
    name: t.name,
    progress: Math.max(0, Math.min(100, Number(t.progress) || 0)),
    stage: t.stage || '',
    status: t.status || 'processing',
    message: t.message || '',
    dirRel: t.dirRel || ''
  };
  return res.json({ ok: true, task: payload });
});

// 任务进度查询：供 tasks 页面轮询使用
app.get('/api/tasks/:id/progress', (req, res) => {
  try {
    const id = (req.params.id || '').toString();
    const t = activeTasks.get(id);
    if (!t) return res.json({ ok: false, message: 'task not found', progress: 0, status: 'unknown', stage: '' });
    return res.json({
      ok: true,
      progress: Math.max(0, Math.min(100, Number(t.progress) || 0)),
      status: t.status || 'processing',
      stage: t.stage || '',
      message: t.message || ''
    });
  } catch (e) {
    return res.status(500).json({ ok: false, message: 'query failed', detail: String(e && (e.message || e)) });
  }
});

// 停止当前任务并清空队列
app.post('/api/tasks/stop-all', async (req, res) => {
  try {
    try { console.info('[api] POST /api/tasks/stop-all'); } catch (_) { }
    await cancelAllTasks('手动停止', 'api:stop-all');
    return res.json({ ok: true, message: '已停止当前任务并清空队列' });
  } catch (e) {
    return res.status(500).json({ ok: false, message: '停止失败', detail: String(e && (e.message || e)) });
  }
});

// 按文件设为不执行（Idle）：移除队列并取消正在处理的该文件任务
app.post('/api/tasks/idle-from-file', async (req, res) => {
  try {
    const { name, dir, reason } = req.body || {};
    if (!name || typeof name !== 'string') {
      return res.status(400).json({ ok: false, message: '缺少文件名' });
    }
    const baseDir = resolveDirSafe((dir || '').toString()) || MOUNT_DIR;
    const safeName = path.basename(name);
    const videoPath = path.join(baseDir, safeName);
    if (!fs.existsSync(videoPath)) {
      // 文件不存在也视为成功（作为前端操作的幂等），仅用于移除队列中的占位
      const dirRelFallback = path.relative(MOUNT_DIR, baseDir).split(path.sep).join('/');
      const ret = await taskService.setIdleForFile(dirRelFallback, safeName, reason || '手动设为不执行');
      return res.json({ ok: true, ...ret, message: '已设置为不执行' });
    }
    if (!isVideoFileName(videoPath, VIDEO_EXTS)) {
      return res.status(400).json({ ok: false, message: '不支持的视频类型' });
    }
    const dirRel = path.relative(MOUNT_DIR, baseDir).split(path.sep).join('/');
    const ret = await taskService.setIdleForFile(dirRel, safeName, reason || '手动设为不执行');
    return res.json({ ok: true, ...ret, message: '已设置为不执行' });
  } catch (err) {
    console.error('[/api/tasks/idle-from-file] Error:', err && (err.stack || err));
    return res.status(500).json({ ok: false, message: '设置为不执行失败', detail: String((err && (err.message || err)) || err) });
  }
});

app.post('/api/tasks/create-from-file', async (req, res) => {
  try {
    const { name, dir, action, priority } = req.body || {};
    if (!name || typeof name !== 'string') {
      return res.status(400).json({ ok: false, message: '缺少文件名' });
    }
    const baseDir = resolveDirSafe((dir || '').toString()) || MOUNT_DIR;
    const safeName = path.basename(name);
    const videoPath = path.join(baseDir, safeName);
    if (!fs.existsSync(videoPath)) {
      return res.status(404).json({ ok: false, message: '视频文件不存在' });
    }
    if (!isVideoFileName(videoPath, VIDEO_EXTS)) {
      return res.status(400).json({ ok: false, message: '不支持的视频类型' });
    }

    // 新增：记录任务所属目录（相对挂载点），用于列表页按目录匹配
    const dirRel = path.relative(MOUNT_DIR, baseDir).split(path.sep).join('/');

    // priority: 将 sequence 设为全局最大值+1
    if (priority) {
      try {
        const maxRow = await prisma.mediaFile.findFirst({ orderBy: { sequence: 'desc' }, select: { sequence: true } });
        const nextSeq = ((maxRow && (maxRow.sequence || 0)) || 0) + 1;
        await prisma.mediaFile.upsert({
          where: { dirRel_name: { dirRel, name: safeName } },
          create: { dirRel, name: safeName, sequence: nextSeq },
          update: { sequence: nextSeq },
        });
      } catch (_) { }

      // 仅设置优先时不创建任务，直接返回
      if (!action) {
        return res.json({ ok: true, message: '已更新优先顺序' });
      }
    }

    const subs = detectSubtitles(baseDir, safeName);
    const act = (action || 'generate').toString();

    if (act === 'generate' && subs && subs.base) {
      return res.status(400).json({ ok: false, code: 'HAS_SUBTITLE', message: '已有原字幕，不能开始生成' });
    }

    // 仅当真正要生成字幕时检查 Key
    if (act === 'generate') {
      try { await getSnClient(); } catch (e) {
        if (e && e.code === 'NO_API_KEY') {
          return res.status(400).json({ ok: false, code: 'NO_API_KEY', message: '未配置识别服务的 API Key，请前往“设置”页面填写 OpenAI Key' });
        }
        return res.status(400).json({ ok: false, message: String((e && e.message) || e) });
      }
    }

    if (act === 'translate_subscribe') {
      const taskId = `tr-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      setTaskState(taskId, { dirRel, name: safeName, status: 'queued', stage: '排队中', progress: 0, message: '等待翻译任务开始…' });
      setTimeout(() => { // 占位：立刻标记为空闲
        const t = activeTasks.get(taskId);
        if (t) { activeTasks.delete(taskId); }
      }, 10 * 1000);
      return res.json({ ok: true, message: '翻译任务已入队（占位）', taskId });
    }

    // 生成字幕：改为进入单并发队列，立即返回 taskId
    const taskId = `gen-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const ext = path.extname(safeName);
    const base = path.basename(safeName, ext);
    const srtPath = path.join(baseDir, `${base}.srt`);

    // 初始化任务状态为排队
    setTaskState(taskId, { dirRel, name: safeName, status: 'queued', stage: '排队中', progress: 0, message: '等待开始…' });

    // 入队并尝试启动
    taskQueue.push({ taskId, baseDir, safeName, videoPath, srtPath });
    startNextTask();

    return res.json({ ok: true, message: '生成字幕任务已入队', taskId });
  } catch (err) {
    console.error('[/api/tasks/create-from-file] Error:', err && (err.stack || err));
    return res.status(500).json({ ok: false, message: '创建任务失败', detail: String((err && (err.message || err)) || err) });
  }
});

// legacy route removed: /transcribe-subtitle-vad

// legacy route removed: /vad-segments

/* duplicate resolveDirSafe/detectSubtitles removed */

// legacy route removed: /api/dirs
