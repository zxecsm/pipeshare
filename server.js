import crypto from 'node:crypto';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import QRCode from 'qrcode';
import { Server } from 'socket.io';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/* -------------------------------------------------------------------------- */
/*                                  配置常量                                   */
/* -------------------------------------------------------------------------- */

const PORT = Number.parseInt(process.env.PORT, 10) || 3000;
const HOST = process.env.HOST || '0.0.0.0';
// 部署在 Nginx 等反向代理后面时设为 1，限流才能按真实客户端 IP 计算
const TRUST_PROXY =
  process.env.TRUST_PROXY === '1' || process.env.TRUST_PROXY === 'true';

const ROOM_CODE_LENGTH = 6;
// 去除容易混淆的 I / L / O / 0 / 1，方便口头传达和手输
const ROOM_CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const MAX_ROOM_MEMBERS = 32; // 单个房间最多设备数
const MAX_ROOMS = 1000; // 服务器最多保留的房间数
const ROOM_EMPTY_TTL = 30 * 60 * 1000; // 房间空置 30 分钟后回收
const INVITE_TTL = 2 * 60 * 1000; // 收件方 2 分钟内未响应则邀请过期
const STALL_TIMEOUT = 60 * 1000; // 传输通道 60 秒无数据则判定卡死并中断
const MAX_TEXT_BYTES = 256 * 1024; // 单条文本消息上限
const MAX_NAME_LENGTH = 24;
const MAX_FILE_NAME_LENGTH = 255;
const MAX_ACTIVE_TASKS = 12; // 单设备同时进行中的传输任务上限
const MAX_BATCH_FILES = 8; // 一次最多批量发送的文件数

// 文件传输改为 WebSocket 分片：单帧 256KB，发送端最多同时保留 16 帧未确认数据。
// 4MB 的在途窗口既能跑满带宽，又能把服务端/接收端的内存占用压在一个固定上限内。
const CHUNK_SIZE = 256 * 1024;
const CHUNK_WINDOW = 16;
// 接收方已确认接收，但还在处理上一批文件时，任务最多保留 2 小时
// （批量里可能有超大文件，排队时间要留足）
const ACCEPTED_TASK_TTL = 2 * 60 * 60 * 1000;

/* -------------------------------------------------------------------------- */
/*                                  全局状态                                   */
/* -------------------------------------------------------------------------- */

/**
 * rooms: code -> { code, members: Map<socketId, { id, name, joinedAt }>, createdAt, emptySince }
 * socketRooms: socketId -> roomCode
 * transferTasks: taskToken -> task
 */
const rooms = new Map();
const socketRooms = new Map();
const transferTasks = new Map();
// 简易滑动窗口限流：key -> { count, resetAt }
const rateLimits = new Map();

const app = express();
if (TRUST_PROXY) app.set('trust proxy', true);
const server = http.createServer(app);
const io = new Server(server, {
  // 缩短心跳探测周期，让“假在线”（拔网线、休眠）更快被发现
  pingInterval: 20 * 1000,
  pingTimeout: 20 * 1000,
  maxHttpBufferSize: 1e6,
  // 信令包都很小，关闭压缩可以省掉 CPU 与内存开销，也避免压缩层成为瓶颈
  perMessageDeflate: false,
});

/* -------------------------------------------------------------------------- */
/*                                  工具函数                                   */
/* -------------------------------------------------------------------------- */

function roomChannel(code) {
  return `room:${code}`;
}

function normalizeRoomCode(raw) {
  if (typeof raw !== 'string') return '';
  return raw
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .slice(0, 12);
}

function sanitizeName(raw, fallback) {
  if (typeof raw !== 'string') return fallback;
  const cleaned = raw
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u001f\u007f]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, MAX_NAME_LENGTH);
  return cleaned || fallback;
}

function sanitizeFileName(raw) {
  if (typeof raw !== 'string') return '';
  return raw
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u001f\u007f]/g, '')
    .trim()
    .slice(0, MAX_FILE_NAME_LENGTH);
}

function sanitizeFileType(raw) {
  if (typeof raw !== 'string') return 'application/octet-stream';
  const cleaned = raw.trim().slice(0, 128);
  // 只接受标准 MIME 形式，避免响应头注入
  return /^[\w.+-]+\/[\w.+-]+(?:\s*;.*)?$/.test(cleaned)
    ? cleaned
    : 'application/octet-stream';
}

function generateRoomCode() {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    let code = '';
    for (let i = 0; i < ROOM_CODE_LENGTH; i += 1) {
      const index = crypto.randomInt(0, ROOM_CODE_ALPHABET.length);
      code += ROOM_CODE_ALPHABET[index];
    }
    if (!rooms.has(code)) return code;
  }
  // 极端情况下退回更长的随机房间码
  return crypto.randomBytes(6).toString('hex').toUpperCase();
}

function notifyError(socketId, message, taskToken, clientTaskId) {
  if (!socketId || !io.sockets.sockets.get(socketId)) return;
  const payload = { message };
  if (taskToken) payload.taskToken = taskToken;
  if (clientTaskId) payload.clientTaskId = clientTaskId;
  io.to(socketId).emit('transfer-error', payload);
}

function memberList(room) {
  return Array.from(room.members.values());
}

function emitRoomUpdate(room) {
  io.to(roomChannel(room.code)).emit('room:update', {
    code: room.code,
    members: memberList(room),
  });
}

function getRoomOfSocket(socketId) {
  const code = socketRooms.get(socketId);
  if (!code) return null;
  const room = rooms.get(code);
  if (!room || !room.members.has(socketId)) return null;
  return room;
}

/**
 * 轻量限流：防止有人疯狂试房间码、批量建房或刷邀请/文本。
 * 返回 true 表示本次操作允许通过。
 */
function allowRate(key, limit, windowMs = 60 * 1000) {
  const now = Date.now();
  let bucket = rateLimits.get(key);
  if (!bucket || now >= bucket.resetAt) {
    bucket = { count: 0, resetAt: now + windowMs };
    rateLimits.set(key, bucket);
  }
  bucket.count += 1;
  return bucket.count <= limit;
}

function clientIp(socket) {
  if (TRUST_PROXY) {
    const forwarded = socket.handshake.headers['x-forwarded-for'];
    if (typeof forwarded === 'string' && forwarded) {
      return forwarded.split(',')[0].trim() || 'unknown';
    }
  }
  return (socket.handshake && socket.handshake.address) || 'unknown';
}

function joinRoom(socket, room, nickname) {
  // 幂等：重复加入（例如断线重连）时先清掉旧房间关系
  if (socketRooms.get(socket.id) === room.code) {
    const existing = room.members.get(socket.id);
    if (existing) {
      existing.name = sanitizeName(
        nickname,
        existing.name || `设备_${socket.id.substring(0, 4)}`,
      );
    }
  } else {
    leaveRoom(socket);
    const name = sanitizeName(
      nickname,
      `设备_${socket.id.substring(0, 4)}`,
    );
    room.members.set(socket.id, {
      id: socket.id,
      name,
      joinedAt: Date.now(),
    });
    socket.join(roomChannel(room.code));
    socketRooms.set(socket.id, room.code);
  }

  room.emptySince = null;

  const me = room.members.get(socket.id);
  socket.emit('room:joined', {
    code: room.code,
    selfId: socket.id,
    selfName: me ? me.name : '',
    members: memberList(room),
    maxMembers: MAX_ROOM_MEMBERS,
    inviteTtlMs: INVITE_TTL,
  });
  emitRoomUpdate(room);
  return me;
}

function leaveRoom(socket) {
  const code = socketRooms.get(socket.id);
  if (!code) return;

  socketRooms.delete(socket.id);
  socket.leave(roomChannel(code));

  const room = rooms.get(code);
  if (!room) return;

  room.members.delete(socket.id);
  if (room.members.size === 0) {
    room.emptySince = Date.now();
  }
  emitRoomUpdate(room);
}

/* -------------------------------------------------------------------------- */
/*                                传输任务管理                                  */
/* -------------------------------------------------------------------------- */

function clearTaskTimers(task) {
  if (task.inviteTimer) {
    clearTimeout(task.inviteTimer);
    task.inviteTimer = null;
  }
  if (task.stallTimer) {
    clearInterval(task.stallTimer);
    task.stallTimer = null;
  }
}

/**
 * 统一的失败收尾函数。
 * 无论链路是“接收方取消 / 发送方断网 / 数据卡死 / 服务端异常”，都从这里退出，
 * 保证定时器、任务表不会残留。
 */
function abortTask(task, { senderMessage, targetMessage } = {}) {
  if (!task || !transferTasks.has(task.token)) return;
  transferTasks.delete(task.token);
  clearTaskTimers(task);
  task.aborted = true;

  if (senderMessage) {
    notifyError(task.senderId, senderMessage, task.token, task.clientTaskId);
  }
  if (targetMessage) {
    notifyError(task.targetId, targetMessage, task.token, task.clientTaskId);
  }

  // 只有在“等待接收方确认”阶段才需要帮对方关掉弹窗，
  // 已开始传输后的中断由 transfer-error 通知即可，避免重复提示。
  if (task.status === 'waiting-receiver') {
    io.to(task.targetId).emit('invite-cancelled', {
      taskToken: task.token,
      clientTaskId: task.clientTaskId,
      reason: targetMessage || senderMessage || '',
    });
  }
}

function completeTask(task) {
  if (!task || !transferTasks.has(task.token)) return;
  transferTasks.delete(task.token);
  clearTaskTimers(task);
  task.completed = true;

  const payload = {
    taskToken: task.token,
    clientTaskId: task.clientTaskId,
    bytes: task.bytes,
    total: task.fileSize,
    durationMs: Date.now() - task.startedAt,
  };
  io.to(task.senderId).emit('transfer-complete', payload);
  io.to(task.targetId).emit('transfer-complete', payload);
}

/**
 * 卡死监控：只要通道长时间没有任何字节流动，就主动掐断。
 * 这比“永远挂着一个半死连接”更可靠，也能避免浏览器一直转圈。
 */
function startStallWatch(task) {
  if (task.stallTimer) clearInterval(task.stallTimer);
  task.stallTimer = setInterval(() => {
    if (!transferTasks.has(task.token)) {
      clearInterval(task.stallTimer);
      task.stallTimer = null;
      return;
    }
    if (Date.now() - task.lastActivity > STALL_TIMEOUT) {
      const seconds = Math.round(STALL_TIMEOUT / 1000);
      abortTask(task, {
        senderMessage: `传输已中断：通道超过 ${seconds} 秒没有数据流动。`,
        targetMessage: `传输已中断：通道超过 ${seconds} 秒没有数据流动。`,
      });
    }
  }, 5000);
  if (typeof task.stallTimer.unref === 'function') task.stallTimer.unref();
}

function countActiveTasksFor(socketId) {
  let count = 0;
  for (const task of transferTasks.values()) {
    if (task.senderId === socketId || task.targetId === socketId) count += 1;
  }
  return count;
}

function abortTasksForSocket(socketId) {
  for (const task of Array.from(transferTasks.values())) {
    if (task.senderId === socketId) {
      abortTask(task, {
        targetMessage: '对方已离线，传输任务已终止。',
      });
    } else if (task.targetId === socketId) {
      abortTask(task, {
        senderMessage: '对方已离线，传输任务已终止。',
      });
    }
  }
}

function clearInviteTimer(task) {
  if (task && task.inviteTimer) {
    clearTimeout(task.inviteTimer);
    task.inviteTimer = null;
  }
}

/**
 * Socket.IO 在 Node 侧会把二进制附件解析成 Buffer，浏览器侧是 ArrayBuffer。
 * 统一算出字节数，避免把非法载荷当成 0 字节放过去。
 */
function chunkByteLength(value) {
  if (value == null) return -1;
  if (Buffer.isBuffer(value) || ArrayBuffer.isView(value)) return value.byteLength;
  if (value instanceof ArrayBuffer) return value.byteLength;
  return -1;
}

/** 进度包节流到 400ms，避免信令通道被淹没 */
function emitProgress(task) {
  const now = Date.now();
  if (now - task.lastProgressAt < 400) return;
  task.lastProgressAt = now;
  const progress = {
    taskToken: task.token,
    clientTaskId: task.clientTaskId,
    bytes: task.bytes,
    total: task.fileSize,
    elapsedMs: now - task.startedAt,
  };
  io.to(task.senderId).emit('transfer-progress', progress);
  io.to(task.targetId).emit('transfer-progress', progress);
}

/* -------------------------------------------------------------------------- */
/*                                   HTTP 接口                                 */
/* -------------------------------------------------------------------------- */

app.disable('x-powered-by');
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  next();
});

// 静态资源：HTML 每次校验，避免更新后浏览器继续跑旧页面
app.use(
  express.static(path.join(__dirname, 'public'), {
    etag: true,
    maxAge: 0,
    setHeaders(res, filePath) {
      const name = path.basename(filePath);
      if (name.endsWith('.html')) {
        res.setHeader('Cache-Control', 'no-cache');
      }
      // Service Worker 与 Manifest 必须每次回源校验，否则更新无法生效
      if (name === 'sw.js' || name === 'manifest.webmanifest') {
        res.setHeader('Cache-Control', 'no-cache');
      }
    },
  }),
);

// 健康检查，方便 Docker / 反向代理探活
app.get('/healthz', (req, res) => {
  res.json({
    ok: true,
    uptime: Math.round(process.uptime()),
    rooms: rooms.size,
    online: io.engine.clientsCount,
    transfers: transferTasks.size,
  });
});

// 房间邀请二维码（服务端生成 SVG，不依赖外网 CDN，内网也能用）
app.get('/api/qr', async (req, res) => {
  const text = typeof req.query.text === 'string' ? req.query.text : '';
  if (!text || text.length > 1024) {
    return res.status(400).type('text/plain').send('缺少二维码内容');
  }

  try {
    const svg = await QRCode.toString(text, {
      type: 'svg',
      margin: 1,
      width: 280,
      errorCorrectionLevel: 'M',
      color: { dark: '#0f172a', light: '#ffffff' },
    });
    res
      .type('image/svg+xml')
      .setHeader('Cache-Control', 'private, max-age=300')
      .send(svg);
  } catch (err) {
    console.error('[qr] 生成二维码失败:', err.message);
    res.status(500).type('text/plain').send('二维码生成失败');
  }
});

/* -------------------------------------------------------------------------- */
/*                                 WebSocket 信令                               */
/* -------------------------------------------------------------------------- */

io.on('connection', (socket) => {
  socket.emit('server:ready', {
    socketId: socket.id,
    inviteTtlMs: INVITE_TTL,
    maxRoomMembers: MAX_ROOM_MEMBERS,
  });

  /* ------------------------------- 房间操作 ------------------------------- */

  socket.on('room:create', (data) => {
    try {
      if (!allowRate(`create:${clientIp(socket)}`, 20)) {
        return socket.emit('room:error', {
          message: '创建房间过于频繁，请稍后再试。',
        });
      }
      if (rooms.size >= MAX_ROOMS) {
        return socket.emit('room:error', {
          message: '服务器房间数量已达上限，请稍后再试。',
        });
      }
      const { nickname } = data || {};
      const code = generateRoomCode();
      const room = {
        code,
        members: new Map(),
        createdAt: Date.now(),
        emptySince: null,
      };
      rooms.set(code, room);
      joinRoom(socket, room, nickname);
      console.log(`[room] ${socket.id} 创建房间 ${code}`);
    } catch (err) {
      console.error('[room:create]', err);
      socket.emit('room:error', { message: '创建房间失败，请重试。' });
    }
  });

  socket.on('room:join', (data) => {
    try {
      if (!allowRate(`join:${clientIp(socket)}`, 60)) {
        return socket.emit('room:error', {
          message: '尝试加入房间过于频繁，请稍后再试。',
        });
      }
      const { roomCode, nickname } = data || {};
      const code = normalizeRoomCode(roomCode);
      if (!code) {
        return socket.emit('room:error', { message: '请输入有效的房间码。' });
      }

      const room = rooms.get(code);
      if (!room) {
        return socket.emit('room:error', {
          code,
          message: '房间不存在或已过期，请确认房间码后重试。',
        });
      }

      if (!room.members.has(socket.id) && room.members.size >= MAX_ROOM_MEMBERS) {
        return socket.emit('room:error', {
          code,
          message: `房间人数已满（最多 ${MAX_ROOM_MEMBERS} 台设备）。`,
        });
      }

      joinRoom(socket, room, nickname);
      console.log(`[room] ${socket.id} 加入房间 ${code}`);
    } catch (err) {
      console.error('[room:join]', err);
      socket.emit('room:error', { message: '加入房间失败，请重试。' });
    }
  });

  socket.on('room:leave', () => {
    // 离开房间意味着放弃当前房间内的所有传输，避免任务悬挂
    abortTasksForSocket(socket.id);
    leaveRoom(socket);
    socket.emit('room:left');
  });

  /* ------------------------------- 文本快传 ------------------------------- */

  socket.on('text-share', (data) => {
    try {
      const { targetId, content } = data || {};

      const room = getRoomOfSocket(socket.id);
      if (!room) return notifyError(socket.id, '请先加入房间后再发送。');
      if (!allowRate(`text:${clientIp(socket)}`, 240)) {
        return notifyError(socket.id, '发送过于频繁，请稍后再试。');
      }

      if (typeof content !== 'string' || !content) {
        return notifyError(socket.id, '文本发送失败：内容为空。');
      }
      if (Buffer.byteLength(content, 'utf8') > MAX_TEXT_BYTES) {
        return notifyError(socket.id, '文本发送失败：内容超过 256KB。');
      }
      if (!targetId || targetId === socket.id) {
        return notifyError(socket.id, '文本发送失败：目标设备无效。');
      }
      if (!room.members.has(targetId)) {
        return notifyError(socket.id, '文本发送失败：目标设备不在当前房间。');
      }

      const sender = room.members.get(socket.id);
      io.to(targetId).emit('incoming-text', {
        senderId: socket.id,
        senderName: sender ? sender.name : '未知设备',
        content,
        receivedAt: Date.now(),
      });
    } catch (err) {
      console.error('[text-share]', err);
      notifyError(socket.id, '服务器处理文本传输时发生未知错误。');
    }
  });

  /* ------------------------------- 文件快传 ------------------------------- */

  socket.on('file-share-invite', (data) => {
    const clientTaskId =
      typeof data?.clientTaskId === 'string'
        ? data.clientTaskId.slice(0, 64)
        : '';
    try {
      const { targetId, fileName, fileType, fileSize } = data || {};

      const room = getRoomOfSocket(socket.id);
      if (!room) {
        return notifyError(
          socket.id,
          '请先加入房间后再发送文件。',
          null,
          clientTaskId,
        );
      }
      if (!allowRate(`invite:${clientIp(socket)}`, 120)) {
        return notifyError(
          socket.id,
          '发起传输过于频繁，请稍后再试。',
          null,
          clientTaskId,
        );
      }

      const safeName = sanitizeFileName(fileName);
      if (!targetId || targetId === socket.id || !safeName) {
        return notifyError(
          socket.id,
          '发送传输邀请失败：参数不完整。',
          null,
          clientTaskId,
        );
      }
      if (!room.members.has(targetId)) {
        return notifyError(
          socket.id,
          '发送传输邀请失败：目标设备不在当前房间。',
          null,
          clientTaskId,
        );
      }

      // 批量发送参数：batchId 相同的一组邀请会合并成接收端的一次确认
      const batchId =
        typeof data?.batchId === 'string' ? data.batchId.slice(0, 64) : '';
      let batchTotal = 1;
      let batchIndex = 1;
      if (batchId) {
        const parsedTotal = Number.parseInt(data?.batchTotal, 10);
        const parsedIndex = Number.parseInt(data?.batchIndex, 10);
        if (
          !Number.isInteger(parsedTotal) ||
          parsedTotal < 1 ||
          parsedTotal > MAX_BATCH_FILES ||
          !Number.isInteger(parsedIndex) ||
          parsedIndex < 1 ||
          parsedIndex > parsedTotal
        ) {
          return notifyError(
            socket.id,
            `批量发送参数无效（单次最多 ${MAX_BATCH_FILES} 个文件）。`,
            null,
            clientTaskId,
          );
        }
        batchTotal = parsedTotal;
        batchIndex = parsedIndex;
      }

      if (countActiveTasksFor(socket.id) >= MAX_ACTIVE_TASKS) {
        return notifyError(
          socket.id,
          `同时进行的传输任务过多（上限 ${MAX_ACTIVE_TASKS} 个），请稍候。`,
          null,
          clientTaskId,
        );
      }
      if (countActiveTasksFor(targetId) >= MAX_ACTIVE_TASKS) {
        return notifyError(
          socket.id,
          '对方当前进行中的传输任务过多，请稍后再试。',
          null,
          clientTaskId,
        );
      }

      const parsedSize = Number(fileSize);
      const safeSize =
        Number.isFinite(parsedSize) && parsedSize >= 0 ? parsedSize : 0;

      const token = `task_${crypto.randomBytes(16).toString('hex')}`;
      const task = {
        token,
        clientTaskId,
        batchId,
        batchTotal,
        batchIndex,
        senderId: socket.id,
        targetId,
        roomCode: room.code,
        fileName: safeName,
        fileType: sanitizeFileType(fileType),
        fileSize: safeSize,
        status: 'waiting-receiver',
        bytes: 0,
        nextSeq: 0,
        ackedSeq: -1,
        createdAt: Date.now(),
        acceptedAt: 0,
        startedAt: 0,
        lastActivity: Date.now(),
        lastProgressAt: 0,
        inviteTimer: null,
        stallTimer: null,
      };

      // 邀请超时：双方都收到明确反馈，任务不会泄漏
      task.inviteTimer = setTimeout(() => {
        if (!transferTasks.has(token) || task.status !== 'waiting-receiver') {
          return;
        }
        abortTask(task, {
          senderMessage: `对方在 ${Math.round(INVITE_TTL / 1000)} 秒内没有响应，邀请已过期。`,
          targetMessage: '传输邀请已过期。',
        });
      }, INVITE_TTL);
      if (typeof task.inviteTimer.unref === 'function') {
        task.inviteTimer.unref();
      }

      transferTasks.set(token, task);

      const sender = room.members.get(socket.id);
      io.to(targetId).emit('incoming-file', {
        taskToken: token,
        clientTaskId,
        batchId: task.batchId,
        batchTotal: task.batchTotal,
        batchIndex: task.batchIndex,
        fileName: task.fileName,
        fileType: task.fileType,
        fileSize: task.fileSize,
        senderId: socket.id,
        senderName: sender ? sender.name : '未知设备',
        expiresAt: Date.now() + INVITE_TTL,
      });
    } catch (err) {
      console.error('[file-share-invite]', err);
      notifyError(
        socket.id,
        '服务器处理传输邀请时发生未知错误。',
        null,
        clientTaskId,
      );
    }
  });

  socket.on('file-share-decline', (data) => {
    const { taskToken } = data || {};
    const task = transferTasks.get(taskToken);
    if (!task) return;
    // 只有接收方可以拒绝
    if (task.targetId !== socket.id) return;

    abortTask(task, {
      senderMessage: '对方拒绝了您的传输邀请。',
    });
  });

  socket.on('file-share-cancel', (data) => {
    const { taskToken } = data || {};
    const task = transferTasks.get(taskToken);
    if (!task) return;
    if (task.senderId !== socket.id && task.targetId !== socket.id) return;

    const bySender = task.senderId === socket.id;
    abortTask(task, {
      senderMessage: bySender ? '已取消传输。' : '对方取消了传输。',
      targetMessage: bySender ? '对方取消了传输。' : '已取消传输。',
    });
  });

  /* ------------------------------ 分片传输协议 ------------------------------ */

  /**
   * 接收方确认接收：先清掉邀请倒计时，任务进入 accepted 状态。
   * 批量接收时，接收方可以先把整批都 accept，再按顺序发 file-start，
   * 这样后面的文件不会因为等待前面的文件而邀请过期。
   */
  socket.on('file-accept', (data) => {
    const task = transferTasks.get(data && data.taskToken);
    if (!task) {
      return notifyError(
        socket.id,
        '传输任务不存在或已过期。',
        data && data.taskToken,
        data && data.clientTaskId,
      );
    }
    if (task.targetId !== socket.id) return;
    if (task.status !== 'waiting-receiver') return;
    if (!io.sockets.sockets.get(task.senderId)) {
      return abortTask(task, {
        targetMessage: '发送端已离线，传输已取消。',
      });
    }

    clearInviteTimer(task);
    task.status = 'accepted';
    task.acceptedAt = Date.now();
  });

  /** 接收方准备好落盘位置后，通知发送端开始按窗口推分片 */
  socket.on('file-start', (data) => {
    const task = transferTasks.get(data && data.taskToken);
    if (!task) {
      return notifyError(
        socket.id,
        '传输任务不存在或已过期，请让对方重新发送。',
        data && data.taskToken,
        data && data.clientTaskId,
      );
    }
    if (task.targetId !== socket.id) return;
    // 重复的 file-start 按幂等处理，不再重复通知发送端
    if (task.status === 'transferring') return;
    if (task.status !== 'accepted' && task.status !== 'waiting-receiver') {
      return;
    }
    if (!io.sockets.sockets.get(task.senderId)) {
      return abortTask(task, {
        targetMessage: '发送端已离线，传输已取消。',
      });
    }

    clearInviteTimer(task);
    task.status = 'transferring';
    task.startedAt = Date.now();
    task.lastActivity = Date.now();
    task.bytes = 0;
    task.nextSeq = 0;
    task.ackedSeq = -1;
    task.lastProgressAt = 0;

    io.to(task.senderId).emit('start-upload-now', {
      taskToken: task.token,
      clientTaskId: task.clientTaskId,
      fileName: task.fileName,
      fileSize: task.fileSize,
      chunkSize: CHUNK_SIZE,
      windowSize: CHUNK_WINDOW,
    });

    startStallWatch(task);
  });

  /**
   * 发送端分片：服务端只做转发，不缓存。接收方写完后回 ack，
   * 服务端把 ack 转给发送端；发送端凭 ack 滑动窗口，
   * 因此在途数据永远不超过 windowSize 个分片，内存占用有上界。
   */
  socket.on('file-chunk', (data) => {
    const task = transferTasks.get(data && data.taskToken);
    // 任务已被取消/超时清理：迟到的分片直接丢弃
    if (!task) return;
    if (task.senderId !== socket.id) return;
    if (task.status !== 'transferring') return;

    const seq = Number(data.seq);
    const size = chunkByteLength(data.data);

    if (!Number.isInteger(seq) || seq < 0 || size < 0) {
      return abortTask(task, {
        senderMessage: '分片格式无效，传输已中断。',
        targetMessage: '发送端数据异常，传输已中断。',
      });
    }
    if (seq !== task.nextSeq) {
      return abortTask(task, {
        senderMessage: `分片顺序异常（收到 #${seq}，期望 #${task.nextSeq}），传输已中断。`,
        targetMessage: '发送端数据顺序异常，传输已中断。',
      });
    }
    if (size > CHUNK_SIZE) {
      return abortTask(task, {
        senderMessage: `单个分片超过 ${CHUNK_SIZE} 字节上限，传输已中断。`,
        targetMessage: '发送端数据异常，传输已中断。',
      });
    }
    if (task.fileSize >= 0 && task.bytes + size > task.fileSize) {
      return abortTask(task, {
        senderMessage: '发送的数据超过声明的文件大小，传输已中断。',
        targetMessage: '收到的数据异常，传输已中断。',
      });
    }
    // 防御性流控：正常客户端最多 CHUNK_WINDOW 片未确认；
    // 留 2 倍余量，超过就说明对端没有遵守窗口，直接中断避免撑爆内存。
    if (task.nextSeq - (task.ackedSeq + 1) >= CHUNK_WINDOW * 2) {
      return abortTask(task, {
        senderMessage: '发送端未遵守流控窗口，传输已中断。',
        targetMessage: '数据流异常，传输已中断。',
      });
    }

    io.to(task.targetId).emit('file-chunk', {
      taskToken: task.token,
      clientTaskId: task.clientTaskId,
      seq,
      data: data.data,
    });

    task.nextSeq += 1;
    task.bytes += size;
    task.lastActivity = Date.now();
    emitProgress(task);
  });

  /** 接收方每写完一个分片才回 ack，形成端到端背压 */
  socket.on('file-chunk-ack', (data) => {
    const task = transferTasks.get(data && data.taskToken);
    if (!task) return;
    if (task.targetId !== socket.id) return;

    const seq = Number(data.seq);
    if (!Number.isInteger(seq) || seq < 0) return;

    if (seq > task.ackedSeq) task.ackedSeq = seq;
    task.lastActivity = Date.now();
    io.to(task.senderId).emit('file-chunk-ack', {
      taskToken: task.token,
      clientTaskId: task.clientTaskId,
      seq,
    });
  });

  /** 发送端确认所有分片都已 ack，服务端做最后的字节数校验 */
  socket.on('file-complete', (data) => {
    const task = transferTasks.get(data && data.taskToken);
    if (!task) return;
    if (task.senderId !== socket.id) return;
    if (task.status !== 'transferring') return;

    if (task.bytes !== task.fileSize) {
      return abortTask(task, {
        senderMessage: `数据量校验失败（已传 ${task.bytes} / 应为 ${task.fileSize} 字节），传输已中断。`,
        targetMessage: '收到的数据不完整，传输已中断，请让对方重新发送。',
      });
    }
    if (task.ackedSeq !== task.nextSeq - 1) {
      return abortTask(task, {
        senderMessage: '接收端尚未确认全部分片，传输已中断。',
        targetMessage: '数据接收未完成，传输已中断。',
      });
    }

    completeTask(task);
  });

  /** 接收端写盘失败（磁盘满、权限被收回等） */
  socket.on('file-receive-error', (data) => {
    const task = transferTasks.get(data && data.taskToken);
    if (!task || task.targetId !== socket.id) return;
    const raw = typeof data.message === 'string' ? data.message : '';
    const message = raw.slice(0, 200) || '未知错误';
    abortTask(task, {
      senderMessage: `接收端保存文件失败：${message}`,
      targetMessage: `保存文件失败：${message}`,
    });
  });

  /** 发送端读取本地文件失败（文件被移动、权限被撤销等） */
  socket.on('file-send-error', (data) => {
    const task = transferTasks.get(data && data.taskToken);
    if (!task || task.senderId !== socket.id) return;
    const raw = typeof data.message === 'string' ? data.message : '';
    const message = raw.slice(0, 200) || '未知错误';
    abortTask(task, {
      senderMessage: `读取本地文件失败：${message}`,
      targetMessage: '发送端读取文件失败，传输已中断。',
    });
  });

  /* -------------------------------- 断线处理 ------------------------------- */

  socket.on('disconnect', (reason) => {
    abortTasksForSocket(socket.id);
    leaveRoom(socket);
    if (reason !== 'client namespace disconnect') {
      console.log(`[socket] ${socket.id} 断开连接: ${reason}`);
    }
  });

  socket.on('error', (err) => {
    console.error(`[socket] ${socket.id} 错误:`, err && err.message);
  });
});

/* -------------------------------------------------------------------------- */
/*                              后台清理与优雅退出                              */
/* -------------------------------------------------------------------------- */

// 定期回收空房间，避免房间表无限增长
const roomGcTimer = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (room.members.size === 0 && room.emptySince && now - room.emptySince > ROOM_EMPTY_TTL) {
      rooms.delete(code);
    }
  }
  for (const [key, bucket] of rateLimits) {
    if (now >= bucket.resetAt) rateLimits.delete(key);
  }
  // 已确认接收但迟迟不开始的任务（例如接收方一直停留在上一批），避免永久占用
  for (const task of Array.from(transferTasks.values())) {
    if (task.status === 'accepted' && now - task.acceptedAt > ACCEPTED_TASK_TTL) {
      abortTask(task, {
        senderMessage: '接收方长时间未开始接收，传输已取消。',
        targetMessage: '传输等待超时，已取消。',
      });
    }
  }
}, 60 * 1000);
if (typeof roomGcTimer.unref === 'function') roomGcTimer.unref();

function shutdown(signal) {
  console.log(`\n收到 ${signal}，正在关闭服务...`);
  clearInterval(roomGcTimer);
  for (const task of Array.from(transferTasks.values())) {
    abortTask(task, {
      senderMessage: '服务器正在重启，传输已中断。',
      targetMessage: '服务器正在重启，传输已中断。',
    });
  }
  io.close(() => {
    server.close(() => process.exit(0));
  });
  // 兜底：10 秒内没退干净就强制退出
  setTimeout(() => process.exit(0), 10_000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err);
});
process.on('unhandledRejection', (err) => {
  console.error('[unhandledRejection]', err);
});

// 文件传输已改为 WebSocket 分片，不再有长连接 HTTP 上传，
// 这里只需保证普通请求和 WebSocket 心跳有合理的超时。
server.headersTimeout = 65 * 1000;
server.keepAliveTimeout = 72 * 1000;

server.listen(PORT, HOST, () => {
  console.log(`🚀 PipeShare 已启动: http://localhost:${PORT}`);
});
