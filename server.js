const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// 配置静态资源托管，使前端静态页面可以通过根路径直接访问
app.use(express.static(path.join(__dirname, 'public')));

// 全局状态内存存储
let clients = {}; // 记录当前在线的设备信息。结构: { socketId: { id, name } }
let transferTasks = {}; // 记录进行中的文件传输任务。结构: { taskToken: { senderId, targetId, downloadRes, uploadReq, status } }

/**
 * 封装统一的错误通知函数
 * 当后端发生逻辑错误、参数校验失败或通道异常断开时，通过 Socket 实时通知前端并在日志面板标红
 * @param {string} socketId - 接收通知的目标 Socket ID
 * @param {string} message - 错误描述信息
 */
function notifyError(socketId, message) {
  if (io.sockets.sockets.get(socketId)) {
    io.to(socketId).emit('transfer-error', { message });
  }
}

// 监听信令服务器的 WebSocket 连接
io.on('connection', (socket) => {
  // 当有新设备连接时，为其生成一个基于 ID 缩写的设备别名
  clients[socket.id] = {
    id: socket.id,
    name: `设备_${socket.id.substring(0, 4)}`,
  };
  // 广播全网：更新所有人控制面板上的在线设备列表
  io.emit('update-room', Object.values(clients));

  /**
   * 功能一：处理普通文本/网址的实时快传
   * 由于文本体积小，直接通过 WebSocket 信令通道安全、秒级地分发，不需要走 HTTP 流管道
   */
  socket.on('text-share', (data) => {
    try {
      const { targetId, content } = data || {};

      // 安全与边界校验
      if (!targetId || !content) {
        return notifyError(socket.id, '文本发送失败：参数不完整。');
      }
      if (!clients[targetId]) {
        return notifyError(socket.id, '文本发送失败：目标设备已离线。');
      }

      // 直接向目标接收端投递文本内容，附带发送者昵称
      io.to(targetId).emit('incoming-text', {
        senderName: clients[socket.id].name,
        content: content,
      });
    } catch (err) {
      notifyError(socket.id, '服务器处理文本传输时发生未知内部错误。');
    }
  });

  /**
   * 功能二：收到发送方发起的【文件】传输邀请
   */
  socket.on('file-share-invite', (data) => {
    try {
      // 防御空数据解构报错
      const { targetId, fileName, fileType } = data || {};

      if (!targetId || !fileName) {
        return notifyError(socket.id, '发送传输邀请失败：参数不完整。');
      }
      if (!clients[targetId]) {
        return notifyError(socket.id, '发送传输邀请失败：目标设备已离线。');
      }

      // 动态生成唯一传输令牌（Task Token），作为后续两端 HTTP 握手、配对的唯一凭证
      const taskToken = 'task_' + Math.random().toString(36).substring(2, 9);

      // 初始化传输任务上下文，留空 HTTP 句柄，等待双方建立连接
      transferTasks[taskToken] = {
        senderId: socket.id,
        targetId: targetId,
        downloadRes: null, // 预留给接收端下载响应
        uploadReq: null, // 预留给发送端上传请求
        status: 'waiting-receiver',
      };

      // 将文件元数据通过信令通道转发给接收方，触发前端的确认弹窗
      io.to(targetId).emit('incoming-file', {
        taskToken,
        fileName,
        fileType,
        senderName: clients[socket.id].name,
      });
    } catch (err) {
      notifyError(socket.id, '服务器处理传输邀请时发生未知内部错误。');
    }
  });

  /**
   * 功能三：处理接收方主动拒绝文件邀请
   */
  socket.on('file-share-decline', (data) => {
    const { taskToken } = data || {};
    if (!taskToken) return;

    const task = transferTasks[taskToken];
    if (task) {
      // 通知发送方：对方拒绝了你
      io.to(task.senderId).emit('invite-declined', { taskToken });
      // 及时销毁对应的内存任务对象
      delete transferTasks[taskToken];
    }
  });

  /**
   * 功能四：监听设备意外离线（如关闭网页、断网）
   */
  socket.on('disconnect', () => {
    // 遍历任务，强行终结与该断开设备关联的所有未完成传输任务
    for (const token in transferTasks) {
      const task = transferTasks[token];
      if (task.senderId === socket.id || task.targetId === socket.id) {
        // 算出谁是被留在原地的另一方设备 (Peer)
        const peerId =
          task.senderId === socket.id ? task.targetId : task.senderId;
        notifyError(peerId, '由于对方断开网络，当前传输任务已被动终止。');

        // 强行关闭还吊着的 HTTP 响应和请求，防止连接悬挂、内存泄漏
        if (task.downloadRes && !task.downloadRes.writableEnded) {
          task.downloadRes.end();
        }
        if (task.uploadReq) {
          task.uploadReq.destroy(); // 强制掐断发送方的 POST 请求流
        }
        delete transferTasks[token];
      }
    }
    // 移出在线名册，同步通知所有人
    delete clients[socket.id];
    io.emit('update-room', Object.values(clients));
  });
});

/**
 * 功能五：接收端长连接下载接口 (HTTP GET)
 * 接收端点击同意后会触发。此接口不读取磁盘，而是保持连接（死等发送方推流）。
 */
app.get('/download', (req, res) => {
  const { taskToken, fileName, fileType } = req.query;

  // 参数边界校验
  if (!taskToken || !transferTasks[taskToken]) {
    return res.status(444).send('任务不存在、已过期或格式非法');
  }

  const task = transferTasks[taskToken];

  try {
    // 使用 RFC 5987 规范解决中文文件名在浏览器的下载乱码问题
    const safeFileName = encodeURIComponent(fileName || 'download');
    res.setHeader('Content-Type', fileType || 'application/octet-stream');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename*=UTF-8''${safeFileName}`,
    );
    // 核心：强制开启分块传输编码（Chunked Stream），允许流式写入，不需要预知文件总大小
    res.setHeader('Transfer-Encoding', 'chunked');

    // 挂载响应句柄，供后续 POST 上传流读取并直接对尾拼接
    task.downloadRes = res;
    task.status = 'ready';

    // 监听连接异常关闭（如用户在浏览器下载中途点击了“取消”）
    res.on('close', () => {
      if (transferTasks[taskToken]) {
        notifyError(task.senderId, '接收端已取消下载，推流强制中止。');
        if (task.uploadReq) {
          task.uploadReq.destroy(); // 顺便掐断发送方正在上传的数据，避免浪费带宽
        }
        delete transferTasks[taskToken];
      }
    });

    // 接收端管道已开辟，下发 WebSocket 信令，命令发送方立刻发起 POST 推流
    io.to(task.senderId).emit('start-upload-now', { taskToken });
  } catch (error) {
    notifyError(task.senderId, '服务器初始化下载流管道失败。');
    res.status(500).send('Internal Server Error');
  }
});

/**
 * 功能六：发送端推流接口 (HTTP POST)
 * 接收到 `start-upload-now` 信令后，发送方把本地选中的文件以二进制流形式 POST 过来。
 */
app.post('/upload', (req, res) => {
  const { taskToken } = req.query;

  if (!taskToken || !transferTasks[taskToken]) {
    return res.status(400).send('非法的传输令牌或任务已超时');
  }

  const task = transferTasks[taskToken];

  // 严苛验证：如果接收方的长连接在此时因为网络抖动断开了，拒绝推流
  if (!task.downloadRes || task.downloadRes.writableEnded) {
    notifyError(task.senderId, '推流失败：接收端的下载通道尚未建立或已关闭。');
    return res.status(400).send('接收端通道未就绪');
  }

  // 留存请求句柄，便于异常发生时主动销毁对冲
  task.uploadReq = req;

  /**
   * 🚀 实时流双向对撞核心 (req -> res)
   * 将发送方的请求可读流 (req)，利用内置管道 (pipe) 瞬间泵入接收方的响应可写流 (task.downloadRes)。
   * 数据在内存缓冲区横向转发，服务器磁盘零读写，极具高性能。
   */
  req.pipe(task.downloadRes);

  // 监听发送方网络崩溃、强制断网或异常报错
  req.on('error', (err) => {
    notifyError(task.targetId, '发送端上传数据流发生网络异常，传输中断。');
    // 发送方崩了，必须主动关掉接收方的长连接，避免接收方浏览器无休止地挂起卡死
    if (task.downloadRes && !task.downloadRes.writableEnded) {
      task.downloadRes.end();
    }
    delete transferTasks[taskToken];
  });

  // 正常流完结收尾
  req.on('end', () => {
    if (!res.writableEnded) res.status(200).send('上传成功'); // 响应发送方
    // 优雅关闭接收端通道，使其浏览器完成下载落盘
    if (task.downloadRes && !task.downloadRes.writableEnded) {
      task.downloadRes.end();
    }
    delete transferTasks[taskToken]; // 内存清理
  });
});

server.listen(3000, () => {
  console.log('🚀 项目已在本地运行: http://localhost:3000');
});
