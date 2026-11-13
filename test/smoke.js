/**
 * 端到端冒烟测试：
 *   node test/smoke.js
 *
 * 覆盖：房间隔离、文本快传、WebSocket 分片文件传输、二维码生成、PWA 资源、错误分支。
 */

import assert from 'node:assert';
import http from 'node:http';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { io } from 'socket.io-client';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = Number(process.env.TEST_PORT) || 3123;
const BASE = `http://127.0.0.1:${PORT}`;

let serverProcess;

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function once(socket, event, timeoutMs = 5000) {
  const promise = new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.off(event, handler);
      reject(new Error(`等待事件超时: ${event}`));
    }, timeoutMs);
    function handler(payload) {
      clearTimeout(timer);
      socket.off(event, handler);
      resolve(payload);
    }
    socket.on(event, handler);
  });
  // 失败场景下可能有未被 await 的监听器，避免产生 unhandledRejection 噪音
  promise.catch(() => {});
  return promise;
}

function collect(socket, event, count, timeoutMs = 10000) {
  const promise = new Promise((resolve, reject) => {
    const items = [];
    const timer = setTimeout(() => {
      socket.off(event, handler);
      reject(new Error(`等待 ${count} 个 ${event} 超时，只收到 ${items.length} 个`));
    }, timeoutMs);
    function handler(payload) {
      items.push(payload);
      if (items.length >= count) {
        clearTimeout(timer);
        socket.off(event, handler);
        resolve(items);
      }
    }
    socket.on(event, handler);
  });
  promise.catch(() => {});
  return promise;
}

async function waitForServer() {
  for (let i = 0; i < 60; i += 1) {
    try {
      const res = await fetch(`${BASE}/healthz`);
      if (res.ok) return await res.json();
    } catch {
      /* 还没起来 */
    }
    await wait(150);
  }
  throw new Error('服务端启动超时');
}

function connectClient() {
  return new Promise((resolve, reject) => {
    const socket = io(BASE, {
      transports: ['websocket'],
      reconnection: false,
      timeout: 4000,
    });
    socket.once('connect', () => resolve(socket));
    socket.once('connect_error', reject);
  });
}

function httpGet(url) {
  return new Promise((resolve) => {
    http
      .get(url, (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () =>
          resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }),
        );
      })
      .on('error', (err) => resolve({ status: 0, error: err, body: Buffer.alloc(0) }));
  });
}

/**
 * 模拟接收端：收到分片后立即可靠落盘（这里是内存），写完才回 ack。
 * 返回 Promise，transfer-complete 时给出拼好的完整文件。
 */
function receiveFile(socket, invite, { timeoutMs = 30000, autoStart = true } = {}) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let settled = false;

    const timer = setTimeout(() => {
      finish(new Error(`接收超时: ${invite.fileName}`));
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timer);
      socket.off('file-chunk', onChunk);
      socket.off('transfer-complete', onComplete);
      socket.off('transfer-error', onError);
    }

    function finish(err, payload) {
      if (settled) return;
      settled = true;
      cleanup();
      if (err) reject(err);
      else
        resolve({
          buffer: Buffer.concat(chunks),
          bytes: payload.bytes,
          durationMs: payload.durationMs,
        });
    }

    function onChunk(data) {
      if (!data || data.taskToken !== invite.taskToken) return;
      const chunk = Buffer.isBuffer(data.data)
        ? data.data
        : Buffer.from(data.data);
      chunks.push(Buffer.from(chunk));
      socket.emit('file-chunk-ack', {
        taskToken: invite.taskToken,
        seq: data.seq,
      });
    }

    function onComplete(data) {
      if (!data || data.taskToken !== invite.taskToken) return;
      finish(null, data);
    }

    function onError(data) {
      if (!data || data.taskToken !== invite.taskToken) return;
      finish(new Error(data.message || '传输错误'));
    }

    socket.on('file-chunk', onChunk);
    socket.on('transfer-complete', onComplete);
    socket.on('transfer-error', onError);

    if (autoStart) {
      socket.emit('file-accept', { taskToken: invite.taskToken });
      socket.emit('file-start', { taskToken: invite.taskToken });
    }
  });
}

/**
 * 模拟发送端：按服务端下发的窗口/分片大小推送数据，收到 ack 才继续。
 * opts.byteLimit 可以少发一些字节，用于测试“声明大小与实际不符”。
 * opts.cancelAfterChunks 在发送若干片后主动取消，用于测试中途终止。
 */
function sendFile(socket, clientTaskId, buffer, opts = {}) {
  const {
    timeoutMs = 30000,
    byteLimit = buffer.length,
    cancelAfterChunks = null,
  } = opts;
  const payload = buffer.subarray(0, byteLimit);

  return new Promise((resolve, reject) => {
    let taskToken = '';
    let state = null;
    let settled = false;

    const timer = setTimeout(() => {
      fail(new Error(`发送超时: ${clientTaskId}`));
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timer);
      socket.off('start-upload-now', onStart);
      socket.off('file-chunk-ack', onAck);
      socket.off('transfer-complete', onComplete);
      socket.off('transfer-error', onError);
    }

    function succeed(payloadData) {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(payloadData);
    }

    function fail(err) {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    }

    function maybeFinish() {
      if (!state || state.completed || !taskToken) return;
      if (state.sentSeq >= state.totalChunks && state.inFlight === 0) {
        state.completed = true;
        socket.emit('file-complete', {
          taskToken,
          bytes: buffer.length,
        });
      }
    }

    function pump() {
      if (!state || state.pumping || state.completed) return;
      state.pumping = true;
      try {
        while (
          !state.completed &&
          state.inFlight < state.window &&
          state.sentSeq < state.totalChunks
        ) {
          const start = state.sentSeq * state.chunk;
          const end = Math.min(start + state.chunk, payload.length);
          const chunk = payload.subarray(start, end);

          socket.emit('file-chunk', {
            taskToken,
            seq: state.sentSeq,
            data: chunk,
          });
          state.sentSeq += 1;
          state.inFlight += 1;

          if (
            cancelAfterChunks != null &&
            state.sentSeq >= cancelAfterChunks
          ) {
            state.completed = true;
            socket.emit('file-share-cancel', { taskToken });
            succeed({ cancelled: true });
            return;
          }
        }
        maybeFinish();
      } finally {
        state.pumping = false;
      }
    }

    function onStart(data) {
      if (!data || data.clientTaskId !== clientTaskId) return;
      taskToken = data.taskToken;
      const chunk = Number(data.chunkSize) || 512 * 1024;
      state = {
        chunk,
        window: Number(data.windowSize) || 16,
        totalChunks: Math.ceil(payload.length / chunk),
        sentSeq: 0,
        ackedSeq: -1,
        inFlight: 0,
        pumping: false,
        completed: false,
      };
      pump();
    }

    function onAck(data) {
      if (!state || !data || data.taskToken !== taskToken) return;
      if (data.seq > state.ackedSeq) {
        state.ackedSeq = data.seq;
        state.inFlight = Math.max(0, state.sentSeq - (state.ackedSeq + 1));
      }
      pump();
    }

    function onComplete(data) {
      if (!data || data.clientTaskId !== clientTaskId) return;
      succeed(data);
    }

    function onError(data) {
      if (!data || data.clientTaskId !== clientTaskId) return;
      fail(new Error(data.message || '传输错误'));
    }

    socket.on('start-upload-now', onStart);
    socket.on('file-chunk-ack', onAck);
    socket.on('transfer-complete', onComplete);
    socket.on('transfer-error', onError);
  });
}

function joinRoom(socket, roomCode, nickname) {
  const joined = once(socket, 'room:joined');
  socket.emit('room:join', { roomCode, nickname });
  return joined;
}

async function main() {
  serverProcess = spawn(process.execPath, ['server.js'], {
    cwd: path.join(__dirname, '..'),
    env: { ...process.env, PORT: String(PORT), HOST: '127.0.0.1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let serverLog = '';
  serverProcess.stdout.on('data', (d) => {
    serverLog += d.toString();
  });
  serverProcess.stderr.on('data', (d) => {
    serverLog += d.toString();
  });

  const sockets = [];
  try {
    const health = await waitForServer();
    assert.strictEqual(health.ok, true, 'healthz 应返回 ok');

    /* ------------------------------ PWA 资源 ------------------------------ */
    const manifestRes = await httpGet(`${BASE}/manifest.webmanifest`);
    assert.strictEqual(manifestRes.status, 200);
    assert.match(
      manifestRes.headers['content-type'] || '',
      /application\/manifest\+json/,
    );
    const manifest = JSON.parse(manifestRes.body.toString('utf8'));
    assert.strictEqual(manifest.display, 'standalone');
    assert.ok(
      manifest.icons.some((icon) => icon.sizes === '192x192'),
      'manifest 应包含 192 图标',
    );
    assert.ok(
      manifest.icons.some(
        (icon) => icon.sizes === '512x512' && icon.purpose === 'maskable',
      ),
      'manifest 应包含 512 maskable 图标',
    );

    const swRes = await httpGet(`${BASE}/sw.js`);
    assert.strictEqual(swRes.status, 200);
    assert.match(swRes.headers['content-type'] || '', /javascript/);
    const swBody = swRes.body.toString('utf8');
    assert.match(swBody, /CACHE_NAME/);
    assert.match(swBody, /notificationclick/);
    assert.match(swBody, /pipeshare-shell-v5/);
    assert.match(swRes.headers['cache-control'] || '', /no-cache/);

    const iconRes = await httpGet(`${BASE}/icons/icon-512.png`);
    assert.strictEqual(iconRes.status, 200);
    assert.match(iconRes.headers['content-type'] || '', /image\/png/);
    assert.strictEqual(iconRes.body.readUInt32BE(16), 512);
    assert.strictEqual(iconRes.body.readUInt32BE(20), 512);

    const alice = await connectClient();
    const bob = await connectClient();
    const carol = await connectClient();
    sockets.push(alice, bob, carol);

    /* ------------------------------ 房间创建 / 加入 ------------------------------ */
    const aliceJoined = once(alice, 'room:joined');
    alice.emit('room:create', { nickname: 'Alice' });
    const room = await aliceJoined;
    assert.match(room.code, /^[A-Z0-9]{6}$/, '房间码格式应为 6 位');
    assert.strictEqual(room.selfName, 'Alice');

    const membersPromise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        alice.off('room:update', handler);
        reject(new Error('等待房间成员更新超时'));
      }, 5000);
      function handler(data) {
        if (data.members && data.members.length === 2) {
          clearTimeout(timer);
          alice.off('room:update', handler);
          resolve(data);
        }
      }
      alice.on('room:update', handler);
    });

    const bobJoined = await joinRoom(bob, room.code, 'Bob');
    assert.strictEqual(bobJoined.code, room.code);
    const membersUpdate = await membersPromise;
    assert.strictEqual(membersUpdate.members.length, 2, '房间内应有 2 台设备');

    /* ------------------------------ 错误房间码 ------------------------------ */
    const carolError = once(carol, 'room:error');
    carol.emit('room:join', { roomCode: 'ZZZZZZ', nickname: 'Carol' });
    const err = await carolError;
    assert.match(err.message, /不存在|过期/);

    /* ------------------------------ 文本快传 ------------------------------ */
    const aliceText = once(alice, 'incoming-text');
    bob.emit('text-share', { targetId: alice.id, content: 'hello alice' });
    const textPayload = await aliceText;
    assert.strictEqual(textPayload.content, 'hello alice');
    assert.strictEqual(textPayload.senderName, 'Bob');

    // 不同房间隔离：Carol 自己建房后不能给 Alice 发消息
    const carolRoom = await new Promise((resolve) => {
      carol.emit('room:create', { nickname: 'Carol' });
      carol.once('room:joined', resolve);
    });
    assert.notStrictEqual(carolRoom.code, room.code);

    const isolationError = once(carol, 'transfer-error');
    carol.emit('text-share', { targetId: alice.id, content: 'should fail' });
    const isoErr = await isolationError;
    assert.match(isoErr.message, /不在当前房间|请先加入房间/);

    /* ------------------------------ 跨房间文件邀请拒绝 ------------------------------ */
    const crossFileError = once(carol, 'transfer-error');
    carol.emit('file-share-invite', {
      targetId: alice.id,
      fileName: 'cross-room.txt',
      fileType: 'text/plain',
      fileSize: 10,
      clientTaskId: 'cross-room-task',
    });
    const crossErr = await crossFileError;
    assert.match(crossErr.message, /不在当前房间/);
    assert.strictEqual(crossErr.clientTaskId, 'cross-room-task');

    /* ------------------------------ 拒绝邀请流程 ------------------------------ */
    const declineInvite = once(bob, 'incoming-file');
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: 'decline.txt',
      fileType: 'text/plain',
      fileSize: 10,
      clientTaskId: 'decline-task',
    });
    const declinePayload = await declineInvite;
    const declineError = once(alice, 'transfer-error');
    bob.emit('file-share-decline', { taskToken: declinePayload.taskToken });
    const declineErr = await declineError;
    assert.match(declineErr.message, /拒绝/);

    /* ------------------------------ WebSocket 分片传输 ------------------------------ */
    const fileBuffer = Buffer.alloc(512 * 1024 + 13);
    for (let i = 0; i < fileBuffer.length; i += 1) fileBuffer[i] = i % 251;

    const bobInvitePromise = once(bob, 'incoming-file');
    const senderPromise = sendFile(alice, 'client-task-1', fileBuffer);
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: '测试文件.bin',
      fileType: 'application/octet-stream',
      fileSize: fileBuffer.length,
      clientTaskId: 'client-task-1',
    });

    const invite = await bobInvitePromise;
    assert.strictEqual(invite.fileName, '测试文件.bin');
    assert.strictEqual(invite.clientTaskId, 'client-task-1');

    const receivePromise = receiveFile(bob, invite);
    const receiveResult = await receivePromise;
    const sendResult = await senderPromise;

    assert.strictEqual(receiveResult.buffer.length, fileBuffer.length, '接收端字节数应一致');
    assert.ok(receiveResult.buffer.equals(fileBuffer), '接收端内容应与发送端一致');
    assert.strictEqual(sendResult.bytes, fileBuffer.length);

    /* ------------------------------ 空文件 ------------------------------ */
    const emptyInvitePromise = once(bob, 'incoming-file');
    const emptySender = sendFile(alice, 'empty-task', Buffer.alloc(0));
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: 'empty.bin',
      fileType: 'application/octet-stream',
      fileSize: 0,
      clientTaskId: 'empty-task',
    });
    const emptyInvite = await emptyInvitePromise;
    const emptyReceive = await receiveFile(bob, emptyInvite);
    await emptySender;
    assert.strictEqual(emptyReceive.buffer.length, 0, '空文件应能正常完成');

    /* ------------------------------ 并发多任务互不串包 ------------------------------ */
    const bufA = Buffer.alloc(700 * 1024 + 7, 65);
    const bufB = Buffer.alloc(900 * 1024 + 11, 66);

    const invitesPromise = collect(bob, 'incoming-file', 2, 8000);
    const sendAPromise = sendFile(alice, 'concurrent-a', bufA);
    const sendBPromise = sendFile(alice, 'concurrent-b', bufB);
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: 'concurrent-a.bin',
      fileType: 'application/octet-stream',
      fileSize: bufA.length,
      clientTaskId: 'concurrent-a',
      batchId: 'batch-concurrent',
      batchTotal: 2,
      batchIndex: 1,
    });
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: 'concurrent-b.bin',
      fileType: 'application/octet-stream',
      fileSize: bufB.length,
      clientTaskId: 'concurrent-b',
      batchId: 'batch-concurrent',
      batchTotal: 2,
      batchIndex: 2,
    });
    const invites = await invitesPromise;
    const inviteA = invites.find((i) => i.clientTaskId === 'concurrent-a');
    const inviteB = invites.find((i) => i.clientTaskId === 'concurrent-b');
    assert.ok(inviteA && inviteB, '两个并发邀请都应收到');
    assert.strictEqual(inviteA.batchId, 'batch-concurrent');
    assert.strictEqual(inviteA.batchTotal, 2);
    assert.strictEqual(inviteA.batchIndex, 1);
    assert.strictEqual(inviteB.batchIndex, 2);

    const [receivedA, receivedB, sentA, sentB] = await Promise.all([
      receiveFile(bob, inviteA),
      receiveFile(bob, inviteB),
      sendAPromise,
      sendBPromise,
    ]);
    assert.ok(receivedA.buffer.equals(bufA), '并发任务 A 内容应一致');
    assert.ok(receivedB.buffer.equals(bufB), '并发任务 B 内容应一致');
    assert.strictEqual(sentA.bytes, bufA.length);
    assert.strictEqual(sentB.bytes, bufB.length);

    /* ------------------------------ 批量参数校验 ------------------------------ */
    const badBatchError = once(alice, 'transfer-error');
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: 'bad-batch.bin',
      fileType: 'application/octet-stream',
      fileSize: 10,
      clientTaskId: 'bad-batch-task',
      batchId: 'bad-batch',
      batchTotal: 99,
      batchIndex: 1,
    });
    const badBatchErr = await badBatchError;
    assert.match(badBatchErr.message, /批量发送参数无效/);
    assert.strictEqual(badBatchErr.clientTaskId, 'bad-batch-task');

    /* ------------------------------ 传输中途取消 ------------------------------ */
    const abortBuffer = Buffer.alloc(2 * 1024 * 1024, 7);
    const abortInvitePromise = once(bob, 'incoming-file');
    const abortSenderError = once(alice, 'transfer-error', 8000);
    const abortSender = sendFile(alice, 'abort-task', abortBuffer, {
      cancelAfterChunks: 2,
    });
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: 'abort-me.bin',
      fileType: 'application/octet-stream',
      fileSize: abortBuffer.length,
      clientTaskId: 'abort-task',
    });
    const abortInvite = await abortInvitePromise;
    const abortReceive = receiveFile(bob, abortInvite);
    await assert.rejects(abortReceive, /取消|中断|终止|失败/);
    const abortSendMsg = await abortSenderError;
    assert.match(abortSendMsg.message, /取消/);
    const abortSendResult = await abortSender;
    assert.strictEqual(abortSendResult.cancelled, true);

    // 已中止的任务不能再被启动
    const replayError = once(alice, 'transfer-error', 5000);
    alice.emit('file-start', { taskToken: abortInvite.taskToken });
    const replayErr = await replayError;
    assert.match(replayErr.message, /不存在|过期/);

    /* ------------------------------ 字节数不匹配保护 ------------------------------ */
    const sizeInvitePromise = once(bob, 'incoming-file');
    const sizeSender = sendFile(alice, 'size-check-task', Buffer.alloc(512, 1), {
      byteLimit: 512,
    });
    sizeSender.catch(() => {});
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: 'size-mismatch.bin',
      fileType: 'application/octet-stream',
      fileSize: 1024,
      clientTaskId: 'size-check-task',
    });
    const sizeInvite = await sizeInvitePromise;
    const sizeReceive = receiveFile(bob, sizeInvite);
    await assert.rejects(sizeReceive, /不完整|中断|失败/);
    await assert.rejects(sizeSender, /校验失败|中断/);

    /* ------------------------------ 已完成任务不可重放 ------------------------------ */
    const completedReplayError = once(alice, 'transfer-error', 5000);
    alice.emit('file-start', { taskToken: invite.taskToken });
    const completedReplayErr = await completedReplayError;
    assert.match(completedReplayErr.message, /不存在|过期/);

    /* ------------------------------ 二维码 ------------------------------ */
    const qr = await httpGet(`${BASE}/api/qr?text=${encodeURIComponent(`${BASE}/?room=${room.code}`)}`);
    assert.strictEqual(qr.status, 200);
    assert.match(qr.headers['content-type'] || '', /image\/svg\+xml/);
    assert.match(qr.body.toString('utf8'), /<svg/i);

    const qrBad = await httpGet(`${BASE}/api/qr`);
    assert.strictEqual(qrBad.status, 400);

    /* ------------------------------ 退出房间 ------------------------------ */
    const left = once(bob, 'room:left');
    bob.emit('room:leave');
    await left;

    console.log('\n✅ 所有冒烟测试通过');
  } catch (err) {
    console.error('\n❌ 测试失败:', err);
    if (serverLog) console.error('\n--- 服务端日志 ---\n' + serverLog);
    process.exitCode = 1;
  } finally {
    sockets.forEach((socket) => socket.close());
    if (serverProcess) serverProcess.kill('SIGTERM');
    await wait(300);
  }
}

main();
