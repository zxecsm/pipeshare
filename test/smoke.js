/**
 * 端到端冒烟测试：
 *   node test/smoke.js
 *
 * 覆盖：房间隔离、文本快传、文件流式中转、二维码生成、PWA 资源、错误分支。
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

function startDownload(taskToken) {
  return new Promise((resolve) => {
    const chunks = [];
    let settled = false;
    const done = (extra) => {
      if (settled) return;
      settled = true;
      resolve({ body: Buffer.concat(chunks), ...extra });
    };

    const req = http.get(`${BASE}/download?taskToken=${taskToken}`, (res) => {
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () =>
        done({
          status: res.statusCode,
          headers: res.headers,
          complete: res.complete !== false,
        }),
      );
      res.on('close', () =>
        done({
          status: res.statusCode,
          headers: res.headers,
          complete: res.complete,
          aborted: !res.complete,
        }),
      );
      res.on('error', (err) =>
        done({ status: 0, complete: false, aborted: true, error: err }),
      );
    });
    // 传输中途被服务端主动断开时，客户端会收到 ECONNRESET，
    // 这本身就是“失败且未完成”的预期结果，统一返回给调用方判断。
    req.on('error', (err) =>
      done({ status: 0, complete: false, aborted: true, error: err }),
    );
  });
}

function postFile(taskToken, buffer) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      `${BASE}/upload?taskToken=${taskToken}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Length': buffer.length,
        },
      },
      (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () =>
          resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }),
        );
      },
    );
    req.on('error', reject);
    req.end(buffer);
  });
}

function postPartialFile(taskToken, buffer, bytesToSend = 4096) {
  return new Promise((resolve) => {
    const req = http.request(
      `${BASE}/upload?taskToken=${taskToken}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Length': buffer.length,
        },
      },
      (res) => res.resume(),
    );
    req.on('error', () => resolve());
    req.write(buffer.subarray(0, Math.min(bytesToSend, buffer.length)));
    setTimeout(() => {
      req.destroy(new Error('模拟发送端网络中断'));
      resolve();
    }, 250);
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
    assert.match(swBody, /pipeshare-shell-v3/);
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

    /* ------------------------------ 文件流式传输 ------------------------------ */
    const fileBuffer = Buffer.alloc(512 * 1024 + 13);
    for (let i = 0; i < fileBuffer.length; i += 1) fileBuffer[i] = i % 251;

    const aliceStart = once(alice, 'start-upload-now', 8000);
    const bobComplete = once(bob, 'transfer-complete', 15000);
    const aliceComplete = once(alice, 'transfer-complete', 15000);

    const bobInvite = once(bob, 'incoming-file');
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: '测试文件.bin',
      fileType: 'application/octet-stream',
      fileSize: fileBuffer.length,
      clientTaskId: 'client-task-1',
    });

    const invite = await bobInvite;
    assert.strictEqual(invite.fileName, '测试文件.bin');
    assert.strictEqual(invite.clientTaskId, 'client-task-1');

    const statusBefore = await httpGet(
      `${BASE}/api/task-status?taskToken=${encodeURIComponent(invite.taskToken)}`,
    );
    assert.strictEqual(statusBefore.status, 200);
    assert.strictEqual(JSON.parse(statusBefore.body.toString('utf8')).ok, true);

    const downloadPromise = startDownload(invite.taskToken);
    const start = await aliceStart;
    assert.strictEqual(start.taskToken, invite.taskToken);
    assert.strictEqual(start.clientTaskId, 'client-task-1');

    const uploadResult = await postFile(invite.taskToken, fileBuffer);
    assert.strictEqual(uploadResult.status, 200, `上传应成功: ${uploadResult.body}`);

    const download = await downloadPromise;
    assert.strictEqual(download.status, 200);
    assert.strictEqual(download.complete, true, '正常下载应是完整响应');
    assert.strictEqual(download.body.length, fileBuffer.length, '接收端字节数应一致');
    assert.ok(download.body.equals(fileBuffer), '接收端内容应与发送端一致');
    assert.match(
      download.headers['content-disposition'] || '',
      /filename\*=UTF-8''/,
      '应包含 RFC5987 文件名',
    );

    const [bobDone, aliceDone] = await Promise.all([bobComplete, aliceComplete]);
    assert.strictEqual(bobDone.bytes, fileBuffer.length);
    assert.strictEqual(aliceDone.bytes, fileBuffer.length);

    /* ------------------------------ 并发多任务互不串包 ------------------------------ */
    const bufA = Buffer.from('concurrent-file-A-'.repeat(4096));
    const bufB = Buffer.from('concurrent-file-B-'.repeat(8192));

    const invitesPromise = collect(bob, 'incoming-file', 2, 8000);
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

    const startsPromise = collect(alice, 'start-upload-now', 2, 10000);
    const downloadAPromise = startDownload(inviteA.taskToken);
    const downloadBPromise = startDownload(inviteB.taskToken);
    const starts = await startsPromise;
    assert.deepStrictEqual(
      starts.map((s) => s.clientTaskId).sort(),
      ['concurrent-a', 'concurrent-b'],
      '推流指令应携带正确的 clientTaskId',
    );

    const completionsPromise = Promise.all([
      collect(alice, 'transfer-complete', 2, 15000),
      collect(bob, 'transfer-complete', 2, 15000),
    ]);

    const [uploadA, uploadB] = await Promise.all([
      postFile(inviteA.taskToken, bufA),
      postFile(inviteB.taskToken, bufB),
    ]);
    assert.strictEqual(uploadA.status, 200, uploadA.body);
    assert.strictEqual(uploadB.status, 200, uploadB.body);

    const [downloadA, downloadB] = await Promise.all([
      downloadAPromise,
      downloadBPromise,
    ]);
    assert.strictEqual(downloadA.status, 200);
    assert.strictEqual(downloadB.status, 200);
    assert.ok(downloadA.body.equals(bufA), '并发任务 A 内容应一致');
    assert.ok(downloadB.body.equals(bufB), '并发任务 B 内容应一致');

    const completions = await completionsPromise;
    assert.strictEqual(completions[0].length, 2);
    assert.strictEqual(completions[1].length, 2);

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

    /* ------------------------------ 传输中途断网 ------------------------------ */
    const abortBuffer = Buffer.alloc(2 * 1024 * 1024, 7);
    const abortInvitePromise = once(bob, 'incoming-file');
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: 'abort-me.bin',
      fileType: 'application/octet-stream',
      fileSize: abortBuffer.length,
      clientTaskId: 'abort-task',
    });
    const abortInvite = await abortInvitePromise;
    const abortDownloadPromise = startDownload(abortInvite.taskToken);
    await once(alice, 'start-upload-now', 8000);
    const abortErrorPromise = once(bob, 'transfer-error', 10000);
    await postPartialFile(abortInvite.taskToken, abortBuffer);

    const abortedDownload = await abortDownloadPromise;
    assert.ok(
      !abortedDownload.complete || abortedDownload.body.length < abortBuffer.length,
      '中断的下载不应被浏览器当作完整文件',
    );
    const abortError = await abortErrorPromise;
    assert.match(abortError.message, /中断|异常|失败/);

    const abortReplay = await httpGet(
      `${BASE}/download?taskToken=${encodeURIComponent(abortInvite.taskToken)}`,
    );
    assert.strictEqual(abortReplay.status, 410, '中断后的任务也应被回收');

    /* ------------------------------ 字节数不匹配保护 ------------------------------ */
    const sizeInvitePromise = once(bob, 'incoming-file');
    alice.emit('file-share-invite', {
      targetId: bob.id,
      fileName: 'size-mismatch.bin',
      fileType: 'application/octet-stream',
      fileSize: 1024,
      clientTaskId: 'size-check-task',
    });
    const sizeInvite = await sizeInvitePromise;
    const sizeDownloadPromise = startDownload(sizeInvite.taskToken);
    await once(alice, 'start-upload-now', 8000);
    const sizeErrorPromise = once(bob, 'transfer-error', 10000);
    const sizeUpload = await postFile(
      sizeInvite.taskToken,
      Buffer.alloc(512, 1),
    );
    assert.strictEqual(sizeUpload.status, 502, '字节数不符时应拒绝完成');

    const sizeDownload = await sizeDownloadPromise;
    assert.ok(
      !sizeDownload.complete || sizeDownload.body.length < 1024,
      '字节数不符的下载不能被视为完整文件',
    );
    const sizeErr = await sizeErrorPromise;
    assert.match(sizeErr.message, /中断|异常|失败/);

    /* ------------------------------ 重复下载保护 ------------------------------ */
    const replay = await httpGet(
      `${BASE}/download?taskToken=${encodeURIComponent(invite.taskToken)}`,
    );
    assert.strictEqual(replay.status, 410, '已完成任务的下载链接应失效');

    const statusAfter = await httpGet(
      `${BASE}/api/task-status?taskToken=${encodeURIComponent(invite.taskToken)}`,
    );
    assert.strictEqual(statusAfter.status, 410, '已完成任务的状态预检也应失效');

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
