'use strict';

/**
 * PipeShare Service Worker
 *
 * 目标：
 * 1. 满足 PWA 可安装条件；
 * 2. 离线时至少能加载应用外壳；
 * 3. 绝不缓存文件传输、信令和 API 请求。
 *
 * 注意：修改静态资源后请同步提升 CACHE_NAME，让旧缓存被清理。
 */

const CACHE_NAME = 'pipeshare-shell-v4';
const SCOPE = self.registration.scope;
const SHELL_URL = new URL('./index.html', SCOPE).href;
const HOME_URL = new URL('./', SCOPE).href;

const APP_SHELL = [
  './',
  './index.html',
  './favicon.svg',
  './manifest.webmanifest',
  './socket.io/socket.io.esm.min.js',
  './icons/icon.svg',
  './icons/icon-192.png',
  './icons/icon-512.png',
  './icons/icon-maskable-512.png',
  './icons/apple-touch-icon.png',
].map((path) => new URL(path, SCOPE).href);

function isSocketIoClientScript(path) {
  return /\/socket\.io\/socket\.io(?:\.esm\.min|\.min)?\.js$/.test(path);
}

/** 这些请求绝不能进缓存：信令、API */
function shouldBypass(url) {
  const path = url.pathname;

  // socket.io 客户端脚本本身可以缓存，其余信令/长轮询请求直接放行
  if (path.includes('/socket.io/') && !isSocketIoClientScript(path)) {
    return true;
  }
  if (/\/api\//.test(path)) return true;
  if (/\/healthz$/.test(path)) return true;
  return false;
}

self.addEventListener('install', (event) => {
  event.waitUntil(
    (async () => {
      const cache = await caches.open(CACHE_NAME);
      // 单个资源失败不应导致整个 Service Worker 安装失败
      await Promise.all(
        APP_SHELL.map(async (url) => {
          try {
            const response = await fetch(url, { cache: 'reload' });
            if (response && response.ok) {
              await cache.put(url, response);
            }
          } catch {
            /* 忽略单个资源失败 */
          }
        }),
      );
      await self.skipWaiting();
    })(),
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(
        keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)),
      );
      await self.clients.claim();
    })(),
  );
});

/**
 * 点击系统通知：
 * 1. 立即关闭通知；
 * 2. 如果当前页面已打开则聚焦；
 * 3. 否则导航到通知里的页面地址，或打开新窗口。
 */
self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  const targetUrl = new URL(
    (event.notification.data && event.notification.data.url) || './',
    SCOPE,
  ).href;

  event.waitUntil(
    (async () => {
      const windowClients = await self.clients.matchAll({
        type: 'window',
        includeUncontrolled: true,
      });

      const exactClient = windowClients.find(
        (client) => client.url === targetUrl,
      );
      if (exactClient) {
        await exactClient.focus();
        return;
      }

      const target = new URL(targetUrl);
      const sameOriginClient = windowClients.find((client) => {
        try {
          return new URL(client.url).origin === target.origin;
        } catch {
          return false;
        }
      });

      if (sameOriginClient) {
        if ('navigate' in sameOriginClient) {
          try {
            await sameOriginClient.navigate(targetUrl);
          } catch {
            /* 某些浏览器不允许 SW 导航，退化为聚焦 */
          }
        }
        await sameOriginClient.focus();
        return;
      }

      if (self.clients.openWindow) {
        await self.clients.openWindow(targetUrl);
      }
    })(),
  );
});

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  if (shouldBypass(url)) return;

  if (request.mode === 'navigate') {
    event.respondWith(networkFirst(request));
    return;
  }

  // Socket.IO 客户端脚本需要跟随服务端版本，在线时优先取最新
  if (isSocketIoClientScript(url.pathname)) {
    event.respondWith(networkFirstAsset(request));
    return;
  }

  event.respondWith(staleWhileRevalidate(request));
});

async function networkFirstAsset(request) {
  const cache = await caches.open(CACHE_NAME);
  try {
    const response = await fetch(request);
    if (response && response.ok) {
      await cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await cache.match(request);
    return cached || new Response('', { status: 503 });
  }
}

async function networkFirst(request) {
  const cache = await caches.open(CACHE_NAME);

  try {
    const response = await fetch(request);
    if (response && response.ok) {
      // 只缓存一份应用外壳，避免 ?room=xxx 产生无限缓存条目
      await cache.put(SHELL_URL, response.clone());
    }
    return response;
  } catch {
    const cached =
      (await cache.match(SHELL_URL)) || (await cache.match(HOME_URL));
    if (cached) return cached;

    return new Response('PipeShare 当前离线，请恢复网络后重试。', {
      status: 503,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    });
  }
}

async function staleWhileRevalidate(request) {
  const cache = await caches.open(CACHE_NAME);
  const cached = await cache.match(request);

  const network = fetch(request)
    .then((response) => {
      if (response && response.ok) {
        cache.put(request, response.clone());
      }
      return response;
    })
    .catch(() => null);

  return cached || (await network) || new Response('', { status: 503 });
}
