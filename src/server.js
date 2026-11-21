'use strict';
/**
 * ws-broker — 云端 WebSocket 命令中继（Node/Render Free 版）
 *
 * 对齐 docs/cloud-ws-broker-design-v2.md：
 * - 信封协议 { ok, from, to, channel, type, data }
 * - 连接注册（role=phone/console，分组）→ type:registered 回执
 * - 按 to 转发：deviceId / ['a','b'] / group-G / '*'（仅 console 白名单 channel）
 * - 心跳 ping→pong；断连清理注册表；内存离线队列（重启即清，客户端 seq 补拉兜底）
 *
 * Render 适配硬性要求（本次实现全部满足）：
 * - 监听 process.env.PORT（Render 注入）
 * - HTTP 与 WS 同端口（ws 库 noServer + upgrade 路由）
 * - /health 200 探活端点
 * - 状态仅内存（Render 免费档临时盘 + 随时重启）
 */

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const BROKER_TOKEN = process.env.BROKER_TOKEN || ''; // 为空则不校验（阶段0）；上线前在 Render 环境变量里设置
const ALLOW_WILDCARD_CHANNELS = new Set(['control', 'telemetry']);

/** @type {Map<string, {ws:import('ws').WebSocket, role:string, groups:Set<string>, lastSeen:number}>} */
const registry = new Map();
/** @type {Map<string, Array<{env:object, ts:number}>>} 离线补投队列 deviceId→envelopes */
const offlineQueue = new Map();
const OFFLINE_QUEUE_MAX = 100;

function now() { return Date.now(); }

function log(...args) { console.log(new Date().toISOString(), ...args); }

// ---------- 令牌校验（阶段0：共享密钥，URL query 传入） ----------
function checkToken(url) {
  if (!BROKER_TOKEN) return true; // 未配置则放行（开发态）
  const t = url.searchParams.get('token');
  if (!t || t.length < 8) return false;
  // 常数时间比较防时序侧信道
  const a = Buffer.from(t);
  const b = Buffer.from(BROKER_TOKEN);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// ---------- to 解析 ----------
function resolveTargets(to) {
  if (to === '*' || to === null || to === undefined) return { ids: ['*'], wildcard: true };
  if (Array.isArray(to)) return { ids: to.filter(x => typeof x === 'string'), wildcard: false };
  if (typeof to === 'string') return { ids: [to], wildcard: false };
  return { ids: [], wildcard: false };
}

function sendTo(env, targetId) {
  if (targetId.startsWith('group-')) {
    // 组广播：发给所有加入该组的客户端
    let n = 0;
    for (const [, rec] of registry) {
      if (rec.groups.has(targetId.slice(6)) && rec.ws.readyState === 1) {
        rec.ws.send(JSON.stringify(env)); n++;
      }
    }
    return n;
  }
  const rec = registry.get(targetId);
  if (rec && rec.ws.readyState === 1) { rec.ws.send(JSON.stringify(env)); return 1; }
  // 离线：入队补投（仅命令级 channel）
  if (env.channel === 'control' && targetId.startsWith('phone-')) {
    let q = offlineQueue.get(targetId);
    if (!q) { q = []; offlineQueue.set(targetId, q); }
    if (q.length >= OFFLINE_QUEUE_MAX) q.shift();
    q.push({ env, ts: now() });
  }
  return 0;
}

function handleEnvelope(fromId, rec, raw) {
  let env;
  try { env = JSON.parse(raw); } catch { return; }
  if (!env || typeof env !== 'object') return;

  rec.lastSeen = now();

  if (env.type === 'ping') {
    rec.ws.send(JSON.stringify({ ok: true, type: 'pong', ts: now() }));
    return;
  }

  const { ids: to, wildcard } = resolveTargets(env.to);
  if (wildcard) {
    // '*' 全局广播：仅 console + 白名单 channel
    if (rec.role !== 'console' || !ALLOW_WILDCARD_CHANNELS.has(env.channel)) return;
  }
  if (!env.from) env.from = fromId;

  let delivered = 0, offline = 0;
  for (const t of to) delivered += sendTo(env, t);
  offline = to.length - delivered;
  if (rec.role === 'console' && env.type === 'command') {
    rec.ws.send(JSON.stringify({ ok: true, type: 'ack', data: { to, delivered, offline } }));
  }
}

// ---------- HTTP ----------
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/health') {
    const body = JSON.stringify({ ok: true, clients: registry.size, queued: offlineQueue.size, ts: now() });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(body);
    return;
  }
  if (url.pathname === '/stats' && rec0Allowed(url)) {
    const members = [];
    for (const [id, r] of registry) members.push({ id, role: r.role, groups: [...r.groups], lastSeen: r.lastSeen });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, data: { clients: registry.size, members } }));
    return;
  }
  res.writeHead(404); res.end('not found');
});

function rec0Allowed(url) {
  if (!BROKER_TOKEN) return true;
  return checkToken(url);
}

// ---------- WS ----------
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname !== '/ws') { socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); socket.destroy(); return; }
  if (!checkToken(url)) { socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n'); socket.destroy(); return; }

  wss.handleUpgrade(req, socket, head, (ws) => {
    const deviceId = url.searchParams.get('deviceId') || `anon-${crypto.randomUUID().slice(0, 8)}`;
    const role = url.searchParams.get('role') === 'console' ? 'console' : 'phone';
    const groups = new Set((url.searchParams.get('groups') || 'default').split(',').filter(Boolean));

    // 覆盖旧记录（同 id 重连无假在线）
    const old = registry.get(deviceId);
    if (old && old.ws.readyState === 1) old.ws.close();

    registry.set(deviceId, { ws, role, groups, lastSeen: now() });
    log('connected', deviceId, role, [...groups], 'total=', registry.size);

    ws.send(JSON.stringify({
      ok: true, type: 'registered',
      data: { deviceId, role, groups: [...groups], members: registry.size }
    }));

    // 离线补投
    const q = offlineQueue.get(deviceId);
    if (q && q.length) {
      for (const item of q) ws.send(JSON.stringify(item.env));
      log('offline-redelivery', deviceId, q.length);
      offlineQueue.delete(deviceId);
    }

    ws.on('message', (raw) => handleEnvelope(deviceId, registry.get(deviceId), raw.toString()));

    ws.on('close', () => {
      const cur = registry.get(deviceId);
      if (cur && cur.ws === ws) { registry.delete(deviceId); log('disconnected', deviceId, 'total=', registry.size); }
    });
    ws.on('error', (e) => log('ws-error', deviceId, e.message));
  });
});

// 心跳超时清理（5 分钟无任何消息视为死链，ws 层 Pong 由 ws 库自动处理）
setInterval(() => {
  const cutoff = now() - 5 * 60 * 1000;
  for (const [id, rec] of registry) {
    if (rec.lastSeen < cutoff) {
      try { rec.ws.terminate(); } catch {}
      registry.delete(id);
      log('sweep-dead', id);
    }
  }
}, 60 * 1000).unref();

server.listen(PORT, '0.0.0.0', () => log(`ws-broker listening on :${PORT}`));
