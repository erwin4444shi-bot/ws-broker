'use strict';
// 本地冒烟测试：起 broker → 双客户端连入 → 单发/组广播/离线补投 全链路验证
process.env.PORT = '18080';
const { spawn } = require('child_process');
const path = require('path');
const WebSocket = require('ws');

const srv = spawn(process.execPath, [path.join(__dirname, '..', 'src', 'server.js')], {
  stdio: 'inherit',
  env: { ...process.env, PORT: '18080' }
});
srv.on('error', e => { console.error('spawn error', e); process.exit(1); });

let pass = 0, fail = 0;
function check(name, cond) { cond ? (pass++, console.log('PASS', name)) : (fail++, console.log('FAIL', name)); }
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

(async () => {
  // 轮询等待服务就绪（最长 10s）
  let ready = false;
  for (let i = 0; i < 40 && !ready; i++) {
    await sleep(250);
    try { const r = await fetch('http://127.0.0.1:18080/health'); if (r.ok) ready = true; } catch {}
  }
  if (!ready) { console.error('server not ready'); srv.kill(); process.exit(1); }
  const base = 'ws://127.0.0.1:18080/ws';

  const console_ = new WebSocket(base + '?deviceId=console-1&role=console&groups=g1');
  const got = { console: [], pa: [], pb: [] };
  console_.on('message', m => got.console.push(JSON.parse(m)));
  await new Promise(r => console_.on('open', r));

  const phone1 = new WebSocket(base + '?deviceId=phone-a&role=phone&groups=g1');
  const phone2 = new WebSocket(base + '?deviceId=phone-b&role=phone&groups=g1');
  phone1.on('message', m => got.pa.push(JSON.parse(m)));
  phone2.on('message', m => got.pb.push(JSON.parse(m)));
  await Promise.all([new Promise(r => phone1.on('open', r)), new Promise(r => phone2.on('open', r))]);
  await sleep(200);

  // 1) registered 回执
  await sleep(150);
  check('phone-a 收到 registered', got.pa.some(m => m.type === 'registered'));

  // 2) 单发 console→phone-a
  console_.send(JSON.stringify({ ok: true, to: 'phone-a', channel: 'control', type: 'command', data: { act: 'ping2' } }));
  await sleep(200);
  check('单发到达 phone-a', got.pa.some(m => m.data && m.data.act === 'ping2'));
  check('console 收到 ack', got.console.some(m => m.type === 'ack' && m.data.delivered === 1));

  // 3) 组广播
  console_.send(JSON.stringify({ ok: true, to: 'group-g1', channel: 'control', type: 'command', data: { act: 'broadcast' } }));
  await sleep(200);
  check('组广播到达 phone-a', got.pa.some(m => m.data && m.data.act === 'broadcast'));
  check('组广播到达 phone-b', got.pb.some(m => m.data && m.data.act === 'broadcast'));

  // 4) 离线补投：phone-b 断开 → console 发 → phone-b 重连收到
  phone2.close();
  await sleep(150);
  console_.send(JSON.stringify({ ok: true, to: 'phone-b', channel: 'control', type: 'command', data: { act: 'redeliver' } }));
  await sleep(150);
  const pb2 = new WebSocket(base + '?deviceId=phone-b&role=phone&groups=g1');
  let redelivered = false;
  pb2.on('message', m => { const e = JSON.parse(m); if (e.data && e.data.act === 'redeliver') redelivered = true; });
  await new Promise(r => pb2.on('open', r));
  await sleep(300);
  check('离线补投送达', redelivered);

  // 5) health
  const res = await fetch('http://127.0.0.1:18080/health');
  const h = await res.json();
  check('health 200', res.status === 200 && h.ok === true);

  console.log(`\n== ${pass} passed, ${fail} failed ==`);
  srv.kill();
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); srv.kill(); process.exit(1); });
