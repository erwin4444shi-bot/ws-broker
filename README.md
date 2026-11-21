# ws-broker — 云端 WebSocket 命令中继

对齐 `docs/cloud-ws-broker-design-v2.md` 的 Node/Render Free 实现。云端只做命令中继，媒体字节走 CDN（七牛），客户端（手机/中台）零改动——仅 role 不同（`phone` / `console`）。

## 连接方式

```
wss://<你的服务>.onrender.com/ws?deviceId=<id>&role=<phone|console>&groups=<g1,g2>&token=<BROKER_TOKEN>
```

| 参数 | 说明 |
|---|---|
| `deviceId` | 全局唯一，如 `phone-a`、`console-1`（重连同 id 自动覆盖旧连接，无假在线） |
| `role` | `phone` 或 `console` |
| `groups` | 逗号分隔组 id，默认 `default` |
| `token` | 与服务端 `BROKER_TOKEN` 环境变量一致（服务端未配置时省略） |

## 信封协议（与设计文档 v2 一致）

```json
{ "ok": true, "from": "console-1", "to": "phone-a", "channel": "control", "type": "command", "data": {} }
```

- `to`：`phone-a` ｜ `["phone-a","phone-b"]` ｜ `group-g1` ｜ `"*"`（仅 console + control/telemetry）
- `type: ping` → 回 `pong`
- `type: registered`：连入后服务端回执，客户端收到才算在线
- `type: ack`：console 发 command 后回 `{to, delivered, offline}`
- 离线补投：`phone-*` 的 `control` 消息在目标离线时入内存队列（≤100 条），重连后补投。broker 重启队列清空——客户端按 seq 补拉兜底（U1 协议已有）

## 部署到 Render Free（5 分钟）

1. 本仓库推到 GitHub（Public 或 Private + 连接 Git Provider 均可）
2. Render Dashboard → **New → Web Service** → 选中仓库
3. 配置：
   - Runtime: **Node**
   - Build Command: `npm install`
   - Start Command: `npm start`
   - Health Check Path: **`/health`**
   - Instance Type: **Free**
4. Environment 里加 `BROKER_TOKEN`（随机长字符串，客户端 URL 带同样的值）
5. Deploy → 得到 `https://xxx.onrender.com`

## 验证

```bash
npm test          # 本地全链路：单发/组广播/离线补投/health
curl https://xxx.onrender.com/health
```

## Render Free 注意事项

- 750 实例小时/月 ≈ 一台 24/7，不要再开第二个免费服务
- WS 消息计入活跃度（2026-02-24 起），心跳不断即不休眠
- 临时文件系统：状态仅内存，重启靠客户端重连 + seq 补拉自愈
