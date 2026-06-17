# mqlite 实现进度记录（BUILD-LOG）

> 从设计文档 `../mqlite-design.md` 到可运行实现的过程记录。
> 目标：完整可用的 v0.1 —— 本地 SQLite + 远程 Turso 同一引擎，serve / embedded 双形态，Go SDK + CLI，端到端测试（含真机 Turso）。

## 状态总览

| 模块 | 状态 | 说明 |
|---|---|---|
| 存储层（本地 modernc / 远程 libSQL） | ✅ | 同一套 SQL；连接串从环境读取，绝不入码 |
| 引擎核心（enqueue/claim/settle） | ✅ | Peek-Lock + 四件套 + fencing + 可见性超时 |
| 顺序模型（MessageGroupId） | ✅ | 默认近似有序；`session_id` 组内强序、组间并行 |
| Topic 扇出 + 订阅过滤 | ✅ | 等值 AND + 主题前缀，发布时 evaluate |
| 去重（dedup） | ✅ | 滑动窗口；同键异体 → DedupConflict（不静默） |
| 重试/DLQ/Redrive | ✅ | 超 max_delivery 进 DLQ；Redrive 原队列原地 / 跨队列重 INSERT |
| 定时/延迟/Defer | ✅ | 统一 `visible_at`；后台激活扫描 |
| 后台任务 | ✅ | reaper / scheduled 激活 / TTL→DLQ / dedup 清理 |
| 崩溃恢复 | ✅ | 单 broker：重启把 locked 重置 active |
| Connect 风格 HTTP server + 鉴权 | ✅ | JSON over HTTP，curl 可调；静态 Bearer token |
| Go SDK（远程 Client + 嵌入 Embedded） | ✅ | message-as-handle；Receiver.Run + 自动续锁 |
| 同库事务入队（embedded `Tx`） | ✅ | 业务写 + enqueue 原子提交（天然 outbox） |
| CLI（单二进制子命令） | ✅ | serve/send/receive/peek/metrics/list/redrive/subscribe |
| 测试（本地 TCK + 真机 Turso） | ✅ | 16 引擎不变量用例 + SDK 端到端 + 真机集成 |
| Docker（linux/amd64） | ✅ | 纯 Go 静态二进制，alpine 运行镜像 |

## 关键工程决策（落地时）

1. **单写者 = `MaxOpenConns(1)`（本地）。** 不另起 writer goroutine —— database/sql 用单连接天然串行化所有写，零文件级锁竞争，claim 原子。比自管 channel 队列更简单可靠（符合 D3 精神，去掉了不必要的复杂度）。
2. **远程 Turso 用小连接池而非单连。** Turso primary 替我们串行化写，单连反而踩到「空闲 Hrana stream 被服务端关闭」的坑（实测 `stream is closed: driver: bad connection`）。改为 `MaxOpenConns(4) + ConnMaxIdleTime(3s)`，让 database/sql 主动回收空闲连接、不复用已关闭的 stream。
3. **传输层：手写 Connect 风格 JSON-over-HTTP，不引 buf/protoc。** 环境无 protoc 工具链；且设计的核心卖点是「unary RPC = 一个 HTTP POST，curl 可调」。`wire` 包做唯一契约源，server 与 client 共用，杜绝漂移。Proto/二进制编码留作后续增量（JSON 已满足 curl + 强结构）。
4. **`wire` 单一契约包。** server 与 SDK client 都 import 它，JSON 字段一处定义；`[]byte` body 走 base64（Go `encoding/json` 原生），与设计 §7.4 的 curl 示例一致。
5. **settler 接口统一结算。** `*mqlite.Message` 的 Complete/Abandon/… 绑定内部 lock token（不外泄），远程 Client 与嵌入 Embedded 各自实现 settler —— 同一份消息句柄代码两种形态通用。

## 构建/验证记录

### 本地测试（hermetic）
`go test ./...` 全绿。引擎不变量用例（TCK 雏形）：
- send→receive→complete、fencing token 安全失败（LockLost）
- Abandon 重投 + 超 max → DLQ
- 可见性超时 reaper 重投（delivery_count+1）
- Scheduled 到点激活、Defer/ReceiveDeferred
- dedup 窗内静默丢弃 + 同键异体 DedupConflict
- MessageGroupId：组内强序（队头在飞则后续不投）、组间并行
- Topic 扇出 + 前缀过滤
- Redrive 回流、崩溃恢复（重开库 locked→active）
- 嵌入 `Tx` 原子入队（提交入队 / 回滚不入队）
- Receive-and-Delete 取即删

SDK 端到端（httptest 起 server）：远程 round-trip、鉴权（错 token 被拒）、`Receiver.Run` 并发消费 5 条。

### 真机 Turso 集成（`libsql://…aws-ap-northeast-1`）
`go test ./engine -run TestTursoIntegration -v` → **PASS（~11.6s）**：
创建队列 → 批量发送(seq=[1,2]) → 接收+Complete → 接收+Abandon+重投(delivery_count 递增)+Complete → 队列排空。
> 连接串与 token 仅经环境变量 `MQLITE_TEST_DB` / `MQLITE_TEST_DB_AUTH_TOKEN` 注入，未写入任何提交文件。

### 端到端服务化冒烟（本地 broker :8099）
- `/healthz` → ok；无 token → **401**
- `curl Send`（base64 body）→ `{"seq_numbers":[1]}`
- `curl Receive`（长轮询）→ 返回 lock_token + base64 body
- `curl Complete`（正确 token）→ `{"ok":true}`；过期 token → `{"ok":false}`（LockLost，安全失败）
- CLI `send`/`receive`/`list`/`metrics` 与 curl 互通

### Docker
`docker build --platform linux/amd64 -t mqlite:0.1.0 .` —— 纯 Go 静态二进制（`CGO_ENABLED=0`），alpine 运行镜像，run 起来 `/healthz` 通过。

## 已修复的真实缺陷（过程中发现）

- **CLI 旗标解析：** Go `flag` 在首个位置参数处停止解析，导致 `send orders "body" --message-id x` 把旗标当成了 body、`create-queue q --max-delivery 5` 静默忽略旗标。加 `parseInterspersed` 允许旗标与位置参数任意顺序后修复。
- **Turso 空闲 stream 关闭：** 见上「关键决策 2」。

## v0.1 范围与有意延后（对照设计）

**已实现并验证**：核心契约不变量 I1–I14 的可测子集、ASB 四件套语义、MessageGroupId、Topic、dedup、Redrive、定时、嵌入 `Tx`、崩溃恢复、curl 契约、双形态。

**有意延后到 v1.x（设计里有、v0.1 未做）**：
- Protobuf 二进制编码 / 服务端流式 Receive（当前 JSON unary + 长轮询）；正式 `.proto` + buf 生成。
- group-commit 跨请求批量（当前单写连接已串行，SendBatch 已单事务多行；跨请求合并 fsync 留待优化）。
- 结算回执表 `settlement_receipts`（D22，丢响应幂等成功）；消费端 `receive_attempt_id`（D24）。
- Web UI（`--ui`）、MCP server、gocloud.dev/pubsub driver（D16）。
- Litestream/LiteFS 等 HA 档位的打包（远程已由 Turso 托管覆盖）。
- 远程写的「连接错误自动重试」：当前靠空闲回收 + at-least-once 契约（消费端必须幂等）；未做整操作盲重试，避免放大重复语义。
- Redrive 限速（`rate_per_sec` 入参已留，v0.1 未实际节流）。

## 下一步候选

1. 正式 `.proto` + buf + Protobuf 编码（满足「PB 也行」与机器生成文档）。
2. `settlement_receipts` + `receive_attempt_id`（吸收同事版，消除丢响应歧义）。
3. 只读 Web 运维面（`--ui`），唯一写动作 = DLQ Redrive(admin)。
4. 远程读操作的幂等自动重试（read-only 安全）。
