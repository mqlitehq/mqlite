# mqlite 压测工具（本地 SQLite）

驱动**嵌入引擎**（无 HTTP）跑高频请求矩阵，在 **Docker** 内做系统监控，进程内探针读 `/proc/self` 做每场景 CPU/磁盘归因。

| 文件 | 作用 |
|---|---|
| `main.go`（本目录） | load generator：9 场景 + µs 延迟直方图 + `/proc/self/io`·`/proc/self/stat` 探针，输出 `results.json` |
| `Dockerfile` | bench 镜像（golang + sysstat/procps），**原生 arch**（不强制 amd64，避免 qemu 失真）|
| `entry.sh` | 容器内：起 `iostat`/`vmstat` 采样 → 跑 bench → 收尾，记 `env.txt` |
| `run-bench.sh` | 宿主：build 镜像 → 跑（DB 在容器 fs，非 bind-mount）→ `docker cp` 取结果到 `out/` |

## 跑

```bash
cd mqlite
./test/bench/run-bench.sh                                 # 5s/场景，256B
BENCH_DUR=10s BENCH_MSG=1024 ./test/bench/run-bench.sh    # 自定义时长/消息体
```

产物在 `test/bench/out/`：`results.json`、`iostat.log`、`vmstat.log`、`env.txt`、各场景 `*.db`。

## 后端：本地文件 vs 远程 Turso（MQLITE-41）

默认每个场景开一个本地 SQLite 文件（本地磁盘压测）。两个开关支持把同一套矩阵跑在
**远程 Turso** 上，做"本地 SSD vs 云端 Turso"对比：

- `-db libsql://<host>`（或 `BENCH_DB`）：所有场景共享这一个远程库，每场景用独立队列隔离；
  鉴权 token 走 `MQLITE_DB_AUTH_TOKEN` 环境变量。远程模式下文件体积类指标不适用。
- `-prefillcap N`（或 `BENCH_PREFILLCAP`）：给 drain/bloat 的预灌量封顶，使其在 ~几十-上百 ms/op
  的慢速远程后端上可行；三种后端传同一个 cap 才好逐项对比。

> 远程每次入队是一次持久化 Hrana 提交往返（~45–57ms，即便同区），吞吐比本地 SSD 低 100–1000×。
> 三方对比报告见 `MQLite-cloud-bench-report.{md,html}`（原始数据 `bench-3way-raw/`）。

## 场景

produce ×{1,4,8 生产者} · batch ×{16,64} · e2e(4×4) · drain(20 万预灌排空) · sessions(64 组) · produce FULL(对照 fsync)。

完整结果与解读见设计仓库的压测报告（`mqlite-stress-report.{md,html}`）。

> 口径：Docker Linux VM（Apple Silicon），非裸金属；比例可迁移，绝对值不可照搬。探针对 µs 级操作有个位数百分比开销。
