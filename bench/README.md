# mqlite 压测工具（本地 SQLite）

驱动**嵌入引擎**（无 HTTP）跑高频请求矩阵，在 **Docker** 内做系统监控，进程内探针读 `/proc/self` 做每场景 CPU/磁盘归因。

| 文件 | 作用 |
|---|---|
| `../cmd/mqlite-bench/` | load generator：9 场景 + µs 延迟直方图 + `/proc/self/io`·`/proc/self/stat` 探针，输出 `results.json` |
| `Dockerfile` | bench 镜像（golang + sysstat/procps），**原生 arch**（不强制 amd64，避免 qemu 失真）|
| `entry.sh` | 容器内：起 `iostat`/`vmstat` 采样 → 跑 bench → 收尾，记 `env.txt` |
| `run-bench.sh` | 宿主：build 镜像 → 跑（DB 在容器 fs，非 bind-mount）→ `docker cp` 取结果到 `out/` |

## 跑

```bash
cd mqlite
./bench/run-bench.sh                                 # 5s/场景，256B
BENCH_DUR=10s BENCH_MSG=1024 ./bench/run-bench.sh    # 自定义时长/消息体
```

产物在 `bench/out/`：`results.json`、`iostat.log`、`vmstat.log`、`env.txt`、各场景 `*.db`。

## 场景

produce ×{1,4,8 生产者} · batch ×{16,64} · e2e(4×4) · drain(20 万预灌排空) · sessions(64 组) · produce FULL(对照 fsync)。

完整结果与解读见 **[`../../mqlite-stress-report.md`](../../mqlite-stress-report.md)**（交互版 `mqlite-stress-report.html`）。

> 口径：Docker Linux VM（Apple Silicon），非裸金属；比例可迁移，绝对值不可照搬。探针对 µs 级操作有个位数百分比开销。
