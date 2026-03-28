# sysbenchtest — Dstore OLTP 基准测试模块

模拟 sysbench `oltp_*` 脚本的行为，直接调用 Dstore 存储引擎 API，无需 MySQL/PostgreSQL，用于评估 Dstore 在 OLTP 场景下的吞吐量和延迟。

---

## 目录结构

```
tests/sysbenchtest/
├── config.json              # 运行参数配置（表数、线程数、时长、模式等）
├── CMakeLists.txt           # 构建脚本
├── include/
│   ├── sysbench_common.h    # 表结构定义、枚举类型、SysbenchConfig 结构体
│   ├── sysbench_client.h    # SysbenchStorage / SysbenchWorker 声明
│   ├── sysbench_server.h    # SysbenchStorageInstance（存储引擎生命周期）
│   └── sysbench_stats.h     # ThreadStats / SysbenchStats（统计与报告）
└── src/
    ├── main.cpp             # 入口：初始化引擎、按 command 分发各阶段
    ├── sysbench_client.cpp  # Prepare/Run/Cleanup 实现、SysbenchWorker
    ├── sysbench_server.cpp  # 存储引擎 Init/Start/Stop 实现
    └── sysbench_stats.cpp   # 统计聚合、TPS/延迟百分位输出
```

---

## 表结构（sbtest）

与标准 sysbench `oltp_common.lua` 相同，每张表命名为 `sbtest1`、`sbtest2`、…：

| 列名 | 类型        | 说明               |
|------|-------------|--------------------|
| id   | INT4        | 主键（主索引）     |
| k    | INT4        | 可选二级索引列     |
| c    | VARCHAR(120)| 随机字符串         |
| pad  | VARCHAR(60) | 填充字符串         |

索引：
- **主索引**（唯一）：`id`
- **二级索引**（非唯一，可选）：`k`，由 `config.json` 中 `secondary: 1` 控制

---

## 执行阶段

### 生命周期

```
Init (initdb) → InitFinished → Start
    → [Prepare] CreateTables → LoadData → CreateIndexes
    → [Run]     RecoverTables → Warmup → Measure → PrintFinalReport
    → [Cleanup] DropTables
Stop
```

### command 说明

| command   | 执行内容                                      |
|-----------|-----------------------------------------------|
| `prepare` | CreateTables + LoadData + CreateIndexes       |
| `run`     | RecoverTables + Execute（warmup + measure）   |
| `cleanup` | DropTables（清除 simulator 中的表上下文）     |
| `all`     | 按顺序执行 prepare → run → cleanup（默认）    |

> **注意**：`run` 命令复用已有数据目录，`Execute()` 开头会调用 `RecoverTables()`，通过 `DstoreTableHandler::RecoveryTable()` 将已有 heap/index 重新加载进 simulator map，否则 `GetTableHandler()` 会触发断言失败（segfault）。

### mode 说明

| mode         | 读操作 | 写操作 | 对应 sysbench 脚本      | 备注 |
|--------------|--------|--------|-------------------------|------|
| `read_write` | ✓      | ✓      | oltp_read_write（默认） | |
| `read_only`  | ✓      | ✗      | oltp_read_only          | skip_trx 强制=on，仅执行 point_select |
| `write_only` | ✗      | ✓      | oltp_write_only         | |

每个事务包含的操作数由 config.json 单独控制，`mode` 控制哪些操作实际生效。

### TPS:QPS 比例

| mode         | TPS:QPS | reads/tx | writes/tx | others/tx | 说明 |
|--------------|---------|----------|-----------|-----------|------|
| `read_only`  | **1:10**  | 10 | 0 | 0 | skip_trx=on，不含 begin/commit |
| `write_only` | **1:6**   | 0  | 2 | 4 | 2 updates + 2(delete+insert) + begin + commit |
| `read_write` | **1:20**  | 14 | 2 | 4 | 10 point + 4 range + 2 writes + 2(d/i) + begin + commit |

---

## config.json 参数说明

```json
{
    "tables":            1,     // 表数量，表名 sbtest1..sbtestN
    "table_size":    10000,     // 每表行数
    "threads":           8,     // 并发 worker 线程数
    "time":             60,     // 测量阶段时长（秒）
    "warmup_time":      10,     // 预热阶段时长（秒，不计入统计）
    "report_interval":  10,     // 每隔多少秒打印一次中间报告（0=关闭）
    "range_size":      100,     // 范围扫描行数

    // 每事务操作次数（mode 决定哪些生效）
    "point_selects":    10,     // SELECT c WHERE id=?
    "simple_ranges":     1,     // SELECT ... WHERE id BETWEEN ? AND ?
    "sum_ranges":        1,     // SELECT SUM(k) WHERE id BETWEEN ? AND ?
    "order_ranges":      1,     // SELECT ... ORDER BY k（内存排序模拟）
    "distinct_ranges":   1,     // SELECT DISTINCT k WHERE id BETWEEN ? AND ?
    "index_updates":     1,     // UPDATE SET k=k+1 WHERE id=?（主索引扫描）
    "non_index_updates": 1,     // UPDATE SET c=? WHERE id=?
    "delete_inserts":    1,     // DELETE + INSERT

    "skip_trx":          0,     // 1=不统计 begin/commit 到 QPS；read_only 模式强制=1（事务本身仍执行）
    "secondary":         0,     // 1=创建 k 列二级索引
    "auto_inc":          1,     // 1=id 自增（加载阶段使用）
    "mode":    "read_write",    // read_write / read_only / write_only
    "command":       "all"      // prepare / run / cleanup / all
}
```

---

## 编译与运行

> 所有操作必须在 Docker 容器 `dstore_env` 内执行，参考项目根目录 `CLAUDE.md`。

### 编译

```bash
# 容器内，先确保 dstore 主库和 utils 已编译
cd /opt/project/dstore
bash build.sh -m release -co "-DENABLE_SYSBENCH=ON"
```

编译产物：`output/bin/sysbenchtest`，配置文件安装到构建目录。

### 运行

```bash
# 进入构建输出目录（config.json 和 guc.json 在此）
cd /opt/project/dstore/output/bin   # 或 tmp_build/

# 全流程（prepare + run + cleanup）
./sysbenchtest

# 仅 prepare（建表 + 加载数据）
# 修改 config.json: "command": "prepare"，然后运行
./sysbenchtest

# 仅 run（复用已有数据）
# 修改 config.json: "command": "run"，然后运行
./sysbenchtest
```

数据目录：运行目录下的 `sysbenchdir/`（prepare 时自动创建）。

---

## 统计输出

### 中间报告（每 `report_interval` 秒）

```
[10s] TPS: 8521.3  reads: 853245  writes: 85213  other: 85213  errors: 0
```

### 最终报告

```
SQL statistics:
    queries performed:
        read:   5123450
        write:   512340
        other:   512340
        total:  6148130
    transactions:         512340 (8539.0 per sec.)
    queries:             6148130 (102468.1 per sec.)
    errors:                    0 (0.0 per sec.)

Latency (ms):
    min:    0.35
    avg:    0.94
    max:   15.23
    95th:   1.47
    99th:   2.11
```

---

## 核心类关系

```
main.cpp
 └─ SysbenchStorage          (sysbench_client.h)
     ├─ LoadConfig()          从 config.json 填充 SysbenchConfig
     ├─ CreateTables/Indexes  使用 DstoreTableHandler 创建 heap/index
     ├─ LoadData()            多线程并行填充，每表独立线程组
     ├─ RecoverTables()       run 模式下重新打开已有表到 simulator map
     └─ Execute()
         ├─ warmup phase      runPhase(warmupSec, measuring=false)
         └─ measure phase     runPhase(durationSec, measuring=true)
             └─ SysbenchWorker (per thread)
                 └─ RunOltpTransaction()
                     ├─ DoPointSelect / DoSimpleRange / DoSumRange
                     ├─ DoOrderRange / DoDistinctRange
                     └─ DoIndexUpdate / DoNonIndexUpdate / DoDeleteInsert

 └─ SysbenchStorageInstance  (sysbench_server.h)
     ├─ Init()      initdb（prepare 阶段调用）
     ├─ Start()     从已有目录启动引擎
     └─ Stop()      刷盘并销毁

 └─ SysbenchStats            (sysbench_stats.h)
     ├─ RecordTransaction()  worker 线程写入，使用 reservoir sampling
     ├─ PrintIntervalReport() 周期性 TPS 报告
     └─ PrintFinalReport()   含延迟百分位的最终汇总
```

---

## 已知限制

- `DropTables` 仅清除 simulator 上下文，不调用 Dstore DropTable API（该 API 未通过 TableHandler 暴露）
- `order_ranges` 在内存中排序模拟 ORDER BY，不经过索引
- 二级索引（`k` 列）目前仅创建，`write_only` 模式下更新操作不更新二级索引
