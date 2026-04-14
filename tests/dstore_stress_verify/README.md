# dstore_stress_verify — DFX 页面校验长稳验证工具

集成页面校验 DFX + 故障注入 + 崩溃恢复验证，用于验证页面校验框架的正确性和稳定性。

---

## 编译与运行

前置条件：Docker 容器内，已 `source buildenv`。

```bash
# 进入容器
docker exec -it dstore-dev bash
source /opt/project/dstore/buildenv
```

### 快速运行

```bash
# 编译 + 运行长稳验证（一行搞定）
cd tests && bash build_and_run_stress_verify.sh

# 崩溃恢复验证
cd tests && bash build_and_run_stress_verify.sh -c

# 已编译过，跳过编译直接运行
cd tests && bash build_and_run_stress_verify.sh -r false

# ASan 模式
cd tests && bash build_and_run_stress_verify.sh -a ON
```

### 手动运行（调试时推荐）

```bash
# 编译
bash build.sh -m debug -co "-DDSTORE_TEST_TOOL=ON"

# 运行
cd tmp_build/bin
vi config.json          # 按需调整参数
./dstore_stress_verify

# 崩溃恢复
bash ../../tests/dstore_stress_verify/run_crash_recovery_test.sh
```

### CMake target

```bash
cd tmp_build
make run_dstore_stress_verify         # 长稳验证
make run_dstore_crash_recovery_test   # 崩溃恢复
make run_dstore_stress_verify_asan    # ASan 模式
```

---

## 分步执行

修改 `config.json` 中的 `"command"` 字段：

```bash
cd tmp_build/bin

# 1. 建库建表灌数据（只需一次）
#    "command": "prepare"
./dstore_stress_verify

# 2. 跑 OLTP（可反复执行，每次改 verify_level 等参数）
#    "command": "run"
./dstore_stress_verify

# 3. 清理
#    "command": "cleanup"
./dstore_stress_verify

# 一键全流程
#    "command": "all"
./dstore_stress_verify
```

---

## config.json 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `tables` | 表数量 | 1 |
| `table_size` | 每表行数 | 10000 |
| `threads` | 并发线程数 | 8 |
| `time` | 运行秒数 | 60 |
| `mode` | read_only / write_only / read_write | read_write |
| `command` | prepare / run / cleanup / all | all |
| `verify_level` | NONE / LIGHT / MEDIUM / HEAVY | LIGHT |
| `verify_modules` | heap,index,undo（逗号分隔） | heap,index,undo |
| `verify_on_write` | 写路径校验（失败 PANIC） | true |
| `verify_on_read` | 读路径校验（失败 ERROR） | true |
| `fault_inject_enabled` | 故障注入开关 | false |
| `fault_inject_time` | 注入时机（运行多少秒后） | 30 |
| `fault_type` | 故障类型（见下表） | crc_error |
| `fault_target_module` | 目标模块 | heap |
| `fault_target_page` | 目标页号（0=随机） | 0 |

### 校验级别

| 级别 | 复杂度 | 场景 |
|------|--------|------|
| NONE | - | 对照组，不校验 |
| LIGHT | O(1) | 生产热路径，仅页头 |
| MEDIUM | O(n) | 日常巡检，遍历 item |
| HEAVY | 深度穷举 | 故障排查，undo 链 + TD 状态 + tuple 重叠 |

### 故障类型

| fault_type | 说明 |
|------------|------|
| `crc_error` | CRC 校验失败 |
| `page_type_invalid` | 非法页面类型 |
| `page_id_mismatch` | 页号不匹配 |
| `boundary_error` | lower/upper/special 越界 |
| `heap_td_count_error` | Heap TD 数量异常 |
| `heap_itemid_align_error` | ItemId 对齐错误 |
| `heap_tuple_overlap` | Tuple 空间重叠 |
| `heap_tuple_size_error` | Tuple 大小不匹配 |
| `index_page_type_error` | B-tree 页类型非法 |
| `index_meta_page_error` | Meta page ID 非法 |
| `index_queue_error` | Queue head/tail/size 不一致 |

---

## 典型场景

### 高并发写冲突

```json
{ "tables": 1, "table_size": 1000, "threads": 8,
  "mode": "write_only", "verify_level": "HEAVY", "command": "all" }
```

### 不同校验级别对比

```bash
# prepare 一次，然后切换级别反复 run
for level in NONE LIGHT MEDIUM HEAVY; do
    sed -i "s/\"verify_level\":.*/\"verify_level\": \"$level\",/" config.json
    sed -i "s/\"command\":.*/\"command\": \"run\",/" config.json
    echo "=== $level ===" && ./dstore_stress_verify
done
```

### 批量故障注入

```bash
for fault in crc_error page_type_invalid heap_td_count_error; do
    sed -i "s/\"fault_type\":.*/\"fault_type\": \"$fault\",/" config.json
    echo "=== $fault ===" && ./dstore_stress_verify
    echo "exit_code=$?"
done
```

---

## 返回码

| 码 | 含义 |
|----|------|
| 0 | PASS，无校验错误 |
| 1 | FAIL，校验发现错误 |
| 2 | 运行时错误 |
| 3 | 配置错误 |

---

## 目录结构

```
tests/
├── build_and_run_stress_verify.sh      # 编译+运行入口
└── dstore_stress_verify/
    ├── CMakeLists.txt
    ├── config.json                     # 运行参数
    ├── run_crash_recovery_test.sh      # 崩溃恢复测试
    ├── include/                        # 头文件
    └── src/                            # 源码
```
