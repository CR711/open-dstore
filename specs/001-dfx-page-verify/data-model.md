# Data Model: DFX Page Verification

> Updated 2026-04-14 to align with API Contract v3.0 (`contracts/page-verify-api.md`)

## Core Entities

### VerifyLevel (enum class : uint8)

校验等级，GUC 参数值。

```
NONE          = 0   // 不校验（redo 强制；关闭校验）
LIGHT         = 1   // O(1) 写/读热路径
MEDIUM        = 2   // O(n) 页内遍历（巡检）
HEAVY         = 3   // O(n^2) 完备校验（巡检/离线）
```

恢复阶段覆盖：redo = NONE, undo <= LIGHT, 正常运行 = GUC。

### VerifyModule (enum class : uint8)

校验模块选择，64 位位掩码 GUC 参数。每个枚举值表示 bit 位索引，运行时通过 `1 << module` 转为位掩码。

```
HEAP      = 0   // bit 0 (0x01) — heap 相关页面
INDEX     = 1   // bit 1 (0x02) — index 相关页面
UNDO      = 2   // bit 2 (0x04) — undo 相关页面
SEGMENT   = 3   // bit 3 (0x08) — segment/tablespace 相关页面
FSM       = 4   // bit 4 (0x10) — FSM 相关页面
ALL       = 5   // 兼容：启用所有模块
```

GUC 默认值 `0x07` = HEAP | INDEX | UNDO。通过 `SetDfxVerifyModules(uint64)` / `GetDfxVerifyModules()` 访问，`IsModuleEnabled(VerifyModule)` 判断单模块是否启用。

### VerifySeverity (enum class : uint8)

校验结果严重级别。

```
SEVERITY_INFO      = 0   // 信息性（如跳过 in-progress tuple）
SEVERITY_WARNING   = 1   // 可疑但可能是瞬态状态（如 mid-split）
SEVERITY_ERROR     = 2   // 确定性损坏
SEVERITY_FATAL     = 3   // 致命错误（写路径触发 PANIC）
```

### VerifyCode (enum class : uint32)

校验错误码，按模块分段。

| 模块 | 范围 | 前缀 | 说明 |
|------|------|------|------|
| Generic | `0x0001`-`0x000F` | `PAGE_` | 通用页面校验（类型、ID、CRC、边界等） |
| Heap | `0x0100`-`0x012F` | `HEAP_` | Heap 页面/元组校验 |
| Index | `0x0200`-`0x022F` | `BTR_`/`INDEX_` | Index 单页校验 |
| Index 跨页 | `0x0230`-`0x024F` | `BTR_` | B-tree 跨页结构校验 |
| Index-Heap 一致性 | `0x0250`-`0x025F` | `INDEX_HEAP_` | Index-Heap 对应校验 |
| Undo | `0x0300`-`0x031F` | `UNDO_` | Undo 记录/事务槽校验 |
| Segment | `0x0400`-`0x041F` | `SEG_`/`BITMAP_`/`FILE_`/`SPACE_` | Segment/Tablespace 校验 |

完整定义见 `include/dfx/dstore_verify_report.h`。

### VerifyResult

单条校验结果。

| Field | Type | Description |
|-------|------|-------------|
| severity | VerifySeverity | 严重级别 |
| code | VerifyCode | 错误码（模块分段，默认 OK） |
| targetType | const char* | 校验目标类型（"page"/"tuple"/"btree"/"segment"/"metadata"） |
| targetId | PageId | 校验目标标识 |
| checkName | const char* | 检查项名称（如 "crc_mismatch", "itemid_overflow"） |
| expected | uint64 | 期望值 |
| actual | uint64 | 实际值 |
| message | char[256] | 可读诊断信息 |

### VerifyReport

校验结果聚合器。非线程安全，每个校验会话必须使用独立实例。

| Field | Type | Description |
|-------|------|-------------|
| m_results | std::vector\<VerifyResult\> | 结果集合 |
| m_totalChecks | uint64 | 总检查项数 |
| m_passedChecks | uint64 | 通过数 |
| m_failedChecks | uint64 | 失败数 |
| m_warningCount | uint64 | 警告数 |
| m_fatalCount | uint64 | 致命错误数 |
| m_startTime | TimestampTz | 开始时间 |
| m_endTime | TimestampTz | 结束时间 |

**Methods**:
- `AddResult(const VerifyResult &)` — 添加一条结果
- `AddResult(severity, targetType, targetId, checkName, expected, actual, format, ...)` — 构造并添加
- `AddResultWithCode(severity, code, targetType, targetId, checkName, expected, actual, format, ...)` — 携带 VerifyCode 构造并添加
- `HasError() -> bool` — 是否包含 ERROR 级别结果
- `HasFatal() -> bool` — 是否包含 FATAL 级别结果
- `GetErrorCount() -> uint64` — ERROR 数量
- `GetWarningCount() -> uint64` — WARNING 数量
- `GetFatalCount() -> uint64` — FATAL 数量
- `GetTotalChecks() -> uint64` — 总检查项数
- `GetRetStatus() -> RetStatus` — 转换为 RetStatus（HasError() ? DSTORE_FAIL : DSTORE_SUCC）
- `FormatText() -> std::string` — 格式化为可读文本（日志/CLI）
- `FormatJson() -> std::string` — 格式化为 JSON（CLI 工具用）
- `GetResults() -> const std::vector<VerifyResult>&` — 获取结果集

热路径禁止无条件构造 VerifyReport（含 `std::vector` 和 `GetCurrentTimestamp()`），必须走两阶段优化（读路径 `report=nullptr` 零分配）。

### PageVerifyFunc

页面校验函数签名。

```cpp
using PageVerifyFunc = RetStatus (*)(const Page* page, VerifyLevel level, VerifyReport* report);
```

### PageVerifyEntry

注册表条目，包含三级校验函数和模块归属。

| Field | Type | Description |
|-------|------|-------------|
| pageType | PageType | 页面类型枚举 |
| typeName | const char* | 类型名称（用于诊断输出） |
| moduleGroup | VerifyModule | 所属模块（用于 GUC 位掩码过滤） |
| lightFunc | PageVerifyFunc | LIGHT 级校验函数（O(1)） |
| mediumFunc | PageVerifyFunc | MEDIUM 级校验函数（O(n)） |
| heavyFunc | PageVerifyFunc | HEAVY 级校验函数（O(n^2)） |

### PageVerifyRegistry

单页面校验注册表（全局单例）。

| Field | Type | Description |
|-------|------|-------------|
| m_entries | std::array\<PageVerifyEntry, PAGE_TYPE_COUNT\> | 按 PageType 索引的函数表 |
| m_registered | std::array\<bool, PAGE_TYPE_COUNT\> | 是否已注册 |

**Methods**:
- `Register(PageType, typeName, VerifyModule, lightFunc, mediumFunc, heavyFunc)` — 注册校验函数
- `Verify(const Page*, VerifyLevel, VerifyReport*) -> RetStatus` — 根据 page type 分发校验（fail-fast）
- `VerifyFull(const Page*, VerifyLevel, VerifyReport*) -> RetStatus` — 巡检模式，收集所有问题
- `IsRegistered(PageType) -> bool` — 查询是否已注册
- `Find(PageType) -> const PageVerifyEntry*` — 查找注册条目
- `static ResolveModule(PageType) -> VerifyModule` — 根据页面类型推断所属模块

`Verify()` 按级别链式调用 light -> medium -> heavy，fail-fast。`report` 可为 nullptr。

**模块注册函数**：
`RegisterHeapPageVerifier()`, `RegisterIndexPageVerifier()`, `RegisterFsmPageVerifiers()`, `RegisterUndoPageVerifiers()`, `RegisterSegmentPageVerifiers()`, `RegisterTablespacePageVerifiers()`, `RegisterBtrRecyclePageVerifiers()`。

**三场景入口**：
- `VerifyPageOnWrite(page, level)` — 写路径：失败先 ERROR 落盘诊断，再 PANIC
- `VerifyPageOnRead(page, level, report)` — 读路径：失败返回错误码；report 可为 nullptr（两阶段优化第一阶段零分配）
- `VerifyPageFull(page, level, report)` — 巡检：收集问题，总是返回 DSTORE_SUCC，通过 `report->HasError()` 查询

写/读路径 `level` 仅控制是否跳过（NONE 时跳过），实际强制 LIGHT。巡检支持全部级别。

### VerifyContext

跨页面校验的共享上下文。

| Field | Type | Description |
|-------|------|-------------|
| report | VerifyReport* | 结果收集器 |
| snapshot | SnapshotData* | MVCC 快照（online 模式） |
| sampleRatio | float | 采样比例（0.0~1.0） |
| isOnline | bool | 是否在线模式 |
| visitedPages | std::unordered_set\<uint64\> | 已访问页面集（环检测） |
| maxErrors | uint32 | 最大错误数（达到后停止） |

VerifyContext 非线程安全（无锁），每个校验会话必须使用独立实例，禁止跨线程共享。

### MetadataInputStruct

上层（InnoDB）传入的元数据校验结构体。

| Field | Type | Description |
|-------|------|-------------|
| tableOid | Oid | 表 OID |
| heapSegmentId | SegmentId | Heap segment ID |
| lobSegmentId | SegmentId | LOB segment ID（INVALID if none） |
| indexCount | uint16 | 索引数量 |
| indexes | IndexMetaEntry[] | 索引元数据数组 |
| tablespaceId | uint32 | Tablespace ID |
| ownerTablespaceId | uint32 | 表所属 Tablespace ID |
| heapRowFormat | uint16 | Heap 行格式 |
| indexRowFormat | uint16 | Index 行格式 |

### IndexMetaEntry

单个索引的元数据。

| Field | Type | Description |
|-------|------|-------------|
| indexOid | Oid | 索引 OID |
| indexSegmentId | SegmentId | 索引 segment ID |
| nKeyAtts | uint16 | 索引键列数 |
| attTypeIds | Oid[] | 键列类型 OID 数组 |

## Entity Relationships

```
PageVerifyRegistry  1 ──contains── N  PageVerifyEntry
                                       │
                                       │ dispatches to (light -> medium -> heavy)
                                       ▼
                                  PageVerifyFunc ──produces──> VerifyResult(code, severity) ──collected by──> VerifyReport

VerifyContext  1 ──holds── 1  VerifyReport
               1 ──holds── 1  SnapshotData (optional)

BtreeVerifier ──uses── VerifyContext
HeapSegmentVerifier ──uses── VerifyContext
SegmentVerifier ──uses── VerifyContext
MetadataVerifier ──uses── VerifyContext + MetadataInputStruct
```

## State Transitions

### VerifyReport Lifecycle

```
Created (empty)
  → Collecting (AddResult / AddResultWithCode being called)
    → Finalized (endTime set, ready for output)
```

### GUC Parameter State

```
VerifyLevel:   NONE ↔ LIGHT ↔ MEDIUM ↔ HEAVY
               (dynamic, any transition allowed at runtime, atomic memory_order_relaxed)

VerifyModules: 64-bit bitmask (default 0x07 = HEAP|INDEX|UNDO)
               (dynamic, any combination allowed at runtime, atomic memory_order_relaxed)
```
