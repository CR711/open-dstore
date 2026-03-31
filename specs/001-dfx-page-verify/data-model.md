# Data Model: DFX Page Verification

## Core Entities

### VerifyLevel (enum class)

校验等级，GUC 参数值。

```
OFF           = 0   // 不校验
LIGHTWEIGHT   = 1   // 轻量级（写入路径）
HEAVYWEIGHT   = 2   // 重量级（完备校验）
```

### VerifyModule (enum class)

校验模块选择，GUC 参数值。

```
HEAP    = 0   // 仅 heap 相关页面
INDEX   = 1   // 仅 index 相关页面
ALL     = 2   // 所有类型
```

### VerifySeverity (enum class)

校验结果严重级别。

```
INFO      = 0   // 信息性（如跳过 in-progress tuple）
WARNING   = 1   // 可疑但可能是瞬态状态（如 mid-split）
ERROR     = 2   // 确定性损坏
```

### VerifyResult

单条校验结果。

| Field | Type | Description |
|-------|------|-------------|
| severity | VerifySeverity | 严重级别 |
| targetType | const char* | 校验目标类型（"page"/"tuple"/"btree"/"segment"/"metadata"） |
| targetId | PageId / ItemPointer / SegmentId | 校验目标标识 |
| checkName | const char* | 检查项名称（如 "crc_mismatch", "itemid_overflow"） |
| expected | uint64 | 期望值 |
| actual | uint64 | 实际值 |
| message | char[256] | 可读诊断信息 |

### VerifyReport

校验结果聚合器。

| Field | Type | Description |
|-------|------|-------------|
| results | std::vector\<VerifyResult\> | 结果集合 |
| totalChecks | uint64 | 总检查项数 |
| passedChecks | uint64 | 通过数 |
| failedChecks | uint64 | 失败数 |
| warningCount | uint64 | 警告数 |
| startTime | TimestampTz | 开始时间 |
| endTime | TimestampTz | 结束时间 |

**Methods**:
- `AddResult(VerifyResult)` — 添加一条结果
- `HasError() -> bool` — 是否包含 ERROR 级别结果
- `GetRetStatus() -> RetStatus` — 转换为 RetStatus
- `FormatText() -> std::string` — 格式化为可读文本
- `FormatJson() -> std::string` — 格式化为 JSON（CLI 工具用）

### PageVerifyFunc

页面校验函数签名。

```cpp
using PageVerifyFunc = RetStatus (*)(const Page* page, VerifyLevel level, VerifyReport* report);
```

### PageVerifyEntry

注册表条目，包含轻量级和重量级校验函数。

| Field | Type | Description |
|-------|------|-------------|
| pageType | PageType | 页面类型枚举 |
| typeName | const char* | 类型名称（用于诊断输出） |
| lightweightFunc | PageVerifyFunc | 轻量级校验函数 |
| heavyweightFunc | PageVerifyFunc | 重量级校验函数 |

### PageVerifyRegistry

单页面校验注册表（全局单例）。

| Field | Type | Description |
|-------|------|-------------|
| entries | std::array\<PageVerifyEntry, MAX_PAGE_TYPE\> | 按 PageType 索引的函数表 |
| registered | std::array\<bool, MAX_PAGE_TYPE\> | 是否已注册 |

**Methods**:
- `Register(PageType, typeName, lightweightFunc, heavyweightFunc)` — 注册校验函数
- `Verify(const Page*, VerifyLevel, VerifyReport*) -> RetStatus` — 根据 page type 分发校验
- `IsRegistered(PageType) -> bool` — 查询是否已注册

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
                                       │ dispatches to
                                       ▼
                                  PageVerifyFunc ──produces──> VerifyResult ──collected by──> VerifyReport

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
  → Collecting (AddResult being called)
    → Finalized (endTime set, ready for output)
```

### GUC Parameter State

```
OFF ↔ LIGHTWEIGHT ↔ HEAVYWEIGHT
(dynamic, any transition allowed at runtime)
```
