# DStore DFX 单页面逻辑校验 设计方案

| 版本 | 作者 | 日期 | 状态 |
|------|------|------|------|
| 1.0 | DStore Team | 2026-03-24 | Draft |

---

## 1. 背景与目标

### 1.1 背景

DStore 存储引擎采用 8KB 固定大小的页面作为基本存储单元，共定义了 17 种页面类型（PageType），涵盖 heap 数据页面、index 索引页面、FSM 空闲空间映射、segment 元数据、tablespace 位图、undo 事务日志等。

目前 DStore 已有的页面校验机制较为分散：
- `Page::CheckPageCrcMatch()` — CRC 校验，仅在特定路径调用
- `DataPage::CheckSanity()` — 仅在 `DSTORE_USE_ASSERT_CHECKING` 宏开启时生效，生产环境不可用
- `IndexPage::CheckSanity()` — 同上，仅 debug 模式

这些机制存在以下问题：
1. **覆盖不全面**：仅 heap/index 数据页面有结构校验，其余 15 种页面类型无校验逻辑
2. **不可配置**：debug 模式全开或全关，无法在生产环境按需启用
3. **无统一框架**：各模块校验逻辑分散，无法统一管理和扩展
4. **缺乏写入路径拦截**：无法在数据写入时主动拦截校验，损坏数据可能已持久化

### 1.2 目标

设计并实现一套**可扩展、可配置的单页面校验框架**，实现以下目标：

1. **全覆盖**：为所有 17 种 PageType 提供专用校验函数
2. **双级校验**：
   - **轻量级（Lightweight）**：嵌入写入路径（CRUD + 刷脏），仅校验 header 级信息，性能开销极低
   - **重量级（Heavyweight）**：按需触发，校验页面完整结构，保证完备性
3. **可配置**：通过运行时 GUC 参数动态控制校验级别和模块，无需重启
4. **可扩展**：基于 Registry 模式，新增页面类型只需注册校验函数，不修改框架代码

### 1.3 参考

- PostgreSQL `amcheck` 扩展 — 模块化的页面校验设计（verify_nbtree.c / verify_heapam.c 各自独立）
- PostgreSQL `Page` 校验 — header 通用校验 + 类型专用校验分离
- DStore 现有 `CheckSanity()` / `CheckPageCrcMatch()` 实现

---

## 2. 总体架构

### 2.1 架构概览

```
                          ┌─────────────────────────────────┐
                          │     写入路径调用点                │
                          │  (Insert/Delete/Update/Flush)    │
                          └──────────────┬──────────────────┘
                                         │ VerifyPageInline(page)
                                         ▼
                          ┌─────────────────────────────────┐
                          │        GUC 参数检查              │
                          │  dfx_verify_level == OFF ?  ──→ 跳过
                          │  dfx_verify_module 匹配 ?   ──→ 跳过
                          └──────────────┬──────────────────┘
                                         │
                                         ▼
                          ┌─────────────────────────────────┐
                          │   通用 Header 校验（框架层）      │
                          │  CRC / bounds / LSN / type       │
                          └──────────────┬──────────────────┘
                                         │ 通过后
                                         ▼
                          ┌─────────────────────────────────┐
                          │     PageVerifyRegistry            │
                          │  entries[page->GetType()]         │
                          │  按 PageType 枚举值 O(1) 分发     │
                          └──────────────┬──────────────────┘
                                         │
          ┌──────────┬──────────┬────────┼────────┬──────────┬──────────┐
          ▼          ▼          ▼        ▼        ▼          ▼          ▼
     ┌─────────┐┌─────────┐┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐
     │  Heap   ││  Index  ││  FSM   ││  Undo  ││ TxnSlot││ Bitmap ││ Extent │ ...
     │ Verify  ││ Verify  ││ Verify ││ Verify ││ Verify ││ Verify ││ Verify │
     └────┬────┘└────┬────┘└────┬───┘└────┬───┘└────┬───┘└────┬───┘└────┬───┘
          │          │          │         │         │         │         │
          └──────────┴──────────┴─────────┴─────────┴─────────┴─────────┘
                                         │
                                         ▼
                          ┌─────────────────────────────────┐
                          │       VerifyReport               │
                          │  收集结果 → 返回 RetStatus        │
                          └─────────────────────────────────┘
```

### 2.2 设计原则

| 原则 | 说明 |
|------|------|
| **零开销原则** | GUC=OFF 时，VerifyPageInline() 仅读取一个 atomic 变量后立即返回，无额外开销 |
| **框架-插件分离** | 框架负责通用 header 校验 + 分发；各模块注册自己的类型专用校验函数 |
| **不中断语义** | 校验函数仅报告问题 + 返回错误码，不主动中断操作；由调用方决策 |
| **生产可用** | 不依赖 `DSTORE_USE_ASSERT_CHECKING` 宏，生产环境可通过 GUC 动态启用 |

---

## 3. 详细设计

### 3.1 数据结构定义

#### 3.1.1 校验级别与模块枚举

```cpp
// include/dfx/dstore_verify_report.h

namespace DSTORE {

enum class VerifyLevel : uint8_t {
    OFF         = 0,    // 不校验
    LIGHTWEIGHT = 1,    // 轻量级（写入路径）
    HEAVYWEIGHT = 2     // 重量级（完备校验）
};

enum class VerifyModule : uint8_t {
    HEAP  = 0,    // heap 及其关联页面（FSM、HeapSegmentMeta）
    INDEX = 1,    // index 及其关联页面（BtrRecycle）
    ALL   = 2     // 所有类型
};

enum class VerifySeverity : uint8_t {
    INFO    = 0,    // 信息性（跳过 in-progress tuple 等）
    WARNING = 1,    // 可疑但可能是瞬态（mid-split 等）
    ERROR   = 2     // 确定性损坏
};

}  // namespace DSTORE
```

**PageType 到 VerifyModule 的归类规则**：

| VerifyModule | 包含的 PageType |
|-------------|----------------|
| HEAP | HEAP_PAGE_TYPE, FSM_PAGE_TYPE, FSM_META_PAGE_TYPE, HEAP_SEGMENT_META_PAGE_TYPE |
| INDEX | INDEX_PAGE_TYPE, BTR_QUEUE_PAGE_TYPE, BTR_RECYCLE_PARTITION_META_PAGE_TYPE, BTR_RECYCLE_ROOT_META_PAGE_TYPE |
| 通用（HEAP 和 INDEX 都包含） | TRANSACTION_SLOT_PAGE, UNDO_PAGE_TYPE, DATA_SEGMENT_META_PAGE_TYPE, UNDO_SEGMENT_META_PAGE_TYPE, TBS_EXTENT_META_PAGE_TYPE, TBS_BITMAP_PAGE_TYPE, TBS_BITMAP_META_PAGE_TYPE, TBS_FILE_META_PAGE_TYPE, TBS_SPACE_META_PAGE_TYPE |

> 通用类型在 HEAP 或 INDEX 模式下均会被校验；仅当 module=HEAP 时跳过 INDEX 专属类型，反之亦然。

#### 3.1.2 校验结果

```cpp
// include/dfx/dstore_verify_report.h

namespace DSTORE {

struct VerifyResult {
    VerifySeverity severity;
    const char*    targetType;              // "page" / "itemid" / "td" / "tuple"
    PageId         pageId;                  // 目标页面 ID
    uint16         offsetNum;               // 页内偏移号（如适用，否则 0）
    const char*    checkName;               // 检查项标识（如 "crc_mismatch"）
    uint64         expected;                // 期望值
    uint64         actual;                  // 实际值
    char           message[256];            // 可读诊断信息
};

class VerifyReport {
public:
    VerifyReport();
    ~VerifyReport();

    DISALLOW_COPY_AND_MOVE(VerifyReport);

    // 添加校验结果
    void AddResult(VerifySeverity severity, const char* targetType,
                   const PageId& pageId, uint16 offsetNum,
                   const char* checkName, uint64 expected, uint64 actual,
                   const char* format, ...);

    // 查询
    bool HasError() const;
    uint64 GetErrorCount() const;
    uint64 GetWarningCount() const;
    uint64 GetTotalChecks() const;
    RetStatus GetRetStatus() const;    // HasError() ? DSTORE_FAIL : DSTORE_SUCC

    // 输出
    std::string FormatText() const;    // 可读文本
    std::string FormatJson() const;    // JSON 格式

    const std::vector<VerifyResult>& GetResults() const;

private:
    std::vector<VerifyResult> m_results;
    uint64 m_totalChecks = 0;
    uint64 m_passedChecks = 0;
    uint64 m_errorCount = 0;
    uint64 m_warningCount = 0;
};

}  // namespace DSTORE
```

#### 3.1.3 校验函数签名与注册表

```cpp
// include/dfx/dstore_page_verify.h

namespace DSTORE {

// 页面校验函数签名
// page: 待校验页面指针（const，只读）
// level: 当前校验级别
// report: 结果收集器
// 返回值: DSTORE_SUCC=通过, DSTORE_FAIL=发现问题
using PageVerifyFunc = RetStatus (*)(const Page* page, VerifyLevel level, VerifyReport* report);

// 注册表条目
struct PageVerifyEntry {
    PageType      pageType      = PageType::INVALID_PAGE_TYPE;
    const char*   typeName      = nullptr;          // 类型名称（诊断输出用）
    PageVerifyFunc lightweightFunc = nullptr;        // 轻量级校验
    PageVerifyFunc heavyweightFunc = nullptr;        // 重量级校验
    VerifyModule   moduleGroup   = VerifyModule::ALL; // 所属模块分组
};

// 全局注册表
class PageVerifyRegistry {
public:
    static PageVerifyRegistry& Instance();

    // 注册（各模块初始化时调用）
    void Register(PageType type, const char* typeName,
                  PageVerifyFunc lightweightFunc,
                  PageVerifyFunc heavyweightFunc,
                  VerifyModule moduleGroup = VerifyModule::ALL);

    // 校验分发（内部先做通用 header 校验，再分发到类型专用函数）
    RetStatus Verify(const Page* page, VerifyLevel level, VerifyReport* report) const;

    bool IsRegistered(PageType type) const;

private:
    PageVerifyRegistry() = default;
    DISALLOW_COPY_AND_MOVE(PageVerifyRegistry);

    std::array<PageVerifyEntry, static_cast<size_t>(PageType::MAX_PAGE_TYPE)> m_entries{};
    std::array<bool, static_cast<size_t>(PageType::MAX_PAGE_TYPE)> m_registered{};
};

}  // namespace DSTORE
```

#### 3.1.4 GUC 参数

```cpp
// include/dfx/dstore_page_verify.h（续）

namespace DSTORE {

// GUC 全局变量（atomic，支持并发读写无锁）
extern std::atomic<VerifyLevel>  g_dfxVerifyLevel;    // 默认 OFF
extern std::atomic<VerifyModule> g_dfxVerifyModule;    // 默认 ALL

// Setter（供 SET 命令调用）
void SetDfxVerifyLevel(VerifyLevel level);
void SetDfxVerifyModule(VerifyModule module);

// Getter（inline，性能关键路径）
inline VerifyLevel GetDfxVerifyLevel()
{
    return g_dfxVerifyLevel.load(std::memory_order_relaxed);
}

inline VerifyModule GetDfxVerifyModule()
{
    return g_dfxVerifyModule.load(std::memory_order_relaxed);
}

}  // namespace DSTORE
```

### 3.2 公共接口

```cpp
// include/dfx/dstore_page_verify.h（续）

namespace DSTORE {

// ========== 写入路径 inline 校验 ==========

// 根据 GUC 自动决定是否执行和级别
// GUC=OFF 时仅读取一个 atomic 变量后返回 DSTORE_SUCC（零开销）
// 返回 DSTORE_FAIL 时调用方决定是否中断写入
inline RetStatus VerifyPageInline(const Page* page)
{
    VerifyLevel level = GetDfxVerifyLevel();
    if (level == VerifyLevel::OFF) {
        return DSTORE_SUCC;
    }
    // 模块过滤由 Registry::Verify 内部处理
    return PageVerifyRegistry::Instance().Verify(page, level, nullptr);
}

// 带 report 的版本（需要收集诊断信息时使用）
inline RetStatus VerifyPageInlineWithReport(const Page* page, VerifyReport* report)
{
    VerifyLevel level = GetDfxVerifyLevel();
    if (level == VerifyLevel::OFF) {
        return DSTORE_SUCC;
    }
    return PageVerifyRegistry::Instance().Verify(page, level, report);
}

// ========== 按需校验 ==========

// 指定 level 的单页面校验
RetStatus VerifyPage(const Page* page, VerifyLevel level, VerifyReport* report);

// 校验指定 PageId（自动从 buffer 读取页面）
RetStatus VerifyPageById(const PageId& pageId, VerifyLevel level, VerifyReport* report);

// ========== 初始化 ==========

// 注册所有 17 种 PageType 的校验函数（在 StorageInstance 初始化时调用）
void InitPageVerifiers();

}  // namespace DSTORE
```

### 3.3 通用 Header 校验逻辑

在 `PageVerifyRegistry::Verify()` 中，分发到类型专用函数之前，先执行以下通用校验：

```
通用 Header 校验（所有 PageType 共享）
├── 1. All-zero page 检测
│     如果整个页面全为 0，视为合法的未初始化页面，跳过后续校验，返回 SUCC
│
├── 2. CRC 校验
│     调用 Page::CheckPageCrcMatch()
│     失败 → 报告 ERROR "crc_mismatch"
│
├── 3. PageType 有效性
│     page->GetType() 必须在 [1, MAX_PAGE_TYPE) 范围内
│     失败 → 报告 ERROR "invalid_page_type"
│
├── 4. VerifyModule 过滤
│     根据 GUC dfx_verify_module 和 PageType 的 moduleGroup 判断是否跳过
│
├── 5. Lower/Upper bounds 一致性
│     m_lower <= m_upper <= BLCKSZ
│     m_lower >= sizeof(PageHeader)（至少包含 header）
│     失败 → 报告 ERROR "bounds_invalid"
│
├── 6. LSN 合理性
│     glsn != UINT64_MAX
│     对已初始化页面：glsn > 0（非零）
│     失败 → 报告 WARNING "lsn_anomaly"
│
└── 7. Special region offset
      如果 m_special.m_offset > 0：offset <= BLCKSZ
      失败 → 报告 ERROR "special_offset_invalid"
```

### 3.4 各 PageType 专用校验逻辑

#### 3.4.1 Heap 页面（HEAP_PAGE_TYPE）

**轻量级校验**（在通用 header 校验之后）：
- HeapPageHeader.potentialDelSize <= 可用空间
- HeapPageHeader.fsmIndex.page 非 INVALID 或页面为首次写入

**重量级校验**：

```
Heap 页面重量级校验
│
├── 1. TD 数组校验
│     ├── tdCount 在 [MIN_TD_COUNT(2), MAX_TD_COUNT(128)] 范围内
│     ├── TD 数组空间不超过 m_lower
│     └── 每个 TD 的状态校验：
│           ├── m_status 值在 {UNOCCUPY_AND_PRUNEABLE, OCCUPY_TRX_IN_PROGRESS, OCCUPY_TRX_END} 中
│           ├── UNOCCUPY_AND_PRUNEABLE: m_xid 应为 INVALID 或有效 recycled xid
│           ├── OCCUPY_TRX_IN_PROGRESS: m_xid != INVALID, m_undoRecPtr != INVALID
│           ├── OCCUPY_TRX_END: m_xid != INVALID, m_csn 应有效
│           └── m_csnStatus 在 {IS_INVALID, IS_PREV_XID_CSN, IS_CUR_XID_CSN} 中
│
├── 2. ItemId 数组校验
│     遍历所有 ItemId（从 FIRST_ITEM_OFFSET_NUMBER 到 MaxOffsetNumber）：
│     ├── ITEM_ID_UNUSED (flags=0): m_len == 0, m_offset == 0
│     ├── ITEM_ID_NORMAL (flags=1):
│     │     ├── m_len > 0
│     │     ├── m_offset >= DataHeaderSize + TdDataSize
│     │     ├── m_offset + m_len <= BLCKSZ（或 special region offset）
│     │     └── 记录 (offset, len) 用于重叠检测
│     ├── ITEM_ID_UNREADABLE_RANGE_HOLDER (flags=2): m_len > 0
│     └── ITEM_ID_NO_STORAGE (flags=3):
│           ├── m_tdId < tdCount
│           ├── m_tdStatus 在有效范围 [0, 2]
│           └── m_tupLiveMode 在有效范围 [0, 6]
│
├── 3. Item 存储区域重叠检测
│     将所有 NORMAL ItemId 的 (offset, offset+len) 区间排序
│     检测是否存在区间重叠
│
├── 4. ItemId datalen vs Tuple size 一致性
│     对每个 NORMAL ItemId：
│     ├── 读取对应 offset 位置的 HeapDiskTuple
│     └── ItemId.m_len == tuple.m_ext_info.m_tuple_info.m_size
│         不一致 → 报告 ERROR "itemid_tuple_size_mismatch"
│
└── 5. TD-Tuple 交叉引用
      对每个 NORMAL ItemId 的 tuple：
      ├── tuple.m_ext_info.m_tuple_info.m_tdId < tdCount
      └── 引用的 TD 状态与 tuple 状态逻辑一致
```

#### 3.4.2 Index 页面（INDEX_PAGE_TYPE）

**轻量级校验**：
- Special region offset 和大小能容纳 `BtrPageLinkAndStatus`
- BtrPageLinkAndStatus.status.bitVal.type 在 {LEAF_PAGE, INTERNAL_PAGE, META_PAGE} 中

**重量级校验**：

```
Index 页面重量级校验
│
├── 1. BtrPageLinkAndStatus 完整性
│     ├── level <= BTREE_HIGHEST_LEVEL (32)
│     ├── type 在有效枚举范围内
│     ├── splitStat 在 {SPLIT_COMPLETE, SPLIT_INCOMPLETE} 中
│     ├── liveStat 在有效枚举范围内
│     └── prev/next sibling link 格式有效（INVALID 或合法 PageId）
│
├── 2. High Key 校验
│     ├── 非最右页面：ItemId[BTREE_PAGE_HIKEY(1)] 必须为 NORMAL 状态
│     ├── 读取 high key 值
│     └── high key >= 所有其他 key（遍历 offset 2..MaxOffset，逐一比较）
│
├── 3. 页内 Key 排序
│     ├── 确定起始 offset（最右页面从 HIKEY，非最右从 FIRSTKEY）
│     └── 相邻 key 满足排序关系（升序）
│         key[i] <= key[i+1]
│
├── 4. Key 类型一致性（需要 BtrMeta 信息时）
│     如果可访问 BtrMeta：
│     ├── 每个 key 的属性数量 <= BtrMeta.nkeyAtts
│     └── key 值类型与 BtrMeta.attTypeIds 一致
│
└── 5. ItemId 状态校验（复用通用 ItemId 校验逻辑）
```

#### 3.4.3 其他页面类型

| PageType | 轻量级特有校验 | 重量级特有校验 |
|----------|------------|------------|
| **FSM_PAGE_TYPE** | 无额外 | FSM entry 值在有效 category 范围内 |
| **FSM_META_PAGE_TYPE** | 无额外 | numFsmLevels <= MAX_LEVEL, listRange 递增, numTotalPages >= numUsedPages |
| **DATA_SEGMENT_META_PAGE_TYPE** | 无额外 | segmentType 有效, totalBlockCount > 0, extent 链头指针格式合法 |
| **HEAP_SEGMENT_META_PAGE_TYPE** | 无额外 | 继承 DataSegmentMeta 校验 + numFsms <= MAX_FSM_TREE, dataFirst/dataLast 格式合法 |
| **UNDO_SEGMENT_META_PAGE_TYPE** | 无额外 | segmentType == UNDO, extent 链头指针格式合法 |
| **TRANSACTION_SLOT_PAGE** | 无额外 | version 有效, 每个 slot status 在 7 种合法状态中, CSN-status 一致性 |
| **UNDO_PAGE_TYPE** | 无额外 | UndoRecordPageHeader prev/next 格式合法, version 有效 |
| **TBS_EXTENT_META_PAGE_TYPE** | magic == EXTENT_META_MAGIC | extSize 在 {8,128,1024,8192} 中, nextExtMetaPageId 格式合法 |
| **TBS_BITMAP_PAGE_TYPE** | 无额外 | allocatedExtentCount == popcount(bitmap), firstDataPageId 格式合法 |
| **TBS_BITMAP_META_PAGE_TYPE** | 无额外 | groupCount <= MAX_BITMAP_GROUP_CNT(512), extentSize 有效 |
| **TBS_FILE_META_PAGE_TYPE** | 无额外 | 文件元数据字段范围合法 |
| **TBS_SPACE_META_PAGE_TYPE** | 无额外 | tablespace 元数据字段合法 |
| **BTR_QUEUE_PAGE_TYPE** | 无额外 | queue 结构字段范围合法 |
| **BTR_RECYCLE_PARTITION_META_PAGE_TYPE** | 无额外 | partition 元数据一致性 |
| **BTR_RECYCLE_ROOT_META_PAGE_TYPE** | 无额外 | root 元数据一致性 |

### 3.5 模块注册机制

各模块在各自的源文件中实现校验函数并提供注册入口：

```cpp
// src/heap/dstore_heap_page_verify.cpp

namespace DSTORE {

static RetStatus VerifyHeapPageLightweight(const Page* page, VerifyLevel level, VerifyReport* report)
{
    const auto* heapPage = static_cast<const DataPage*>(page);
    // ... heap-specific lightweight checks ...
    return DSTORE_SUCC;
}

static RetStatus VerifyHeapPageHeavyweight(const Page* page, VerifyLevel level, VerifyReport* report)
{
    const auto* heapPage = static_cast<const DataPage*>(page);
    // ... TD 校验, ItemId 校验, 重叠检测, datalen-tuple 一致性 ...
    return (report && report->HasError()) ? DSTORE_FAIL : DSTORE_SUCC;
}

void RegisterHeapPageVerifier()
{
    PageVerifyRegistry::Instance().Register(
        PageType::HEAP_PAGE_TYPE,
        "HeapPage",
        VerifyHeapPageLightweight,
        VerifyHeapPageHeavyweight,
        VerifyModule::HEAP);
}

}  // namespace DSTORE
```

统一初始化：

```cpp
// src/dfx/dstore_page_verify.cpp

namespace DSTORE {

// 各模块注册函数声明
void RegisterHeapPageVerifier();
void RegisterIndexPageVerifier();
void RegisterFsmPageVerifier();
void RegisterFsmMetaPageVerifier();
void RegisterUndoPageVerifier();
void RegisterTransactionSlotPageVerifier();
void RegisterDataSegmentMetaPageVerifier();
void RegisterHeapSegmentMetaPageVerifier();
void RegisterUndoSegmentMetaPageVerifier();
void RegisterExtentMetaPageVerifier();
void RegisterBitmapPageVerifier();
void RegisterBitmapMetaPageVerifier();
void RegisterFileMetaPageVerifier();
void RegisterSpaceMetaPageVerifier();
void RegisterBtrQueuePageVerifier();
void RegisterBtrRecyclePartitionMetaPageVerifier();
void RegisterBtrRecycleRootMetaPageVerifier();

void InitPageVerifiers()
{
    RegisterHeapPageVerifier();
    RegisterIndexPageVerifier();
    RegisterFsmPageVerifier();
    RegisterFsmMetaPageVerifier();
    RegisterUndoPageVerifier();
    RegisterTransactionSlotPageVerifier();
    RegisterDataSegmentMetaPageVerifier();
    RegisterHeapSegmentMetaPageVerifier();
    RegisterUndoSegmentMetaPageVerifier();
    RegisterExtentMetaPageVerifier();
    RegisterBitmapPageVerifier();
    RegisterBitmapMetaPageVerifier();
    RegisterFileMetaPageVerifier();
    RegisterSpaceMetaPageVerifier();
    RegisterBtrQueuePageVerifier();
    RegisterBtrRecyclePartitionMetaPageVerifier();
    RegisterBtrRecycleRootMetaPageVerifier();
}

}  // namespace DSTORE
```

### 3.6 写入路径集成点

以下是需要嵌入 `VerifyPageInline()` 调用的关键位置：

| 调用点 | 文件 | 函数 | 时机 |
|--------|------|------|------|
| 脏页刷盘 | `src/buffer/dstore_bg_disk_page_writer.cpp` | `BgDiskPageMasterWriter::FlushAllDirtyPages()` | 写入磁盘前 |
| Heap 插入 | `src/heap/dstore_heap_insert.cpp` | `HeapInsertHandler::Insert()` | 页面修改后、解锁前 |
| Heap 删除 | `src/heap/dstore_heap_delete.cpp` | `HeapDeleteHandler::Delete()` | 标记删除后 |
| Heap 更新 | `src/heap/dstore_heap_update.cpp` | `HeapUpdateHandler::Update()` | 更新完成后 |
| Index 插入 | `src/index/` | BtrInsert 相关函数 | 页面修改后 |
| Index 删除 | `src/index/` | BtrDelete 相关函数 | 页面修改后 |
| Index 分裂 | `src/index/` | 页面分裂完成后 | 分裂完成后 |

**集成方式**：

```cpp
// 示例：在 FlushAllDirtyPages 中集成
RetStatus ret = VerifyPageInline(page);
if (ret != DSTORE_SUCC) {
    ErrLog(DSTORE_ERROR, MODULE_DFX,
           ErrMsg("Page verify failed before flush, pageId=(%u,%u), type=%u"),
           page->GetFileId(), page->GetBlockId(), static_cast<uint8>(page->GetType()));
    // 调用方决策：可以选择跳过此页面的写入，或记录后继续
}
```

### 3.7 Transient State 处理

| 瞬态场景 | 处理方式 |
|----------|---------|
| All-zero page（未初始化） | 通用 header 校验阶段识别并跳过，返回 SUCC |
| B-tree mid-split (SPLIT_INCOMPLETE) | 重量级校验时识别 splitStat，对 SPLIT_INCOMPLETE 的页面放宽 sibling link 校验，报告 WARNING 而非 ERROR |
| In-progress transaction（TD 状态 OCCUPY_TRX_IN_PROGRESS） | 单页面校验时作为合法 TD 状态接受；跨页面校验时由 VerifyContext 的 MVCC 快照处理 |
| 页面正在被修改 | 写入路径 inline 校验在页面修改完成后、解锁前执行，此时页面内容已稳定 |

---

## 4. 文件组织

```
include/dfx/
├── dstore_page_verify.h          # Registry, GUC, VerifyPageInline, InitPageVerifiers
└── dstore_verify_report.h        # VerifyLevel, VerifyModule, VerifySeverity, VerifyResult, VerifyReport

src/dfx/
├── dstore_page_verify.cpp        # Registry 实现, 通用 header 校验, InitPageVerifiers
└── dstore_verify_report.cpp      # VerifyReport 实现

src/heap/
└── dstore_heap_page_verify.cpp   # Heap 轻量级+重量级校验, RegisterHeapPageVerifier

src/index/
├── dstore_index_page_verify.cpp  # Index 轻量级+重量级校验, RegisterIndexPageVerifier
└── dstore_btr_recycle_page_verify.cpp  # BtrQueue/Recycle 校验

src/page/
└── dstore_fsm_page_verify.cpp    # FSM/FSMMeta 校验

src/undo/
└── dstore_undo_page_verify.cpp   # Undo/TxnSlot 校验

src/tablespace/
└── dstore_tbs_page_verify.cpp    # Bitmap/BitmapMeta/Extent/File/Space 校验

tests/unittest/ut_dfx/
├── ut_page_verify_registry.cpp   # Registry 注册/分发/GUC 测试
├── ut_verify_report.cpp          # Report 格式化测试
├── ut_heap_page_verify.cpp       # Heap 校验测试
└── ut_index_page_verify.cpp      # Index 校验测试
```

---

## 5. 测试策略

### 5.1 单元测试

| 测试用例 | 输入 | 期望结果 |
|----------|------|---------|
| 有效 heap page 轻量级校验 | 正确构造的 heap page | 返回 SUCC，report 无 ERROR |
| CRC 损坏 | 修改页面数据但不更新 checksum | 通用校验阶段报告 ERROR "crc_mismatch" |
| ItemId offset 越界 | NORMAL ItemId.offset > BLCKSZ | 报告 ERROR "itemid_offset_overflow" |
| ItemId datalen 与 tuple size 不一致 | ItemId.len = 100, tuple.size = 80 | 报告 ERROR "itemid_tuple_size_mismatch" |
| TD 状态非法 | TD.m_status = 3（超出枚举范围） | 报告 ERROR "td_status_invalid" |
| NO_STORAGE ItemId tupLiveMode 非法 | m_tupLiveMode = 7 | 报告 ERROR "itemid_livemode_invalid" |
| Item 存储区域重叠 | 两个 NORMAL ItemId 区间有交集 | 报告 ERROR "item_overlap" |
| All-zero page | 全零页面 | 返回 SUCC（合法未初始化） |
| Index high key 违规 | high key < 某个页内 key | 报告 ERROR "highkey_violation" |
| Index 页内无序 | key[3] < key[2] | 报告 ERROR "key_order_violation" |
| GUC=OFF | 任何页面 | VerifyPageInline 立即返回 SUCC |
| GUC module 过滤 | module=HEAP, 传入 INDEX page | 跳过校验，返回 SUCC |
| 未注册的 PageType | type=INVALID_PAGE_TYPE | 通用校验报告 ERROR "invalid_page_type" |

### 5.2 性能测试

- GUC=OFF 时 VerifyPageInline 调用耗时应 < 10ns（仅一次 atomic load + 分支）
- 轻量级校验单页耗时 < 1μs
- 重量级校验单页耗时 < 100μs（取决于页面内 tuple 数量）

---

## 6. 风险与应对

| 风险 | 影响 | 应对措施 |
|------|------|---------|
| 轻量级校验影响写入路径性能 | 写入吞吐下降 | GUC 默认 OFF；轻量级仅做 header 检查，开销 < 1μs |
| 校验误报（false positive） | 干扰正常操作 | 充分处理 transient state；使用 WARNING 区分可疑与确定性问题 |
| 新增 PageType 忘记注册 | 新类型无校验 | InitPageVerifiers 中添加编译期 static_assert 确保注册数量 == MAX_PAGE_TYPE - 1 |
| std::vector 内存分配与 DstoreMemoryContext 冲突 | 潜在内存管理问题 | VerifyReport 生命周期短（单次校验），使用默认 allocator 独立于 MemoryContext；如需集成可提供自定义 allocator |
