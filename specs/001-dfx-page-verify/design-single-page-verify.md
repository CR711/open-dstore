# DStore DFX 单页面逻辑校验 设计方案

| 版本 | 作者 | 日期 | 状态 |
|------|------|------|------|
| 1.0 | DStore Team | 2026-03-24 | Draft |
| 3.0 | DStore Team | 2026-04-09 | 同步实现代码 |
| 3.1 | DStore Team | 2026-04-14 | 合并 v1 + v3.0 |

**关联文档**：
- [spec.md](spec.md) — 需求规格（User Story + 验收场景）
- [contracts/page-verify-api.md](contracts/page-verify-api.md) — API 接口契约（精简版）
- [data-model.md](data-model.md) — 数据模型定义
- [design-cross-page-verify.md](design-cross-page-verify.md) — 跨页面校验设计
- [tasks.md](tasks.md) — 任务分解与进度

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
2. **四级校验**：
   - **NONE**：不校验（redo 阶段强制）
   - **LIGHT**：O(1) 只读，热路径可用
   - **MEDIUM**：O(n) 页内遍历，巡检用
   - **HEAVY**：页内深度穷举，离线用
3. **场景分离**：写入 PANIC、读取 ERROR、巡检收集
4. **可配置**：通过运行时 GUC 参数动态控制校验级别和模块，无需重启
5. **可扩展**：基于 Registry 模式，新增页面类型只需注册校验函数，不修改框架代码

**覆盖范围（17 种）**：

| 模块 | 页面类型 |
|------|----------|
| Heap | HEAP_PAGE_TYPE、HEAP_SEGMENT_META_PAGE_TYPE |
| Index | INDEX_PAGE_TYPE、BTR_QUEUE_PAGE_TYPE、BTR_RECYCLE_PARTITION_META_PAGE_TYPE、BTR_RECYCLE_ROOT_META_PAGE_TYPE |
| Undo | TRANSACTION_SLOT_PAGE、UNDO_PAGE_TYPE、UNDO_SEGMENT_META_PAGE_TYPE |
| Segment | DATA_SEGMENT_META_PAGE_TYPE、TBS_EXTENT_META_PAGE_TYPE、TBS_BITMAP_PAGE_TYPE、TBS_BITMAP_META_PAGE_TYPE、TBS_FILE_META_PAGE_TYPE、TBS_SPACE_META_PAGE_TYPE |
| FSM | FSM_PAGE_TYPE、FSM_META_PAGE_TYPE |

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

### 2.3 四级校验模型

| 级别 | 热路径 | 巡检 | redo | undo |
|------|--------|------|------|------|
| NONE | - | - | 强制 | - |
| LIGHT | ✅ | ✅ | - | 最高 |
| MEDIUM | ❌ | ✅ | - | - |
| HEAVY | ❌ | ✅ | - | - |

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
    ERROR   = 2,    // 确定性损坏
    FATAL   = 3     // 页面不可用
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

#### 3.1.2 错误码

```cpp
enum class VerifyCode : uint32_t {
    OK = 0,

    // 通用 (0x0001-0x000F)
    PAGE_TYPE_INVALID           = 0x0001,
    PAGE_ID_INVALID             = 0x0002,
    PAGE_ID_MISMATCH            = 0x0003,
    PAGE_CRC_MISMATCH           = 0x0004,
    PAGE_BOUNDARY_INVALID       = 0x0005,
    PAGE_MAGIC_MISMATCH         = 0x0006,
    PAGE_NULL                   = 0x000A,

    // Heap (0x0100-0x012F)
    HEAP_SPECIAL_OFFSET_MISMATCH    = 0x0110,
    HEAP_HEADER_OFFSET_INVALID      = 0x0111,
    HEAP_TD_COUNT_OVERFLOW          = 0x0112,
    HEAP_ITEMID_ALIGNMENT_INVALID   = 0x0113,
    HEAP_FSM_SLOT_INVALID           = 0x0114,
    HEAP_TUPLE_OVERLAP              = 0x0115,
    HEAP_TD_SANITY_FAIL             = 0x0116,
    HEAP_TUPLE_HEADER_SIZE_INVALID  = 0x0117,
    HEAP_TUPLE_NUM_COLUMN_INVALID   = 0x0118,
    HEAP_TUPLE_SIZE_MISMATCH        = 0x011A,
    HEAP_TUPLE_FLAG_INCONSISTENT    = 0x011D,

    // Index (0x0200-0x022F)
    BTR_PAGE_TYPE_INVALID       = 0x0200,
    BTR_SPLIT_STAT_INVALID      = 0x0201,
    BTR_SPECIAL_OFFSET_INVALID  = 0x0203,
    BTR_META_PAGE_ID_INVALID    = 0x0204,
    INDEX_TUPLE_SIZE_MISMATCH   = 0x0210,
    BTR_QUEUE_INCONSISTENT      = 0x0223,

    // Undo (0x0300-0x031F)
    UNDO_SLOT_STATE_INVALID     = 0x0300,
    UNDO_SLOT_XID_INVALID       = 0x0301,
    UNDO_REC_TYPE_INVALID       = 0x0310,
    UNDO_SEG_FIRST_PAGE_INVALID = 0x0320,

    // Segment (0x0400-0x041F)
    SEG_MAGIC_MISMATCH              = 0x0400,
    SEG_EXT_SIZE_INVALID            = 0x0401,
    SEG_SEGMENT_TYPE_INVALID        = 0x0402,
    BITMAP_META_EXTENT_SIZE_INVALID = 0x0410,
    BITMAP_ALLOCATED_CNT_MISMATCH   = 0x0415,
    FILE_BLOCK_ID_INVALID           = 0x0420,
    SPACE_PAGE_VERSION_INVALID      = 0x0430,
};
```

#### 3.1.3 校验结果

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

#### 3.1.4 校验函数签名与注册表

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

#### 3.1.5 GUC 参数

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

**性能约束**：GUC 参数在 Buffer 读写热路径上每次 I/O 都被读取，必须使用 `memory_order_relaxed`。GUC 值是配置提示（hint），无需与其他数据保持顺序一致性——实现为原子变量，非 GUC 框架直接注册，通过 Set/Get 函数访问。

### 3.2 公共接口

#### 3.2.1 写入路径 inline 校验

```cpp
// include/dfx/dstore_page_verify.h（续）

namespace DSTORE {

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

}  // namespace DSTORE
```

#### 3.2.2 三场景入口

```cpp
/* 写路径：ERROR/FATAL 时触发 PANIC，report 为 nullptr 时不收集诊断 */
RetStatus VerifyPageOnWrite(const Page *page, VerifyLevel level);

/* 读路径：返回错误码，不 PANIC，通过 report 收集诊断（report 可为 nullptr） */
RetStatus VerifyPageOnRead(const Page *page, VerifyLevel level, VerifyReport *report);

/* 巡检模式：收集问题，不 PANIC，总是返回 DSTORE_SUCC（结果通过 report->HasError() 查询） */
RetStatus VerifyPageFull(const Page *page, VerifyLevel level, VerifyReport *report);

/* 注册 */
RetStatus RegisterPageVerifier(PageType type, const char *typeName, VerifyModule module,
    PageVerifyFunc lightFunc, PageVerifyFunc mediumFunc, PageVerifyFunc heavyFunc);
void InitPageVerifiers();
```

**设计决策**：接口采用扁平参数（`VerifyLevel level, VerifyReport *report`）而非 `VerifyContext` 上下文结构体，原因：
1. 热路径上减少一层间接访问，`level` 参数直接由调用方从 GUC 缓存传入
2. `report` 参数可为 `nullptr`，支持 Buffer 读路径的**两阶段优化**（见 3.8 节）
3. 校验函数不需要 `pdbId`、`bufMgr` 等上下文，避免不必要依赖

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

#### 3.3.1 通用 LIGHT 校验

| # | 校验项 | 错误码 |
|---|--------|--------|
| 1 | `page != nullptr` | PAGE_NULL |
| 2 | 全零页放行 | - |
| 3 | `GetType()` 合法 | PAGE_TYPE_INVALID |
| 4 | `GetSelfPageId().IsValid()` | PAGE_ID_INVALID |
| 5 | 页号与期望一致（若 checkPageId） | PAGE_ID_MISMATCH |
| 6 | CRC 正确（若 checkCrc） | PAGE_CRC_MISMATCH |
| 7 | 边界合法：lower/upper/special | PAGE_BOUNDARY_INVALID |

**约束**：

- 热路径不得访问其他页面、不得加锁、不得修改页内容。
- CR 页面不进入页面校验流程，发现页为 CR 页面时直接跳过，不做 CRC、页头或页型特有校验。

#### 3.3.2 通用 MEDIUM 校验

| # | 校验项 |
|---|--------|
| 1 | `m_lower >= sizeof(PageHeader)` |
| 2 | `m_lower <= m_upper <= pageSize` |
| 3 | `m_special.m_offset` 合法且对齐 |
| 4 | 非全零页不能 `PageNoInit()` |

### 3.4 各 PageType 专用校验逻辑

#### 3.4.1 Heap 模块（2 种）

**HEAP_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | `GetType() == HEAP_PAGE_TYPE` | PAGE_TYPE_INVALID |
| LIGHT | `specialOffset == pageSize` | HEAP_SPECIAL_OFFSET_MISMATCH |
| LIGHT | `headerOffset == HEAP_PAGE_DATA_OFFSET` | HEAP_HEADER_OFFSET_INVALID |
| LIGHT | TD count 不超限 | HEAP_TD_COUNT_OVERFLOW |
| MEDIUM | ItemId 数组对齐 | HEAP_ITEMID_ALIGNMENT_INVALID |
| MEDIUM | `m_lower` 与 itemCount 精确等式 | HEAP_ITEMID_ALIGNMENT_INVALID |
| MEDIUM | FSM slot 范围合法 | HEAP_FSM_SLOT_INVALID |
| MEDIUM | TD 数组状态合法 | HEAP_TD_SANITY_FAIL |
| HEAVY | ItemId 状态-len 不变量 | HEAP_TUPLE_HEADER_SIZE_INVALID |
| HEAVY | tuple 边界无重叠 | HEAP_TUPLE_OVERLAP |
| HEAVY | tuple size 与 ItemId.len 一致 | HEAP_TUPLE_SIZE_MISMATCH |
| HEAVY | 列数 <= 1664 | HEAP_TUPLE_NUM_COLUMN_INVALID |
| HEAVY | flag 组合合理 | HEAP_TUPLE_FLAG_INCONSISTENT |
| HEAVY | 列长度累加（需 TupleDesc） | HEAP_TUPLE_SIZE_MISMATCH |

**HEAP_SEGMENT_META_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | `segmentType ∈ {HEAP, HEAP_TEMP}` | SEG_SEGMENT_TYPE_INVALID |
| MEDIUM | `numFsms <= MAX` | HEAP_FSM_SLOT_INVALID |
| MEDIUM | fsmMetaPageId 有效性 | HEAP_FSM_SLOT_INVALID |
| HEAVY | assignedNodeId 有效 | HEAP_FSM_SLOT_INVALID |

---

#### 3.4.2 Index 模块（4 种）

**INDEX_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | `BtrPageType ∈ {LEAF, INTERNAL, META}` | BTR_PAGE_TYPE_INVALID |
| LIGHT | 非 META 页有合法 metaPageId | BTR_META_PAGE_ID_INVALID |
| LIGHT | splitStat / liveStat 枚举范围 | BTR_SPLIT_STAT_INVALID |
| LIGHT | specialOffset 正确 | BTR_SPECIAL_OFFSET_INVALID |
| MEDIUM | headerOffset 正确 | PAGE_BOUNDARY_INVALID |
| MEDIUM | leaf/internal 的 m_lower 精确等式 | PAGE_BOUNDARY_INVALID |
| HEAVY | tuple 无重叠 | INDEX_TUPLE_SIZE_MISMATCH |
| HEAVY | IndexTuple.size == ItemId.len | INDEX_TUPLE_SIZE_MISMATCH |
| HEAVY | pivot tuple key num 合法 | INDEX_TUPLE_SIZE_MISMATCH |
| HEAVY | 列长度（需 BtrMeta*） | INDEX_TUPLE_SIZE_MISMATCH |

**BTR_QUEUE_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | head/tail/size/capacity 基本边界 | BTR_QUEUE_INCONSISTENT |
| MEDIUM | `capacity > 0` | BTR_QUEUE_INCONSISTENT |
| MEDIUM | `size <= capacity` | BTR_QUEUE_INCONSISTENT |
| MEDIUM | `size==0 → head==tail` | BTR_QUEUE_INCONSISTENT |
| MEDIUM | `(head+size)%capacity == tail` | BTR_QUEUE_INCONSISTENT |
| HEAVY | 队列元素不越界 | BTR_QUEUE_INCONSISTENT |

**BTR_RECYCLE_PARTITION_META_PAGE_TYPE**

| 级别 | 校验项 |
|------|--------|
| LIGHT | `createdXid != INVALID_XID` |
| MEDIUM | recycleQueueHead / freeQueueHead 合法 |

**BTR_RECYCLE_ROOT_META_PAGE_TYPE**

| 级别 | 校验项 |
|------|--------|
| LIGHT | `createdXid != INVALID_XID` |
| MEDIUM | recyclePartitionMeta[] 条目有效 |

---

#### 3.4.3 Undo 模块（3 种）

**TRANSACTION_SLOT_PAGE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | `lower ∈ {sizeof(Page), TRX_PAGE_HEADER_SIZE}` | PAGE_BOUNDARY_INVALID |
| LIGHT | `upper == BLCKSZ` | PAGE_BOUNDARY_INVALID |
| HEAVY | nextFreeLogicSlotId 不超范围 | UNDO_SLOT_STATE_INVALID |
| HEAVY | 每个 slot 状态合法 | UNDO_SLOT_STATE_INVALID |
| HEAVY | committed/aborted/prepared slot 有合法 CSN | UNDO_SLOT_XID_INVALID |

**实现说明**：`Page::Init()` 设置 `m_lower = sizeof(Page)` = 42，而 `InitTxnSlotPage` 不修正 `m_lower`。因此空页面的 `lower == sizeof(Page)` 是正常状态。校验必须同时接受 42 和 `TRX_PAGE_HEADER_SIZE` (58)。

**UNDO_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | `lower >= sizeof(Page) && lower <= upper` | PAGE_BOUNDARY_INVALID |
| LIGHT | `cur == selfPageId`（若 cur 非 INVALID） | PAGE_ID_MISMATCH |
| HEAVY | prev/next 不指向自身 | PAGE_ID_INVALID |
| HEAVY | undo record type 合法 | UNDO_REC_TYPE_INVALID |
| HEAVY | undo record 长度不越界 | PAGE_BOUNDARY_INVALID |

**实现说明**：`InitUndoRecPage` 调用 `Page::Init` 后不修正 `m_lower`，因此空页面的 `lower == sizeof(Page)` = 42 是正常状态（非 `UNDO_RECORD_PAGE_HEADER_SIZE` = 64）。

**UNDO_SEGMENT_META_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | `magic == SEGMENT_META_MAGIC` | SEG_MAGIC_MISMATCH |
| LIGHT | `segmentType == UNDO_SEGMENT_TYPE` | SEG_SEGMENT_TYPE_INVALID |
| MEDIUM | firstUndoPageId 合法 | UNDO_SEG_FIRST_PAGE_INVALID |
| MEDIUM | plsn/glsn 与 header 一致 | PAGE_BOUNDARY_INVALID |

---

#### 3.4.4 Segment 模块（6 种）

**DATA_SEGMENT_META_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | `magic == SEGMENT_META_MAGIC` | SEG_MAGIC_MISMATCH |
| LIGHT | segmentType 合法 | SEG_SEGMENT_TYPE_INVALID |
| MEDIUM | totalBlockCount > 0 | SEG_SEGMENT_TYPE_INVALID |
| MEDIUM | dataBlockCount <= totalBlockCount | SEG_SEGMENT_TYPE_INVALID |
| MEDIUM | dataFirst/dataLast 与 count 一致 | SEG_SEGMENT_TYPE_INVALID |
| MEDIUM | plsn/glsn 一致 | PAGE_BOUNDARY_INVALID |

**TBS_EXTENT_META_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | `magic == EXTENT_META_MAGIC` | SEG_MAGIC_MISMATCH |
| LIGHT | extSize ∈ {8, 128, 1024, 8192} | SEG_EXT_SIZE_INVALID |
| MEDIUM | nextExtMetaPageId 合法 | PAGE_ID_INVALID |
| HEAVY | blockId % extSize == 0 | PAGE_ID_INVALID |

**TBS_BITMAP_META_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | extentSize 合法 | BITMAP_META_EXTENT_SIZE_INVALID |
| LIGHT | groupCount <= MAX | BITMAP_META_EXTENT_SIZE_INVALID |
| MEDIUM | validOffset 范围合法 | BITMAP_META_EXTENT_SIZE_INVALID |
| MEDIUM | validOffset 精确等式 | BITMAP_META_EXTENT_SIZE_INVALID |
| HEAVY | firstBitmapPageId 有效 | PAGE_ID_INVALID |

**TBS_BITMAP_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | firstDataPageId 有效 | PAGE_ID_INVALID |
| LIGHT | allocatedExtentCount <= DF_BITMAP_BIT_CNT | BITMAP_ALLOCATED_CNT_MISMATCH |
| MEDIUM | allocatedExtentCount == popcount(bitmap) | BITMAP_ALLOCATED_CNT_MISMATCH |

**TBS_FILE_META_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | blockId == 0 | FILE_BLOCK_ID_INVALID |
| LIGHT | ddlXid != INVALID_XID | FILE_BLOCK_ID_INVALID |
| MEDIUM | pageBaseGlsn 合法 | PAGE_BOUNDARY_INVALID |
| HEAVY | hwm <= datafileBlockCount | FILE_BLOCK_ID_INVALID |

**TBS_SPACE_META_PAGE_TYPE**

| 级别 | 校验项 | 错误码 |
|------|--------|--------|
| LIGHT | pageVersion 在支持范围 | SPACE_PAGE_VERSION_INVALID |

---

#### 3.4.5 FSM 模块（2 种）

**FSM_PAGE_TYPE**

| 级别 | 校验项 |
|------|--------|
| LIGHT | 通用 LIGHT |
| MEDIUM | 通用 MEDIUM |

**FSM_META_PAGE_TYPE**

| 级别 | 校验项 |
|------|--------|
| LIGHT | 通用 LIGHT |
| MEDIUM | 通用 MEDIUM |

**说明**：FSM 结构简单，暂无高价值特有校验，复用通用校验即可。

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

三槽位注册示例（使用自由函数接口）：

```cpp
/* 各模块在独立 cpp 文件中实现注册函数 */

// src/heap/dstore_heap_page_verify.cpp
void RegisterHeapPageVerifier() {
    (void)RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapPageLightweight, VerifyHeapPageMediumweight, VerifyHeapPageHeavyweight);
    (void)RegisterPageVerifier(PageType::HEAP_SEGMENT_META_PAGE_TYPE, "HeapSegmentMetaPage",
        VerifyModule::HEAP,
        VerifyHeapSegMetaLightweight, nullptr, VerifyHeapSegMetaHeavyweight);
}

// src/undo/dstore_undo_page_verify.cpp
void RegisterUndoPageVerifiers() {
    (void)RegisterPageVerifier(PageType::UNDO_PAGE_TYPE, "UndoRecordPage", VerifyModule::UNDO,
        VerifyUndoRecordPageLightweight, nullptr, VerifyUndoRecordPageHeavyweight);
    (void)RegisterPageVerifier(PageType::TRANSACTION_SLOT_PAGE, "TransactionSlotPage",
        VerifyModule::UNDO,
        VerifyTransactionSlotPageLightweight, nullptr, VerifyTransactionSlotPageHeavyweight);
}
```

**说明**：
- `mediumFunc` 参数可为 `nullptr`，表示该页面类型无 MEDIUM 级校验
- `RegisterPageVerifier` 是自由函数，内部操作全局 `g_pageVerifyRegistry`
- 各模块的 Register 函数在引擎启动时由 `InitPageVerifiers()` 统一调用

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

### 3.7 特殊场景

| 场景 | 处理 | 实现位置 |
|------|------|----------|
| 全零页 | `IsAllZeroPage()` 直接放行 | Registry::Verify 入口 |
| redo 阶段 | 强制 NONE（由调用方控制 level） | Buffer 层 |
| undo 阶段 | 最高 LIGHT（由调用方控制 level） | Buffer 层 |
| CR 页面 | `ShouldSkipCrPage()` 跳过，**必须在 CRC 通过后调用** | Registry::Verify，ValidateGenericLight 之后 |
| B-tree mid-split | LIGHT 放宽 splitStat 检查，HEAVY 报 WARNING | IndexPageVerifier |
| Undo 并发 purge | 不持锁，lower 范围放宽（接受 sizeof(Page)） | UndoPageVerifier |
| Page::Init 后 lower 未修正 | Undo/TxnSlot 页面 lower=42 为合法状态 | UndoPageVerifier |

### 3.8 Buffer 层集成（两阶段优化）

Buffer 读写路径是校验框架最关键的性能约束点。每次 I/O 都经过校验，必须最小化成功路径的开销。

**写路径**（`dstore_buf_mgr.cpp: WriteBlock / WriteBlockAsync / FlushDirtyPageUnderLockIfRetry`）：
```cpp
RetStatus VerifyPageBeforePersist(Page *page) {
    VerifyLevel level = GetDfxVerifyLevel();  /* relaxed load */
    if (page == nullptr || level == VerifyLevel::NONE || !IsPageVerifierRegistered(page->GetType())) {
        return DSTORE_SUCC;
    }
    page->SetChecksum();
    return VerifyPageOnWrite(page, level);  /* 失败触发 PANIC */
}
```

**读路径**（`dstore_buf_mgr.cpp: ReadBlock`）-- **两阶段**：
```cpp
RetStatus VerifyPageAfterRead(Page *page) {
    VerifyLevel level = GetDfxVerifyLevel();  /* relaxed load，缓存避免重复读 */
    if (page == nullptr || level == VerifyLevel::NONE || !IsPageVerifierRegistered(page->GetType())) {
        return DSTORE_SUCC;
    }

    /* 第一阶段：快速校验，report=nullptr，不做任何堆分配 */
    RetStatus ret = VerifyPageOnRead(page, level, nullptr);
    if (STORAGE_FUNC_SUCC(ret)) {
        return DSTORE_SUCC;  /* 成功路径：零分配 */
    }

    /* 第二阶段：失败后构造 VerifyReport 获取诊断信息 */
    VerifyReport report;
    (void)VerifyPageOnRead(page, level, &report);
    std::string msg = report.FormatText();
    ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("[PAGE_VERIFY_FAILED] ...%s", msg.c_str()));
    storage_set_error(BUFFER_ERROR_PAGE_VERIFY_FAILED);
    return ret;
}
```

**设计要点**：
- 成功路径（绝大多数页面）：零堆分配、零系统调用、两次 `relaxed` 原子 load（level + modules）
- 失败路径（极少数）：允许 `std::vector`/`std::string` 分配，因为即将记录 ERROR 日志
- CRC 校验由 Buffer 层独立处理（PANIC on mismatch），DFX 框架补充非 CRC 检查

**性能约束**：`VerifyReport` 内含 `std::vector`，构造函数调用 `GetCurrentTimestamp()`。**禁止在 Buffer 读写热路径上无条件构造**——必须使用两阶段模式。

### 3.9 Registry 分发流程

```cpp
RetStatus PageVerifyRegistry::Verify(const Page *page, VerifyLevel level, VerifyReport *report) const
{
    PageType type = page->GetType();
    if (!m_registered[static_cast<size_t>(type)]) {
        return DSTORE_SUCC;  /* 未注册类型，跳过 */
    }

    /* 模块过滤 */
    const PageVerifyEntry &entry = m_entries[static_cast<size_t>(type)];
    if (!IsModuleEnabledInternal(entry.moduleGroup)) {
        return DSTORE_SUCC;
    }

    /* 全零页 / CR 页 跳过（CR 页必须在 CRC 通过后判断） */
    if (IsAllZeroPage(page)) return DSTORE_SUCC;

    /* 通用校验 */
    RetStatus ret = ValidateGenericLight(page, report);
    if (ret != DSTORE_SUCC) return ret;

    /* CRC 通过后安全判断 CR 页 */
    if (ShouldSkipCrPage(page)) return DSTORE_SUCC;

    if (level >= VerifyLevel::MEDIUM) {
        ret = ValidateGenericMedium(page, report);
        if (ret != DSTORE_SUCC) return ret;
    }

    /* 页面特有校验：light → medium → heavy 链式调用 */
    if (entry.lightFunc) {
        ret = entry.lightFunc(page, level, report);
        if (ret != DSTORE_SUCC) return ret;
    }
    if (level >= VerifyLevel::MEDIUM && entry.mediumFunc) {
        ret = entry.mediumFunc(page, level, report);
        if (ret != DSTORE_SUCC) return ret;
    }
    if (level >= VerifyLevel::HEAVY && entry.heavyFunc) {
        ret = entry.heavyFunc(page, level, report);
        if (ret != DSTORE_SUCC) return ret;
    }

    return DSTORE_SUCC;
}
```

**关键设计决策**：
- `ShouldSkipCrPage` 必须在 `ValidateGenericLight`（含 CRC）之后调用，因为 CR 判断需要 `static_cast<DataPage*>` 读取页面内容，在 CRC 未验证前类型字段不可信
- Fail-fast：任一级别失败立即返回，不继续后续检查。巡检模式 `VerifyPageFull` 在外层处理收集逻辑
- 校验函数签名 `(const Page*, VerifyLevel, VerifyReport*)` 统一三个级别，`report` 可为 `nullptr`（此时仅判断 pass/fail，不收集诊断信息）
- 不使用单例 `Instance()` 模式的全局 Registry 通过全局变量 `g_pageVerifyRegistry` + 自由函数 `RegisterPageVerifier()` 对外暴露，避免静态初始化顺序问题，`InitPageVerifiers()` 在引擎启动时显式调用

---

## 4. 文件组织

```
include/dfx/
├── dstore_page_verify.h          # Registry、入口函数、GUC 接口
└── dstore_verify_report.h        # VerifyReport、VerifyResult、枚举定义

src/dfx/
├── dstore_page_verify.cpp        # Registry 实现、三场景入口、GUC 原子变量
├── dstore_verify_report.cpp      # VerifyReport 实现（FormatText/FormatJson）
└── dstore_heap_verify.cpp        # HeapSegmentVerifier（段级遍历）

src/heap/       dstore_heap_page_verify.cpp           # Heap 单页校验
src/index/      dstore_index_page_verify.cpp          # Index 单页校验
                dstore_btr_recycle_page_verify.cpp    # BtrRecycle 校验
src/undo/       dstore_undo_page_verify.cpp           # Undo 单页校验
src/page/       dstore_fsm_page_verify.cpp            # FSM 校验
src/tablespace/ dstore_tbs_page_verify.cpp            # Tablespace 校验
src/dfx/        dstore_segment_page_verify.cpp        # Segment 校验

src/buffer/     dstore_buf_mgr.cpp                    # Buffer 集成（读写路径）
                dstore_buf_mgr_temporary.cpp          # 临时表 Buffer 集成

interface/errorcode/
                dstore_buf_error_code.h               # PAGE_VERIFY_FAILED 错误码
                dstore_buf_error_code_map.h           # 错误码映射

tests/unittest/ut_dfx/                                # 单元测试（9 个文件）
tests/dstore_stress_verify/                           # 长稳 + 故障注入 + 崩溃恢复验证工具
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

## 6. GUC 参数配置

### 6.1 参数定义

```sql
-- 校验级别：NONE=0, LIGHT=1, MEDIUM=2, HEAVY=3
dstore_page_verify_level = 'LIGHT'

-- 启用模块：逗号分隔，空字符串表示全部禁用
dstore_page_verify_modules = 'heap,index,undo'
```

### 6.2 模块与页类型映射

| 模块名 | 位掩码 | 覆盖页类型 | 默认 |
|--------|--------|------------|------|
| `heap` | bit0 | HEAP_PAGE、HEAP_SEGMENT_META | ✅ |
| `index` | bit1 | INDEX_PAGE、BTR_QUEUE、BTR_RECYCLE_* | ✅ |
| `undo` | bit2 | TRANSACTION_SLOT、UNDO_PAGE、UNDO_SEGMENT_META | ✅ |
| `segment` | bit3 | DATA_SEGMENT_META、TBS_* (6 种) | ❌ |
| `fsm` | bit4 | FSM_PAGE、FSM_META | ❌ |

### 6.3 恢复阶段覆盖

| 阶段 | GUC 覆盖规则 |
|------|-------------|
| redo | 强制 `NONE`，忽略 GUC 设置 |
| undo | 最高 `LIGHT`，若 GUC 设置更高则降级 |
| 正常运行 | 使用 GUC 设置 |

### 6.4 使用示例

```sql
-- 关闭所有校验
SET dstore_page_verify_level = 'NONE';

-- 仅开启 heap 模块的 LIGHT 校验
SET dstore_page_verify_level = 'LIGHT';
SET dstore_page_verify_modules = 'heap';

-- 开启 heap/index/undo/segment 的 MEDIUM 校验（巡检场景）
SET dstore_page_verify_level = 'MEDIUM';
SET dstore_page_verify_modules = 'heap,index,undo,segment';

-- 开启全部模块的 HEAVY 校验（离线排障）
SET dstore_page_verify_level = 'HEAVY';
SET dstore_page_verify_modules = 'heap,index,undo,segment,fsm';
```

---

## 7. 风险与应对

| 风险 | 影响 | 应对措施 |
|------|------|---------|
| 轻量级校验影响写入路径性能 | 写入吞吐下降 | GUC 默认 OFF；轻量级仅做 header 检查，开销 < 1μs |
| 校验误报（false positive） | 干扰正常操作 | 充分处理 transient state；使用 WARNING 区分可疑与确定性问题 |
| 新增 PageType 忘记注册 | 新类型无校验 | InitPageVerifiers 中添加编译期 static_assert 确保注册数量 == MAX_PAGE_TYPE - 1 |
| std::vector 内存分配与 DstoreMemoryContext 冲突 | 潜在内存管理问题 | VerifyReport 生命周期短（单次校验），使用默认 allocator 独立于 MemoryContext；如需集成可提供自定义 allocator |

---

## 8. 配套验证工具

为配合页面校验 DFX 能力落地，需要提供一个独立于上层 SQL 引擎的验证工具，用于在纯存储引擎环境下验证页面校验框架是否按设计生效。

该工具的目的不是替代单元测试或业务压测，而是补足两类验证能力：

- **长稳验证**：在类似 `sysbench` / `tpcc` 的持续读写负载下开启页面校验，验证校验逻辑在长时间运行中的稳定性、性能影响和误报情况。
- **故障发现验证**：对 heap/index/undo/segment 等页面注入典型损坏，验证 `OnWrite`、`OnRead`、`VerifyPageFull` 三类路径都能按预期发现问题，并输出清晰诊断结果。

该工具应服务于以下目标：

- 在没有上层 SQL 引擎的情况下，直接驱动 open-dstore 存储引擎完成校验验证
- 复用现有 workload 与 DFX 校验接口，降低验证成本
- 支持构造可重复的损坏场景，用于回归测试和问题复现
- 为后续演示、联调、验收提供统一入口
