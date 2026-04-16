# Contract: Page Verification API

**v3.0** — 取代 v1 两级模型（LIGHTWEIGHT/HEAVYWEIGHT），v1 已废弃。

## 1. 四级校验模型

```cpp
enum class VerifyLevel : uint8 { NONE = 0, LIGHT = 1, MEDIUM = 2, HEAVY = 3 };
```

| 级别 | 复杂度 | 用途 |
|------|--------|------|
| NONE | - | redo 强制；关闭校验 |
| LIGHT | O(1) | 写/读热路径 |
| MEDIUM | O(n) | 巡检 |
| HEAVY | O(n^2) | 巡检/离线 |

恢复阶段覆盖：redo = NONE, undo <= LIGHT, 正常运行 = GUC。

## 2. 三场景入口

```cpp
/* 写路径：失败 PANIC（先 ERROR 落盘诊断，再 PANIC） */
RetStatus VerifyPageOnWrite(const Page *page, VerifyLevel level);

/* 读路径：失败返回错误码。report 可为 nullptr（两阶段优化第一阶段零分配） */
RetStatus VerifyPageOnRead(const Page *page, VerifyLevel level, VerifyReport *report);

/* 巡检：收集问题，总是返回 DSTORE_SUCC，通过 report->HasError() 查询 */
RetStatus VerifyPageFull(const Page *page, VerifyLevel level, VerifyReport *report);
```

写/读路径 `level` 仅控制是否跳过（NONE 时跳过），实际强制 LIGHT。巡检支持全部级别。

## 3. PageVerifyRegistry

```cpp
using PageVerifyFunc = RetStatus (*)(const Page *page, VerifyLevel level, VerifyReport *report);

struct PageVerifyEntry {
    PageType pageType; const char *typeName; VerifyModule moduleGroup;
    PageVerifyFunc lightFunc, mediumFunc, heavyFunc;
};

class PageVerifyRegistry {
public:
    RetStatus Register(PageType type, const char *typeName, VerifyModule module,
                       PageVerifyFunc lightFunc, PageVerifyFunc mediumFunc, PageVerifyFunc heavyFunc);
    RetStatus Verify(const Page *page, VerifyLevel level, VerifyReport *report) const;
    bool IsRegistered(PageType type) const;
    const PageVerifyEntry* Find(PageType type) const;
    static VerifyModule ResolveModule(PageType type);
};
```

`Verify()` 按级别链式调用 light -> medium -> heavy，fail-fast。`report` 可为 nullptr。

全局注册函数：

```cpp
RetStatus RegisterPageVerifier(PageType type, const char *typeName, VerifyModule module,
    PageVerifyFunc lightFunc, PageVerifyFunc mediumFunc, PageVerifyFunc heavyFunc);
void InitPageVerifiers();  /* 引擎启动时调用 */
```

模块注册：`RegisterHeapPageVerifier()`, `RegisterIndexPageVerifier()`, `RegisterFsmPageVerifiers()`, `RegisterUndoPageVerifiers()`, `RegisterSegmentPageVerifiers()`, `RegisterTablespacePageVerifiers()`, `RegisterBtrRecyclePageVerifiers()`。

## 4. GUC 配置

原子变量实现，`memory_order_relaxed`，零锁。

```cpp
void SetDfxVerifyLevel(VerifyLevel level);
VerifyLevel GetDfxVerifyLevel();
void SetDfxVerifyModules(uint64 modules);   /* 位掩码，默认 0x07 = HEAP|INDEX|UNDO */
uint64 GetDfxVerifyModules();
bool IsModuleEnabled(VerifyModule module);
```

```cpp
enum class VerifyModule : uint8 {
    HEAP = 0, INDEX = 1, UNDO = 2, SEGMENT = 3, FSM = 4, ALL = 5
};
```

## 5. VerifyReport

```cpp
struct VerifyResult {
    VerifySeverity severity; VerifyCode code;
    const char *targetType; PageId targetId; const char *checkName;
    uint64 expected, actual; char message[256];
};

class VerifyReport {
public:
    void AddResult(const VerifyResult &result);
    void AddResult(VerifySeverity severity, const char *targetType, const PageId &targetId,
        const char *checkName, uint64 expected, uint64 actual, const char *format, ...);
    void AddResultWithCode(VerifySeverity severity, VerifyCode code, const char *targetType,
        const PageId &targetId, const char *checkName, uint64 expected, uint64 actual,
        const char *format, ...);

    bool HasError() const;
    bool HasFatal() const;
    uint64 GetErrorCount() const;
    uint64 GetWarningCount() const;
    uint64 GetFatalCount() const;
    uint64 GetTotalChecks() const;
    RetStatus GetRetStatus() const;       /* HasError() ? DSTORE_FAIL : DSTORE_SUCC */

    std::string FormatText() const;       /* 日志/CLI */
    std::string FormatJson() const;       /* CLI 工具 */
    const std::vector<VerifyResult> &GetResults() const;
};
```

热路径禁止无条件构造（含 `std::vector` 和 `GetCurrentTimestamp()`），必须走两阶段优化。
VerifyReport 和 VerifyContext 均非线程安全（无锁），每个校验会话必须使用独立实例，禁止跨线程共享。

## 6. VerifyCode 分段规则

| 模块 | 范围 | 前缀 |
|------|------|------|
| Generic | `0x0001`-`0x000F` | `PAGE_` |
| Heap | `0x0100`-`0x012F` | `HEAP_` |
| Index | `0x0200`-`0x022F` | `BTR_`/`INDEX_` |
| Undo | `0x0300`-`0x031F` | `UNDO_` |
| Segment | `0x0400`-`0x041F` | `SEG_`/`BITMAP_`/`FILE_`/`SPACE_` |

完整定义见 `include/dfx/dstore_verify_report.h`。

## 7. VerifySeverity

```cpp
enum class VerifySeverity : uint8 {
    SEVERITY_INFO = 0, SEVERITY_WARNING = 1, SEVERITY_ERROR = 2, SEVERITY_FATAL = 3
};
```

## 8. 跨页校验接口（未实现，API 预留，Phase 5-8）

```cpp
/* B-tree 校验 */
RetStatus VerifyBtreeIndex(StorageRelationData *indexRel, StorageRelationData *heapRel,
    const BtreeVerifyOptions &options, VerifyReport *report);

/* Heap Segment 校验 */
RetStatus VerifyHeapSegment(StorageRelationData *heapRel,
    const HeapVerifyOptions &options, VerifyReport *report);

/* Segment 元数据校验 */
RetStatus VerifySegment(const SegmentId &segmentId,
    const SegmentVerifyOptions &options, VerifyReport *report);

/* 上层元数据一致性校验 */
RetStatus VerifyMetadataConsistency(const MetadataInputStruct *metadata, VerifyReport *report);

/* 表级聚合校验 */
RetStatus VerifyTable(StorageRelationData *heapRel,
    const TableVerifyOptions &options, VerifyReport *report);
```

Options 结构体定义见 `../design-cross-page-verify.md`（3.6 节 `TableVerifyOptions`）。

## 9. 变更记录

**v1**: 两级模型 LIGHTWEIGHT/HEAVYWEIGHT；单入口 `VerifyPageInline()`；注册两个函数槽；无 VerifyCode。

**v2**: 四级模型 NONE/LIGHT/MEDIUM/HEAVY；VerifyModule 位掩码 GUC；三函数槽注册；引入 VerifyCode 分段枚举和 `AddResultWithCode()`。

**v3.0** (当前):
- 三场景入口取代 `VerifyPageInline`：OnWrite(PANIC) / OnRead(ERROR) / Full(收集)
- Buffer 两阶段优化：读路径 `report=nullptr` 零分配
- 恢复阶段覆盖：redo=NONE, undo<=LIGHT
- Register 增加 VerifyModule 参数
- VerifyReport 新增 HasFatal/GetFatalCount，VerifyResult 新增 VerifyCode 字段
- GUC 改用原子变量 `memory_order_relaxed`
- 17 种页面类型全覆盖
- PANIC 前先 ERROR 落盘诊断信息

---

头文件：`include/dfx/dstore_page_verify.h`、`include/dfx/dstore_verify_report.h`
