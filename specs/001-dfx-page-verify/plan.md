# Implementation Plan: DFX Page Verification

**Branch**: `001-dfx-page-verify` | **Date**: 2026-03-24 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-dfx-page-verify/spec.md`

## Summary

为 dstore 存储引擎实现 DFX 页面校验功能，包含两大部分：

1. **单页面校验框架**：Registry 模式的可扩展框架，各 PageType 模块注册自己的轻量级/重量级校验函数。轻量级校验嵌入写入路径（CRUD + 刷脏），重量级校验通过函数接口和 CLI 工具触发。
2. **跨页面校验引擎**：参考 PostgreSQL amcheck 架构，采用 Orchestrator + Context 模式，按校验维度（btree 结构、index-heap 一致性、heap tuple、segment/extent、元数据）分别实现独立的校验器。

## Technical Context

**Language/Version**: C++14 (CMakeLists.txt: `CMAKE_CXX_STANDARD 14`)
**Primary Dependencies**: 无外部依赖，基于 dstore 内部框架（buffer、page、heap、index、transaction）
**Storage**: dstore 自有页面存储（8KB pages, segments, extents, tablespace bitmaps）
**Testing**: Google Test (GTest), 现有 `tests/unittest/` 基础设施
**Target Platform**: Linux (aarch64/x86_64), macOS (开发)
**Project Type**: Storage engine library + CLI verification tool
**Performance Goals**: 轻量级校验 < 1μs/page（写入路径），重量级不限
**Constraints**: 不阻塞正常读写操作（non-intrusive），与现有 `ErrLog`/`RetStatus` 错误处理兼容
**Scale/Scope**: 17 种 PageType，支持任意大小的表/索引

## Constitution Check

*Constitution 未定义具体 gates。按照通用软件工程原则执行。*

## Project Structure

### Documentation (this feature)

```text
specs/001-dfx-page-verify/
├── spec.md              # 需求规格
├── plan.md              # 本文件
├── research.md          # 研究结论
├── data-model.md        # 数据模型
├── quickstart.md        # 快速上手
├── contracts/
│   └── page-verify-api.md  # API 契约
└── tasks.md             # 任务列表（/speckit.tasks 生成）
```

### Source Code (repository root)

```text
include/dfx/                          # 校验框架头文件
├── dstore_page_verify.h              # 核心框架：Registry, VerifyLevel, VerifyModule, VerifyPageInline
├── dstore_verify_report.h            # VerifyReport, VerifyResult, VerifySeverity
├── dstore_verify_context.h           # VerifyContext（跨页面校验共享上下文）
├── dstore_btree_verify.h             # BtreeVerifier + BtreeVerifyOptions
├── dstore_heap_verify.h              # HeapSegmentVerifier + HeapVerifyOptions
├── dstore_segment_verify.h           # SegmentVerifier + SegmentVerifyOptions
└── dstore_metadata_verify.h          # MetadataVerifier + MetadataInputStruct

src/dfx/                              # 校验框架实现
├── dstore_page_verify.cpp            # PageVerifyRegistry + 通用 header 校验
├── dstore_verify_report.cpp          # VerifyReport 实现
├── dstore_verify_context.cpp         # VerifyContext 实现
├── dstore_btree_verify.cpp           # BtreeVerifier 实现
├── dstore_heap_verify.cpp            # HeapSegmentVerifier 实现
├── dstore_segment_verify.cpp         # SegmentVerifier 实现
└── dstore_metadata_verify.cpp        # MetadataVerifier 实现

src/heap/dstore_heap_page_verify.cpp  # Heap 页面校验注册（轻量级 + 重量级）
src/index/dstore_index_page_verify.cpp # Index 页面校验注册
src/page/dstore_fsm_page_verify.cpp   # FSM 页面校验注册
src/undo/dstore_undo_page_verify.cpp  # Undo/TxnSlot 页面校验注册
src/tablespace/dstore_tbs_page_verify.cpp # Bitmap/Extent/FileMeta 页面校验注册

tools/dstore_verify/                  # CLI 离线工具
├── CMakeLists.txt
└── dstore_verify_main.cpp            # main 入口 + 参数解析

tests/unittest/ut_dfx/               # 校验框架单元测试
├── ut_page_verify_registry.cpp       # Registry 注册/分发测试
├── ut_verify_report.cpp              # Report 格式化测试
├── ut_heap_page_verify.cpp           # Heap 页面校验测试
├── ut_index_page_verify.cpp          # Index 页面校验测试
├── ut_btree_verify.cpp               # Btree 跨页面校验测试
├── ut_heap_segment_verify.cpp        # Heap segment 校验测试
├── ut_segment_verify.cpp             # Segment/extent 校验测试
└── ut_metadata_verify.cpp            # 元数据一致性校验测试
```

**Structure Decision**: 新建 `include/dfx/` 和 `src/dfx/` 作为校验框架的核心模块。各页面类型的校验函数实现放在对应模块目录下（`src/heap/`、`src/index/` 等），通过 Registry 注册机制解耦。CLI 工具放在 `tools/dstore_verify/`。测试放在 `tests/unittest/ut_dfx/`。

## Architecture Design

### 1. Single Page Verification Framework

**设计模式**: Registry + Strategy (enum-indexed dispatch)

**参考**: PostgreSQL amcheck 的模块化设计（verify_nbtree.c / verify_heapam.c 各自独立）

```
                    ┌──────────────────────────┐
                    │   Inline Call Sites       │
                    │ (CRUD, FlushDirty, etc.)  │
                    └──────────┬───────────────┘
                               │ VerifyPageInline(page)
                               ▼
                    ┌──────────────────────────┐
                    │  GUC Check               │
                    │  dfx_verify_level == OFF? │──→ skip (DSTORE_SUCC)
                    │  dfx_verify_module match? │──→ skip (DSTORE_SUCC)
                    └──────────┬───────────────┘
                               │
                               ▼
                    ┌──────────────────────────┐
                    │  PageVerifyRegistry       │
                    │  entries[page->GetType()] │
                    └──────────┬───────────────┘
                               │ dispatch by PageType
                    ┌──────────┼──────────┐
                    ▼          ▼          ▼
             ┌──────────┐ ┌──────────┐ ┌──────────┐
             │ Heap     │ │ Index    │ │ FSM      │ ...（17 种）
             │ Verifier │ │ Verifier │ │ Verifier │
             └──────────┘ └──────────┘ └──────────┘
                    │
                    ▼
             ┌──────────────────────────┐
             │  VerifyReport            │
             │  (collect results)       │
             └──────────────────────────┘
```

**通用 Header 校验**（框架层，所有 PageType 共享）：
在分发到具体 PageType 校验函数之前，框架先执行通用 header 校验：
- CRC checksum（`CheckPageCrcMatch()`）
- Page type 有效性
- lower/upper bounds 一致性（`lower <= upper <= BLCKSZ`）
- LSN 合理性（non-zero for initialized page, no UINT64_MAX）
- Special region offset 合理性

**各模块注册方式**：
```cpp
// src/heap/dstore_heap_page_verify.cpp
void RegisterHeapPageVerifier()
{
    RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage",
                         VerifyHeapPageLightweight,
                         VerifyHeapPageHeavyweight);
}

// src/dfx/dstore_page_verify.cpp — 统一初始化
void InitPageVerifiers()
{
    RegisterHeapPageVerifier();
    RegisterIndexPageVerifier();
    RegisterFsmPageVerifier();
    // ... 所有 17 种
}
```

### 2. Cross-Page Verification Engine

**设计模式**: Orchestrator + Context（参考 PostgreSQL amcheck 的 `BtreeCheckState`）

**参考**: PostgreSQL `bt_check_every_level()` → `bt_check_level_from_leftmost()` → `bt_target_page_check()` 层次化遍历模式

```
                    ┌──────────────────────────────────┐
                    │        VerifyTable()             │
                    │   (aggregation entry point)      │
                    └──────────┬───────────────────────┘
                               │ creates VerifyContext
                    ┌──────────┼──────────────────────────┐
                    │          │                           │
                    ▼          ▼                           ▼
          ┌─────────────┐ ┌──────────────────┐  ┌─────────────────┐
          │ BtreeVerifier│ │HeapSegmentVerifier│  │SegmentVerifier  │
          └──────┬──────┘ └────────┬─────────┘  └───────┬─────────┘
                 │                 │                     │
     ┌───────────┼──────────┐     │              ┌──────┼──────────┐
     ▼           ▼          ▼     ▼              ▼      ▼          ▼
  Structure   IndexHeap   SameLevel  TupleFormat  ExtentChain  FSM-Heap
  Check       1:1 Check   KeyOrder   BigTuple     Bitmap Match  Consistency
              + DataMatch             Chunk
```

#### 2.1 BtreeVerifier

**遍历策略**: 层次遍历（root → leaf），逐层从最左页面沿 sibling links 遍历

**核心检查项**:
1. **结构完整性**: sibling link 双向一致、level 递减、split status
2. **页内 key 排序**: 每页内 tuples 有序
3. **High key 校验**: high key >= 所有页内 key
4. **同层跨页 key 排序**: 相邻页面 key 单调递增/递减
5. **Parent-child 一致性**: 内部页面 key 与 downlink 子页面首个 tuple 匹配
6. **Index-Heap 1:1 对应**: leaf entry 指向的 heap tuple 存在，反向也成立
7. **Index-Heap 数据一致性**: key 值与 heap tuple 列值匹配（支持采样）

**可见性处理**: online 模式使用 `SNAPSHOT_MVCC`，仅校验 committed & visible tuples

**环检测**: 使用 `VerifyContext.visitedPages` 集合检测 sibling link 环

#### 2.2 HeapSegmentVerifier

**遍历策略**: 顺序扫描 segment 内所有 heap page

**核心检查项**:
1. **Tuple 格式**: size vs column count, null bitmap length
2. **Big tuple chain**: 从 first chunk 沿 CTID 遍历，校验 m_linkInfo 和 chunk count
3. **FSM 一致性**: 页面实际 free space vs FSM 记录的 free space category

#### 2.3 SegmentVerifier

**遍历策略**: 从 segment meta page 沿 extent chain walk

**核心检查项**:
1. **Segment meta**: magic number, segment type 有效性
2. **Extent chain**: 链表完整性（无断链、无环）、magic number、extent size 正确性
3. **Page count**: totalBlockCount vs 实际 extent 总页面数
4. **dataFirst/dataLast**: 与实际数据页面一致
5. **Extent-Bitmap 一致性**: 每个 extent 在 bitmap 中标记为 allocated，无重叠
6. **Index leafPageCount**: 实际 level-0 page 数与预期一致

#### 2.4 MetadataVerifier

**输入**: `MetadataInputStruct`（上层传入）

**核心检查项**:
1. heap segment 存在且类型正确
2. LOB segment 存在（如果非 INVALID）且类型正确
3. 每个 index segment 存在、类型为 INDEX、BtrMeta 属性与传入的 attTypeIds 一致
4. tablespace ID 与 segment 所在 tablespace 一致

### 3. GUC Parameters

| Parameter | Type | Default | Values |
|-----------|------|---------|--------|
| `dfx_verify_level` | enum | OFF | OFF, LIGHTWEIGHT, HEAVYWEIGHT |
| `dfx_verify_module` | enum | ALL | HEAP, INDEX, ALL |

- 存储在现有 GUC 框架中（如有）或作为全局变量 + SET 命令处理
- 读取无锁（atomic load），写入通过 SET 命令

### 4. CLI Tool Architecture

```
dstore_verify_main.cpp
├── ParseArgs()          → VerifyCliOptions
├── OpenDataDir()        → 直接读取数据文件（离线模式）
├── RunVerification()
│   ├── VerifyPage()     → 单页面
│   ├── VerifyBtreeIndex()
│   ├── VerifyHeapSegment()
│   ├── VerifySegment()
│   └── VerifyMetadataConsistency()  (if --metadata-file provided)
└── OutputReport()       → text / json
```

- 离线模式：直接 mmap 或 read 数据文件，不经过 buffer manager
- 在线模式：连接运行中的 dstore 实例，通过 buffer manager 读页面

### 5. Error Handling Strategy

```
校验函数内部
  │
  ├─ 发现问题 → report->AddResult(severity, ...)
  │             继续检查下一项（不中断）
  │
  ├─ 达到 maxErrors → 提前终止，report 记录 "max errors reached"
  │
  └─ 全部检查完成 → return report->HasError() ? DSTORE_FAIL : DSTORE_SUCC

调用方（写入路径）
  │
  ├─ DSTORE_SUCC → 继续正常操作
  │
  └─ DSTORE_FAIL → ErrLog + 根据业务逻辑决定中断或继续
```

### 6. C++ Best Practices Applied

| 改进点 | 做法 |
|--------|------|
| RAII | VerifyReport 使用 std::vector 管理结果，自动释放 |
| enum class | VerifyLevel, VerifyModule, VerifySeverity 全部使用 enum class |
| const correctness | 校验函数接收 `const Page*`，VerifyReport 查询方法全部 `const` |
| DISALLOW_COPY_AND_MOVE | VerifyReport, VerifyContext 使用此宏 |
| 初始化 | 结构体使用 braced initialization + 默认值 |
| 命名 | 遵循项目 PascalCase 规范，`m_` 前缀成员变量 |
| Include guards | 使用 `#ifndef` 风格（与项目一致） |
| Namespace | `namespace DSTORE { ... }` |

## Complexity Tracking

> 无 Constitution gate violation。

| 设计决策 | 理由 | 替代方案及拒绝原因 |
|----------|------|-------------------|
| 独立 `src/dfx/` 模块 | 校验逻辑横切多个模块，需要独立代码组织 | 散布在各模块中：难以统一测试和维护 |
| VerifyReport 使用 std::vector | 校验结果数量不确定，需要动态增长 | 固定大小数组：可能溢出或浪费 |
| 跨页面校验按维度分离为独立 Verifier | 各维度遍历策略不同，独立性强 | 单一 TableVerifier：职责过重，难以独立测试 |
