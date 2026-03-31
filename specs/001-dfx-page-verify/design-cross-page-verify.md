# DStore DFX 跨页面逻辑校验 设计方案

| 版本 | 作者 | 日期 | 状态 |
|------|------|------|------|
| 1.0 | DStore Team | 2026-03-24 | Draft |

---

## 1. 背景与目标

### 1.1 背景

单页面校验（详见《单页面逻辑校验设计方案》）解决了每个页面内部结构的一致性问题，但存储引擎中许多数据损坏问题发生在**页面之间的逻辑关系**层面：

- **B-tree 索引结构损坏**：sibling link 断裂、层级不一致、key 跨页排序违规、parent-child 不匹配
- **Index-Heap 不一致**：索引指向不存在的 heap tuple，或 heap tuple 缺少对应索引项，导致查询结果错误
- **Big Tuple 链断裂**：跨页面的大对象 chunk 链中断，导致数据截断或读取异常
- **Segment/Extent 元数据损坏**：extent 链断裂或成环、分配位图与实际不一致，可能导致数据覆盖
- **FSM-Heap 不一致**：空闲空间记录与实际不符，导致空间浪费或分配冲突
- **上层元数据与 DStore 数据不一致**：InnoDB 系统表记录的 segment/tablespace 信息与实际存储布局不符

这些跨页面问题无法通过单页面校验发现，需要专门的跨页面校验引擎。

### 1.2 目标

设计并实现一套**跨页面逻辑校验引擎**，覆盖以下校验维度：

1. **B-tree 结构校验**：从 root 到 leaf 的层次遍历，验证树结构完整性
2. **Index-Heap 一致性校验**：索引项与 heap tuple 的 1:1 对应和数据一致性
3. **Heap 数据校验**：tuple 格式验证、big tuple chunk 链完整性
4. **FSM-Heap 一致性校验**：空闲空间记录与实际页面一致性
5. **Segment/Extent 校验**：extent 链完整性、分配位图一致性
6. **元数据一致性校验**：上层元数据与 DStore 数据的一致性（提供接口，由上层调用）

### 1.3 参考

- **PostgreSQL amcheck 扩展**
  - `verify_nbtree.c`：`bt_check_every_level()` → `bt_check_level_from_leftmost()` → `bt_target_page_check()` 层次化遍历模式
  - `verify_heapam.c`：heap tuple 逐行校验 + 可见性判断
  - `BtreeCheckState`：跨页面遍历状态对象，持有 snapshot、错误收集器、已访问页面集合
- **PostgreSQL `heapallindexed` 选项**：Bloom filter 做索引-heap 全量一致性校验的思路
- DStore 现有 `DataSegmentContext::MoveExtents()` — extent chain walk 实现

---

## 2. 总体架构

### 2.1 架构概览

```
                    ┌──────────────────────────────────────────┐
                    │          VerifyTable()                    │
                    │     表级聚合入口（可选）                    │
                    └──────────────────┬───────────────────────┘
                                       │ 创建 VerifyContext
                    ┌──────────────────┼───────────────────────────────┐
                    │                  │                                │
                    ▼                  ▼                                ▼
          ┌─────────────────┐ ┌────────────────────┐  ┌──────────────────────┐
          │  BtreeVerifier  │ │HeapSegmentVerifier  │  │  SegmentVerifier     │
          │  B-tree 结构    │ │Heap 数据 + FSM      │  │  Extent + Bitmap     │
          └────────┬────────┘ └─────────┬──────────┘  └──────────┬───────────┘
                   │                    │                         │
     ┌─────────────┼──────────┐        │              ┌──────────┼──────────┐
     ▼             ▼          ▼        ▼              ▼          ▼          ▼
  结构完整性   Index-Heap   同层Key   Tuple格式     Extent链    Bitmap     PageCount
  sibling     1:1对应      排序校验  BigTuple链    完整性校验   一致性     一致性
  level一致   数据一致性             FSM一致性
  parent-child (采样支持)

                    ┌──────────────────────────────────────────┐
                    │         MetadataVerifier                  │
                    │  InnoDB 元数据 ↔ DStore 数据一致性         │
                    │  （由上层传入 MetadataInputStruct 触发）    │
                    └──────────────────────────────────────────┘

所有 Verifier 共享：
     ┌──────────────────────────────────────────┐
     │            VerifyContext                  │
     │  VerifyReport* + Snapshot* + visitedPages │
     │  + sampleRatio + maxErrors + isOnline     │
     └──────────────────────────────────────────┘
```

### 2.2 设计原则

| 原则 | 说明 |
|------|------|
| **Orchestrator + Context** | 每种校验维度一个 Orchestrator 类，共享 VerifyContext 管理遍历状态和结果收集 |
| **按维度独立** | 各 Verifier 遍历策略不同（btree 层次遍历、segment chain walk、heap 顺序扫描），独立实现更清晰 |
| **不中断语义** | 发现错误后继续检查下一项，直到达到 maxErrors 上限 |
| **可见性感知** | Online 模式使用 MVCC 快照，仅对 committed & visible tuple 做交叉校验 |
| **环检测** | 所有链式遍历使用 visitedPages 集合检测循环引用，保证有界终止 |
| **采样支持** | Index-Heap 数据一致性校验支持 page-level 随机采样，降低 IO 开销 |

### 2.3 设计模式选择

**模式**: Orchestrator + Context（参考 PostgreSQL amcheck 的 `BtreeCheckState`）

**选择理由**:
- 跨页面校验天然需要状态（已访问页面集合、遍历位置、累积错误、采样计数器），适合用对象封装
- 不同校验场景差异大（btree 层次遍历 vs segment chain walk vs heap 顺序扫描），各自独立更清晰
- 共享基础设施（错误报告、page 读取、可见性判断）通过 VerifyContext 注入

**否决方案**:
- Visitor 模式：不适合，因为不同校验需要不同的遍历顺序
- 单一 TableVerifier 类：职责过重，难以独立测试和复用
- 纯函数式（无状态 + 参数传递）：状态太多，参数列表会爆炸

---

## 3. 详细设计

### 3.1 共享上下文 — VerifyContext

```cpp
// include/dfx/dstore_verify_context.h

namespace DSTORE {

class VerifyContext {
public:
    VerifyContext(VerifyReport* report, bool isOnline = true);
    ~VerifyContext();

    DISALLOW_COPY_AND_MOVE(VerifyContext);

    // ========== 结果收集 ==========
    VerifyReport* GetReport() const { return m_report; }

    // ========== 可见性 ==========
    void SetSnapshot(SnapshotData* snapshot);
    SnapshotData* GetSnapshot() const { return m_snapshot; }
    bool IsOnline() const { return m_isOnline; }

    // ========== 采样 ==========
    void SetSampleRatio(float ratio);       // 0.0 ~ 1.0
    float GetSampleRatio() const { return m_sampleRatio; }
    bool ShouldSamplePage() const;          // 根据 ratio 随机决定是否采样此页

    // ========== 环检测 ==========
    // 返回 true 表示该页面是首次访问；false 表示已访问过（检测到环）
    bool MarkPageVisited(const PageId& pageId);
    bool IsPageVisited(const PageId& pageId) const;
    void ResetVisitedPages();               // 切换校验维度时重置

    // ========== 错误上限 ==========
    void SetMaxErrors(uint32 maxErrors);
    bool HasReachedMaxErrors() const;

private:
    VerifyReport*                   m_report;
    SnapshotData*                   m_snapshot = nullptr;
    bool                            m_isOnline;
    float                           m_sampleRatio = 1.0f;
    std::unordered_set<uint64>      m_visitedPages;   // pageId hash
    uint32                          m_maxErrors = 1000;
};

}  // namespace DSTORE
```

**PageId 哈希方式**：将 `(fileId, blockId)` 组合为一个 `uint64` 作为 key：

```cpp
static inline uint64 PageIdToHash(const PageId& pid)
{
    return (static_cast<uint64>(pid.m_fileId) << 32) | pid.m_blockId;
}
```

### 3.2 BtreeVerifier — B-tree 结构与 Index-Heap 一致性

#### 3.2.1 校验选项

```cpp
// include/dfx/dstore_btree_verify.h

namespace DSTORE {

struct BtreeVerifyOptions {
    bool checkStructure       = true;    // 结构完整性（sibling links, levels, key ordering）
    bool checkHeapConsistency = true;    // index-heap 1:1 对应
    bool checkDataConsistency = false;   // index-heap 数据值一致性（需要访问 heap）
    float sampleRatio         = 1.0f;    // 采样比例（0.0~1.0，仅对 data consistency 生效）
    bool isOnline             = true;    // 是否在线模式（考虑事务可见性）
    uint32 maxErrors          = 1000;    // 最大错误数
};

// 校验入口
RetStatus VerifyBtreeIndex(StorageRelationData* indexRel,
                           StorageRelationData* heapRel,
                           const BtreeVerifyOptions& options,
                           VerifyReport* report);

}  // namespace DSTORE
```

#### 3.2.2 遍历策略

**参考 PostgreSQL**: `bt_check_every_level()` → `bt_check_level_from_leftmost()` → `bt_target_page_check()`

```
BtreeVerifier 遍历策略（层次遍历，root → leaf）

1. 读取 BtrMeta page，获取 rootPage 和 rootLevel
2. 从 root 开始，逐层向下遍历：
   for level = rootLevel down to 0:
     ├── 定位当前层最左页面（从 root 沿最左 downlink 下降）
     ├── 沿 sibling links（next pointer）从左到右遍历当前层所有页面
     │     对每个页面执行 TargetPageCheck():
     │     ├── 单页面校验（调用 PageVerifyRegistry）
     │     ├── 结构校验（如启用 checkStructure）
     │     ├── Index-Heap 校验（如启用，仅 level=0）
     │     └── 环检测（visitedPages）
     └── 记录当前层统计信息（页面数、key 数）
```

#### 3.2.3 结构完整性校验（checkStructure）

```
B-tree 结构校验项
│
├── 1. Sibling Link 双向一致性
│     对每个非最右页面：
│     ├── 读取 next page
│     ├── next_page.prev == current_page（双向校验）
│     └── 不一致 → ERROR "sibling_link_broken"
│         报告: current_pageId, next_pageId, next.prev_actual
│
├── 2. Level 递减一致性
│     ├── 同层所有页面 level 值相同
│     ├── 子层 level == 父层 level - 1
│     └── 不一致 → ERROR "level_inconsistent"
│
├── 3. Split Status 处理
│     ├── SPLIT_COMPLETE: 正常校验
│     └── SPLIT_INCOMPLETE: 报告 WARNING "page_mid_split"
│         放宽 sibling link 校验（next page 可能尚未更新 prev）
│
├── 4. 页内 Key 排序（intra-page）
│     ├── 确定起始 offset（最右页面从 HIKEY，非最右从 FIRSTKEY）
│     ├── 相邻 key 满足排序关系: key[i] <= key[i+1]
│     └── 违规 → ERROR "intra_page_key_order_violation"
│         报告: pageId, offset_i, offset_i+1, key_i, key_i+1
│
├── 5. High Key 校验
│     ├── 非最右页面: high key(offset 1) >= 所有页内 key
│     └── 违规 → ERROR "highkey_violation"
│
├── 6. 同层跨页 Key 排序（inter-page）
│     ├── 当前页面最后一个 key <= 下一个页面第一个 key
│     ├── 或者: 当前页面 high key <= next 页面第一个 key
│     └── 违规 → ERROR "inter_page_key_order_violation"
│         报告: current_pageId, next_pageId, last_key, first_key
│
└── 7. Parent-Child Key 一致性
      ├── 内部页面的每个 downlink entry:
      │     ├── 读取子页面
      │     ├── 子页面的第一个 key (或 high key boundary) 应与 parent 中对应 key 一致
      │     └── 不一致 → ERROR "parent_child_key_mismatch"
      └── 注意: 最右子页面无 high key，使用 parent 的下一个 key 作为上界
```

#### 3.2.4 Index-Heap 1:1 对应校验（checkHeapConsistency）

仅在 leaf level (level=0) 执行：

```
Index-Heap 1:1 对应校验
│
├── 方向 1: Index → Heap（正向）
│     遍历所有 leaf page 的 index entry:
│     ├── 从 index entry 取出 ItemPointer (heapPageId + offsetNum)
│     ├── 读取 heap page
│     ├── 检查 offsetNum 处的 ItemId 是否为 NORMAL 状态
│     ├── 如果 ItemId 为 UNUSED → ERROR "index_points_to_dead_tuple"
│     └── 在线模式: 使用 MVCC snapshot 判断 heap tuple 可见性
│           ├── 可见 → 纳入校验
│           ├── In-progress → 跳过，记录 INFO "skipped_in_progress"
│           └── 不可见(aborted/deleted) → 记录 WARNING "index_points_to_invisible"
│
└── 方向 2: Heap → Index（反向）
      遍历所有 heap page 的可见 tuple:
      ├── 根据 tuple 的 key 列值构造 index key
      ├── 在 btree 中查找该 key
      ├── 如果找不到 → ERROR "heap_tuple_missing_index_entry"
      │     报告: heapPageId, offsetNum, key_value
      └── 注意: 仅对 committed & visible tuple 执行此检查
```

#### 3.2.5 Index-Heap 数据一致性校验（checkDataConsistency）

在 1:1 对应校验的正向路径中附加数据值比对：

```
Index-Heap 数据一致性校验（支持 page-level 采样）
│
├── 采样策略
│     ├── 按 page-level 采样（随机选取一定比例的 leaf page）
│     ├── 选中的 page 做全量 tuple 校验
│     ├── 使用 VerifyContext::ShouldSamplePage() 决定是否采样
│     └── sampleRatio=1.0 等价于全量校验
│
└── 数据比对
      对选中 page 的每个 index entry:
      ├── 从 index entry 提取 key 值（attTypeIds 指导解码）
      ├── 从 heap tuple 提取对应列值
      ├── 逐列比较
      └── 不一致 → ERROR "index_heap_data_mismatch"
            报告: indexPageId, heapPageId, offsetNum, column_index,
                  index_value, heap_value
```

**采样实现**（page-level 随机采样）：

```cpp
bool VerifyContext::ShouldSamplePage() const
{
    if (m_sampleRatio >= 1.0f) {
        return true;    // 全量
    }
    if (m_sampleRatio <= 0.0f) {
        return false;
    }
    // 伪随机，每个 page 独立决定
    return (static_cast<float>(rand()) / RAND_MAX) < m_sampleRatio;
}
```

> **为什么选择 page-level 采样而非 tuple-level**：page-level 采样 IO 效率更高——读一个 page 就全量校验该 page 的所有 tuple，避免了 tuple-level 随机读的 IO 放大。

#### 3.2.6 可见性处理

```
Online 模式可见性处理流程
│
├── 初始化
│     ├── 获取 SNAPSHOT_MVCC 快照
│     └── 设置到 VerifyContext
│
├── 校验过程中对每个 tuple 判断可见性
│     ├── 通过 tuple 的 tdId 获取 TD slot
│     ├── 根据 TD 的 xid 和 csn 判断事务状态
│     ├── 使用 snapshot 的 snapshotCsn 做可见性判断：
│     │     ├── committed && csn <= snapshotCsn → 可见，纳入校验
│     │     ├── in-progress → 跳过，记录 INFO
│     │     ├── aborted → 跳过，记录 INFO
│     │     └── csn > snapshotCsn → 不可见，跳过
│     └── 仅对可见 tuple 做 Index-Heap 交叉校验
│
└── Offline 模式
      ├── snapshot = nullptr
      ├── 所有非 aborted tuple 均视为需要校验
      └── 不考虑可见性（离线场景无并发）
```

#### 3.2.7 环检测

```cpp
// 在遍历每个页面前调用
if (!ctx->MarkPageVisited(currentPageId)) {
    report->AddResult(VerifySeverity::ERROR, "btree",
        currentPageId, 0, "sibling_link_cycle",
        0, 0, "Cycle detected: page (%u,%u) visited twice at level %u",
        currentPageId.m_fileId, currentPageId.m_blockId, currentLevel);
    break;  // 终止当前层遍历
}
```

### 3.3 HeapSegmentVerifier — Heap 数据与 FSM 一致性

#### 3.3.1 校验选项

```cpp
// include/dfx/dstore_heap_verify.h

namespace DSTORE {

struct HeapVerifyOptions {
    bool checkTupleFormat     = true;    // tuple 格式校验（size, null bitmap, column count）
    bool checkBigTupleChains  = true;    // linked tuple chunk 完整性
    bool checkFsmConsistency  = true;    // FSM-heap free space 一致性
    bool isOnline             = true;    // 是否在线模式
    uint32 maxErrors          = 1000;    // 最大错误数
};

// 校验入口
RetStatus VerifyHeapSegment(StorageRelationData* heapRel,
                            const HeapVerifyOptions& options,
                            VerifyReport* report);

}  // namespace DSTORE
```

#### 3.3.2 遍历策略

```
HeapSegmentVerifier 遍历策略（顺序扫描）

1. 从 segment meta page 获取 dataFirst 和 dataLast
2. 从 dataFirst 开始，顺序遍历 segment 内所有 heap page：
   for each heap page in [dataFirst, dataLast]:
     ├── 先执行单页面校验（调用 PageVerifyRegistry，已覆盖 header + 页内结构）
     ├── Tuple 格式校验（如启用 checkTupleFormat）
     ├── Big tuple 起始 chunk 标记（如启用 checkBigTupleChains）
     └── 记录实际 free space（如启用 checkFsmConsistency）
3. Big tuple chain 遍历（延迟到所有页面扫描完成后）
4. FSM 一致性校验（对比记录的 free space 与 FSM 数据）
```

#### 3.3.3 Tuple 格式校验（checkTupleFormat）

```
Tuple 格式校验
│
├── 1. Tuple Size 合理性
│     ├── tuple.m_size >= MinTupleHeaderSize
│     ├── tuple.m_size <= BLCKSZ - PageHeaderSize
│     └── 违规 → ERROR "tuple_size_invalid"
│
├── 2. Null Bitmap 长度一致性
│     ├── 如果 tuple 有 null bitmap (HEAP_HASNULL flag):
│     │     bitmap 长度 = BITMAPLEN(nattrs)
│     │     bitmap 不应超出 tuple 边界
│     └── 违规 → ERROR "null_bitmap_overflow"
│
├── 3. Column Count 合理性
│     ├── 如果可获取 relation 的 column 定义:
│     │     tuple 的 nattrs <= relation.maxAttrs
│     └── 违规 → WARNING "column_count_mismatch"
│         （WARNING 因为可能是 ALTER TABLE ADD COLUMN 后的旧 tuple）
│
└── 4. Tuple 数据区域合理性
      ├── data offset >= header size + null bitmap size
      ├── data offset < tuple size
      └── 违规 → ERROR "tuple_data_offset_invalid"
```

#### 3.3.4 Big Tuple Chain 校验（checkBigTupleChains）

```
Big Tuple (Linked Tuple) Chain 校验

遍历策略: 在顺序扫描 heap page 时，收集所有 first chunk 的信息，
         扫描完成后逐一验证每条 chain。

对每条 big tuple chain（从 first chunk 开始）:
│
├── 1. First Chunk 验证
│     ├── m_linkInfo == TUP_LINK_FIRST_CHUNK_TYPE
│     ├── 记录预期 chunk 总数（存储在 first chunk header 中）
│     └── 提取 next-chunk CTID (heapPageId + offsetNum)
│
├── 2. 沿 CTID 逐步遍历后续 chunk
│     while next CTID != INVALID:
│     ├── 读取目标 page（可能跨 extent 边界）
│     ├── 读取目标 offset 处的 tuple
│     ├── 验证 m_linkInfo == TUP_LINK_NOT_FIRST_CHUNK_TYPE
│     │     不是 → ERROR "chunk_linkinfo_invalid"
│     ├── chunk 计数 +1
│     ├── 环检测（visited chunk set）
│     │     重复 → ERROR "chunk_chain_cycle"
│     └── 提取下一个 CTID
│
├── 3. Chain 完整性验证
│     ├── 实际 chunk count == first chunk 记录的预期 count
│     │     不一致 → ERROR "chunk_count_mismatch"
│     │     报告: first_chunk_pageId, expected_count, actual_count
│     └── 最后一个 chunk 的 next CTID == INVALID
│           不是 → ERROR "chunk_chain_unterminated"
│
└── 4. 跨 Extent 边界处理
      chunk chain 的 CTID 可能指向不同 extent 的 page
      ├── 遍历时不假设 chunk 在同一 extent 内
      └── 通过 buffer manager（online）或直接读文件（offline）获取任意 page
```

#### 3.3.5 FSM-Heap 一致性校验（checkFsmConsistency）

```
FSM-Heap 一致性校验

1. 在顺序扫描 heap page 时，记录每个 page 的实际 free space:
   actualFreeSpace[pageId] = page->GetFreeSpace()

2. 读取 FSM tree，获取每个 heap page 对应的 free space category:
   fsmCategory[pageId] = ReadFsmEntry(heapRel, pageBlockNum)

3. 一致性比较:
   for each heap page:
   ├── 将 actualFreeSpace 映射到 FSM category
   │     fsmCatExpected = SpaceToCategory(actualFreeSpace)
   ├── 比较 fsmCatExpected 与 fsmCategory
   ├── 完全一致 → PASS
   ├── fsmCategory > fsmCatExpected (FSM 高估可用空间)
   │     → WARNING "fsm_overestimate"
   │     （可能导致分配失败，但不是数据损坏）
   └── fsmCategory < fsmCatExpected - TOLERANCE (FSM 严重低估)
         → WARNING "fsm_underestimate"
         （可能导致空间浪费，但不是数据损坏）

注意:
- Online 模式下，并发写入可能导致 heap page free space 和 FSM 暂时不一致
- 因此 FSM 不一致仅报告 WARNING，不报告 ERROR
- TOLERANCE 设置为 1 个 category 级别（允许 1 级偏差）
```

### 3.4 SegmentVerifier — Segment 元数据与 Extent 分配

#### 3.4.1 校验选项

```cpp
// include/dfx/dstore_segment_verify.h

namespace DSTORE {

struct SegmentVerifyOptions {
    bool checkExtentChain  = true;     // extent chain 完整性
    bool checkExtentBitmap = true;     // extent-bitmap 一致性
    bool checkPageCounts   = true;     // page count 一致性
    uint32 maxErrors       = 1000;     // 最大错误数
};

// 校验入口（按 SegmentId 校验）
RetStatus VerifySegment(const PageId& segmentMetaPageId,
                        const SegmentVerifyOptions& options,
                        VerifyReport* report);

}  // namespace DSTORE
```

#### 3.4.2 遍历策略

```
SegmentVerifier 遍历策略（extent chain walk）

1. 读取 segment meta page
   ├── 验证 magic number
   ├── 验证 segment type (HEAP/INDEX/UNDO/TEMP)
   └── 获取 extent chain 头指针、totalBlockCount 等

2. 沿 extent chain walk:
   currentExtent = firstExtentMetaPageId
   while currentExtent != INVALID:
     ├── 读取 extent meta page
     ├── 环检测（visitedPages）
     │     重复 → ERROR "extent_chain_cycle"，终止
     ├── 验证 magic == EXTENT_META_MAGIC (0xB1B2B3B4B5B6B7B8)
     ├── 验证 extSize 在合法范围 {8, 128, 1024, 8192}
     ├── 累加 totalPages += extSize
     ├── 记录 extent 覆盖的 page 范围（用于 bitmap 校验）
     └── currentExtent = nextExtMetaPageId

3. 校验 totalBlockCount（如启用 checkPageCounts）
4. 校验 dataFirst/dataLast（如适用）
5. 校验 extent-bitmap 一致性（如启用 checkExtentBitmap）
6. 校验 index leafPageCount（如为 INDEX 类型 segment）
```

#### 3.4.3 Extent Chain 完整性校验

```
Extent Chain 完整性校验
│
├── 1. Magic Number 校验
│     每个 extent meta page 的 magic 必须为 EXTENT_META_MAGIC
│     失败 → ERROR "extent_magic_invalid"
│     报告: extentPageId, expected_magic, actual_magic
│
├── 2. Extent Size 校验
│     extSize 必须在 {8, 128, 1024, 8192} 中
│     └── 进阶: extSize 应符合动态扩展规则
│         （segment 前 N 个 extent 为 8 page，之后逐步增大）
│     失败 → ERROR "extent_size_invalid"
│
├── 3. Chain Linkage 校验
│     ├── 每个 next pointer 必须指向合法的 page（或 INVALID 表示末尾）
│     ├── 环检测: visitedPages 集合
│     └── 断链检测: next 指向不存在的 page
│           失败 → ERROR "extent_chain_broken"
│
├── 4. Total Block Count 一致性
│     ├── 遍历完成后: sum(all extSize) == segment.totalBlockCount
│     └── 不一致 → ERROR "block_count_mismatch"
│         报告: expected (from meta), actual (from walk)
│
└── 5. dataFirst / dataLast 一致性（HeapSegmentMeta 专属）
      ├── dataFirst 应落在第一个 extent 的数据页面范围内
      ├── dataLast 应落在最后一个 extent 的数据页面范围内
      └── 不一致 → ERROR "data_range_mismatch"
```

#### 3.4.4 Extent-Bitmap 一致性校验

```
Extent-Bitmap 一致性校验
│
├── 1. 正向校验: Extent → Bitmap
│     对 segment 的每个 extent:
│     ├── 计算该 extent 在 tablespace bitmap 中对应的 bit 位置
│     ├── 读取 bitmap page
│     ├── 检查对应 bit 是否为 1（已分配）
│     └── bit == 0 → ERROR "extent_not_in_bitmap"
│         报告: extentPageId, bitmapPage, bitOffset
│
├── 2. 重叠检测
│     ├── 收集所有 segment 的所有 extent 的 page 范围
│     ├── 排序后检测是否有区间重叠
│     └── 重叠 → ERROR "extent_overlap"
│         报告: segment1, segment2, overlapping_range
│     注意: 此检查需要跨 segment 信息，
│           通常由 VerifyTable 或 CLI 工具在校验多个 segment 时执行
│
└── 3. Bitmap allocatedExtentCount 一致性
      ├── 对每个 bitmap page:
      │     allocatedExtentCount == popcount(bitmap_data)
      └── 不一致 → ERROR "bitmap_count_mismatch"
          （此检查也在单页面校验中执行，这里是跨页面视角的复核）
```

#### 3.4.5 Index Leaf Page Count 校验

```
Index Segment Leaf Page Count 校验

仅对 INDEX 类型 segment:
├── 从 btree root 沿最左 downlink 下降到 level 0
├── 沿 sibling links 遍历 level 0 所有页面，计数
├── 比较 actualLeafCount 与 BtrMeta 或 segment metadata 中记录的 leafPageCount
└── 不一致 → WARNING "leaf_page_count_mismatch"
    （WARNING 因为可能是统计信息延迟更新，不一定是损坏）
```

### 3.5 MetadataVerifier — 上层元数据一致性

#### 3.5.1 元数据输入结构

```cpp
// include/dfx/dstore_metadata_verify.h

namespace DSTORE {

struct IndexMetaEntry {
    Oid       indexOid;                           // 索引 OID
    PageId    indexSegmentId;                      // 索引 segment meta page ID
    uint16    nKeyAtts;                            // 索引键列数
    Oid       attTypeIds[INDEX_MAX_KEY_NUM];       // 键列类型 OID 数组
};

struct MetadataInputStruct {
    Oid       tableOid;                // 表 OID
    PageId    heapSegmentId;           // Heap segment meta page ID
    PageId    lobSegmentId;            // LOB segment meta page ID（INVALID 表示无 LOB）
    uint16    indexCount;              // 索引数量
    IndexMetaEntry* indexes;           // 索引元数据数组（长度 = indexCount）
    uint32    tablespaceId;            // Tablespace ID
    uint32    ownerTablespaceId;       // 表所属 Tablespace ID
    uint16    heapRowFormat;           // Heap 行格式
    uint16    indexRowFormat;          // Index 行格式
};

// 校验入口
RetStatus VerifyMetadataConsistency(const MetadataInputStruct* metadata,
                                    VerifyReport* report);

}  // namespace DSTORE
```

#### 3.5.2 校验逻辑

```
MetadataVerifier 校验项
│
├── 1. Heap Segment 校验
│     ├── 读取 heapSegmentId 对应的 segment meta page
│     ├── segment 存在 → 继续
│     │     不存在 → ERROR "heap_segment_not_found"
│     ├── segment type == HEAP
│     │     不是 → ERROR "heap_segment_type_mismatch"
│     └── segment 所在 tablespace == metadata.tablespaceId
│           不一致 → ERROR "tablespace_mismatch"
│
├── 2. LOB Segment 校验（如 lobSegmentId != INVALID）
│     ├── 读取 lobSegmentId 对应的 segment meta page
│     ├── segment 存在且类型正确
│     └── 不一致 → ERROR "lob_segment_invalid"
│
├── 3. Index Segment 校验（遍历 indexes 数组）
│     for each IndexMetaEntry:
│     ├── 读取 indexSegmentId 对应的 segment meta page
│     ├── segment 存在 → 继续
│     │     不存在 → ERROR "index_segment_not_found"
│     ├── segment type == INDEX
│     │     不是 → ERROR "index_segment_type_mismatch"
│     ├── 读取 BtrMeta page
│     ├── BtrMeta.nKeyAtts == entry.nKeyAtts
│     │     不一致 → ERROR "index_key_count_mismatch"
│     └── BtrMeta.attTypeIds[i] == entry.attTypeIds[i]（逐列比较）
│           不一致 → ERROR "index_att_type_mismatch"
│           报告: indexOid, column_index, expected_type, actual_type
│
└── 4. Tablespace 一致性
      ├── metadata.tablespaceId == metadata.ownerTablespaceId
      │     （通常应一致，不一致可能表示表被 MOVE 但元数据未更新）
      └── 不一致 → WARNING "tablespace_owner_mismatch"
```

### 3.6 VerifyTable — 表级聚合入口

```cpp
// include/dfx/dstore_page_verify.h（或独立头文件）

namespace DSTORE {

struct TableVerifyOptions {
    VerifyLevel pageLevel = VerifyLevel::HEAVYWEIGHT;  // 单页面校验级别
    BtreeVerifyOptions btreeOptions;                    // B-tree 校验选项
    HeapVerifyOptions heapOptions;                      // Heap 数据校验选项
    SegmentVerifyOptions segmentOptions;                // Segment 校验选项
    bool checkMetadata = true;                          // 是否校验元数据
    const MetadataInputStruct* metadata = nullptr;      // 元数据（由上层传入，可选）
};

// 对整表执行全量校验（聚合所有校验维度）
RetStatus VerifyTable(StorageRelationData* heapRel,
                      const TableVerifyOptions& options,
                      VerifyReport* report);

}  // namespace DSTORE
```

**VerifyTable 执行流程**：

```
VerifyTable 执行流程
│
├── 1. 创建 VerifyContext（共享 report, snapshot, maxErrors）
│
├── 2. Heap Segment 校验
│     ├── VerifySegment(heapSegmentMetaPageId, segmentOptions, report)
│     └── VerifyHeapSegment(heapRel, heapOptions, report)
│
├── 3. Index 校验（遍历 relation 的所有 index）
│     for each index of heapRel:
│     ├── VerifySegment(indexSegmentMetaPageId, segmentOptions, report)
│     └── VerifyBtreeIndex(indexRel, heapRel, btreeOptions, report)
│
├── 4. 元数据一致性校验（如 checkMetadata && metadata != nullptr）
│     └── VerifyMetadataConsistency(metadata, report)
│
├── 5. 汇总结果
│     ├── 设置 report endTime
│     └── 返回 report->HasError() ? DSTORE_FAIL : DSTORE_SUCC
│
└── 注意: 各 Verifier 之间无依赖，理论上可并行执行
          但首版实现串行执行，确保正确性优先
```

### 3.7 CLI 离线工具 — dstore_verify

#### 3.7.1 命令行接口

```
Usage: dstore_verify [OPTIONS] <datadir>

Options:
  --table <oid>           校验指定表（by OID）
  --segment <segid>       校验指定 segment（by segmentMetaPageId）
  --page <fileid:blockid> 校验指定页面
  --level <lw|hw>         校验级别（lightweight/heavyweight，默认 heavyweight）
  --check-btree           执行 btree 结构校验
  --check-heap            执行 heap 数据校验
  --check-segment         执行 segment 元数据校验
  --check-extent          执行 extent 分配一致性校验
  --sample-ratio <0-100>  index-heap 数据校验采样比例（默认 100）
  --max-errors <N>        最大错误数（默认 1000）
  --format <text|json>    输出格式（默认 text）
  --all                   执行所有校验
  --metadata-file <path>  元数据文件路径（JSON 格式，用于 metadata consistency）

Exit codes:
  0   所有校验通过
  1   发现 ERROR 级别问题
  2   仅有 WARNING 级别问题
  3   工具执行错误（参数错误、文件不可读等）
```

#### 3.7.2 工具架构

```
dstore_verify_main.cpp
│
├── ParseArgs(argc, argv) → VerifyCliOptions
│     ├── 解析命令行参数
│     ├── 参数合法性校验
│     └── 构造对应的 VerifyOptions
│
├── OpenDataDir(datadir)
│     ├── 离线模式: 直接读取数据文件（mmap 或 pread）
│     │     不经过 buffer manager
│     │     适用于 dstore 未运行时的离线检查
│     └── 在线模式（未来扩展）: 连接运行中的 dstore 实例
│           通过 buffer manager 读页面
│
├── RunVerification(options)
│     ├── --page → VerifyPage()
│     ├── --check-btree → VerifyBtreeIndex()
│     ├── --check-heap → VerifyHeapSegment()
│     ├── --check-segment → VerifySegment()
│     ├── --check-extent → extent-bitmap 一致性
│     ├── --metadata-file → VerifyMetadataConsistency()
│     └── --all / --table → VerifyTable()
│
└── OutputReport(report, format)
      ├── format=text → report.FormatText() → stdout
      └── format=json → report.FormatJson() → stdout
```

#### 3.7.3 离线模式 Page 读取

```cpp
// 离线模式下的页面读取（绕过 buffer manager）
class OfflinePageReader {
public:
    explicit OfflinePageReader(const std::string& dataDir);
    ~OfflinePageReader();

    // 读取指定 pageId 的页面数据
    // 返回指向 8KB 内存的指针（内部管理生命周期）
    const Page* ReadPage(const PageId& pageId);

private:
    std::string m_dataDir;
    // 文件句柄缓存: fileId → fd
    std::unordered_map<uint32, int> m_fdCache;
    // 页面缓存（LRU 或简单 map，避免重复读取）
    std::unordered_map<uint64, std::vector<char>> m_pageCache;
};
```

---

## 4. 文件组织

```
include/dfx/
├── dstore_page_verify.h          # （已有）VerifyTable 接口声明追加于此
├── dstore_verify_report.h        # （已有）VerifyReport, VerifyResult
├── dstore_verify_context.h       # VerifyContext 定义
├── dstore_btree_verify.h         # BtreeVerifyOptions + VerifyBtreeIndex
├── dstore_heap_verify.h          # HeapVerifyOptions + VerifyHeapSegment
├── dstore_segment_verify.h       # SegmentVerifyOptions + VerifySegment
└── dstore_metadata_verify.h      # MetadataInputStruct + VerifyMetadataConsistency

src/dfx/
├── dstore_page_verify.cpp        # （已有）追加 VerifyTable 实现
├── dstore_verify_report.cpp      # （已有）
├── dstore_verify_context.cpp     # VerifyContext 实现
├── dstore_btree_verify.cpp       # BtreeVerifier 实现
├── dstore_heap_verify.cpp        # HeapSegmentVerifier 实现
├── dstore_segment_verify.cpp     # SegmentVerifier 实现
└── dstore_metadata_verify.cpp    # MetadataVerifier 实现

tools/dstore_verify/
├── CMakeLists.txt                # CLI 工具构建配置
└── dstore_verify_main.cpp        # main 入口 + 参数解析 + OfflinePageReader

tests/unittest/ut_dfx/
├── ut_btree_verify.cpp           # Btree 跨页面校验测试
├── ut_heap_segment_verify.cpp    # Heap segment 校验测试
├── ut_segment_verify.cpp         # Segment/extent 校验测试
└── ut_metadata_verify.cpp        # 元数据一致性校验测试
```

---

## 5. 测试策略

### 5.1 单元测试

| 测试用例 | 输入 | 期望结果 |
|----------|------|---------|
| **BtreeVerifier — 结构完整** | 正确构造的 3 层 btree（root + internal + leaf） | 返回 SUCC，report 无 ERROR |
| **BtreeVerifier — sibling 断裂** | leaf page A.next = B, 但 B.prev != A | ERROR "sibling_link_broken" |
| **BtreeVerifier — 同层 key 无序** | 相邻 leaf page 的 key 不单调递增 | ERROR "inter_page_key_order_violation" |
| **BtreeVerifier — parent-child 不匹配** | internal key 与 child 首 key 不一致 | ERROR "parent_child_key_mismatch" |
| **BtreeVerifier — sibling 环** | A.next = B, B.next = A | ERROR "sibling_link_cycle" |
| **BtreeVerifier — mid-split** | page splitStat = SPLIT_INCOMPLETE | WARNING "page_mid_split"，不报 ERROR |
| **Index-Heap — 正常 1:1** | 所有 index entry 指向有效 heap tuple | 返回 SUCC |
| **Index-Heap — 悬空引用** | index entry 指向 UNUSED ItemId | ERROR "index_points_to_dead_tuple" |
| **Index-Heap — 缺失索引项** | 可见 heap tuple 在 index 中找不到 | ERROR "heap_tuple_missing_index_entry" |
| **Index-Heap — 数据不一致** | index key != heap tuple column value | ERROR "index_heap_data_mismatch" |
| **Index-Heap — 采样模式** | sampleRatio=0.1, 1000 个 leaf page | 大约 100 个 page 被校验 |
| **HeapSegment — tuple 格式正常** | 所有 tuple size/null bitmap 合法 | 返回 SUCC |
| **HeapSegment — null bitmap 溢出** | bitmap 长度超出 tuple 边界 | ERROR "null_bitmap_overflow" |
| **BigTuple — chain 完整** | 3 chunk chain，count=3，link 正确 | 返回 SUCC |
| **BigTuple — chain 断裂** | chunk 2 的 next CTID 指向不存在的 page | ERROR "chunk_chain_broken" |
| **BigTuple — count 不匹配** | first chunk 记录 count=3，实际只有 2 chunk | ERROR "chunk_count_mismatch" |
| **FSM-Heap — 一致** | free space 与 FSM category 匹配 | 返回 SUCC |
| **FSM-Heap — 不一致** | FSM 高估可用空间 2 个 category | WARNING "fsm_overestimate" |
| **Segment — extent chain 完整** | 3 个 extent，chain 正常终止 | 返回 SUCC |
| **Segment — extent 环** | extent A → B → A | ERROR "extent_chain_cycle" |
| **Segment — magic 错误** | extent meta magic != EXTENT_META_MAGIC | ERROR "extent_magic_invalid" |
| **Segment — block count 不匹配** | meta.totalBlockCount != sum(extSizes) | ERROR "block_count_mismatch" |
| **Segment — bitmap 不一致** | extent 已分配但 bitmap bit 为 0 | ERROR "extent_not_in_bitmap" |
| **Metadata — 正常** | 所有 segment 存在且类型正确 | 返回 SUCC |
| **Metadata — segment 不存在** | heapSegmentId 指向无效 page | ERROR "heap_segment_not_found" |
| **Metadata — index 类型不匹配** | index segment type != INDEX | ERROR "index_segment_type_mismatch" |
| **Metadata — attType 不一致** | BtrMeta.attTypeIds != MetadataInput | ERROR "index_att_type_mismatch" |

### 5.2 测试构造方法

跨页面校验测试需要构造多页面的数据结构，采用以下策略：

```
测试数据构造
│
├── 1. Mock Page Builder
│     ├── 提供 HeapPageBuilder、IndexPageBuilder 等
│     ├── 可构造合法或故意损坏的页面
│     └── 支持设置 sibling links、key values、tuple data 等
│
├── 2. In-Memory Page Store
│     ├── 实现简化的 page 读取接口
│     ├── 通过 map<PageId, Page*> 管理测试页面
│     └── 替代 buffer manager，无需真实数据文件
│
└── 3. 构造场景
      ├── BuildValidBtree(nLevels, nKeysPerPage) → 返回 root PageId
      ├── BuildBrokenSiblingBtree() → sibling link 故意断裂
      ├── BuildBigTupleChain(nChunks) → 返回 first chunk PageId
      └── BuildSegmentWithExtents(nExtents) → 返回 segment meta PageId
```

### 5.3 性能测试

| 场景 | 规模 | 预期耗时 |
|------|------|---------|
| Btree 结构校验（不含 Index-Heap） | 10,000 个 leaf page | < 10s |
| Index-Heap 1:1 校验 | 10,000 个 leaf page, 100,000 tuple | < 60s |
| Index-Heap 数据校验（10% 采样） | 10,000 个 leaf page | < 10s |
| Heap segment 扫描 | 10,000 个 heap page | < 30s |
| Big tuple chain 校验 | 1,000 条 chain，平均 5 chunk | < 5s |
| Segment extent chain walk | 100 个 extent | < 1s |

---

## 6. 风险与应对

| 风险 | 影响 | 应对措施 |
|------|------|---------|
| Index-Heap 反向校验（Heap→Index）IO 开销大 | 需要对每个 heap tuple 做 btree 查找 | 支持采样降低开销；可分离为独立选项，按需执行 |
| Online 校验期间并发修改导致误报 | 校验看到不一致的中间状态 | MVCC snapshot 隔离；transient state（mid-split、in-progress txn）报 WARNING 不报 ERROR |
| Extent chain 成环导致无限循环 | 工具挂起 | visitedPages 集合检测环，检测到立即终止并报告 |
| Big tuple chain 跨大量 extent | 单条 chain 校验耗时长 | 设置单条 chain 最大遍历步数（如 10,000），超出报 WARNING |
| 离线模式下数据文件被其他进程修改 | 读到不一致数据 | 建议在 dstore 停止后执行离线校验；工具输出提示信息 |
| MetadataInputStruct 由上层传入，可能本身有误 | 误报元数据不一致 | 校验报告中明确区分"DStore 侧确认损坏"与"传入元数据可能有误" |
| VerifyContext visitedPages 内存占用 | 大表可能有百万级页面 | `unordered_set<uint64>` 每项约 16 bytes，100 万项约 16MB，可接受 |

---

## 7. 与单页面校验的关系

跨页面校验**依赖**单页面校验作为基础：

```
执行顺序
│
├── 1. 对每个被访问的页面，先通过 PageVerifyRegistry 执行单页面校验
│     （通用 header 校验 + 类型专用校验）
│     如果单页面校验发现 ERROR，记录到 report 但继续跨页面校验
│     （某些跨页面检查即使页面有小损坏也有诊断价值）
│
├── 2. 在单页面校验通过的基础上，执行跨页面逻辑校验
│     （key 排序、sibling link、Index-Heap 对应等）
│
└── 3. 如果单页面校验发现 CRC 错误等严重问题
      对该页面的跨页面校验结果降级为 WARNING
      （因为页面数据本身不可信，跨页面比对结果不可靠）
```

**共享基础设施**：
- `VerifyReport` — 单页面和跨页面校验使用同一个 report 实例
- `VerifySeverity` — 统一的严重级别枚举
- `VerifyLevel` 和 `VerifyModule` — 跨页面校验通常在 HEAVYWEIGHT 级别执行，不受 GUC 限制（由调用方显式触发）
