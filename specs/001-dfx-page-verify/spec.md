# Feature Specification: DFX Page Verification for DStore Storage Engine

**Feature Branch**: `001-dfx-page-verify`
**Created**: 2026-03-24
**Status**: Draft
**Input**: User description: "给dstore存储引擎加上dfx页面校验逻辑，分为单页面校验和跨页面校验两部分"

## Clarifications

### Session 2026-03-24

- Q: 轻量级校验的性能预算（写入路径）？ → A: Header + 基本结构校验（checksum、bounds、page type、special region offset），不遍历 line pointers 或 tuples
- Q: 检测到损坏时的行为？ → A: 报告 + 记录日志 + 返回错误码给调用方，由调用方决定是否中断写入
- Q: 重量级校验的触发方式？ → A: 同时提供函数接口 + 独立 CLI 工具，支持在线和离线校验
- Q: InnoDB 侧元数据传入方式？ → A: 定义元数据结构体（包含 segment id、tablespace id、tableoid、index info、行格式等）作为校验函数的输入参数
- Q: 轻量级校验覆盖的 PageType 范围？ → A: 全部 17 种 PageType 都实现专用的轻量级校验函数

### Session 2026-03-24 (Supplement)

- Q: 索引-Heap 数据一致性抽样策略？ → A: 按比例随机采样，比例可配置（如 1%、10%、100%）
- Q: 校验等级配置方式？ → A: 运行时 GUC 参数，动态生效无需重启

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Single Page Integrity Verification (Priority: P1)

As a DBA or storage engine developer, I want to verify the internal consistency of individual pages so that I can detect page-level corruption before it propagates into cross-page data inconsistency.

Single page verification provides two levels:

- **Lightweight verification**: Runs inline during CRUD operations and dirty page flush. Covers header + basic structure checks: CRC checksum, upper/lower bounds consistency, LSN validity, page type, special region offset. Each of the 17 PageTypes has a dedicated lightweight verification function. Does not traverse line pointers or tuples to minimize performance impact.

- **Heavyweight verification**: Runs on demand via function interface or standalone CLI tool (online and offline). Performs comprehensive structural validation including:
  - **Heap pages**: ItemId datalen vs actual tuple length consistency, ItemId state validity (UNUSED: len=0; NORMAL: len>0 and offset valid; NO_STORAGE: tdId < tdCount, tdStatus and tupLiveMode in valid ranges; UNREADABLE_RANGE_HOLDER: len>0), no overlapping item storage regions, TD state validity (UNOCCUPY_AND_PRUNEABLE / OCCUPY_TRX_IN_PROGRESS / OCCUPY_TRX_END), TD count within bounds (MIN_TD_COUNT..MAX_TD_COUNT), TD-tuple state consistency (tuple's tdId references valid TD slot, TD status matches tuple live mode expectations)
  - **Index pages**: BtrPageLinkAndStatus special region validation (page type, level, split status, sibling links), high key (ItemId offset 1) must be >= all other keys on the page, intra-page key ordering (tuples must be sorted), key-value type consistency with BtrMeta attribute definitions
  - **FSM pages**: Free space category entry validity, FSM tree structure consistency
  - **Segment meta pages**: Extent chain pointer validity, segment type and magic number
  - **Undo pages**: Undo record header validity (valid UndoType, valid ctid, file version), prev/next chain linkage
  - **Transaction slot pages**: Slot status validity (FROZEN/IN_PROGRESS/PENDING_COMMIT/COMMITTED/ABORTED/FAILED/PREPARED), CSN-status consistency
  - **Bitmap pages**: allocatedExtentCount matches actual set bits in bitmap
  - **Extent meta pages**: Magic number (EXTENT_META_MAGIC), extent size validity, next pointer format

**Verification is configurable** via a runtime GUC parameter (dynamic, no restart required):
- **Verification level**: OFF (no verification), LIGHTWEIGHT, HEAVYWEIGHT
- **Verification module**: HEAP, INDEX, ALL (selects which page types are verified)

**Invocation modes**:
- **Inline**: Called during insert/update/delete/select operations and dirty page flush, using the configured verification level
- **Offline tool**: Standalone CLI tool for heavyweight verification on data files, can operate without a running dstore instance

**Why this priority**: Single page verification is the foundation — it catches the most common corruption issues at the lowest level. All cross-page verification depends on each individual page being internally consistent first. The lightweight variant enables proactive detection at write time. The configurable level allows balancing performance vs detection coverage per environment.

**Independent Test**: Can be fully tested by loading any individual page and running its type-specific verification function at both levels, which returns a pass/fail result with detailed diagnostics.

**Acceptance Scenarios**:

1. **Given** a valid heap page with correct header, line pointers, and tuples, **When** single page verification is invoked (either level), **Then** the verification passes with no errors reported.
2. **Given** a heap page where an ItemId's datalen does not match the actual tuple length at its offset, **When** heavyweight verification is invoked, **Then** the mismatch is detected and reported with the ItemId offset number, expected and actual lengths.
3. **Given** a heap page with a TD in OCCUPY_TRX_END status but the corresponding tuple's tdId references a different TD slot, **When** heavyweight verification is invoked, **Then** the TD-tuple inconsistency is detected.
4. **Given** an index page where the high key (offset 1) is less than a key at offset 3, **When** heavyweight verification is invoked, **Then** the high key violation is detected and reported.
5. **Given** an index page with unsorted tuples, **When** heavyweight verification is invoked, **Then** the ordering violation is detected.
6. **Given** a page with an invalid CRC checksum being written during a flush, **When** lightweight verification runs inline, **Then** the verification returns an error code to the caller.
7. **Given** any of the 17 PageTypes, **When** lightweight verification is invoked, **Then** a dedicated verification function for that specific PageType executes (not a generic fallback).
8. **Given** verification level GUC is set to OFF, **When** a page write occurs, **Then** no verification is performed (zero overhead).
9. **Given** verification module GUC is set to HEAP, **When** an index page write occurs, **Then** no verification is performed on that page.
10. **Given** an ItemId with NO_STORAGE flag but m_tupLiveMode > 6, **When** heavyweight verification is invoked, **Then** the invalid state is detected.

---

### User Story 2 - Heap Data and Tuple Verification (Priority: P2)

As a DBA, I want to verify heap tuple data integrity including row format, linked/big tuples, and data visibility, so that I can detect row-level corruption and data consistency issues.

**Why this priority**: Heap pages store the actual user data. After confirming individual page structure is sound, verifying row-level data integrity is the next logical step to ensure data correctness.

**Independent Test**: Can be tested by scanning heap pages within a segment and validating each tuple against the expected row format.

**Acceptance Scenarios**:

1. **Given** a heap page with tuples whose sizes match their declared column counts and null bitmaps, **When** heap data verification runs, **Then** all tuples pass verification.
2. **Given** a heap page containing a tuple whose declared size exceeds the available space between its offset and the next tuple, **When** heap data verification runs, **Then** the corrupted tuple is identified by its ItemPointer (page ID + offset number).
3. **Given** a heap page with a tuple whose null bitmap length does not match the declared column count, **When** heap data verification runs, **Then** the inconsistency is detected and reported.
4. **Given** a linked (big) tuple whose first chunk (m_linkInfo = TUP_LINK_FIRST_CHUNK_TYPE) has a next-chunk CTID pointing to a non-existent page, **When** big tuple chunk verification runs, **Then** the broken chain is detected and reported.
5. **Given** a linked tuple with 3 chunks, **When** big tuple chunk verification traverses the chain, **Then** the chunk count stored in the first chunk's header matches the actual number of chunks found, and every chunk has correct m_linkInfo values (FIRST_CHUNK for first, NOT_FIRST_CHUNK for subsequent).
6. **Given** an online table-level verification, **When** scanning heap tuples, **Then** data visibility is considered: only committed and visible tuples are included in cross-reference checks, in-progress/aborted tuples are skipped or flagged separately.

---

### User Story 3 - B-tree Structure and Index-Heap Consistency Verification (Priority: P2)

As a DBA, I want to verify B-tree index structural integrity, key ordering across pages, and data consistency with heap tuples, so that I can detect index corruption and index-heap mismatches.

**Why this priority**: B-tree integrity is critical for query correctness. A structurally broken index can cause silent wrong query results.

**Independent Test**: Can be tested by traversing a B-tree index from root to leaves, validating structural properties, and cross-referencing against heap tuples.

**Acceptance Scenarios**:

1. **Given** a structurally valid B-tree index, **When** B-tree verification runs, **Then** the entire tree passes: all sibling links are bidirectionally consistent, levels are monotonically decreasing from root to leaf, and key ordering is correct.
2. **Given** a B-tree where a leaf page's "next" pointer references a page whose "prev" pointer does not point back, **When** B-tree verification runs, **Then** the broken sibling link is detected and reported with both page IDs.
3. **Given** B-tree leaf pages at the same level linked left-to-right, **When** same-level key ordering verification runs, **Then** keys across sibling pages are monotonically increasing (or decreasing per index definition), with no ordering violation at page boundaries.
4. **Given** an internal B-tree page with a key at offset N and a downlink to a child page, **When** parent-child key consistency verification runs, **Then** the internal page key matches the first key (or high key boundary) of the referenced child page.
5. **Given** a B-tree leaf whose index entries reference heap tuple locations, **When** index-heap 1:1 correspondence verification runs (full or sampled), **Then** every checked index entry points to a valid existing heap tuple, and every checked visible heap tuple has a corresponding index entry.
6. **Given** index-heap data consistency verification in sampling mode at 10%, **When** verification runs, **Then** approximately 10% of index entries are randomly selected and their key values are compared against the corresponding heap tuple's column values to confirm data matches.
7. **Given** an online B-tree/heap consistency check, **When** verifying correspondence, **Then** visibility is considered: only committed, visible tuples and index entries are cross-referenced; in-progress tuples are excluded from mismatch detection.

---

### User Story 4 - Segment and Extent Verification (Priority: P3)

As a storage engine developer, I want to verify segment metadata integrity, extent allocation consistency, and FSM-heap data consistency, so that I can detect segment-level corruption before it causes data loss.

**Why this priority**: Segment-level corruption is rarer but has wider blast radius. Verifying segment integrity ensures the container structures that hold all pages and extents are sound.

**Independent Test**: Can be tested by loading a segment's meta page and walking its extent chain, verifying metadata, page counts, and cross-referencing with bitmap and FSM data.

**Acceptance Scenarios**:

1. **Given** a heap segment with a valid extent chain, **When** segment verification runs, **Then** the total block count in the segment meta page matches the sum of all extent sizes, and the extent chain terminates properly.
2. **Given** a segment where an extent meta page's next pointer references a non-existent page, **When** segment verification runs, **Then** the broken chain is detected and reported with the specific extent location.
3. **Given** a heap segment meta page whose dataFirst/dataLast pointers are inconsistent with the actual extent data, **When** segment verification runs, **Then** the pointer inconsistency is detected and reported.
4. **Given** segment metadata, **When** segment metadata verification runs, **Then** magic number and segment type (HEAP/INDEX/UNDO/TEMP) are validated against expected values.
5. **Given** a heap segment, **When** FSM-heap consistency verification runs, **Then** heap pages' actual free space is consistent with the FSM tree's recorded free space categories (within acceptable tolerance for concurrent modifications).
6. **Given** an index segment, **When** leaf page count verification runs, **Then** the actual number of leaf pages found by traversing level-0 sibling links matches the expected count from the segment/btree metadata.
7. **Given** a tablespace with allocated extents, **When** extent allocation consistency verification runs, **Then**: (a) no two segments' extents overlap, (b) every extent in a segment's chain has a corresponding "allocated" bit in the tablespace bitmap, (c) extent sizes match the dynamic sizing rules for their position in the segment.

---

### User Story 5 - InnoDB System Table to DStore Data Consistency Verification (Priority: P3)

As a DBA, I want to verify that the metadata from the upper layer (InnoDB system tables) is consistent with the actual dstore segment data, so that I can detect mismatches between table metadata and the physical storage layout.

This feature is scoped to the **dstore side only**: dstore provides verification interfaces and implementations. The upper layer (MySQL/InnoDB) is responsible for calling these interfaces and passing in the metadata via a defined input structure. Upper layer code is not part of this project.

The metadata passed from the upper layer includes:
- **Segment metadata**: heap segment ID, LOB segment ID, index segment ID, index OID, table OID
- **Tablespace metadata**: tablespace ID, table's owning tablespace ID
- **Row format**: heap row format, index row format

**Why this priority**: Metadata-data mismatch is the most dangerous form of inconsistency — it can cause one table's queries to silently read another table's data. While rare, the consequences are catastrophic.

**Independent Test**: Can be tested by constructing a metadata input struct with known values and verifying that dstore's verification interface correctly validates the corresponding segment existence, type, and schema consistency.

**Acceptance Scenarios**:

1. **Given** a metadata input struct pointing to a valid heap segment of the correct type, **When** dstore's metadata consistency verification interface is called, **Then** the verification passes.
2. **Given** a metadata input struct referencing a segment ID that does not exist or belongs to a different relation, **When** dstore's metadata consistency verification interface is called, **Then** the mismatch is detected and reported with both the expected and actual segment information.
3. **Given** a metadata input struct with index information referencing index segments, **When** dstore's metadata consistency verification interface is called, **Then** each index segment exists, is of type INDEX, and its B-tree meta page's attribute information is consistent with the provided schema.

---

### Edge Cases

- What happens when a page is all-zero (newly allocated but uninitialized)? Verification should recognize this as a valid uninitialized state, not corruption.
- How does verification handle pages that are currently being modified by an active transaction? Verification should either skip in-flight pages or operate on a consistent snapshot.
- What happens when the segment extent chain is circular (corruption)? Verification must detect loops and terminate rather than running infinitely.
- How does verification handle a B-tree that is mid-split (split status is SPLIT_INCOMPLETE)? Verification should account for known transient states and not flag them as corruption.
- What happens when a page's checksum is valid but its logical content is inconsistent? Both checksum and logical verification should run independently to catch different classes of corruption.
- What happens when lightweight verification fails on a write path? The verification returns an error code; the caller decides whether to abort or proceed with the write.
- What happens when a linked (big) tuple chain crosses extent boundaries? Chunk verification must follow CTID pointers across pages regardless of extent membership.
- What happens when online verification encounters a tuple whose visibility cannot be determined (e.g., transaction slot is recycled)? Verification should flag it as WARNING rather than ERROR.
- What happens during sampling verification when the random sample happens to miss a corrupted region? Sampling provides probabilistic coverage; full verification is needed for deterministic guarantees.

## Requirements *(mandatory)*

### Functional Requirements

**Single Page Verification — Lightweight:**

- **FR-001**: System MUST provide dedicated lightweight verification functions for all 17 PageTypes: HEAP_PAGE_TYPE, INDEX_PAGE_TYPE, TRANSACTION_SLOT_PAGE, UNDO_PAGE_TYPE, FSM_PAGE_TYPE, FSM_META_PAGE_TYPE, DATA_SEGMENT_META_PAGE_TYPE, HEAP_SEGMENT_META_PAGE_TYPE, UNDO_SEGMENT_META_PAGE_TYPE, TBS_EXTENT_META_PAGE_TYPE, TBS_BITMAP_PAGE_TYPE, TBS_BITMAP_META_PAGE_TYPE, TBS_FILE_META_PAGE_TYPE, BTR_QUEUE_PAGE_TYPE, BTR_RECYCLE_PARTITION_META_PAGE_TYPE, BTR_RECYCLE_ROOT_META_PAGE_TYPE, TBS_SPACE_META_PAGE_TYPE.
- **FR-002**: Lightweight verification MUST check: CRC checksum, upper/lower bounds consistency, LSN validity (no regression, no UINT64_MAX), page type validity, and special region offset consistency.
- **FR-003**: Lightweight verification MUST be callable inline during insert, update, delete, select operations and dirty page flush.

**Single Page Verification — Heavyweight:**

- **FR-004**: Heavyweight heap page verification MUST validate: ItemId datalen vs actual tuple length at offset, ItemId state-specific invariants (UNUSED: len=0; NORMAL: len>0, offset valid; NO_STORAGE: tdId/tdStatus/tupLiveMode in valid ranges; UNREADABLE_RANGE_HOLDER: len>0), no overlapping item storage regions, TD count within [MIN_TD_COUNT, MAX_TD_COUNT], TD state validity (UNOCCUPY_AND_PRUNEABLE / OCCUPY_TRX_IN_PROGRESS / OCCUPY_TRX_END), TD-tuple state consistency (tuple's tdId references valid TD, TD CSN status matches TD state).
- **FR-005**: Heavyweight index page verification MUST validate: BtrPageLinkAndStatus special region (page type, level, split status, sibling link format), high key at offset 1 is >= all other keys on the page, intra-page tuple ordering (keys are sorted), key-value type consistency with BtrMeta attribute definitions.
- **FR-006**: Heavyweight verification for other page types MUST validate type-specific invariants: FSM entry validity, segment meta magic/type, undo record header fields (UndoType, ctid, file version), transaction slot status-CSN consistency, bitmap allocatedExtentCount vs actual bits, extent meta magic number and size.

**Configuration:**

- **FR-007**: Verification level MUST be configurable via a runtime GUC parameter (dynamic, no restart required) with values: OFF (no verification), LIGHTWEIGHT, HEAVYWEIGHT.
- **FR-008**: Verification module MUST be configurable via a runtime GUC parameter with values: HEAP, INDEX, ALL, allowing selective verification of specific page type families.

**Cross-Page Verification — Index-Heap Consistency:**

- **FR-009**: System MUST verify 1:1 correspondence between index leaf entries and heap tuples: every index entry must point to a valid existing heap tuple, and every visible heap tuple must have a corresponding index entry.
- **FR-010**: System MUST verify data consistency between index tuples and their corresponding heap tuples: the key column values stored in the index must match the column values in the heap tuple. This verification supports both full scan and configurable-ratio random sampling modes.
- **FR-011**: Sampling ratio for index-heap data consistency MUST be configurable (e.g., 1%, 10%, 100%), with 100% equivalent to full verification.

**Cross-Page Verification — B-tree Structure:**

- **FR-012**: B-tree structure verification MUST traverse from root to leaves, validating level consistency, bidirectional sibling link integrity.
- **FR-013**: Same-level key ordering verification MUST validate that keys across sibling pages at the same B-tree level are monotonically ordered (increasing or decreasing per index definition), with no violation at page boundaries.
- **FR-014**: Parent-child key consistency verification MUST validate that each internal page's index tuple matches the downlink-referenced child page's first tuple (or high key boundary).

**Cross-Page Verification — Big Tuple Chunk Integrity:**

- **FR-015**: System MUST verify linked (big) tuple chunk completeness: traverse the chunk chain from first chunk (m_linkInfo = TUP_LINK_FIRST_CHUNK_TYPE) following next-chunk CTIDs, verify each subsequent chunk has m_linkInfo = TUP_LINK_NOT_FIRST_CHUNK_TYPE, and confirm the total chunk count matches the count stored in the first chunk's header.

**Cross-Page Verification — Data Visibility:**

- **FR-016**: When table-level verification runs online, the system MUST consider data visibility: use MVCC snapshot information to determine which heap tuples and index entries are committed and visible, and exclude in-progress/aborted tuples from cross-reference mismatch detection.

**Cross-Page Verification — Segment and Extent:**

- **FR-017**: Segment metadata verification MUST validate magic number and segment type (HEAP/INDEX/UNDO/TEMP) against expected values.
- **FR-018**: Heap segment FSM consistency verification MUST validate that heap pages' actual free space is consistent with the FSM tree's recorded categories.
- **FR-019**: Index segment leaf page count verification MUST validate that the actual number of level-0 pages found by traversal matches the expected count.
- **FR-020**: Extent allocation consistency verification MUST validate: (a) no two segments' extents overlap, (b) every extent in a segment's chain has a corresponding "allocated" bit in the tablespace bitmap, (c) extent sizes match the dynamic sizing rules for their position in the segment.
- **FR-021**: Segment verification MUST walk the extent chain and validate: extent meta page magic numbers, chain linkage consistency (no broken or circular links), and total block count accuracy.

**Cross-Page Verification — InnoDB Metadata:**

- **FR-022**: InnoDB-to-DStore metadata verification MUST define a metadata input structure containing: heap segment ID, LOB segment ID, index segment ID, index OID, table OID, tablespace ID, table's owning tablespace ID, heap row format, and index row format. The verification function accepts this struct and validates the referenced dstore data.

**General:**

- **FR-023**: All verification functions MUST produce structured diagnostic output that includes: the verification target (page ID, segment ID, relation OID), the specific check that failed, the expected vs. actual values, and a severity level (ERROR for definite corruption, WARNING for suspicious but possibly transient states).
- **FR-024**: Verification functions MUST return an error code to the caller upon failure. The caller (not the verification function) decides whether to interrupt the operation.
- **FR-025**: Heavyweight verification MUST be invocable both via a programmatic function interface and a standalone CLI tool that can operate on live systems (online) or data files directly (offline).
- **FR-026**: Verification MUST be able to run on a live system without blocking normal read/write operations (non-intrusive, read-only diagnostic).
- **FR-027**: Verification MUST handle transient states gracefully — pages mid-split, in-progress transactions, uninitialized (all-zero) pages — without false positives.

### Key Entities

- **Page**: The fundamental storage unit (8KB). Has a type-specific header and content layout. Identified by PageId (fileId + blockId). 17 distinct types exist in the system.
- **ItemId**: Line pointer within a data page. 32-bit structure with 4 states: UNUSED (empty slot), NORMAL (points to tuple), NO_STORAGE (pruned, stores redirect info), UNREADABLE_RANGE_HOLDER (rollbacked placeholder).
- **TD (Transaction Directory)**: Per-page transaction slot array for MVCC. Each TD slot tracks Xid, CSN, undo pointer, and status (UNOCCUPY_AND_PRUNEABLE / OCCUPY_TRX_IN_PROGRESS / OCCUPY_TRX_END).
- **Tuple**: A row of data stored within a heap page. Identified by ItemPointer (PageId + offset number). Contains transaction metadata, null bitmap, and column values. May be linked (big tuple) spanning multiple chunks across pages.
- **Linked/Big Tuple**: A tuple too large for a single page, stored as a chain of chunks. First chunk has m_linkInfo = FIRST_CHUNK with next-chunk CTID and total chunk count. Subsequent chunks have m_linkInfo = NOT_FIRST_CHUNK.
- **B-tree Index**: A multi-level tree of index pages. Each non-rightmost page has a high key at offset 1. Leaf entries reference heap tuples via ItemPointers. Internal entries contain downlinks to child pages.
- **Segment**: A logical container of extents that stores all pages for a table (heap segment) or index (index segment). Has a meta page with magic number, segment type, and extent chain.
- **Extent**: A contiguous group of pages (8 to 8192 pages depending on segment position). Linked via extent meta pages. Each has a magic number (EXTENT_META_MAGIC) and size.
- **FSM (Free Space Map)**: Multi-level tree tracking free space categories for heap pages. Each heap page references its FSM slot via FsmIndex.
- **Tablespace Bitmap**: Tracks extent allocation across the tablespace. Each bit represents one extent. allocatedExtentCount must match the number of set bits.
- **Metadata Input Struct**: A dstore-defined structure for receiving upper-layer (InnoDB) metadata, containing segment IDs, tablespace IDs, table OID, index OID, and row format information.
- **Verification Config**: Runtime GUC parameters controlling verification level (OFF/LIGHTWEIGHT/HEAVYWEIGHT) and module scope (HEAP/INDEX/ALL).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Lightweight verification covers all 17 PageTypes with dedicated per-type functions, detecting header-level corruptions (invalid CRC, out-of-bounds lower/upper, LSN anomalies, wrong page type) on the write path.
- **SC-002**: Heavyweight verification detects all structural violations for each PageType, including ItemId datalen-tuple length mismatches, ItemId state violations, TD state/count violations, TD-tuple state inconsistencies, index high key violations, and intra-page key ordering violations.
- **SC-003**: Heap data verification detects tuple-level corruptions (size mismatch, null bitmap inconsistency, overflow) with zero false positives on valid pages.
- **SC-004**: Big tuple chunk verification detects all chain integrity violations: broken links, missing chunks, chunk count mismatches, and incorrect m_linkInfo values.
- **SC-005**: B-tree structure verification detects all structural violations: broken sibling links, level inconsistency, same-level key ordering violations across pages, and parent-child key mismatches.
- **SC-006**: Index-heap consistency verification (full or sampled) detects orphaned index entries, missing index entries, and data value mismatches between index and heap tuples.
- **SC-007**: Online verification correctly handles data visibility: excludes in-progress/aborted tuples from cross-reference checks, producing zero false positives from concurrent transactions.
- **SC-008**: Segment verification detects extent chain corruptions (broken links, circular chains, count mismatches), metadata violations (invalid magic, wrong type), and terminates safely within bounded time.
- **SC-009**: FSM-heap consistency verification detects free space tracking mismatches between heap pages and FSM entries.
- **SC-010**: Extent allocation verification detects overlapping extents, bitmap-extent chain mismatches, and incorrect extent sizes.
- **SC-011**: InnoDB-to-DStore metadata verification, given a populated metadata input struct, detects all segment reference mismatches (missing segments, wrong types, schema inconsistencies).
- **SC-012**: Verification level and module GUC parameters take effect dynamically without restart, and OFF level incurs zero verification overhead.
- **SC-013**: Full verification of a single table completes without impacting concurrent read/write operations on the same table.
- **SC-014**: All verification diagnostics include sufficient detail (page IDs, offset numbers, expected vs actual values, severity) for an operator to locate and understand the issue without additional investigation.
- **SC-015**: The standalone CLI tool can perform heavyweight verification on data files without requiring a running dstore instance (offline mode).
