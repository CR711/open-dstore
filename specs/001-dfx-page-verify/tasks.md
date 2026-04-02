# Tasks: DFX Page Verification

**Input**: Design documents from `/specs/001-dfx-page-verify/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup

**Purpose**: Create directory structure and build configuration for the DFX verification module

- [X] T001 Create directory structure: `include/dfx/`, `src/dfx/`, `tools/dstore_verify/`, `tests/unittest/ut_dfx/`
- [X] T002 Add `src/dfx/CMakeLists.txt` — define dfx library target, add source files, link dependencies (page, buffer, heap, index, transaction modules)
- [X] T003 Add `tools/dstore_verify/CMakeLists.txt` — define dstore_verify executable target, link dfx library
- [X] T004 Add `tests/unittest/ut_dfx/CMakeLists.txt` — define dfx unit test target, link dfx library and GTest
- [X] T005 Update root `CMakeLists.txt` and `src/CMakeLists.txt` to include new `src/dfx/` subdirectory
- [X] T006 Update `tools/CMakeLists.txt` to include `tools/dstore_verify/` subdirectory

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that ALL user stories depend on — VerifyReport, enums, PageVerifyRegistry framework, GUC parameters, VerifyContext

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T007 [P] Implement VerifySeverity, VerifyLevel, VerifyModule enum classes and VerifyResult struct in `include/dfx/dstore_verify_report.h`
- [X] T008 [P] Implement VerifyReport class in `include/dfx/dstore_verify_report.h` and `src/dfx/dstore_verify_report.cpp` — AddResult(), HasError(), GetErrorCount(), GetRetStatus(), FormatText(), FormatJson(), DISALLOW_COPY_AND_MOVE
- [X] T009 Implement PageVerifyFunc typedef, PageVerifyEntry struct, and PageVerifyRegistry class in `include/dfx/dstore_page_verify.h` — Register(), Verify(), IsRegistered(), std::array-based dispatch
- [X] T010 Implement PageVerifyRegistry in `src/dfx/dstore_page_verify.cpp` — generic header validation (CRC via CheckPageCrcMatch, lower/upper bounds, LSN sanity, page type, special region offset) before dispatching to registered type-specific functions
- [X] T011 Implement VerifyPageInline() and VerifyPage() free functions in `src/dfx/dstore_page_verify.cpp` — GUC level/module check, dispatch to registry
- [X] T012 [P] Implement GUC parameters `dfx_verify_level` (OFF/LIGHTWEIGHT/HEAVYWEIGHT) and `dfx_verify_module` (HEAP/INDEX/ALL) as global atomic variables with getter/setter functions in `include/dfx/dstore_page_verify.h` and `src/dfx/dstore_page_verify.cpp`
- [X] T013 [P] Implement VerifyContext class in `include/dfx/dstore_verify_context.h` and `src/dfx/dstore_verify_context.cpp` — holds VerifyReport*, SnapshotData*, sampleRatio, isOnline, visitedPages (std::unordered_set<uint64>), maxErrors
- [X] T014 Write unit tests for VerifyReport in `tests/unittest/ut_dfx/ut_verify_report.cpp` — test AddResult, HasError, GetRetStatus, FormatText, FormatJson
- [X] T015 Write unit tests for PageVerifyRegistry in `tests/unittest/ut_dfx/ut_page_verify_registry.cpp` — test Register, Verify dispatch, unregistered type handling, GUC level filtering

**Checkpoint**: Foundation ready — Registry framework compiles and passes basic tests. User story implementation can now begin.

---

## Phase 3: User Story 1 — Single Page Integrity Verification (Priority: P1) 🎯 MVP

**Goal**: Implement lightweight and heavyweight verification functions for all 17 PageTypes, register them in the framework, and embed inline verification in write paths.

**Independent Test**: Load individual pages of each type and run both lightweight and heavyweight verification; verify pass for valid pages and failure detection for corrupted pages.

### Implementation for User Story 1

**Heap module verifiers:**

- [X] T016 [P] [US1] Implement VerifyHeapPageLightweight() in `src/heap/dstore_heap_page_verify.cpp` — heap-specific header checks beyond generic (potentialDelSize bounds, fsmIndex validity)
- [X] T017 [P] [US1] Implement VerifyHeapPageHeavyweight() in `src/heap/dstore_heap_page_verify.cpp` — ItemId datalen vs tuple length, ItemId state invariants (UNUSED: len=0; NORMAL: len>0 offset valid; NO_STORAGE: tdId/tdStatus/tupLiveMode valid; UNREADABLE_RANGE_HOLDER: len>0), no overlapping items, TD count in [MIN_TD_COUNT, MAX_TD_COUNT], TD state validity (3 states), TD CSN-status consistency, TD-tuple cross-reference
- [X] T018 [P] [US1] Register HeapPage verifier via RegisterHeapPageVerifier() in `src/heap/dstore_heap_page_verify.cpp`

**Index module verifiers:**

- [X] T019 [P] [US1] Implement VerifyIndexPageLightweight() in `src/index/dstore_index_page_verify.cpp` — index-specific header checks (special region offset/size for BtrPageLinkAndStatus)
- [X] T020 [P] [US1] Implement VerifyIndexPageHeavyweight() in `src/index/dstore_index_page_verify.cpp` — BtrPageLinkAndStatus validation (page type, level, split status, sibling link format), high key at offset 1 >= all other keys, intra-page key ordering, key type consistency with BtrMeta
- [X] T021 [P] [US1] Register IndexPage verifier via RegisterIndexPageVerifier() in `src/index/dstore_index_page_verify.cpp`

**FSM module verifiers:**

- [X] T022 [P] [US1] Implement lightweight and heavyweight verifiers for FSM_PAGE_TYPE and FSM_META_PAGE_TYPE in `src/page/dstore_fsm_page_verify.cpp` — heavyweight: FSM entry validity, tree structure consistency, listRange ordering, numTotalPages >= numUsedPages
- [X] T023 [P] [US1] Register FSM and FSM_META verifiers in `src/page/dstore_fsm_page_verify.cpp`

**Undo module verifiers:**

- [X] T024 [P] [US1] Implement lightweight and heavyweight verifiers for UNDO_PAGE_TYPE in `src/undo/dstore_undo_page_verify.cpp` — heavyweight: UndoRecordPageHeader prev/next chain format, undo record header validity (UndoType not UNKNOWN, valid ctid, file version)
- [X] T025 [P] [US1] Implement lightweight and heavyweight verifiers for TRANSACTION_SLOT_PAGE in `src/undo/dstore_undo_page_verify.cpp` — heavyweight: slot status validity (7 valid states), CSN-status consistency, nextFreeLogicSlotId bounds
- [X] T026 [P] [US1] Register Undo and TransactionSlot verifiers in `src/undo/dstore_undo_page_verify.cpp`

**Segment meta verifiers:**

- [X] T027 [P] [US1] Implement lightweight and heavyweight verifiers for DATA_SEGMENT_META_PAGE_TYPE, HEAP_SEGMENT_META_PAGE_TYPE, UNDO_SEGMENT_META_PAGE_TYPE in `src/dfx/dstore_segment_page_verify.cpp` — heavyweight: segment type validity, extent chain head pointer format, totalBlockCount > 0, dataFirst/dataLast format, numFsms bounds (for heap)
- [X] T028 [P] [US1] Register all 3 segment meta verifiers in `src/dfx/dstore_segment_page_verify.cpp`

**Tablespace module verifiers:**

- [X] T029 [P] [US1] Implement lightweight and heavyweight verifiers for TBS_EXTENT_META_PAGE_TYPE in `src/tablespace/dstore_tbs_page_verify.cpp` — heavyweight: magic == EXTENT_META_MAGIC, extSize in valid set {8,128,1024,8192}, nextExtMetaPageId format
- [X] T030 [P] [US1] Implement lightweight and heavyweight verifiers for TBS_BITMAP_PAGE_TYPE and TBS_BITMAP_META_PAGE_TYPE in `src/tablespace/dstore_tbs_page_verify.cpp` — heavyweight: allocatedExtentCount == popcount(bitmap), groupCount bounds, bitmapPagesPerGroup consistency
- [X] T031 [P] [US1] Implement lightweight and heavyweight verifiers for TBS_FILE_META_PAGE_TYPE and TBS_SPACE_META_PAGE_TYPE in `src/tablespace/dstore_tbs_page_verify.cpp`
- [X] T032 [P] [US1] Register all 5 tablespace-related verifiers in `src/tablespace/dstore_tbs_page_verify.cpp`

**Btree recycle verifiers:**

- [X] T033 [P] [US1] Implement lightweight and heavyweight verifiers for BTR_QUEUE_PAGE_TYPE, BTR_RECYCLE_PARTITION_META_PAGE_TYPE, BTR_RECYCLE_ROOT_META_PAGE_TYPE in `src/index/dstore_btr_recycle_page_verify.cpp`
- [X] T034 [P] [US1] Register all 3 btree recycle verifiers in `src/index/dstore_btr_recycle_page_verify.cpp`

**InitPageVerifiers and inline integration:**

- [X] T035 [US1] Implement InitPageVerifiers() in `src/dfx/dstore_page_verify.cpp` — call all 17 Register*Verifier() functions; call from StorageInstance initialization path
- [X] T036 [US1] Embed VerifyPageInline() calls at page write/flush sites — dirty page flush path, and key CRUD page modification paths in `src/buffer/`, `src/heap/`, `src/index/` (add after page modification, before marking dirty or writing out)

**Tests:**

- [X] T037 [P] [US1] Write unit tests for heap page verification (valid page, corrupted ItemId, TD mismatch, ItemId state violations) in `tests/unittest/ut_dfx/ut_heap_page_verify.cpp`
- [X] T038 [P] [US1] Write unit tests for index page verification (valid page, high key violation, unsorted keys, bad special region) in `tests/unittest/ut_dfx/ut_index_page_verify.cpp`
- [X] T039 [P] [US1] Write unit tests for all-zero page handling (uninitialized page should pass lightweight, not be flagged as corruption) in `tests/unittest/ut_dfx/ut_page_verify_registry.cpp`

**Checkpoint**: All 17 PageTypes have registered lightweight + heavyweight verifiers. Inline verification active on write path when GUC enabled. Single page verification is fully functional and independently testable.

---

## Phase 4: User Story 2 — Heap Data and Tuple Verification (Priority: P2)

**Goal**: Implement cross-page heap data verification including tuple format validation, big tuple chunk chain integrity, and FSM-heap consistency.

**Independent Test**: Scan a heap segment and verify all tuple formats, traverse big tuple chains, and compare FSM records with actual page free space.

### Implementation for User Story 2

- [X] T040 [P] [US2] Define HeapVerifyOptions struct and HeapSegmentVerifier class interface in `include/dfx/dstore_heap_verify.h`
- [X] T041 [US2] Implement HeapSegmentVerifier core: sequential scan of all heap pages in segment, per-tuple format validation (size vs column count, null bitmap length, tuple does not overflow allocated space) in `src/dfx/dstore_heap_verify.cpp`
- [X] T042 [US2] Implement big tuple chain verification in HeapSegmentVerifier: traverse linked tuple chunks following CTIDs, validate m_linkInfo progression (FIRST_CHUNK → NOT_FIRST_CHUNK), verify chunk count matches header, detect broken chains in `src/dfx/dstore_heap_verify.cpp`
- [X] T043 [US2] Implement FSM-heap consistency check in HeapSegmentVerifier: for each heap page, compare actual free space (from page header upper-lower) with FSM-recorded free space category, report significant discrepancies in `src/dfx/dstore_heap_verify.cpp`
- [X] T044 [US2] Implement visibility handling in HeapSegmentVerifier: when isOnline=true, use SNAPSHOT_MVCC to skip in-progress/aborted tuples, log skipped tuples at INFO level in `src/dfx/dstore_heap_verify.cpp`
- [X] T045 [US2] Expose VerifyHeapSegment() public function in `src/dfx/dstore_heap_verify.cpp`
- [X] T046 [P] [US2] Write unit tests for heap segment verification (valid segment, corrupted tuple, broken big tuple chain, FSM mismatch) in `tests/unittest/ut_dfx/ut_heap_segment_verify.cpp`

**Checkpoint**: Heap segment verification fully functional — tuple format, big tuple chains, FSM consistency all tested independently.

---

## Phase 5: User Story 3 — B-tree Structure and Index-Heap Consistency (Priority: P2)

**Goal**: Implement B-tree structural verification (levels, sibling links, key ordering across pages, parent-child consistency) and index-heap cross-reference (1:1 correspondence + data value matching with sampling).

**Independent Test**: Traverse a B-tree index from root to leaves, verify all structural invariants, and cross-reference leaf entries against heap tuples with configurable sampling.

### Implementation for User Story 3

- [X] T047 [P] [US3] Define BtreeVerifyOptions struct and BtreeVerifier class interface in `include/dfx/dstore_btree_verify.h`
- [X] T048 [US3] Implement BtreeVerifier level-by-level traversal: from root page, descend to each level, walk sibling links at each level (referencing PostgreSQL amcheck's bt_check_every_level → bt_check_level_from_leftmost pattern) in `src/dfx/dstore_btree_verify.cpp`
- [X] T049 [US3] Implement per-page structural checks in BtreeVerifier: validate BtrPageLinkAndStatus, level consistency, bidirectional sibling link agreement (page.next.prev == page), split status handling in `src/dfx/dstore_btree_verify.cpp`
- [X] T050 [US3] Implement high key and intra-page key ordering check: high key at offset 1 >= all keys, tuples sorted within page in `src/dfx/dstore_btree_verify.cpp`
- [X] T051 [US3] Implement same-level cross-page key ordering: validate keys are monotonically ordered across sibling pages at the same level, no violation at page boundaries in `src/dfx/dstore_btree_verify.cpp`
- [X] T052 [US3] Implement parent-child key consistency: for each internal page tuple, verify that the downlink child page's first tuple (or high key) matches the parent's key in `src/dfx/dstore_btree_verify.cpp`
- [X] T053 [US3] Implement index-heap 1:1 correspondence check: for each leaf entry, verify heap tuple exists at referenced ItemPointer; optionally reverse-check that visible heap tuples have index entries in `src/dfx/dstore_btree_verify.cpp`
- [X] T054 [US3] Implement index-heap data value consistency with sampling: when checkDataConsistency=true, randomly select pages at configured sampleRatio, for selected pages compare all index key values against heap tuple column values in `src/dfx/dstore_btree_verify.cpp`
- [X] T055 [US3] Implement online visibility handling: when isOnline=true, use SNAPSHOT_MVCC, skip in-progress tuples from cross-reference mismatch detection in `src/dfx/dstore_btree_verify.cpp`
- [X] T056 [US3] Implement cycle detection in sibling link traversal using VerifyContext.visitedPages in `src/dfx/dstore_btree_verify.cpp`
- [X] T057 [US3] Expose VerifyBtreeIndex() public function in `src/dfx/dstore_btree_verify.cpp`
- [X] T058 [P] [US3] Write unit tests for btree verification (valid tree, broken sibling link, key ordering violation, high key violation, parent-child mismatch, index-heap mismatch) in `tests/unittest/ut_dfx/ut_btree_verify.cpp`

**Checkpoint**: B-tree verification fully functional — structural checks, cross-page key ordering, index-heap 1:1 and data consistency all tested independently.

---

## Phase 6: User Story 4 — Segment and Extent Verification (Priority: P3)

**Goal**: Verify segment metadata integrity, extent chain consistency, extent-bitmap allocation, and index leaf page count accuracy.

**Independent Test**: Load a segment meta page, walk its extent chain, verify metadata, cross-check with tablespace bitmap, and count leaf pages.

### Implementation for User Story 4

- [X] T059 [P] [US4] Define SegmentVerifyOptions struct and SegmentVerifier class interface in `include/dfx/dstore_segment_verify.h`
- [X] T060 [US4] Implement SegmentVerifier extent chain walk: follow nextExtMetaPageId links from segment meta, validate magic number, extent size per dynamic sizing rules, chain terminates (no broken or circular links via visitedPages) in `src/dfx/dstore_segment_verify.cpp`
- [X] T061 [US4] Implement segment metadata validation: magic number, segment type (HEAP/INDEX/UNDO/TEMP), totalBlockCount matches sum of extent sizes, dataFirst/dataLast consistency with actual data in `src/dfx/dstore_segment_verify.cpp`
- [X] T062 [US4] Implement extent-bitmap consistency check: for each extent in segment chain, verify corresponding bit is set in TbsBitmapPage, verify no overlap between different segments' extents, verify allocatedExtentCount matches actual bits in `src/dfx/dstore_segment_verify.cpp`
- [X] T063 [US4] Implement index segment leaf page count verification: traverse level-0 sibling links and count actual leaf pages, compare with expected count from metadata in `src/dfx/dstore_segment_verify.cpp`
- [X] T064 [US4] Expose VerifySegment() public function in `src/dfx/dstore_segment_verify.cpp`
- [X] T065 [P] [US4] Write unit tests for segment verification (valid segment, broken extent chain, circular chain detection, bitmap mismatch, leaf page count mismatch) in `tests/unittest/ut_dfx/ut_segment_verify.cpp`

**Checkpoint**: Segment verification fully functional — extent chain, metadata, bitmap consistency, and leaf page count all tested independently.

---

## Phase 7: User Story 5 — InnoDB Metadata Consistency Verification (Priority: P3)

**Goal**: Define metadata input struct interface and implement dstore-side verification of upper-layer metadata against actual segment data.

**Independent Test**: Construct MetadataInputStruct with known values and verify that dstore's interface correctly validates segment existence, type, and schema consistency.

### Implementation for User Story 5

- [X] T066 [P] [US5] Define MetadataInputStruct, IndexMetaEntry structs and MetadataVerifier interface in `include/dfx/dstore_metadata_verify.h`
- [X] T067 [US5] Implement MetadataVerifier: validate heap segment exists and type is HEAP, validate LOB segment exists (if non-INVALID) and type correct, validate tablespace ID consistency in `src/dfx/dstore_metadata_verify.cpp`
- [X] T068 [US5] Implement index metadata verification in MetadataVerifier: for each IndexMetaEntry, verify index segment exists, type is INDEX, BtrMeta.nkeyAtts matches nKeyAtts, BtrMeta.attTypeIds matches provided attTypeIds in `src/dfx/dstore_metadata_verify.cpp`
- [X] T069 [US5] Expose VerifyMetadataConsistency() public function in `src/dfx/dstore_metadata_verify.cpp`
- [X] T070 [P] [US5] Write unit tests for metadata verification (valid metadata, missing segment, wrong segment type, attribute mismatch) in `tests/unittest/ut_dfx/ut_metadata_verify.cpp`

**Checkpoint**: Metadata consistency verification fully functional — dstore-side interface ready for upper layer integration.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Table-level aggregation, CLI tool, and integration

- [X] T071 Implement VerifyTable() aggregation function in `src/dfx/dstore_page_verify.cpp` — orchestrate single page + heap segment + btree + segment + metadata verification via TableVerifyOptions
- [X] T072 Implement CLI tool main entry in `tools/dstore_verify/dstore_verify_main.cpp` — argument parsing (--table, --segment, --page, --level, --check-btree, --check-heap, --check-segment, --check-extent, --sample-ratio, --max-errors, --format, --all), offline page reader (direct file read/mmap bypassing buffer manager), dispatch to appropriate verify functions, output VerifyReport in text/json format, exit codes (0/1/2/3)
- [X] T073 Verify build: ensure `make` compiles all new sources, links dfx library, builds dstore_verify tool, runs all ut_dfx tests successfully
- [X] T074 Run full test suite: execute all ut_dfx tests, verify no regressions in existing ut_* tests

### Deferred CLI TODOs

- [ ] T075 Implement offline relation/table resolution for `dstore_verify --table <oid>` — reconstruct `StorageRelation`/index metadata from datadir so table-level verification can run without a live dstore instance
- [ ] T076 Implement offline full-table aggregation for `dstore_verify --all` — enumerate verifiable tables/segments from datadir instead of reporting unsupported for relation-dependent paths
- [ ] T077 Implement offline `--check-btree` and `--check-heap` relation-driven flows — support these options without requiring online relation/cache/catalog services
- [ ] T078 Add CLI coverage tests and documentation updates for offline unsupported vs supported paths — verify exit codes, output, and error messages for `--table/--all/--check-btree/--check-heap`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Phase 2 — BLOCKS Phase 8 (CLI tool needs verifiers registered)
- **US2 (Phase 4)**: Depends on Phase 2 — can run in parallel with US1/US3
- **US3 (Phase 5)**: Depends on Phase 2 — can run in parallel with US1/US2
- **US4 (Phase 6)**: Depends on Phase 2 — can run in parallel with US1/US2/US3
- **US5 (Phase 7)**: Depends on Phase 2 — can run in parallel with all other user stories
- **Polish (Phase 8)**: Depends on Phase 3 (US1) minimum; benefits from all phases complete

### User Story Dependencies

- **US1 (P1)**: Foundation only — no dependency on other stories. **MVP scope.**
- **US2 (P2)**: Foundation only — independent of US1 (uses VerifyContext but not single-page framework)
- **US3 (P2)**: Foundation only — independent of US1/US2
- **US4 (P3)**: Foundation only — independent of all other stories
- **US5 (P3)**: Foundation only — independent of all other stories

### Within Each User Story

- Headers/interfaces before implementation
- Core logic before edge case handling
- Implementation before tests
- Public function exposure as final implementation step

### Parallel Opportunities

Within Phase 3 (US1): T016-T034 are all [P] — each page type verifier can be implemented independently in parallel
Within Phase 2: T007, T008, T012, T013 are [P]
Across phases: US2, US3, US4, US5 can all start in parallel once Phase 2 completes

---

## Parallel Example: User Story 1 (Single Page Verification)

```text
# All page type verifiers can be implemented in parallel:
T016+T017+T018: Heap page verifiers
T019+T020+T021: Index page verifiers
T022+T023:      FSM page verifiers
T024+T025+T026: Undo page verifiers
T027+T028:      Segment meta verifiers
T029+T030+T031+T032: Tablespace verifiers
T033+T034:      Btree recycle verifiers

# Then sequential:
T035: InitPageVerifiers (needs all registers)
T036: Inline integration (needs InitPageVerifiers)
T037+T038+T039: Tests (parallel, after implementation)
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T006)
2. Complete Phase 2: Foundational (T007-T015)
3. Complete Phase 3: User Story 1 (T016-T039)
4. **STOP and VALIDATE**: All 17 page types verified, inline write-path verification works
5. This alone provides significant value: proactive corruption detection at write time

### Incremental Delivery

1. Setup + Foundational → Framework ready
2. Add US1 → Single page verification (MVP!) — proactive write-path detection
3. Add US2 → Heap data deep verification — tuple/chunk/FSM checks
4. Add US3 → B-tree structural + index-heap consistency — query correctness guarantee
5. Add US4 → Segment/extent verification — storage integrity
6. Add US5 → Metadata interface — cross-system consistency
7. Add CLI tool (Phase 8) → Offline verification capability

### Parallel Team Strategy

With multiple developers after Phase 2 completes:

- Developer A: US1 (single page framework + all 17 verifiers)
- Developer B: US3 (btree verifier — most complex)
- Developer C: US2 (heap segment verifier) + US4 (segment verifier)
- Developer D: US5 (metadata verifier) + Phase 8 (CLI tool)

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Commit after each task or logical group
- All new code follows C++14 best practices: enum class, const correctness, RAII, DISALLOW_COPY_AND_MOVE
- File naming convention: `dstore_<module>_verify.cpp` / `dstore_<module>_page_verify.cpp`
- Namespace: all code in `namespace DSTORE { ... }`
- Include guards: `#ifndef DSTORE_<MODULE>_VERIFY_H` style
