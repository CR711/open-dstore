# dstore - Database Storage Engine

C++14 storage engine with CMake build system, gtest testing. Requires GCC 7.3 and specific dependencies.

## Build

On macOS, build inside a Docker container. On WSL/Linux, build directly.

```bash
# macOS: start container in background (first time only)
docker build -t dstore:latest .
docker run -d --name dstore-dev -v $(pwd):/opt/project/dstore dstore:latest tail -f /dev/null

# enter container (can be run anytime, exit without stopping the container)
docker exec -it dstore-dev bash
```

```bash
source buildenv                                    # load build environment
cd utils && bash build.sh -m debug && cd ..        # build utils
bash build.sh -m debug -tm ut                      # build dstore with UT enabled
cd tmp_build && make run_dstore_ut_all             # run all unit tests
```

Incremental rebuild: `cd tmp_build && make -j$(nproc) install`

## Project Structure

```
src/         C++ implementation (by subsystem)
include/     Internal headers (mirrors src/ layout)
interface/   Public API headers
tests/       Unit tests (gtest) and TPCC benchmarks
utils/       Utility library (libgsutils.so)
tools/       Diagnostic tools (pagedump, waldump, etc.)
```

## Commit Message Format

Enforced by `.githooks/commit-msg`. Required format:

```
Description: <summary>
TicketNo: <ticket>          (optional)
Module: <module name>       (required)
```

Valid modules: Transaction State Manager, Centralized Lock Manager, Distributed Lock Manager, Heap Manager, Index Manager, Centralized Buffer Manager, Distributed Buffer Manager, Segment-page Storage Manager, XLog Manager, Undo Manager, Column Data Manager, Column Buffer Manager, SCM Cache Manager, Catalog Table Manager, SQL Engine, Tenant Resource Scheduler, CI

## Module Mapping (path -> Module)

| Path prefix | Module |
|---|---|
| `src/transaction/`, `src/common/snapshot/` | Transaction State Manager |
| `src/lock/` | Centralized Lock Manager |
| `src/heap/` | Heap Manager |
| `src/index/` | Index Manager |
| `src/buffer/` | Centralized Buffer Manager |
| `src/page/`, `src/tablespace/`, `src/fsm/` | Segment-page Storage Manager |
| `src/wal/` | XLog Manager |
| `src/undo/`, `src/flashback/` | Undo Manager |
| `src/catalog/`, `src/systable/` | Catalog Table Manager |
| `src/framework/`, `src/config/`, `src/port/` | CI |
| `src/common/` (general) | CI |
| `utils/`, `tools/`, `cmake/`, `scripts/`, `.github/` | CI |
| `tests/` | Same as the module being tested |

Full mapping: `.claude/docs/module-mapping.md`

## Code Conventions

- **Namespace**: all code in `namespace DSTORE { }`
- **Naming**: PascalCase classes/methods, `m_` prefix + camelCase members, UPPER_SNAKE macros/enums
- **Files**: `dstore_` prefix, snake_case, `.h`/`.cpp`
- **Include guards**: `#ifndef DSTORE_<SUBSYSTEM>_<FILE>_H` (not `#pragma once`)
- **Indentation**: 4 spaces, no tabs
- **Braces**: K&R for control flow; Allman for function definitions
- **Pointers**: `Type *var` (star near variable)
- **Error handling**: `RetStatus` return codes, no exceptions
- **Memory**: custom `DstoreNew(ctx)` allocator, no smart pointers
- **Comments**: prefer `/* */` style
- **Copy control**: `DISALLOW_COPY_AND_MOVE(ClassName)` on major classes

Full style guide: `.claude/docs/coding-style.md`

## Key Patterns

- Initialize/Destroy pattern instead of constructor/destructor logic
- RAII guards: `AutoMemCxtSwitch`, `AutoPdbCxtSwitch`
- Status checks: `STORAGE_FUNC_SUCC(ret)`, `STORAGE_FUNC_FAIL(ret)`
- Assertions: `StorageAssert(cond)`
- Tracing: `storage_trace_entry()` / `storage_trace_exit()`

## Mandatory UT Gates (合入前必须通过)

Any code change touching the following modules **MUST** pass the corresponding DFX page verification unit tests before merging. This is a hard gate — no exceptions.

### DFX Page Verify UT (311 tests, 12 test suites)

```bash
# In Docker container, after build:
source buildenv && cd tmp_build && make -j$(nproc) install

# Run all DFX UT (MUST all pass):
bin/unittest --gtest_filter='UTPageVerifyRegistry*:UTHeapPageVerify*:UTIndexPageVerify*:UTHeapSegmentVerify*:UTVerifyReport*:UTUndoPageVerify*:UTSegmentPageVerify*:UTTbsBtrRecycleVerify*:UTFaultInjectVerify*:UTAttackDefenseVerify*:UTPostRedoVerify*:UTBtreeVerify*'
```

| Test Suite | File | Coverage |
|---|---|---|
| UTPageVerifyRegistry | `tests/unittest/ut_dfx/ut_page_verify_registry.cpp` | Registry, 3 scenarios (Read/Write/Full), GUC, concurrency |
| UTHeapPageVerify | `tests/unittest/ut_dfx/ut_heap_page_verify.cpp` | Heap page LIGHT/MEDIUM/HEAVY, tuple overlap, TD sanity, micro-benchmark |
| UTIndexPageVerify | `tests/unittest/ut_dfx/ut_index_page_verify.cpp` | Index page LIGHT/MEDIUM/HEAVY, meta page, sibling, key ordering |
| UTHeapSegmentVerify | `tests/unittest/ut_dfx/ut_heap_segment_verify.cpp` | Segment-level verify, big tuple chain, cycle detection |
| UTVerifyReport | `tests/unittest/ut_dfx/ut_verify_report.cpp` | Report formatting, severity tracking, JSON escape |
| UTUndoPageVerify | `tests/unittest/ut_dfx/ut_undo_page_verify.cpp` | Undo record/txn slot page verification |
| UTSegmentPageVerify | `tests/unittest/ut_dfx/ut_segment_page_verify.cpp` | Segment meta/extent/bitmap page verification |
| UTTbsBtrRecycleVerify | `tests/unittest/ut_dfx/ut_tbs_btr_recycle_verify.cpp` | Tablespace and B-tree recycle page verification |
| UTFaultInjectVerify | `tests/unittest/ut_dfx/ut_fault_inject_verify.cpp` | Fault injection: 17 corruption scenarios |
| UTAttackDefenseVerify | `tests/unittest/ut_dfx/ut_attack_defense_verify.cpp` | Attack/defense: malicious page construction |
| UTPostRedoVerify | `tests/unittest/ut_dfx/ut_post_redo_verify.cpp` | Post-redo verification integration |
| UTBtreeVerify | `tests/unittest/ut_dfx/ut_btree_verify.cpp` | B-tree cross-page structure, sibling links, key ordering, parent-child, Index-Heap consistency |

**Affected paths** (changes to these files require DFX UT to pass):
- `src/dfx/`, `include/dfx/` — page verify framework
- `src/heap/*verify*`, `src/index/*verify*`, `src/undo/*verify*` — module verifiers
- `src/page/*verify*`, `src/tablespace/*verify*` — segment/tablespace verifiers
- `src/buffer/dstore_buf_mgr*.cpp` — buffer integration (read/write path verify calls)
- `include/dfx/dstore_verify_report.h` — report infrastructure

### Shared Test Utilities

Test utility header: `tests/unittest/ut_dfx/ut_dfx_test_utils.h`
- `PageBuffer` — aligned page buffer
- `ScopedVerifyConfig` — RAII guard for GUC verify level/modules (MUST use instead of manual save/restore)
- `HasVerifyCode()` — check VerifyCode in report (MUST use instead of manual for-loop)

## Reference Docs

Detailed references for agent on-demand loading:
- `.claude/docs/module-mapping.md` - full path-to-module mapping with test targets
- `.claude/docs/coding-style.md` - complete coding conventions and best practices
- `.claude/docs/build-reference.md` - all build options, CMake flags, test commands
