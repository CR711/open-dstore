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

## Reference Docs

Detailed references for agent on-demand loading:
- `.claude/docs/module-mapping.md` - full path-to-module mapping with test targets
- `.claude/docs/coding-style.md` - complete coding conventions and best practices
- `.claude/docs/build-reference.md` - all build options, CMake flags, test commands
