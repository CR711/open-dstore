# Build Reference

## Prerequisites

On macOS, all build commands must run inside a Docker container:
```bash
docker build -t dstore:latest .                                                            # build image (first time)
docker run -d --name dstore-dev -v $(pwd):/opt/project/dstore dstore:latest tail -f /dev/null  # start in background
docker exec -it dstore-dev bash                                                            # enter container anytime
```

On WSL/Linux, build directly. Then load the build environment:
```bash
source buildenv    # Load environment (BUILD_ROOT, LOCAL_LIB_PATH, GCC paths, LD_LIBRARY_PATH)
```

## build.sh Options

```
./build.sh [OPTIONS]
  -m|--compile_mode   debug | release | memcheck | coverage  (default: release)
  -st|--sys_tools     on: system GCC, off: bundled GCC 7.3   (default: off)
  -co|--cmake_opt     Additional CMake flags
  -vb|--verbose       Verbose compilation output
  -tm|--test_mode     ut | fuzz | perf | tpcc | lcov
  clean               Remove build directories
```

## Common Workflows

### Full Debug Build with Tests

```bash
source buildenv
cd utils && bash build.sh -m debug && cd ..
bash build.sh -m debug -tm ut
cd tmp_build && make run_dstore_ut_all
```

### Release Build

```bash
source buildenv
cd utils && bash build.sh -m release && cd ..
bash build.sh -m release
```

### Incremental Rebuild

```bash
cd tmp_build && make -j$(nproc) install
```

### Run Specific Test Suite

```bash
cd tmp_build
make run_dstore_buffer_unittest          # Buffer
make run_dstore_xact_unittest            # Transaction (level0)
make run_dstore_index_unittest           # Index (level0)
make run_dstore_lock_unittest            # Lock
make run_dstore_ha_unittest              # WAL/HA
make run_dstore_framework_unittest       # Framework
make run_dstore_datamanager_unittest     # Heap, tablespace, catalog
make run_dstore_undo_unittest            # Undo
make run_dstore_ut_all                   # All
make run_dstore_ut_asan                  # All with AddressSanitizer
```

### Run with gtest Filter

```bash
cd tests
bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ../utils/output -g "UTBtree*.*"
```

### Code Coverage

```bash
source buildenv
cd utils && bash build.sh -m release -tm lcov && cd ..
bash build.sh -m release -tm lcov
cd tmp_build && make run_dstore_ut_all
```

## Docker Build

```bash
# Full build (alias in Docker container)
dstore-build

# Incremental rebuild
dstore-rebuild
```

## CMake Flags

### Common Flags

| Flag | Values | Description |
|---|---|---|
| `CMAKE_BUILD_TYPE` | debug, release, coverage | Build mode |
| `ENABLE_UT` | ON/OFF | Enable unit tests |
| `ENABLE_TEST` | ON/OFF | Enable test directory (default ON) |
| `UTILS_PATH` | path | Path to pre-built utils output |
| `LOCAL_LIB_PATH` | path | Path to external libraries |

### Testing Flags

| Flag | Description |
|---|---|
| `ENABLE_FUZZ` | Fuzz testing (requires ENABLE_UT) |
| `ENABLE_LCOV` | lcov code coverage |
| `ENABLE_TPCC` | TPCC benchmark tests |
| `ENABLE_PERF` | Performance tests |

### Debug Flags

| Flag | Description |
|---|---|
| `ENABLE_MEMORY_CHECK` | AddressSanitizer |
| `ENABLE_TSAN_CHECK` | ThreadSanitizer |
| `USE_ASSERT_CHECKING` | Enable cassert |
| `ENABLE_FAULT_INJECTION` | Fault injection (auto with UT) |

### Environment Variable Overrides

```bash
GS_BUFFERPOOL_DEBUG=1       # Buffer pool debug mode
GS_LOCK_DEBUG=1             # Lock debugging
GS_BUFFERPOOL_SYNC_LOCK=1   # Sync lock mode
DEBUG_TYPE=debug|release     # Override build type
```

## Build Artifacts

| Artifact | Path |
|---|---|
| Utils library | `utils/output/lib/libgsutils.so` |
| Shared library | `output/lib/libdstore.so` |
| Static library | `output/lib/libdstore.a` |
| Tools | `output/bin/` (pagedump, waldump, etc.) |

## CI Pipeline

GitHub Actions on PR to main:
1. Build utils (release)
2. Build dstore (release)
3. Verify: `libgsutils.so`, `libdstore.so`, `libdstore.a`
