**Dstore is a self-contained, independently compilable and testable database storage engine component.**

> [中文](README_CN.md) | **English**

---

# Environment & Build

## Docker (Recommended)

The repository ships a `Dockerfile` that builds the complete toolchain (GCC 7.3, lz4, cjson, gtest, securec, mockcpp) inside an Ubuntu 20.04 image. The deploy script mounts your local source tree into the container so edits on the host are reflected immediately.

### Prerequisites

- Docker installed and running
- ~5 GB free disk space (image build compiles GCC from source)

### 1. Build the image and start the container

```bash
bash docker_deploy.sh           # build image (first run) then start container
bash docker_deploy.sh --build   # force rebuild the image
bash docker_deploy.sh --shell   # start container and open a shell
bash docker_deploy.sh --stop    # stop and remove the container
```

Your source directory is bind-mounted at `/opt/project/dstore` inside the container.

### 2. Enter the container

```bash
docker exec -it dstore-dev bash
```

### 3. Full build (inside the container)

```bash
# The dstore-build alias is pre-configured in /root/.bashrc:
dstore-build

# Or run the steps manually:
source ${BUILD_ROOT}/buildenv
cd ${BUILD_ROOT}/utils && bash build.sh -m debug
cd ${BUILD_ROOT}
mkdir -p tmp_build && cd tmp_build
cmake .. -DCMAKE_BUILD_TYPE=debug -DUTILS_PATH=../utils/output -DENABLE_UT=ON && make -sj$(($(nproc)-2)) install
```

After a successful build:
- `utils/output/lib/libgsutils.so` — utility library
- `output/lib/libdstore.so` and `output/lib/libdstore.a` — storage engine

### 4. Incremental rebuild (inside the container)

After the first full build, use the `dstore-rebuild` alias for fast incremental compilation:

```bash
dstore-rebuild
# equivalent to: cd ${BUILD_ROOT}/tmp_build && make install -j$(nproc)
```

---

## IDE Integration (VS Code / CLion)

The `.devcontainer/devcontainer.json` configures VS Code Remote Containers and JetBrains Gateway to attach to the `dstore:latest` image with the source mounted automatically.

Open the repository folder in VS Code → **Reopen in Container**.

---

## Manual Setup (without Docker)

If you prefer a native build, install the dependencies below and update `buildenv`.

### 1. Environment Configuration

The following dependencies and recommended versions are required:

```
XXX/
├── dstore/ -------------------- # project root
└── local_libs/ ---------------- # dependency directory
    ├── buildtools/ ------------ # build tools
    │   └── gcc7.3/ ------------ # recommended: v7.3
    │       ├── gcc/
    ├── secure/ ---------------- # recommended: v3.0.9
    │   ├── include/
    │   └── lib/
    ├── lz4/ ------------------- # recommended: v1.10.0
    │   ├── include/
    │   └── lib/
    ├── cjson/ ----------------- # recommended: v1.7.17
    │   ├── include/
    │   └── lib/
    ├── gtest/ ----------------- # recommended: v1.10.0
    │   ├── include/
    │   └── lib/
    └── mockcpp/ --------------- # recommended: master
        ├── include/
        └── lib/
```

#### 1.1 Configure dependencies

Download and compile each dependency at the specified version to avoid compatibility issues.

##### secure
**Download**: https://gitcode.com/opengauss/openGauss-third_party/tree/master/platform/Huawei_Secure_C
**Build**: `./build.sh -m all`

##### cjson
**Download**: https://gitcode.com/opengauss/openGauss-third_party/tree/master/dependency/cJSON
**Build**: `./build.sh -m all`

##### lz4
**Download**: https://github.com/lz4/lz4/releases
**Build**: `make -j$(nproc) && make install PREFIX=xxx/output`

##### gtest
**Download**: https://github.com/google/googletest/releases (recommended: v1.10.0)
**Build**:
```
mkdir build && cd build
cmake -DCMAKE_INSTALL_PREFIX=xxx/gtest -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-D_GLIBCXX_USE_CXX11_ABI=0" -DCMAKE_CXX_COMPILER=g++ -DCMAKE_C_COMPILER=gcc ..
make -j$(nproc) && make install
```

> **Note**:
> - **x86_64**: `-D_GLIBCXX_USE_CXX11_ABI=0` is required. Use `Release` mode (Debug produces `d`-suffixed library names that cause link failures).
> - **aarch64**: Use `Debug` mode (`-DCMAKE_BUILD_TYPE=Debug`). Do **not** add `-D_GLIBCXX_USE_CXX11_ABI=0`.


##### mockcpp
**Download**: https://github.com/sinojelly/mockcpp
**Build**:
```
mkdir build && cd build
cmake .. -DMOCKCPP_XUNIT=gtest -DMOCKCPP_XUNIT_HOME=xxx/gtest -DCMAKE_INSTALL_PREFIX=xxx/mockcpp -DCMAKE_CXX_FLAGS="-D_GLIBCXX_USE_CXX11_ABI=0"
make -j$(nproc) && make install
```

> **Note**:
> - **x86_64**: `-D_GLIBCXX_USE_CXX11_ABI=0` is required, matching gtest.
> - **aarch64**: Do **not** add this flag.

> **Tip**: The build flags above may need adjustment depending on your OS, GCC version, and binutils version. If you encounter link errors, check ABI consistency (`nm libgtest.a | grep EqFailure` — look for `cxx11` in the symbol names).

After building, organize each library's output under its named directory. For example, `local_libs/secure` should contain `include/` and `lib/` subdirectories.

#### 1.2 Update and load the environment file

Set `BUILD_ROOT` in `dstore/buildenv` to the project path on your machine, e.g. `BUILD_ROOT=/opt/project/dstore`

```bash
source dstore/buildenv
```

### 2. Build

#### 2.1 Build the utils module first

```bash
cd dstore/utils
bash build.sh -m release   # or: debug
```

Verify that `libgsutils.so` is present under `utils/output/lib/`.

#### 2.2 Build dstore

```bash
cd dstore
bash build.sh -m release
# or debug：
mkdir -p tmp_build && cd tmp_build
cmake .. -DCMAKE_BUILD_TYPE=debug -DUTILS_PATH=../utils/output -DENABLE_UT=ON && make -sj$(($(nproc)-2)) install
```

After a successful build, `libdstore.so` and `libdstore.a` will be present under `dstore/output/lib/`.

#### 2.3 Incremental rebuild

After the first full build, just run incremental compilation in `tmp_build/`:

```bash
cd dstore/tmp_build
make -sj$(($(nproc)-2)) install
```

---

# Test

## 1. Unit Tests

### 1.1 Run tests

All targets are invoked from the `tmp_build/` directory:

```bash
cd ${BUILD_ROOT}/tmp_build
```

| Target | Coverage |
|---|---|
| `make run_dstore_ut_all` | All unit tests |
| `make run_dstore_buffer_unittest` | Buffer pool, page writer, dirty page queue |
| `make run_dstore_xact_unittest` | Transactions, CSN, undo (level0) |
| `make run_dstore_index_unittest` | B-tree index (level0) |
| `make run_dstore_lock_unittest` | Lock manager (level0) |
| `make run_dstore_ha_unittest` | WAL, replication, backup/restore, recovery |
| `make run_dstore_framework_unittest` | Memory context, common algorithms, task manager |
| `make run_dstore_datamanager_unittest` | Heap, tablespace, control file, VFS, catalog |

Example:

```bash
make run_dstore_buffer_unittest
```

### 1.2 Run with AddressSanitizer (ASan)

```bash
make run_dstore_ut_asan
```

---

## 2. TPCC Test

### 2.1 Build

```bash
cd dstore
bash build.sh -m release
```

### 2.2 Run tpcctest

```bash
cd ${BUILD_ROOT}/tmp_build
make run_dstore_tpcctest
```
