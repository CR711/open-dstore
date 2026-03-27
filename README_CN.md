**Dstore是一个可独立编译，独立测试的数据库存储引擎组件。**

> **中文** | [English](README.md)

---

# 环境配置与编译

## Docker（推荐）

项目提供了一个 `Dockerfile`，可在 Ubuntu 20.04 镜像中构建完整工具链（GCC 7.3、lz4、cjson、gtest、securec、mockcpp）。部署脚本将本地源码目录挂载到容器中，在宿主机上的修改可立即在容器内生效。

### 前置条件

- 已安装并运行 Docker
- 约 5 GB 可用磁盘空间（首次构建需从源码编译 GCC）

### 1. 构建镜像并启动容器

```bash
bash docker_deploy.sh           # 首次运行：构建镜像并启动容器
bash docker_deploy.sh --build   # 强制重新构建镜像
bash docker_deploy.sh --shell   # 启动容器并进入 shell
bash docker_deploy.sh --stop    # 停止并删除容器
```

源码目录将以绑定挂载的方式映射到容器内的 `/opt/project/dstore` (写自己的目录)。

### 2. 进入容器

```bash
docker exec -it dstore-dev bash
```

### 3. 完整构建（容器内）

```bash
# /root/.bashrc 中已预配置 dstore-build 别名：
dstore-build

# 或手动执行以下步骤：
source ${BUILD_ROOT}/buildenv
cd ${BUILD_ROOT}/utils && bash build.sh -m debug
cd ${BUILD_ROOT}
mkdir -p tmp_build && cd tmp_build
cmake .. -DCMAKE_BUILD_TYPE=debug -DUTILS_PATH=../utils/output -DENABLE_UT=ON && make -sj$(($(nproc)-2)) install
```

构建成功后的产物：
- `utils/output/lib/libgsutils.so` — 工具库
- `output/lib/libdstore.so` 和 `output/lib/libdstore.a` — 存储引擎


---

## IDE 集成（VS Code / CLion）

`.devcontainer/devcontainer.json` 已为 VS Code Remote Containers 和 JetBrains Gateway 配置好，可自动挂载源码并连接到 `dstore:latest` 镜像。

在 VS Code 中打开项目目录，选择 **Reopen in Container** 即可。

---

## 手动配置（不使用 Docker）

如果希望在宿主机上直接构建，请按照以下步骤安装依赖并更新 `buildenv`。
建议使用Cladue等AI辅助工具来配置环境

### 1. 环境配置

如下所示为编译 Dstore 所需依赖库及建议版本：

```
XXX/
├── dstore/ -------------------- # 项目入口
└── local_libs/ ---------------- # 依赖库目录
    ├── buildtools/ ------------ # 编译工具
    │   └── gcc7.3/ ------------ # 建议版本：v7.3
    │       ├── gcc/
    ├── secure/ ---------------- # 建议版本：v3.0.9
    │   ├── include/
    │   └── lib/
    ├── lz4/ ------------------- # 建议版本：v1.10.0
    │   ├── include/
    │   └── lib/
    ├── cjson/ ----------------- # 建议版本：v1.7.17
    │   ├── include/
    │   └── lib/
    ├── gtest/ ----------------- # 建议版本：v1.10.0
    │   ├── include/
    │   └── lib/
    └── mockcpp/ --------------- # 建议版本：master
        ├── include/
        └── lib/
```

#### 1.1 配置依赖库

强烈建议按照上述指定版本下载并编译各依赖库，以避免版本不一致引发的兼容性问题！

##### secure
**下载地址**：https://gitcode.com/opengauss/openGauss-third_party/tree/master/platform/Huawei_Secure_C
**编译命令**：`./build.sh -m all`

##### cjson
**下载地址**：https://gitcode.com/opengauss/openGauss-third_party/tree/master/dependency/cJSON
**编译命令**：`./build.sh -m all`

##### lz4
**下载地址**：https://github.com/lz4/lz4/releases
**编译命令**：`make -j$(nproc) && make install PREFIX=xxx/output`

##### gtest
**下载地址**：https://github.com/google/googletest/releases（建议 v1.10.0）
**编译命令**：
```
mkdir build && cd build
cmake -DCMAKE_INSTALL_PREFIX=xxx/gtest -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-D_GLIBCXX_USE_CXX11_ABI=0" -DCMAKE_CXX_COMPILER=g++ -DCMAKE_C_COMPILER=gcc ..
make -j$(nproc) && make install
```

> **注意**：
> - **x86_64**：必须添加 `-D_GLIBCXX_USE_CXX11_ABI=0`，且使用 `Release` 模式（不要用 Debug，否则库名带 `d` 后缀会导致链接失败）。
> - **aarch64**：使用 `Debug` 模式（`-DCMAKE_BUILD_TYPE=Debug`），**不要**添加 `-D_GLIBCXX_USE_CXX11_ABI=0`。

##### mockcpp
**下载地址**：https://github.com/sinojelly/mockcpp
**编译命令**：
```
mkdir build && cd build
cmake .. -DMOCKCPP_XUNIT=gtest -DMOCKCPP_XUNIT_HOME=xxx/gtest -DCMAKE_INSTALL_PREFIX=xxx/mockcpp -DCMAKE_CXX_FLAGS="-D_GLIBCXX_USE_CXX11_ABI=0"
make -j$(nproc) && make install
```

> **注意**：
> - **x86_64**：需要 `-D_GLIBCXX_USE_CXX11_ABI=0`，与 gtest 保持一致。
> - **aarch64**：**不要**添加该参数。

> **提示**：上述编译参数可能因操作系统、GCC 版本和 binutils 版本不同而需要调整。如果遇到链接错误，请检查 ABI 是否一致（`nm libgtest.a | grep EqFailure` 中是否含 `cxx11`）。

依赖库编译完成后，请按库名称调整目录结构。例如，`local_libs/secure` 下应包含对应的 `include` 和 `lib` 子目录。

#### 1.2 更新并加载环境配置文件

修改 `dstore/buildenv` 中 `BUILD_ROOT` 的值为当前项目路径，例如：`BUILD_ROOT=/opt/project/dstore`

```bash
source dstore/buildenv
```

### 2. 编译

#### 2.1 编译前置 utils 模块

```bash
cd dstore/utils
bash build.sh -m release   # 或：debug
```

确认 `utils/output/lib/` 路径下有 `libgsutils.so` 生成。

#### 2.2 编译 dstore

```bash
cd dstore
bash build.sh -m release   
# 或debug：
mkdir -p tmp_build && cd tmp_build
cmake .. -DCMAKE_BUILD_TYPE=debug -DUTILS_PATH=../utils/output -DENABLE_UT=ON && make -sj$(($(nproc)-2)) install
```

编译成功后，`dstore/output/lib/` 路径下应有 `libdstore.so` 与 `libdstore.a` 生成。


---

# 测试

## 1. 单元测试

### 1.1 运行测试

所有测试目标均在 `tmp_build/` 目录下执行：

```bash
cd ${BUILD_ROOT}/tmp_build
```

| 目标 | 覆盖范围 |
|---|---|
| `make run_dstore_ut_all` | 所有单元测试 |
| `make run_dstore_buffer_unittest` | 缓冲池、页面写入器、脏页队列 |
| `make run_dstore_xact_unittest` | 事务、CSN、Undo（level0）|
| `make run_dstore_index_unittest` | B-tree 索引（level0）|
| `make run_dstore_lock_unittest` | 锁管理器（level0）|
| `make run_dstore_ha_unittest` | WAL、副本、备份恢复、故障恢复 |
| `make run_dstore_framework_unittest` | 内存上下文、公共算法、任务管理器 |
| `make run_dstore_datamanager_unittest` | Heap、表空间、控制文件、VFS、系统表 |

示例：

```bash
make run_dstore_buffer_unittest
```

### 1.2 使用 AddressSanitizer（ASan）运行

```bash
make run_dstore_ut_asan
```

---

## 2. TPCC 测试

### 2.1 编译

```bash
cd dstore
bash build.sh -m release
```

### 2.2 运行 tpcctest

```bash
cd ${BUILD_ROOT}/tmp_build
make run_dstore_tpcctest
```
