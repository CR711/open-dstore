# DFX 页面校验测试策略

**版本**: 1.0 | **日期**: 2026-04-10 | **状态**: Phase 1-4 已覆盖，Phase 5-8 待实现

## 1. 测试目标

- 新增代码所有行和分支（if/else/switch/error handling）100% UT 覆盖
- 合法页面在任何 VerifyLevel 下零误报
- 对 `src/dfx/`、`include/dfx/`、`src/*/\*verify\*`、`src/buffer/dstore_buf_mgr*.cpp` 的修改必须通过全部 DFX UT

## 2. 测试分层

| 层级 | 目的 | 对应 Suite |
|---|---|---|
| **L1 单页校验** | 17 种 PageType x LIGHT/MEDIUM/HEAVY x 正向/反向 | UTHeapPageVerify, UTIndexPageVerify, UTUndoPageVerify, UTSegmentPageVerify, UTTbsBtrRecycleVerify |
| **L2 段级校验** | 跨页遍历、Big Tuple 链、环检测、FSM 一致性 | UTHeapSegmentVerify |
| **L3 框架层** | Registry 注册/分发、GUC 动态切换、Report 格式化、恢复阶段覆盖、并发安全 | UTPageVerifyRegistry, UTVerifyReport, UTPostRedoVerify |
| **L4 攻防对抗** | 故障注入、Fuzz 随机攻击、多重错误叠加、IO 故障模拟、NONE 旁路 | UTFaultInjectVerify, UTAttackDefenseVerify, UTPostRedoVerify |

## 3. 测试矩阵

### 3.1 单页校验（已实现）

| PageType | 模块 | LIGHT | MEDIUM | HEAVY | 正向 | 反向 |
|---|---|:---:|:---:|:---:|:---:|:---:|
| HEAP_PAGE_TYPE | Heap | Y | Y | Y | Y | Y |
| INDEX_PAGE_TYPE | Index | Y | Y | Y | Y | Y |
| TRANSACTION_SLOT_PAGE | Undo | Y | Y | Y | Y | Y |
| UNDO_PAGE_TYPE | Undo | Y | Y | Y | Y | Y |
| FSM_PAGE_TYPE / FSM_META | FSM | Y | Y | Y | Y | Y |
| DATA/HEAP/UNDO_SEGMENT_META | Segment | Y | Y | Y | Y | Y |
| TBS_EXTENT/BITMAP/BITMAP_META/FILE_META/SPACE_META | Tablespace | Y | Y | Y | Y | Y |
| BTR_QUEUE/RECYCLE_PARTITION/RECYCLE_ROOT | BtrRecycle | Y | Y | Y | Y | Y |

### 3.2 跨页校验（待实现）

| 校验项 | US | 正向 | 反向 | 关键边界 |
|---|---|:---:|:---:|---|
| B-tree 层次遍历 + sibling link | US3 | - | - | SPLIT_INCOMPLETE、单节点树 |
| 跨页 key 排序 + parent-child | US3 | - | - | 页边界 key 相等 |
| Index-Heap 1:1 + 数据采样 | US3 | - | - | 采样率 0%/100%、在线可见性 |
| Extent 链遍历 + 环检测 | US4 | - | - | 循环链、空 segment |
| Extent-bitmap 一致性 | US4 | - | - | extent 重叠 |
| Metadata segment/schema 校验 | US5 | - | - | 缺失 segment、LOB=INVALID |

## 4. 测试工具

| 工具 | 用途 | 强制规则 |
|---|---|---|
| `PageBuffer` | 8 字节对齐页面缓冲区，`.fill(0)` 初始化后 `reinterpret_cast` 为页面结构 | 所有页面构造必须使用 |
| `ScopedVerifyConfig` | RAII 守卫，构造时保存 GUC level/modules，析构时恢复 | 修改 GUC 的测试必须使用，禁止手动 save/restore |
| `HasVerifyCode(report, code)` | 在 VerifyReport 中查找特定 VerifyCode | 禁止手动遍历 `GetResults()` |
| `EnableAllModules()` / `RestoreDefaultModules()` | 快速切换模块位掩码 | - |

## 5. 现有 Test Suite 清单（283 tests / 11 suites）

| Suite | 文件 | 数量 | 覆盖范围 |
|---|---|---:|---|
| UTPageVerifyRegistry | `ut_page_verify_registry.cpp` | 68 | Registry、GUC、并发（含 concurrent barrier）、恢复阶段、all-zero 页、CrExtend |
| UTHeapPageVerify | `ut_heap_page_verify.cpp` | 43 | Heap LIGHT/MEDIUM/HEAVY、TD、ItemId 四状态、tuple 重叠 |
| UTIndexPageVerify | `ut_index_page_verify.cpp` | 31 | Index 三级校验、high key、key 排序（含跨页排序）、Meta 页、sibling link、对齐校验 |
| UTUndoPageVerify | `ut_undo_page_verify.cpp` | 22 | Undo record/txn slot 页面、7 种 slot status、CSN 一致性 |
| UTSegmentPageVerify | `ut_segment_page_verify.cpp` | 23 | DATA/HEAP/UNDO Segment Meta、magic、type、extent |
| UTTbsBtrRecycleVerify | `ut_tbs_btr_recycle_verify.cpp` | 22 | Tablespace 5 种 + BtrRecycle 3 种页面 |
| UTVerifyReport | `ut_verify_report.cpp` | 11 | Report 格式化、severity、JSON 转义 |
| UTHeapSegmentVerify | `ut_heap_segment_verify.cpp` | 17 | 段级扫描、Big Tuple 链、环检测、FSM、error limit |
| UTFaultInjectVerify | `ut_fault_inject_verify.cpp` | 17 | 定向故障注入、三场景一致性、多重故障、NONE 旁路 |
| UTAttackDefenseVerify | `ut_attack_defense_verify.cpp` | 12 | Fuzz 随机攻击/bit 翻转、多重故障叠加 |
| UTPostRedoVerify | `ut_post_redo_verify.cpp` | 17 | Redo 后校验、IO 故障模拟、Buffer 模拟、并发竞态 |

## 6. 并发测试策略

| 场景 | 方法 | 状态 |
|---|---|---|
| 多线程注册 | N 线程并发 `Register()` 不同 PageType | 已覆盖 |
| GUC 动态切换 | Writer 线程切换 level + Reader 线程执行校验，1000+ 次 | 已覆盖 |
| 并发校验 | 每线程独立 VerifyReport，验证无交叉污染 | 已覆盖 |
| B-tree 并发 split | 校验时模拟 SPLIT_INCOMPLETE 瞬态 | Phase 5 待实现 |
| Segment 并发分配 | 校验时模拟 extent 分配/回收 | Phase 6 待实现 |

## 7. 回归门禁

```bash
# Docker 容器内：
source buildenv && cd tmp_build && make -j$(nproc) install
bin/unittest --gtest_filter='UTPageVerifyRegistry*:UTHeapPageVerify*:UTIndexPageVerify*:UTHeapSegmentVerify*:UTVerifyReport*:UTUndoPageVerify*:UTSegmentPageVerify*:UTTbsBtrRecycleVerify*:UTFaultInjectVerify*:UTAttackDefenseVerify*:UTPostRedoVerify*'
```

**判定**: 283/283 通过 = PASS，任何失败 = 阻断合入，不允许 SKIP。

**触发路径**: `src/dfx/`、`include/dfx/`、`src/heap/*verify*`、`src/index/*verify*`、`src/undo/*verify*`、`src/page/*verify*`、`src/tablespace/*verify*`、`src/buffer/dstore_buf_mgr*.cpp`、`tests/unittest/ut_dfx/`

## 8. 已知限制与改进方向

| 限制 | 改进方向 |
|---|---|
| 无参数化测试，同类用例存在代码重复 | 引入 `INSTANTIATE_TEST_SUITE_P` 减少 30%+ 代码 |
| 每个 suite 独立初始化页面构造 | 抽取 `DfxTestFixture` 基类 |
| 段级 Mock 仅支持函数指针回调 | Phase 5-8 引入 `MockBufferManager` |
| 无性能基准 | 添加 LIGHT 校验微基准（目标 <1us/page） |
| 无覆盖率自动化 | CI 集成 gcov/lcov，阻断覆盖率下降 |
