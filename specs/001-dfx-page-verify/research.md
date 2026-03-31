# Research: DFX Page Verification

## R1: Single Page Verification Framework Architecture

**Decision**: 采用 **Registry + enum-indexed dispatch** 模式。定义一个全局 `PageVerifyRegistry`，内部用 `std::array<VerifyEntry, MAX_PAGE_TYPE>` 按 PageType 枚举值索引。各模块（heap、index、fsm 等）在初始化阶段调用 `RegisterPageVerifier()` 注册自己的轻量级和重量级校验函数。

**Rationale**:
- 参考 PostgreSQL amcheck 的模块化设计：每种 access method 有独立的 verify 模块，共享基础设施
- `std::array` 索引比 `std::unordered_map` 更高效，适合写入路径上的轻量级校验（O(1) 无哈希开销）
- Registry 模式使各页面模块解耦，新增 PageType 只需注册，不修改框架代码
- 与现有代码中 callback registration 模式（如 `RegisterCallback`、`RegisterCrmmRouteInfoCallback` 等）风格一致

**Alternatives considered**:
- 虚函数 + 继承体系（每种 page type 一个 Verifier 子类）：过于重量级，17 个子类膨胀严重，且单页面校验不需要状态保持
- `switch/case` 分发：简单但不可扩展，新增类型需修改中心代码
- `std::function` 回调表：相比函数指针有额外开销（堆分配），写入路径不合适

## R2: Cross-Page Verification Architecture

**Decision**: 采用 **Orchestrator + Context** 模式，参考 PostgreSQL amcheck 的 `BtreeCheckState` 设计。每种跨页面校验场景有一个 Orchestrator 类（如 `BtreeVerifier`、`HeapSegmentVerifier`），持有 `VerifyContext` 管理遍历状态、错误收集、采样配置和可见性快照。

**Rationale**:
- PostgreSQL amcheck 的核心设计：`bt_check_every_level()` → `bt_check_level_from_leftmost()` → `bt_target_page_check()`，层次化遍历 + 状态对象管理
- 跨页面校验天然需要状态（已访问页面集合、遍历位置、累积错误、采样计数器），适合用对象封装
- 不同校验场景（btree 结构、index-heap 一致性、segment extent chain、FSM-heap 一致性）差异大，各自独立的 Orchestrator 比统一框架更清晰
- 共享基础设施（错误报告、page 读取、可见性判断）通过 `VerifyContext` 注入

**Alternatives considered**:
- Visitor 模式（遍历所有页面，每个 visitor 做不同检查）：不适合，因为不同校验需要不同的遍历顺序（btree 需层次遍历、segment 需 chain walk、index-heap 需双向交叉）
- 单一 TableVerifier 类：职责过重，难以独立测试和复用
- 纯函数式（无状态函数 + 参数传递）：状态太多，参数列表会爆炸

## R3: Error Reporting Design

**Decision**: 采用 **结构化诊断报告** 模式，参考 PostgreSQL 的 message/detail/hint 三层报告结构。定义 `VerifyReport` 类收集所有校验结果，每条结果为 `VerifyResult` 结构体（severity + target + check_name + expected + actual + message）。

**Rationale**:
- PostgreSQL amcheck 使用 ERROR/WARNING/DEBUG 分层 + primary message + detail + hint
- MySQL CHECK TABLE 使用表格式结果（Table/Op/Msg_type/Msg_text）
- 结构化输出支持：在线模式通过日志输出，CLI 模式可格式化为可读文本或 JSON
- 与现有 `ErrLog()` 基础设施兼容，同时支持更丰富的诊断信息

**Alternatives considered**:
- 仅使用现有 `ErrLog()`/`StorageAssert()`：信息不够结构化，无法支持 CLI 工具的格式化输出
- 异常机制：项目不使用异常（error code 模式），且校验需要继续运行不中断

## R4: Configuration via GUC Parameters

**Decision**: 定义两个 GUC 参数：`dfx_verify_level`（OFF/LIGHTWEIGHT/HEAVYWEIGHT）和 `dfx_verify_module`（HEAP/INDEX/ALL）。运行时动态生效。轻量级校验在 inline 路径上根据 GUC 值决定是否执行。

**Rationale**:
- 参考 PostgreSQL 的 lock-mode 分级思路，但简化为 GUC 参数（更直接）
- MySQL CHECK TABLE 的 QUICK/MEDIUM/EXTENDED 三级也是同样的渐进深度思路
- 运行时动态可调适合生产环境：平时 OFF 或 LIGHTWEIGHT，排查问题时提升到 HEAVYWEIGHT

**Alternatives considered**:
- 编译期宏开关（`#ifdef ENABLE_VERIFY`）：无法运行时调整
- 每次调用传参控制：inline 调用点太多，改动面过大

## R5: Online Verification Visibility Handling

**Decision**: Online 模式下使用 `SNAPSHOT_MVCC` 快照判断 tuple 可见性，仅对 committed & visible 的 tuple 进行交叉校验。in-progress/aborted tuple 跳过但记录为 INFO 级别。

**Rationale**:
- PostgreSQL amcheck 的 heap verification 使用 snapshot 判断 xmin/xmax 可见性
- 在线校验必须容忍并发修改，MVCC 快照是最自然的隔离方式
- 现有代码中 `SnapshotData` 已有 `SNAPSHOT_MVCC` 支持

**Alternatives considered**:
- `SNAPSHOT_NOW`（只看已提交）：可能在校验过程中看到不一致的中间状态
- `SNAPSHOT_DIRTY`（看所有）：会把 in-progress 的正常中间状态误报为不一致

## R6: Sampling Strategy for Index-Heap Data Verification

**Decision**: 按比例随机采样，使用 page-level 采样（随机选取一定比例的 leaf page，对选中页面做全量 tuple 校验）。比例通过参数控制（1%~100%）。

**Rationale**:
- Page-level 采样比 tuple-level 采样 IO 效率更高（读一个 page 就全量校验该 page 的所有 tuple）
- PostgreSQL amcheck 的 `heapallindexed` 选项使用 Bloom filter 做全量但概率性校验，思路类似
- 可配比例提供灵活性：快速抽检用低比例，确定性验证用 100%

**Alternatives considered**:
- Tuple-level 随机采样：IO 随机读放大严重
- Bloom filter 方式（如 PostgreSQL）：实现复杂度高，且需要额外内存（maintenance_work_mem），首版不需要

## R7: C++ Best Practices Improvements

**Decision**: 在不大规模重构现有代码的前提下，新代码采用以下 C++14 best practices：

| 方面 | 现有项目风格 | 新代码改进 |
|------|-------------|-----------|
| RAII 资源管理 | 手动 Palloc/Pfree | 新模块内部使用 RAII wrapper，与外部接口处做适配 |
| 类型安全枚举 | 部分使用 `enum class` | 全部使用 `enum class`，不使用裸 enum |
| const 正确性 | 部分 | 全面使用 `const`/`constexpr` |
| 初始化 | 混合风格 | 统一使用 braced initialization |
| 返回值 | `RetStatus` | 保持 `RetStatus`，但校验结果用专门的 `VerifyResult` 枚举 |
| 可见性 | 有时忘记 | 新类明确 `public`/`private`/`protected` |

**Rationale**:
- 用户明确要求"对于原项目风格不太好的地方参考 C++11/17 best practice"
- 保持与现有代码的互操作性（接口层面兼容），内部实现提升质量
- 不引入 C++17 特性（项目是 C++14），但可以用 C++14 范围内的 best practice
