# Module Mapping Reference

## Path to Module Mapping

### Transaction State Manager
- `src/transaction/`, `include/transaction/`, `interface/transaction/`
- `src/common/snapshot/`
- Test target: `make run_dstore_xact_unittest`

### Centralized Lock Manager
- `src/lock/`, `include/lock/`, `interface/lock/`
- Test target: `make run_dstore_lock_unittest`

### Distributed Lock Manager
- Distributed lock traces in `src/lock/` (DistributedTableLockMgr, DistributedLockMgr)
- Currently partial implementation

### Heap Manager
- `src/heap/`, `include/heap/`, `interface/heap/`
- Test target: `make run_dstore_datamanager_unittest`

### Index Manager
- `src/index/`, `include/index/`, `interface/index/`
- Test target: `make run_dstore_index_unittest` (level0), `make run_dstore_all_index_unittest` (all)

### Centralized Buffer Manager
- `src/buffer/`, `include/buffer/`, `interface/buffer/`
- Test target: `make run_dstore_buffer_unittest`

### Distributed Buffer Manager
- Anti-cache implementations in `src/buffer/`
- Currently partial implementation

### Segment-page Storage Manager
- `src/page/`, `include/page/`, `interface/page/`
- `src/tablespace/`, `include/tablespace/`, `interface/tablespace/`
- `src/fsm/`, `include/fsm/`
- Test target: `make run_dstore_datamanager_unittest`

### XLog Manager
- `src/wal/`, `include/wal/`, `interface/log/`
- Test target: `make run_dstore_ha_unittest`

### Undo Manager
- `src/undo/`, `include/undo/`
- `src/flashback/`, `include/flashback/`, `interface/flashback/`
- Test target: `make run_dstore_undo_unittest`

### Column Data Manager / Column Buffer Manager / SCM Cache Manager
- Not yet implemented (placeholder modules)

### Catalog Table Manager
- `src/catalog/`, `include/catalog/`, `interface/catalog/`
- `src/systable/`, `include/systable/`, `interface/systable/`
- Test target: `make run_dstore_datamanager_unittest`

### SQL Engine
- External to this repository
- `src/framework/dstore_parallel.cpp` provides parallel execution support

### Tenant Resource Scheduler
- `utils/utils/src/schedule/`
- Distributed multi-tenant scheduling (partial)

### CI
- `cmake/`, `scripts/`, `build_script/`
- `.github/`, `.githooks/`
- `tools/` (pagedump, waldump, htablookup, buflookup, whatiserrcode, concurrency_test)
- `utils/` (libgsutils, VFS)
- `src/framework/`, `src/config/`, `src/port/`
- `src/common/` (when changes are general-purpose: algorithm, datatype, error, memory, concurrent, log)
- Test target: `make run_dstore_framework_unittest`

## Test Target Quick Reference

| Target | Modules covered |
|---|---|
| `run_dstore_ut_all` | All modules |
| `run_dstore_buffer_unittest` | Buffer Manager |
| `run_dstore_xact_unittest` | Transaction (level0) |
| `run_dstore_all_xact_unittest` | Transaction (all levels) |
| `run_dstore_index_unittest` | Index (level0) |
| `run_dstore_all_index_unittest` | Index (all levels) |
| `run_dstore_lock_unittest` | Lock Manager |
| `run_dstore_ha_unittest` | WAL, replication, backup/restore |
| `run_dstore_framework_unittest` | Framework, memory, algorithms |
| `run_dstore_datamanager_unittest` | Heap, tablespace, control, VFS, catalog |
| `run_dstore_undo_unittest` | Undo |
| `run_dstore_tpcctest` | TPCC integration |

## Architecture Notes

- **StorageInstance**: central singleton holding BufMgr, LockMgr (shared), plus per-PDB managers
- **Per-PDB managers**: TransactionMgr, UndoMgr, WalManager, ObjSpaceMgr, CheckpointMgr
- **Pluggable Database (PDB)**: multi-database support, each PDB has isolated managers
