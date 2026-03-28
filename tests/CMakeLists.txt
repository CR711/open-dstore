if (ENABLE_UT STREQUAL "ON")
    add_subdirectory(unittest)
endif()
if (ENABLE_TPCC STREQUAL "ON" OR DSTORE_TEST_TOOL STREQUAL "ON")
    add_subdirectory(tpcctest)
    add_subdirectory(sysbenchtest)
endif()

add_subdirectory(utilities)

if (ENABLE_UT STREQUAL "ON" OR DSTORE_TEST_TOOL STREQUAL "ON")
    unset(UT CACHE)
    unset(DSTORE_TEST_TOOL CACHE)
endif()

#--------add ut test cmake command start--------
add_custom_target(run_dstore_ut_all
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r true
    COMMENT "Running all dstore unit tests"
)

add_custom_target(run_dstore_ut_asan
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r true -a ON
    COMMENT "Running all dstore unit tests"
)

set(DISTRIBUTED_EXCLUDE "-*MultiNode*:-*Distributed*:-*MultiCluster*:-*Distribute*:-UTDistributed*")

set(ut_catalog_filter "RelmapTest.*:SysTableTest.*level0:UTSysTable.*level0:UTRelation.*level0")
set(ut_controlfile_filter "ControlFileTest.*level0:UTControlFileMgrTest.*")
set(ut_pdb_filter "PdbTest.*level0")
set(ut_tablespace_filter "*Segment*.*level0:FreeSpaceMapTest.*level0:ObjSpaceMgrTest.*level0:*TableSpace*.*level0:BitmapTest.*level0")
set(ut_vfs_filter "Vfs*.*")
set(ut_heap_filter "*Heap*.*level0")
set(ut_heap_exclude_filter "-UTHeap.HeapLockBigTupleTest_level0")
add_custom_target(run_dstore_datamanager_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false
     -g ${ut_heap_filter}":"${ut_controlfile_filter}":"${ut_tablespace_filter}":"${ut_pdb_filter}":"${ut_vfs_filter}":"${ut_catalog_filter}":"${ut_heap_exclude_filter}":"${DISTRIBUTED_EXCLUDE}
    COMMENT "Running dstore unit tests: heap, tablespace, control file, pdb, vfs, catalog"
)

set(ut_index_filter "*Btree*.*level0")
set(ut_undo_filter "*Undo*.*level0")
add_custom_target(run_dstore_index_and_undo_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false
     -g ${ut_index_filter}":"${ut_undo_filter}
    COMMENT "Running dstore unit tests: btree, undo"
)

set(ut_lock_filter "*Lock*.*level0:-UTBufferLockFree.*level0")
set(ut_trans_filter "*Csn*.*level0:*Transaction*.*level0:SavepointTest.*level0")
add_custom_target(run_dstore_xact_and_lock_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false
     -g ${ut_trans_filter}":"${ut_scale_filter}":"${ut_lock_filter}
    COMMENT "Running dstore unit tests: lock, scale in&out, transaction"
)

# In order to achieve the goal of access ut time, the ut is broken into an average number,
# so the actual UT run may have more content than the name.
set(ut_index_level0_filter "*Btree*.*level0")
add_custom_target(run_dstore_index_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false -g ${ut_index_level0_filter}":"${DISTRIBUTED_EXCLUDE}
    COMMENT "Running dstore unit tests level0: btree"
)

set(ut_index_filter "*Btree*.*")
add_custom_target(run_dstore_all_index_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false -g ${ut_index_filter}
    COMMENT "Running dstore unit tests: btree"
)

set(ut_memctx_filter "MctxTest.*")
set(ut_threads_task_filter "MultiThreadTaskMgrTest.*")
set(ut_common_filter "ConsistentHashTest.*:UTCpuAutoBinder.*:UTDynaHash.*:Compress*.*")
add_custom_target(run_dstore_framework_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false 
     -g ${ut_scale_filter}":"${ut_threads_task_filter}":"${ut_memctx_filter}":"${ut_common_filter}":"${DISTRIBUTED_EXCLUDE}
    COMMENT "Running dstore unit tests level0: scale in&out, memnode, memctx, and other framework tests"
)
# after sync to master and other dev branch, use run_dstore_framework_unittest instead of this
add_custom_target(run_dstore_undo_unittest 
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
    && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false 
    -g ${ut_scale_filter}":"${ut_threads_task_filter}":"${ut_memctx_filter}":"${ut_common_filter}":"${DISTRIBUTED_EXCLUDE}
    COMMENT "Alias of run_dstore_framework_unittest level0, we should delete this after code synced."
)

set(ut_trans_level0_filter "*Csn*.*level0:*Transaction*.*level0:SavepointTest.*level0")
set(ut_undo_level0_filter "*Undo*.*level0")
add_custom_target(run_dstore_xact_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false 
     -g ${ut_trans_level0_filter}":"${ut_undo_level0_filter}":"${DISTRIBUTED_EXCLUDE}
    COMMENT "Running dstore unit tests level0: transaction undo tests"
)

set(ut_trans_filter "*Csn*.*:*Transaction*.*:SavepointTest.*")
set(ut_undo_filter "*Undo*.*")
add_custom_target(run_dstore_all_xact_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false
     -g ${ut_trans_filter}":"${ut_undo_filter}
    COMMENT "Running dstore unit tests: transaction and undo tests"
)

set(ut_lock_filter "*Lock*.*level0")
add_custom_target(run_dstore_lock_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false -g ${ut_lock_filter}":-"${ut_lock_exlude_filter}":"${DISTRIBUTED_EXCLUDE}
    COMMENT "Running dstore unit tests level0: lock tests"
)

set(ut_buffer_filter1 "*Buffer*.*:DynamicHashTest.*:CandidateListTest.*:*BgPageWriterTest.*:DirtyPageQueueTest.*")
set(ut_buffer_filter2 "HotListTest.*:LruListTest.*:BufLruMultiThreadTest.*:BufLruTest.*")
set(ut_buffer_exclude_filter "-UTMultiNodeBufferPool.*")
add_custom_target(run_dstore_buffer_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
    && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false
    -g ${ut_buffer_filter1}":"${ut_buffer_filter2}":"${ut_buffer_exclude_filter}":"${DISTRIBUTED_EXCLUDE}
    COMMENT "Running dstore unit tests: buffer"
)

set(ut_wal_filter "WAL*.*:Wal*.*")
set(ut_pdb_replica_filter "DISABLEPdbReplica*.*:UTPdbReplicaSinglePdbTest.*")
set(ut_logical_replica_filter "LOGICAL*.*:DECODE*.*")
set(ut_backup_restore_filter "*BackupRestore.*")
set(ut_recorvery_filter "MultiThreadTaskMgrTest.*")
add_custom_target(run_dstore_ha_unittest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_ut.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r false
     -g ${ut_wal_filter}":"${ut_pdb_replica_filter}":"${ut_logical_replica_filter}":"${ut_backup_restore_filter}":"${ut_recorvery_filter}":"${DISTRIBUTED_EXCLUDE}
    COMMENT "Running dstore unit tests: wal, pdb replica, logical replica, backup restore, recovery"
)
#--------add ut test cmake command end--------


#--------add tpcctest cmake command start--------
add_custom_target(run_dstore_tpcctest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_tpcctest.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r true
    COMMENT "run dstore one-node tpcctest"
)

add_custom_target(run_dstore_tpcctest_asan
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && bash build_and_run_tpcctest.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r true -a ON
    COMMENT "run dstore one-node tpcctest"
)
#--------add tpcctest cmake command end--------

#--------add sysbenchtest cmake command start--------
add_custom_target(run_dstore_sysbenchtest
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && sh build_and_run_sysbenchtest.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r true
    COMMENT "run dstore sysbench simulation test"
)

add_custom_target(run_dstore_sysbenchtest_asan
    COMMAND cd ${CMAKE_SOURCE_DIR}/tests
     && sh build_and_run_sysbenchtest.sh -t ${LOCAL_LIB_PATH} -u ${UTILS_HOME} -r true -a ON
    COMMENT "run dstore sysbench simulation test with asan"
)
#--------add sysbenchtest cmake command end--------