/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 *
 * ---------------------------------------------------------------------------------------
 *
 * dstore_pdb.cpp
 *
 * IDENTIFICATION
 *        src/framework/dstore_pdb.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <pthread.h>
#include <csignal>
#include "common/log/dstore_log.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_vfs_adapter.h"
#include "transaction/dstore_csn_mgr.h"
#include "transaction/dstore_transaction_mgr.h"
#include "logical_replication/dstore_logical_replication_mgr.h"
#include "undo/dstore_undo_mgr.h"
#include "wal/dstore_wal.h"
#include "wal/dstore_wal_write_context.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "lock/dstore_lock_interface.h"
#include "port/dstore_port.h"
#include "buffer/dstore_bg_page_writer_mgr.h"
#include "buffer/dstore_bg_disk_page_writer.h"
#include "buffer/dstore_checkpointer.h"
#include "systable/dstore_relation.h"
#include "systable/dstore_systable_interface.h"
#include "systable/dstore_systable_wal.h"
#include "systable/sys_database.h"
#include "systable/dstore_systable_utility.h"
#include "heap/dstore_heap_interface.h"
#include "table/dstore_table_interface.h"
#include "systable/systable_bootstrap_buildin_data.h"
#include "pdb/dstore_pdb_interface.h"
#include "common/datatype/dstore_uuid_utils.h"
#include "control/dstore_control_file.h"
#include "index/dstore_btree_page_recycle.h"
#include "framework/dstore_pdb.h"
namespace DSTORE {

/*
 * If the OID in the initDb phase is greater than or equal to 16384, the OID is reallocated from 10000.
 * If the OID in the non-initDb phase is greater than the maximum value of uint32, the OID is reallocated from 16384.
 *
 * This value is used to ensure that the OIDs allocated in a batch do not cross the boundary
 * because the value can be exactly divided by 6384 and (2 ^ 32 - 16384).
 * That is, (16383 - 10000 + 1) % DSTORE_PREFETCH_OID_COUNT = (2 ^ 32 - 1 - 16384 + 1) % DSTORE_PREFETCH_OID_COUNT = 0.
 */
const uint32 DSTORE_PREFETCH_OID_COUNT = 114;
const uint32 BG_WRITER_SLEEP_TIME = 1000;
constexpr uint32 PS_MAX_READ_BLOCK_COUNT = 127;
const uint32 COPY_ONCE_BLOCK_COUNT = 64;  /* PageStore server supports reading at most 64 blocks at a time. */
const char *g_sysTbsName[] = {
    "invalid_tablespace",
    "gs_global",
    "gs_default",
    "gs_catalog",
    "gs_undo",
    "gs_temporary",
    "gs_catalogaux"
};

StoragePdb::StoragePdb(PdbId pdbId)
    : m_vfs(nullptr),
      m_controlFile(nullptr),
      m_transactionMgr(nullptr),
      m_undoMgr(nullptr),
      m_walMgr(nullptr),
      m_logicalReplicaMgr(nullptr),
      m_objSpaceMgr(nullptr),
      m_checkpointMgr(nullptr),
      m_bgPageWriterMgr(nullptr),
      m_recycleUndoThread(nullptr),
      m_asyncRecoverUndoThread(nullptr),
      m_checkpointThread(nullptr),
      m_objSpaceMgrWorkerList(nullptr),
      m_standbyMonitorThread(nullptr),
      m_barrierCreatorThread(nullptr),
      m_collectMinBarrierThread(nullptr),
      m_updateBarrierCsnThread(nullptr),
      m_stopPdbUpdateBarrier(false),
      m_pdbId(pdbId),
      m_pdbStatus(static_cast<uint8>(PdbStatus::PDB_STATUS_NEED_RELOAD)),
      m_pdbUuid(PDB_DEFAULT_UUID_STR),
      m_hasInitialized(false),
      m_walMgrhasInitialized(false),
      m_inBootStrap(false),
      m_bgWriterSlotId(INVALID_BGWRITER_SLOT_ID),
      m_stopBgThread(false),
      m_stopBarrierThread(false),
      m_numObjSpaceMgrWorkers(0),
      m_pdbRoleMode(static_cast<uint8>(PdbRoleMode::PDB_PRIMARY)),
      m_disableSqlTransaction(false),
      m_activeTransactionNum(0),
      m_tablespaceMgr(nullptr),
      m_rollbackEnd(false),
      m_statMsgBgWriter(),
      m_transactionsNeedRollbackBarrierListHead({nullptr, nullptr}),
      m_numOfTransactionsNeedRollbackBarrier(0),
      m_noSendBarrierListHead({nullptr, nullptr}),
      m_numOfNoSendBarrier(0),
      m_needRollbackBarrierInFailover(false),
      m_rollbackBarrierCsn(INVALID_CSN),
      m_barrierLastEndPlsn(0)
{
    m_asyncStopAndDropWalThrdCnt.store(0, std::memory_order_release);
    RWLockInit(&m_oidGenRWLock, RWLOCK_PREFER_WRITER_NP);
    m_builtinRelMap.sharedRelMap.count = 0;
    m_builtinRelMap.sharedRelMap.isValid.store(false, std::memory_order_release);
    m_builtinRelMap.localRelMap.count = 0;
    m_builtinRelMap.localRelMap.isValid.store(false, std::memory_order_release);
    RWLockInit(&m_builtinRelMap.relMapRWLock, RWLOCK_PREFER_WRITER_NP);
    pthread_rwlock_init(&m_rollbackBarrierListLock, nullptr);
    if (pdbId >= FIRST_USER_PDB_ID || pdbId == PDB_ROOT_ID) {
        UuidGenerate(m_pdbUuid);
    }
}

StoragePdb::StoragePdb(PdbId pdbId, const char* pdbUuid)
    : m_vfs(nullptr),
      m_controlFile(nullptr),
      m_transactionMgr(nullptr),
      m_undoMgr(nullptr),
      m_walMgr(nullptr),
      m_logicalReplicaMgr(nullptr),
      m_objSpaceMgr(nullptr),
      m_checkpointMgr(nullptr),
      m_bgPageWriterMgr(nullptr),
      m_recycleUndoThread(nullptr),
      m_asyncRecoverUndoThread(nullptr),
      m_checkpointThread(nullptr),
      m_objSpaceMgrWorkerList(nullptr),
      m_standbyMonitorThread(nullptr),
      m_barrierCreatorThread(nullptr),
      m_collectMinBarrierThread(nullptr),
      m_updateBarrierCsnThread(nullptr),
      m_stopPdbUpdateBarrier(false),
      m_pdbId(pdbId),
      m_pdbStatus(static_cast<uint8>(PdbStatus::PDB_STATUS_NEED_RELOAD)),
      m_pdbUuid(PDB_DEFAULT_UUID_STR),
      m_hasInitialized(false),
      m_walMgrhasInitialized(false),
      m_inBootStrap(false),
      m_bgWriterSlotId(INVALID_BGWRITER_SLOT_ID),
      m_stopBgThread(false),
      m_stopBarrierThread(false),
      m_numObjSpaceMgrWorkers(0),
      m_pdbRoleMode(static_cast<uint8>(PdbRoleMode::PDB_PRIMARY)),
      m_disableSqlTransaction(false),
      m_activeTransactionNum(0),
      m_tablespaceMgr(nullptr),
      m_rollbackEnd(false),
      m_statMsgBgWriter(),
      m_transactionsNeedRollbackBarrierListHead({nullptr, nullptr}),
      m_numOfTransactionsNeedRollbackBarrier(0),
      m_noSendBarrierListHead({nullptr, nullptr}),
      m_numOfNoSendBarrier(0),
      m_needRollbackBarrierInFailover(false),
      m_rollbackBarrierCsn(INVALID_CSN),
      m_barrierLastEndPlsn(0)
{
    m_asyncStopAndDropWalThrdCnt.store(0, std::memory_order_release);
    RWLockInit(&m_oidGenRWLock, RWLOCK_PREFER_WRITER_NP);
    m_builtinRelMap.sharedRelMap.count = 0;
    m_builtinRelMap.sharedRelMap.isValid.store(false, std::memory_order_release);
    m_builtinRelMap.localRelMap.count = 0;
    m_builtinRelMap.localRelMap.isValid.store(false, std::memory_order_release);
    RWLockInit(&m_builtinRelMap.relMapRWLock, RWLOCK_PREFER_WRITER_NP);
    pthread_rwlock_init(&m_rollbackBarrierListLock, nullptr);
    errno_t rc = strncpy_s(m_pdbUuid, FORMATTED_UUID_ARR_LEN, pdbUuid, strlen(pdbUuid));
    storage_securec_check(rc, "\0", "\0");
}

StoragePdb::~StoragePdb()
{}
FUNCTION_DELETE_LATER
RetStatus StoragePdb::Create()
{
    AutoMemCxtSwitch autoSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));

    if (STORAGE_FUNC_FAIL(InitVFS())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init vfs."));
        return DSTORE_FAIL;
    }
    char *pdbPath = StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, m_pdbId);
    if (STORAGE_VAR_NULL(pdbPath)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to get pdb path."));
        return DSTORE_FAIL;
    }
    (void) CreateWalPath(pdbPath);
    CreateControlFile(m_pdbId, pdbPath);
    CreateDecodeDictFile(m_pdbId, pdbPath);
    StorageReleasePanic(InitControlFile() == DSTORE_FAIL, MODULE_FRAMEWORK, ErrMsg("InitControlfile fail."));
    CreateTemplateTablespace();
    DstorePfree(pdbPath);
    /* Flush all the root db info down to disk so that other processes can read it. */
    (void)g_storageInstance->GetBufferMgr()->FlushAll(true, false, m_pdbId);
    DestroyControlFile();
    return DSTORE_SUCC;
}
FUNCTION_DELETE_LATER
RetStatus StoragePdb::Initialize(bool initCsn)
{
    AutoMemCxtSwitch autoSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    SetPdbTerm(g_storageInstance->GlobalPdbTermAddFetchU64());
    BgPageWriterMgrInit();
    if (STORAGE_FUNC_FAIL(InitVFS())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init vfs."));
        return DSTORE_FAIL;
    }
    StorageReleasePanic(InitControlFile() == DSTORE_FAIL, MODULE_FRAMEWORK, ErrMsg("InitControlfile fail."));
    if (initCsn) {
        (void)g_storageInstance->StorageInstance::InitCsnMgr();
    }
    InitTableSpaceMgr();
    InitWalMgr();
    InitLogicalReplicaMgr();
    InitCheckpointMgr();

    InitUndoMgr();

    LoadUndoMapSegment();

    InitTransactionMgr();
    if (STORAGE_FUNC_FAIL(InitObjSpaceMgr())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init ObjSpaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
#ifndef UT
    StartBgThread();
#endif
    m_hasInitialized = true;
    return DSTORE_SUCC;
}
FUNCTION_DELETE_LATER
RetStatus StoragePdb::Destroy()
{
#ifndef UT
    if (!m_inBootStrap) {
        StopBgThread();
    }
#endif
    DestroyUndoMgr();
    DestroyLogicalReplicaMgr();
    DestroyWalMgr();
    DestroyObjSpaceMgr();
    DestroyCheckpointMgr();
    DestroyTransactionMgr();
    DestroyControlFile();
    RWLockDestroy(&m_oidGenRWLock);
    RWLockDestroy(&m_builtinRelMap.relMapRWLock);
    m_builtinRelMap.sharedRelMap.count = 0;
    m_builtinRelMap.sharedRelMap.isValid.store(false, std::memory_order_release);
    m_builtinRelMap.localRelMap.count = 0;
    m_builtinRelMap.localRelMap.isValid.store(false, std::memory_order_release);
    pthread_rwlock_destroy(&m_rollbackBarrierListLock);
    const char *vfsName = GetVFSName();
    if (STORAGE_FUNC_FAIL(DestroyVFS(vfsName, false, m_pdbId))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to destory vfs, vfs name is %s.", vfsName));
        return DSTORE_FAIL;
    }
    m_hasInitialized = false;
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CreatePdb(bool initCsn)
{
    AutoMemCxtSwitch autoSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    SetPdbTerm(g_storageInstance->GlobalPdbTermAddFetchU64());
    BgPageWriterMgrInit();
    if (STORAGE_FUNC_FAIL(BootstrapVFS())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to bootstrap for vfs."));
        return DSTORE_FAIL;
    }
    char *pdbPath = StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, m_pdbId);
    if (STORAGE_VAR_NULL(pdbPath)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to get pdb path."));
        return DSTORE_FAIL;
    }
    (void) CreateWalPath(pdbPath);
    CreateControlFile(m_pdbId, pdbPath);
    CreateDecodeDictFile(m_pdbId, pdbPath);
    DstorePfree(pdbPath);
    StorageReleasePanic(InitControlFile() == DSTORE_FAIL, MODULE_FRAMEWORK, ErrMsg("InitControlfile fail."));
    if (initCsn) {
        (void)g_storageInstance->StorageInstance::InitCsnMgr();
    }
    InitTableSpaceMgr();
    InitWalMgr();
    InitLogicalReplicaMgr();
    InitUndoMgr();
    InitTransactionMgr();
    if (STORAGE_FUNC_FAIL(InitObjSpaceMgr())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init ObjSpaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    m_inBootStrap = true;
    if (g_storageInstance->GetType() != StorageInstanceType::DISTRIBUTE_COMPUTE) {
        InitRecoveryThreadAndWaitDone();
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CreateVfsAndFiles(PdbId pdbId, const char *vfsName)
{
    AutoMemCxtSwitch autoSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    char *pdbPath = StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, pdbId);
    if (STORAGE_VAR_NULL(pdbPath)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to get pdb path."));
        return DSTORE_FAIL;
    }
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    RetStatus ret = DSTORE_SUCC;
    if (pdbId == PDB_TEMPLATE0_ID) {
        ret = MountExistingVFS(tenantConfig->storageConfig.template0VfsName);
    } else if (vfsName != nullptr) {
        ret = MountExistingVFS(vfsName);
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("pdbId %hhu succ to mount vfs %s", pdbId, vfsName));
    } else {
        if (pdbId == PDB_ROOT_ID) {
            ret = MountExistingVFS(tenantConfig->storageConfig.rootpdbVfsName);
        } else {
            if (STORAGE_FUNC_FAIL(CreateNewVFS())) {
                DstorePfree(pdbPath);
                return DSTORE_FAIL;
            }
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("CreateNewCDB create new VFS success"));
        }
    }

    if (unlikely(ret != DSTORE_SUCC)) {
        DstorePfree(pdbPath);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("failed to mount or create vfs %s", vfsName));
        return DSTORE_FAIL;
    }
    /* in PageStore env, CreateWalPath will fail, ingore it. */
    (void)CreateWalPath(pdbPath);
    if (vfsName == nullptr) {
        CreateControlFile(pdbId, pdbPath);
    }
    if (m_controlFile == nullptr) {
        m_controlFile = DstoreNew(g_dstoreCurrentMemoryContext) ControlFile(pdbId, m_vfs);
        if (STORAGE_VAR_NULL(m_controlFile)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc for controlFile."));
            DstorePfree(pdbPath);
            return DSTORE_FAIL;
        }
    }
    if (vfsName == nullptr) {
        CreateDecodeDictFile(pdbId, pdbPath);
    }

    if (GetPdbRoleMode() != PdbRoleMode::PDB_STANDBY) {
        /* create a new pdb use vfs with multi-tenant isolation, not need input dataDir for Init */
        if (STORAGE_FUNC_FAIL(m_controlFile->Init(pdbPath))) {
            DstorePfree(pdbPath);
            delete m_controlFile;
            m_controlFile = nullptr;
            return DSTORE_FAIL;
        }
    }
    DstorePfree(pdbPath);
    delete m_controlFile;
    m_controlFile = nullptr;
    return DSTORE_SUCC;
}

RetStatus StoragePdb::InitPdb(bool initCsn)
{
    AutoMemCxtSwitch autoSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    SetPdbTerm(g_storageInstance->GlobalPdbTermAddFetchU64());
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> initStart =
            std::chrono::high_resolution_clock::now();
    BgPageWriterMgrInit();
    if (STORAGE_FUNC_FAIL(InitVFS())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init vfs."));
        return DSTORE_FAIL;
    }
    StorageReleasePanic(InitControlFile() == DSTORE_FAIL, MODULE_FRAMEWORK, ErrMsg("InitControlfile fail."));
    if (g_defaultPdbId == PDB_ROOT_ID && IsTemplate(m_pdbId)) {
        return DSTORE_SUCC;
    }
    if (initCsn) {
        (void)g_storageInstance->StorageInstance::InitCsnMgr();
    }
    InitTableSpaceMgr();
    InitWalMgr();
    InitLogicalReplicaMgr();
    InitCheckpointMgr();

    InitUndoMgr();
    LoadUndoMapSegment();

    if (STORAGE_FUNC_FAIL(InitObjSpaceMgr())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init ObjSpaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }

    InitTransactionMgr();
    if (g_storageInstance->GetType() != StorageInstanceType::DISTRIBUTE_COMPUTE) {
        InitRecoveryThreadAndWaitDone();
    }
#ifndef UT
    StartBgThread();
#endif

    m_hasInitialized = true;
    std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - initStart;
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("InitPdb and recovery spend time %f seconds.", spendSecs.count()));
    return DSTORE_SUCC;
}

void StoragePdb::DestroyMgr()
{
    BgPageWriterMgrDestroy();
    DestroyTransactionMgr();
    DestoryTablespaceMgr();
    DestroyUndoMgr();
    DestroyLogicalReplicaMgr();
    DestroyWalMgr();
    DestroyCheckpointMgr();
    DestroyControlFile();
    DestroyObjSpaceMgr();
}

RetStatus StoragePdb::ResetPdb(const char *vfsName, bool dropData)
{
    m_hasInitialized = false;
#ifndef UT
    if (!m_inBootStrap) {
        StopBgThread();
    }
    /* If dropData is true, wal and controlFile may already be deleted, no need to do this */
    if (!dropData && g_storageInstance->GetType() == StorageInstanceType::SINGLE && !IsTemplate(GetPdbId())) {
        TryMarkSelfWalStreamDropping();
    }
#endif

    if (m_tablespaceMgr) {
        m_tablespaceMgr->Destroy();
        delete m_tablespaceMgr;
        m_tablespaceMgr = nullptr;
    }
    DestroyMgr();
    if (STORAGE_FUNC_FAIL(DestroyVFS(vfsName, dropData, m_pdbId))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to destory vfs, vfs name is %s.", vfsName));
        return DSTORE_FAIL;
    }
    /* reset pdbname, GetPdbIdBy name depends on it */
    errno_t rc = memset_s(m_pdbName, DB_NAME_LEN, 0, DB_NAME_LEN);
    storage_securec_check(rc, "\0", "\0");
    m_pdbStatus.store(static_cast<uint8>(PdbStatus::PDB_STATUS_NEED_RELOAD), std::memory_order_release);
    /* reset uuid, backup&restore depends on it */
    rc = memset_s(m_pdbUuid, FORMATTED_UUID_ARR_LEN, 0, FORMATTED_UUID_ARR_LEN);
    storage_securec_check(rc, "\0", "\0");
    m_walMgrhasInitialized = false;
    m_numObjSpaceMgrWorkers = 0;
    m_asyncStopAndDropWalThrdCnt.store(0, std::memory_order_release);
    m_disableSqlTransaction = false;
    m_activeTransactionNum = 0;
    /* reset vfsname */
    rc = memset_s(m_vfsName, MAX_CONFIG_NAME_LENGTH, 0, MAX_CONFIG_NAME_LENGTH);
    storage_securec_check(rc, "\0", "\0");
    
    m_inBootStrap = false;
    m_stopBgThread = false;
    /* reset builtin relmap */
    m_builtinRelMap.sharedRelMap.count = 0;
    m_builtinRelMap.sharedRelMap.isValid.store(false, std::memory_order_release);
    m_builtinRelMap.localRelMap.count = 0;
    m_builtinRelMap.localRelMap.isValid.store(false, std::memory_order_release);
    /* set pdbuuid to default */
    rc = strncpy_s(m_pdbUuid, FORMATTED_UUID_ARR_LEN, PDB_DEFAULT_UUID_STR, strlen(PDB_DEFAULT_UUID_STR));
    storage_securec_check(rc, "\0", "\0");

    m_rollbackEnd = false;
    rc = memset_s(&m_statMsgBgWriter, sizeof(GsStatMsgBgWriter), 0, sizeof(GsStatMsgBgWriter));
    storage_securec_check(rc, "\0", "\0");
    m_bgWriterSlotId = INVALID_BGWRITER_SLOT_ID;
    m_cachedOidCnt = 0;
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CheckPdbInfo(const char *vfsName)
{
    if (m_vfs == nullptr) {
        if (STORAGE_FUNC_FAIL(MountExistingVFS(vfsName))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("failed to mount vfs %s", vfsName));
            return DSTORE_FAIL;
        }
    }

    if (!m_vfs->HasMounted() && STORAGE_FUNC_FAIL(m_vfs->Open(vfsName))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("failed to open vfs %s", vfsName));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::OpenPdb(void *pdbInfoData)
{
    AutoMemCxtSwitch autoSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    SetPdbTerm(g_storageInstance->GlobalPdbTermAddFetchU64());
    if (IsInit()) {
        return DSTORE_SUCC;
    }

    if (STORAGE_VAR_NULL(m_bgPageWriterMgr)) {
        BgPageWriterMgrInit();
    }
    ControlPdbInfoPageItemData *pdbInfo = static_cast<ControlPdbInfoPageItemData *>(static_cast<void *>(pdbInfoData));
    if (STORAGE_FUNC_FAIL(CheckPdbInfo(pdbInfo->vfsName))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("check pdbInfo failed"));
        return DSTORE_FAIL;
    }

    if (m_controlFile == nullptr) {
        m_controlFile = DstoreNew(g_dstoreCurrentMemoryContext) ControlFile(m_pdbId, m_vfs);
        if (STORAGE_VAR_NULL(m_controlFile)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc for controlFile."));
            return DSTORE_FAIL;
        }
    }

    if (!m_controlFile->IsInitialized()) {
        char *pdbPath = StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, static_cast<uint8>(m_pdbId));
        if (STORAGE_VAR_NULL(pdbPath)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to get pdb path."));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(m_controlFile->Init(pdbPath))) {
            DstorePfree(pdbPath);
            DestroyControlFile();
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("Failed to init controlFile for pdb %hhu", m_pdbId));
            return DSTORE_FAIL;
        }
        DstorePfree(pdbPath);
    }
    
    InitTableSpaceMgr();
    InitWalMgr();
    InitLogicalReplicaMgr();
    InitCheckpointMgr();

    InitUndoMgr();
    LoadUndoMapSegment();

    InitTransactionMgr();
    if (STORAGE_FUNC_FAIL(InitObjSpaceMgr())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init ObjSpaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }

    if (pdbInfo->pdbRoleMode == PdbRoleMode::PDB_PRIMARY &&
        g_storageInstance->GetType() != StorageInstanceType::DISTRIBUTE_COMPUTE) {
        InitRecoveryThreadAndWaitDone();
    }
#ifndef UT
    StartBgThread();
#endif
    m_hasInitialized = true;
    return DSTORE_SUCC;
}

RetStatus StoragePdb::ClosePdb(bool needFullCheckpoint)
{
    AutoMemCxtSwitch autoSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    StopBgThread(needFullCheckpoint);
    const char *vfsName = GetVFSName();
    if (STORAGE_FUNC_FAIL(StoragePdb::ResetPdb(vfsName, false))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to reset pdb, pdbId is %d.", m_pdbId));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CleanTempFile(bool isStartUp)
{
#ifndef UT
    /* Global temporary table:remove temp files in temp tablespace */
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
           ErrMsg("Remove temp files for pdb:%s(%u), isStartUp %u.", GetPdbName(), GetPdbId(), isStartUp));
    TablespaceId tempTbsId = static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID);
    TablespaceMgr *tablespaceMgr = GetTablespaceMgr();
    if (tablespaceMgr) {
        LockMode lockMode = isStartUp ? DSTORE::DSTORE_NO_LOCK : DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK;
        TableSpace *tablespace = tablespaceMgr->OpenTablespace(tempTbsId, lockMode);
        if (STORAGE_VAR_NULL(tablespace)) {
            ErrLog(DSTORE_ERROR, MODULE_PDB,
                   ErrMsg("Failed to open tablespace %u.", tempTbsId));
            return DSTORE_FAIL;
        }
        uint32 selfNodeId = g_storageInstance->GetGuc()->selfNodeId;
        if (unlikely(selfNodeId < TMP_TBS_START_FILE_ID || selfNodeId > TMP_TBS_MAX_FILE_ID)) {
            tablespaceMgr->CloseTablespace(tablespace, lockMode);
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("[DeleteTempFile]Wrong nodeId %u.", selfNodeId));
            return DSTORE_FAIL;
        }
        if (tablespace && tablespace->IsTempTbs()) {
            TbsDataFile *tbsDataFile = tablespaceMgr->GetDataFiles()[selfNodeId];
            if (tbsDataFile && STORAGE_FUNC_FAIL(tablespace->FreeAndRemoveDataFile(m_pdbId, tempTbsId, tbsDataFile,
                                                                                   thrd->GetCurrentXid(), false))) {
                ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                       ErrMsg("Failed to delete temp datafile, pdbId %u, nodeId %u.", m_pdbId, selfNodeId));
                tablespaceMgr->CloseTablespace(tablespace, lockMode);
                return DSTORE_FAIL;
            }
        }
        tablespaceMgr->CloseTablespace(tablespace, lockMode);
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("Remove temp files for pdb:%s(%u) success!", GetPdbName(), GetPdbId()));
#endif
    return DSTORE_SUCC;
}

bool StoragePdb::IsReadFileOk() const
{
    return IsInit();
}

RetStatus StoragePdb::GetNewObjectId(bool isInitDb, Oid &next_oid, bool isInplaceUpgrade)
{
    if (GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to allocate oid on standby PDB."));
        return DSTORE_FAIL;
    }
    if (isInplaceUpgrade) {
        if (thrd->m_inplaceUpgradeNextOid >= FIRST_NORMAL_OBJECT_ID) {
            thrd->m_inplaceUpgradeNextOid = FIRST_BOOTSTRAP_OBJECT_ID;
        }
        next_oid = thrd->m_inplaceUpgradeNextOid;
        thrd->m_inplaceUpgradeNextOid++;
        return DSTORE_SUCC;
    }
    /* Only the OID stored in the system tablespace gs_catalog is valid. */
    TablespaceId tablespace_id = static_cast<TablespaceId>(TBS_ID::CATALOG_TABLE_SPACE_ID);
    TablespaceMgr *tablespaceMgr = GetTablespaceMgr();
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to get tablespaceMgr, tablespaceId %hu.", tablespace_id));
        return DSTORE_FAIL;
    }
 
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespace_id, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Failed to get tablespace(%u) when allocating a new oid.", tablespace_id));
        return DSTORE_FAIL;
    }
 
    RWLockWrLock(&m_oidGenRWLock);

    if (m_cachedOidCnt == 0) {
        /*
         * The valid OID is stored in the first data file of gs_catalog system tablespace.
         */
        FileId fileId = tablespace->GetTbsPageItemData().fileIds[0];
        if (unlikely(fileId == INVALID_DATA_FILE_ID)) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to get first fileId."));
            RWLockWrUnlock(&m_oidGenRWLock);
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            return DSTORE_FAIL;
        }

        TbsDataFile *datafile = tablespaceMgr->AcquireDatafile(fileId, LW_SHARED);
        /* The extent size of the first data file must be EXT_SIZE_8. */
        if (STORAGE_VAR_NULL(datafile) || datafile->GetExtentSize() != EXT_SIZE_8) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                   ErrMsg("Failed to obtain valid datafile %hu of tablespace gs_catalog.", fileId));
            tablespaceMgr->ReleaseDatafileLock(datafile);
            RWLockWrUnlock(&m_oidGenRWLock);
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            return DSTORE_FAIL;
        }

        if (STORAGE_FUNC_FAIL(datafile->AllocOid(isInitDb, &m_nextOid, DSTORE_PREFETCH_OID_COUNT))) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                   ErrMsg("Failed to allocate next batch oid in datafile, next oid %u, pdbId %u.",
                          m_nextOid, m_pdbId));
            tablespaceMgr->ReleaseDatafileLock(datafile);
            RWLockWrUnlock(&m_oidGenRWLock);
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            return DSTORE_FAIL;
        }
        m_cachedOidCnt = DSTORE_PREFETCH_OID_COUNT;
        tablespaceMgr->ReleaseDatafileLock(datafile);
    }

    next_oid = m_nextOid++;
    m_cachedOidCnt--;

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
           ErrMsg("Acquire oid %u success, m_cachedOidCnt is %u, pdbId %u, isInitDb %d.", next_oid, m_cachedOidCnt,
                  m_pdbId, isInitDb));
    RWLockWrUnlock(&m_oidGenRWLock);
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    return DSTORE_SUCC;
}

void StoragePdb::TryMarkSelfWalStreamDropping()
{
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
           ErrMsg("TryMarkSelfWalStreamDropping start to drop walstreams of pdb %u.", m_pdbId));
    if (m_walMgr == nullptr || m_walMgr->GetWalStreamManager() == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("TryMarkSelfWalStreamDropping failed for pdb %u, "
            "no valid WalMgr.", m_pdbId));
        return;
    }
    if (m_controlFile == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("TryMarkSelfWalStreamDropping failed for no valid ControlFile."));
        return;
    }

    AutoMemCxtSwitch autoSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    WalStreamManager *walStreamMgr = m_walMgr->GetWalStreamManager();

    WalId walId;
    uint64 walFlushedPlsn;
    dlist_mutable_iter iter = {};
    WalStreamNode *walStreamNode = nullptr;
    WalStream *walStream = nullptr;
    while ((walStreamNode = walStreamMgr->GetNextWalStream(&iter, WalStream::IsWalStreamForWrite)) != nullptr) {
        walStream = walStreamNode->walStream;
        walId = walStream->GetWalId();
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
           ErrMsg("TryMarkSelfWalStreamDropping start to drop wal:%lu of pdb %u.", walId, m_pdbId));
        if (walStream->GetWalStreamState() == WalStreamState::CLOSE_DROPPING) {
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
                ErrMsg("TryMarkSelfWalStreamDropping of wal:%lu whose state is CLOSE_DROPPING and jump over.", walId));
            continue;
        }
        if (STORAGE_FUNC_FAIL(walStreamMgr->StopWalStreamWriteWal(walId))) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                ErrMsg("TryMarkSelfWalStreamFree of wal:%lu failed for StopWalStreamWriteWal failed.", walId));
            return;
        }
        walFlushedPlsn = walStream->GetMaxFlushedPlsn();

        /* Step2: Get checkpoint disk recoveryPlsn from control file */
        ControlWalStreamPageItemData *walStreamInfo = nullptr;
        RetStatus ret = m_controlFile->GetWalStreamInfo(walId, &walStreamInfo);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                ErrMsg("TryMarkSelfWalStreamDropping walId:%lu failed for GetCheckpointInfo from ControlFile fail.",
                walId));
            return;
        }
        uint64 checkpointPlsn = walStreamInfo->lastWalCheckpoint.diskRecoveryPlsn;
        m_controlFile->FreeWalStreamsInfo(walStreamInfo);

        /* Step3: Mark free if diskRecoveryPlsn == walFlushedPlsn, only set after Redo finished */
        if (checkpointPlsn == walFlushedPlsn) {
            walStream->SetWalStreamState(WalStreamState::CLOSE_DROPPING);
            if (STORAGE_FUNC_FAIL(
                m_controlFile->UpdateWalStreamState(walId, static_cast<uint8>(WalStreamState::CLOSE_DROPPING)))) {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                    ErrMsg("TryMarkSelfWalStreamDropping of wal:%lu failed for set control file wal stream state to "
                        "CLOSE_DROPPING failed.", walId));
                return;
            }
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("TryMarkSelfWalStreamDropping of pdb:%d wal:%lu success.",
                m_pdbId, walId));
        } else {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                ErrMsg("TryMarkSelfWalStreamDropping of wal:%lu failed, controlFile checkpoint:%lu != "
                "WalFlushedPlsn:%lu.", walId, checkpointPlsn, walFlushedPlsn));
        }
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
        ErrMsg("TryMarkSelfWalStreamDropping complete drop walstreams of pdb %u.", m_pdbId));
}

VFSAdapter *StoragePdb::GetVFS()
{
    if (STORAGE_VAR_NULL(m_vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("vfs is null"));
    }
    return m_vfs;
}

void StoragePdb::SetVFSName(const char *vfsName)
{
    errno_t rc = memset_s(m_vfsName, MAX_CONFIG_NAME_LENGTH, 0, MAX_CONFIG_NAME_LENGTH);
    storage_securec_check(rc, "\0", "\0");
    rc = memcpy_s(m_vfsName, MAX_CONFIG_NAME_LENGTH, vfsName, DSTORE_VFS_NAME_MAX_LEN);
    storage_securec_check(rc, "\0", "\0");
}

const char *StoragePdb::GetVFSName()
{
    if (strlen(m_vfsName) != 0) {
        return m_vfsName;
    }
    errno_t rc = 0;
    switch (m_pdbId) {
        case PDB_TEMPLATE0_ID: {
            rc = memcpy_s(m_vfsName, MAX_CONFIG_NAME_LENGTH,
                          g_storageInstance->GetGuc()->tenantConfig->storageConfig.template0VfsName,
                          DSTORE_VFS_NAME_MAX_LEN);
            storage_securec_check(rc, "\0", "\0");
            break;
        }
        case PDB_TEMPLATE1_ID: {
            rc = memcpy_s(m_vfsName, MAX_CONFIG_NAME_LENGTH,
                          g_storageInstance->GetGuc()->tenantConfig->storageConfig.template1VfsName,
                          DSTORE_VFS_NAME_MAX_LEN);
            storage_securec_check(rc, "\0", "\0");
            break;
        }
        case PDB_ROOT_ID: {
            rc = memcpy_s(m_vfsName, MAX_CONFIG_NAME_LENGTH,
                          g_storageInstance->GetGuc()->tenantConfig->storageConfig.rootpdbVfsName,
                          DSTORE_VFS_NAME_MAX_LEN);
            storage_securec_check(rc, "\0", "\0");
            break;
        }
        case PDB_TEMPLATEA_ID:
        default: {
            GenerateVfsName(m_pdbId, m_vfsName, MAX_CONFIG_NAME_LENGTH);
            break;
        }
    }
    return m_vfsName;
}

RetStatus StoragePdb::BootstrapVFS()
{
    if (!STORAGE_VAR_NULL(m_vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("vfs is not null"));
        return DSTORE_FAIL;
    }
    m_vfs =
        DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) VFSAdapter(m_pdbId);
    if (STORAGE_VAR_NULL(m_vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to malloc for vfs."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_vfs->Bootstrap())) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to bootstrap for vfs."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CreateNewVFS()
{
    if (!STORAGE_VAR_NULL(m_vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("vfs is not null"));
        return DSTORE_FAIL;
    }
    m_vfs =
        DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) VFSAdapter(m_pdbId);
    if (STORAGE_VAR_NULL(m_vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to malloc for vfs."));
        return DSTORE_FAIL;
    }
    char path[MAXPGPATH] = {};
    int rc = sprintf_s(path, MAXPGPATH, "%s/%s", g_storageInstance->GetGuc()->dataDir, BASE_DIR);
    storage_securec_check_ss(rc);
    const char *newVfsName = GetVFSName();
    uint64 ioFenceFlag = VFS_ENABLE_IO_FENCE_FLAG;
    if (STORAGE_FUNC_FAIL(m_vfs->CreateVfs(path, newVfsName, ioFenceFlag))) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to create vfs."));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus StoragePdb::MountExistingVFS(const char *vfsName)
{
    if (!STORAGE_VAR_NULL(m_vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("vfs is not null"));
        return DSTORE_FAIL;
    }
    m_vfs =
        DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) VFSAdapter(m_pdbId);
    if (STORAGE_VAR_NULL(m_vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc memory for vfs."));
        return DSTORE_FAIL;
    }
    return m_vfs->Open(vfsName);
}

RetStatus StoragePdb::InitVFS()
{
    if (m_vfs) {
        return DSTORE_SUCC;
    }
    m_vfs =
        DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) VFSAdapter(m_pdbId);
    if (unlikely(m_vfs == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc memory for vfs."));
        return DSTORE_FAIL;
    }
    auto tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (m_pdbId == PDB_TEMPLATE1_ID) {
        if (STORAGE_FUNC_FAIL(m_vfs->Initialize(tenantConfig->storageConfig.template1VfsName))) {
            delete m_vfs;
            m_vfs = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("vfs initialize failed. vfsName = %s.", tenantConfig->storageConfig.template1VfsName));
            return DSTORE_FAIL;
        }
    } else if (m_pdbId == PDB_TEMPLATE0_ID) {
        if (STORAGE_FUNC_FAIL(m_vfs->Initialize(tenantConfig->storageConfig.template0VfsName))) {
            delete m_vfs;
            m_vfs = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("vfs initialize failed. vfsName = %s.", tenantConfig->storageConfig.template0VfsName));
            return DSTORE_FAIL;
        }
    } else if (m_pdbId == PDB_TEMPLATEA_ID) {
        char templateaVfsName[MAX_CONFIG_NAME_LENGTH] = {};
        StoragePdb::GenerateVfsName(m_pdbId, templateaVfsName, MAX_CONFIG_NAME_LENGTH);
        if (STORAGE_FUNC_FAIL(m_vfs->Initialize(templateaVfsName))) {
            delete m_vfs;
            m_vfs = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("vfs initialize failed. vfsName = %s.", templateaVfsName));
            return DSTORE_FAIL;
        }
    } else {
        if (STORAGE_FUNC_FAIL(m_vfs->Initialize(tenantConfig->storageConfig.rootpdbVfsName))) {
            delete m_vfs;
            m_vfs = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("vfs initialize failed. vfsName = %s.", tenantConfig->storageConfig.template0VfsName));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::DestroyVFS(const char *vfsName, bool dropData, PdbId pdbId)
{
    if (vfsName == nullptr || strlen(vfsName) == 0) {
        if (m_vfs != nullptr) {
            delete m_vfs;
            m_vfs = nullptr;
        }
        return DSTORE_SUCC;
    }
    if (m_vfs) {
        if (STORAGE_FUNC_FAIL(m_vfs->Destroy(vfsName, dropData))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Destory vfs failed. vfsName = %s.", vfsName));
            return DSTORE_FAIL;
        }
        delete m_vfs;
        m_vfs = nullptr;
        return DSTORE_SUCC;
    } else if (m_vfs == nullptr && dropData && vfsName != nullptr) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("m_vfs is nullptr, need destory data force, vfsName = %s.", vfsName));
        return VFSAdapter::DestroyDataForce(vfsName, pdbId);
    }
    return DSTORE_SUCC;
}

void StoragePdb::LoadUndoMapSegment()
{
    m_undoMgr->LoadUndoMapSegment();
}

void StoragePdb::CreateUndoMapSegment()
{
    m_undoMgr->CreateUndoMapSegment();
}

bool StoragePdb::IsInit() const
{
    return m_hasInitialized;
}

bool StoragePdb::IsWalMgrInit() const
{
    return m_walMgrhasInitialized;
}

#ifdef UT
void StoragePdb::SetInit()
{
    m_hasInitialized = true;
}

void StoragePdb::SetWalMgrInit()
{
    m_walMgrhasInitialized = true;
}
#endif

ControlFile *StoragePdb::GetControlFile()
{
    return m_controlFile;
}

TransactionMgr *StoragePdb::GetTransactionMgr()
{
    return m_transactionMgr;
}

UndoMgr *StoragePdb::GetUndoMgr()
{
    return m_undoMgr;
}

WalManager *StoragePdb::GetWalMgr()
{
    return m_walMgr;
}

LogicalReplicaMgr *StoragePdb::GetLogicalReplicaMgr()
{
    return m_logicalReplicaMgr;
}

ObjSpaceMgr *StoragePdb::GetObjSpaceMgr()
{
    return m_objSpaceMgr;
}

CheckpointMgr *StoragePdb::GetCheckpointMgr()
{
    return m_checkpointMgr;
}

RetStatus StoragePdb::FullCheckpoint()
{
    if (!IsInit() || m_bgPageWriterMgr == nullptr) {
        return DSTORE_SUCC;
    }
    if (m_checkpointMgr == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("CheckpointMgr is nullptr."));
        return DSTORE_FAIL;
    }
    m_bgPageWriterMgr->FlushAllDirtyPages();
    if (GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Full checkpoint not supported for current role mode: %u.",
            static_cast<uint32>(GetPdbRoleMode())));
        return DSTORE_SUCC;
    }
    return m_checkpointMgr->FullCheckpoint(m_pdbId);
}

RetStatus StoragePdb::CreateControlFile(PdbId pdbId, const char *pdbPath)
{
    ControlFile ctrlFile(pdbId, m_vfs, DeployType::CONTROL_FILE_SINGLE_NODE);
    UNUSE_PARAM RetStatus ret = ctrlFile.Create(pdbPath);
    /* The function is used only for bootstrap, so no need to error handling */
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("ctrlFile create fail."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::InitControlFile()
{
    if (m_controlFile == nullptr) {
        m_controlFile =
            DstoreNew(g_dstoreCurrentMemoryContext) ControlFile(m_pdbId, m_vfs, DeployType::CONTROL_FILE_SINGLE_NODE);
        if (m_controlFile == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Alloc memory for control file failed."));
            return DSTORE_FAIL;
        }
    }
    char *pdbPath = StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, static_cast<uint8>(m_pdbId));
    if (STORAGE_VAR_NULL(pdbPath)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to get pdb path."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_controlFile->Init(pdbPath))) {
        DstorePfree(pdbPath);
        DestroyControlFile();
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Init control file failed."));
        return DSTORE_FAIL;
    }
    DstorePfree(pdbPath);
    return DSTORE_SUCC;
}

void StoragePdb::DestroyControlFile()
{
    delete m_controlFile;
    m_controlFile = nullptr;
}

RetStatus StoragePdb::GetControlFileSizeVfs(int64 &partSize)
{
    partSize = (int64)m_controlFile->GetFileSize() * 2;
    return DSTORE_SUCC;
}

/*
 * With VFS maintaining a refCount, this function should be able to open, getsize and close safely.
 * The approach of getsize by fileId is not used, becasue file opening by fileId does not record its opening
 *    status. If we do not close it, it may left open; If we close it, others my still want to have it opened.
 */
RetStatus StoragePdb::GetFileSizeWithOpen(const char *fileName, int64 &fileSize)
{
    FileDescriptor *fileDescriptor;
    fileSize = 0;
    if (m_vfs->OpenFile(fileName, DSTORE_FILE_OPEN_FLAG, &fileDescriptor) != DSTORE_SUCC) {
        ErrLog(DSTORE_ERROR, MODULE_PDB,
            ErrMsg("Failed to open file for getting size. Filename: %s", fileName));
        return DSTORE_FAIL;
    }
    fileSize = m_vfs->GetSize(fileDescriptor);
    if (unlikely(fileSize < 0)) {
        m_vfs->CloseFile(fileDescriptor);
        ErrLog(DSTORE_ERROR, MODULE_PDB,
            ErrMsg("Failed to get file size with file open. Filename: %s", fileName));
        return DSTORE_FAIL;
    }
    m_vfs->CloseFile(fileDescriptor);
    return DSTORE_SUCC;
}

RetStatus StoragePdb::GetFileSizeVfs(int64 &partSize)
{
    partSize = 0;
    char fileName[MAXPGPATH] = {0};
    uint32 fileId;
    int64 fileSize = 0;
    for (fileId = 1; fileId <= MAX_VFS_FILE_ID; fileId++) {
        if (STORAGE_FUNC_FAIL(m_vfs->GetFileNameFromFileId(fileId, fileName))) {
            return DSTORE_FAIL;
        }
        if (!m_vfs->FileExists(fileName)) {
            continue;
        }
        if (GetFileSizeWithOpen(fileName, fileSize) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
        partSize += fileSize;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::DeleteWalFileFromVfs(ControlWalStreamPageItemData *pageInfo)
{
    WalId walId =  pageInfo->walId;
    uint64 walFileSize = static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
    uint32 walFileCnt = 0;
    uint64 walStartPlsn = 0;
    RetStatus ret = DSTORE_SUCC;
    char **fileNames = WalFileManager::GetWalFilesName({m_pdbId, walId, walFileSize,
                                                       pageInfo->lastWalCheckpoint.diskRecoveryPlsn, false},
                                                       walFileCnt, walStartPlsn);
    if (m_walMgr != nullptr) {
        WalStream *stream = m_walMgr->GetWalStreamManager()->GetWalStream(walId);
        if (stream != nullptr) {
            stream->GetWalFileManager()->CloseAllWalFiles();
        }
    }
    for (uint32 j = 0; j < walFileCnt; j++) {
        if (fileNames[j] == nullptr) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("delete wal file failed, filename is null."));
            continue;
        }
        if (STORAGE_FUNC_FAIL(m_vfs->RemoveFile(fileNames[j]))) {
            ret = DSTORE_FAIL;
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("delete wal file(fileName = %s) failed.", fileNames[j]));
            walStartPlsn += walFileSize;
            DstorePfreeExt(fileNames[j]);
        }
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("delete wal file(fileName = %s) success.", fileNames[j]));
        walStartPlsn += walFileSize;
        DstorePfreeExt(fileNames[j]);
    }
    DstorePfreeExt(fileNames);
    return ret;
}

RetStatus StoragePdb::GetWalFileSizeVfs(int64 &partSize)
{
    partSize = 0;
    /* Gather wal streams for size measurement */
    WalId *walIdList = nullptr;
    const uint32 walStreamCount = m_controlFile->GetAllWalStreams(&walIdList);
    if (walStreamCount == 0) {
        return DSTORE_SUCC;
    }
    ControlWalStreamPageItemData **walStreams = static_cast<ControlWalStreamPageItemData **>(DstorePalloc0(
        walStreamCount * sizeof(ControlWalStreamPageItemData *)));
    if (unlikely(walStreams == nullptr)) {
        DstorePfreeExt(walIdList);
        return DSTORE_FAIL;
    }
    for (uint32 i = 0; i < walStreamCount; i++) {
        if (STORAGE_FUNC_FAIL(m_controlFile->GetWalStreamInfo(walIdList[i], &walStreams[i]))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("Get wal stream info(walId: %lu) from pdb %d failed.", walIdList[i], m_pdbId));
            DstorePfreeExt(walIdList);
            m_controlFile->FreeWalStreamsInfoArray(walStreams, walStreamCount);
            return DSTORE_FAIL;
        }
    }
    DstorePfreeExt(walIdList);

    /* Loop though all walstreams */
    for (uint16 i = 0; i < walStreamCount; i++) {
        /* Need fileid for size measurement, but walstream only provides filename(s) */
        WalId walId =  walStreams[i]->walId;
        uint64 walFileSize = static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
        uint32 walFileCnt = 0;
        uint64 walStartPlsn = 0;
        char **fileNames = WalFileManager::GetWalFilesName({m_pdbId, walId, walFileSize,
            walStreams[i]->lastWalCheckpoint.diskRecoveryPlsn, false},
            walFileCnt, walStartPlsn);
        /* sum all files' size; open file to get FD by name, and get size by FD */
        for (uint32 j = 0; j < walFileCnt; j++) {
            int64 fileSize = 0;
            if (GetFileSizeWithOpen(fileNames[j], fileSize) != DSTORE_SUCC) {
                DstorePfreeExt(fileNames);
                m_controlFile->FreeWalStreamsInfoArray(walStreams, walStreamCount);
                return DSTORE_FAIL;
            }
            partSize += fileSize;
        }
        DstorePfreeExt(fileNames);
    }
    m_controlFile->FreeWalStreamsInfoArray(walStreams, walStreamCount);
    return DSTORE_SUCC;
}

RetStatus StoragePdb::GetSizeVfs(int64 &totalSize)
{
    /* Sum all components' file size to obtain total size of this pdb. */
    totalSize = 0;

    int64 partSize = 0;
    if (this->GetFileSizeVfs(partSize) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }
    totalSize += partSize;
    if (this->GetDecodeDictFileSizeVfs(partSize) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }
    totalSize += partSize;
    if (this->GetWalFileSizeVfs(partSize) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }
    totalSize += partSize;
    if (this->GetControlFileSizeVfs(partSize) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }
    totalSize += partSize;

    return DSTORE_SUCC;
}

RetStatus StoragePdb::CopyFilesFromPdb(StoragePdb *srcPdb)
{
    /* Get Files From source pdb */
    ControlFile *srcControlFile = srcPdb->GetControlFile();
    uint32 fileCount = 0;
    dlist_head fileIdListHead;

    DListInit(&fileIdListHead);
    /* if tablespaceId == INVALID_TABLESPACE_ID, scan all tablespace in the control file. */
    if (STORAGE_FUNC_FAIL(srcControlFile->GetFilesFromAllTablespace(&fileIdListHead, &fileCount))) {
        return DSTORE_FAIL;
    }

    if (DListIsEmpty(&fileIdListHead)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("No file exists in all tablespaces."));
        return DSTORE_FAIL;
    }

    /* Get Files From source pdb */
    VFSAdapter *srcVFS = srcPdb->GetVFS();
    VFSAdapter *newVFS = this->GetVFS();
    dlist_iter iter;
    dlist_foreach(iter, &fileIdListHead) {
        FileIdNode *fileIdNode = dlist_container(FileIdNode, node, iter.cur);
        char fileName[MAXPGPATH] = {0};
        if (STORAGE_FUNC_FAIL(srcVFS->GetFileNameFromFileId(fileIdNode->fileId, fileName))) {
            srcControlFile->FreeFileIdList(&fileIdListHead);
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(CopyFileFromVfs(fileIdNode->fileId, fileName, srcVFS, newVFS))) {
            srcControlFile->FreeFileIdList(&fileIdListHead);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("copy file(fileName = %s) failed.", fileName));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("copy file(fileName = %s) success.", fileName));
    }

    srcControlFile->FreeFileIdList(&fileIdListHead);
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CopyFileFromVfs(FileId fileId, const char *fileName, VFSAdapter *srcVfs, VFSAdapter *newVfs)
{
    bool isTemplatePdb = IsTemplate(m_pdbId);
    if (newVfs->FileExists(fileId, fileName)) {
        ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("fileName(%s), fileId(%d) EXIST.", fileName, fileId));
        return DSTORE_SUCC;
    }
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("tenantConfig is null."));
        return DSTORE_FAIL;
    }
    FileParameter filePara = {
        "",
        VFS_DEFAULT_FILE_STREAM_ID,
        IN_PLACE_WRITE_FILE,
        DATA_FILE_TYPE,
        isTemplatePdb ? TEMPLATE_TBS_FILE_RANGE_SIZE : TBS_FILE_RANGE_SIZE, /* for template0 */
        MAX_FILE_SIZE,  /* The tablespace datafile size to be copied must be the same as that when
                           the tablespace is created. See TbsDataFile::InitFilePara. */
        0,
        FILE_READ_AND_WRITE_MODE,
        false
    };
    errno_t rc = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                          tenantConfig->storeSpaces[0].storeSpaceName);
    storage_securec_check(rc, "\0", "\0");

    /* new vfs does not need the full path */
    if (STORAGE_FUNC_FAIL(newVfs->CreateFile(fileId, fileName, filePara))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Create file(fileName = %s) failed.", fileName));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(OpenFileInVfsIfNeed(srcVfs, fileId, fileName))) {
        return DSTORE_FAIL;
    }
    int64 fileSize = srcVfs->GetSize(fileId);
    if (unlikely(fileSize < 0)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Extend controlFile failed, fileId(%d).", fileId));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL,
           ErrMsg("Copy Pdb(%u) fileName(%s), fileId(%d) filesize(%ld).", m_pdbId, fileName, fileId, fileSize));
    /* When the process of initializing template0 reaches this point,
     * special judgment is required. */
    int64 extendSize = isTemplatePdb ? fileSize : (int64)INIT_FILE_SIZE;
    if (STORAGE_FUNC_FAIL(newVfs->Extend(fileId, extendSize))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Extend controlFile failed, fileId(%d).", fileId));
        return DSTORE_FAIL;
    }

    uint32 copySizeOnce = static_cast<uint32>(BLCKSZ * COPY_ONCE_BLOCK_COUNT);
    uint32 copyTimes = static_cast<uint32>(fileSize) / copySizeOnce;
    int64 offset = 0;
    for (uint32 j = 0; j < copyTimes; j++) {
        if (STORAGE_FUNC_FAIL(CopySpecifySizeInFile(newVfs, srcVfs, fileId, copySizeOnce, offset))) {
            return DSTORE_FAIL;
        }
        offset += static_cast<int64>(copySizeOnce);
    }
    uint32 remainingSize = static_cast<uint32>(fileSize) % copySizeOnce;
    if (remainingSize != 0) {
        if (STORAGE_FUNC_FAIL(CopySpecifySizeInFile(newVfs, srcVfs, fileId, remainingSize, offset))) {
            return DSTORE_FAIL;
        }
    }
    if (STORAGE_FUNC_FAIL(newVfs->Fsync(fileId))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Fsync vfs file failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("End Pdb(%u) copy fileName(%s), fileId(%d).", m_pdbId, fileName, fileId));
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CopyWalFilesFromPdb(StoragePdb *srcPdb)
{
    ControlFile *controlFile = srcPdb->GetControlFile();
    /* if tablespaceId == INVALID_TABLESPACE_ID, scan all tablespace in the control file. */
    /* fetch wal streams of all nodes */
    WalId *walIdList;
    const uint32 walStreamCount = controlFile->GetAllWalStreams(&walIdList);
    ControlWalStreamPageItemData **walStreams = static_cast<ControlWalStreamPageItemData **>(DstorePalloc0(
        walStreamCount * sizeof(ControlWalStreamPageItemData *)));
    if (unlikely(walStreams == nullptr)) {
        DstorePfreeExt(walIdList);
        return DSTORE_FAIL;
    }
    for (uint32 i = 0; i < walStreamCount; i++) {
        if (STORAGE_FUNC_FAIL(controlFile->GetWalStreamInfo(walIdList[i], &walStreams[i]))) {
            controlFile->FreeWalStreamsInfoArray(walStreams, walStreamCount);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Get wal stream info(walId: %lu) from source Pdb failed.", walIdList[i]));
            DstorePfreeExt(walIdList);
            return DSTORE_FAIL;
        }
    }
    DstorePfreeExt(walIdList);
    VFSAdapter *srcVFS = srcPdb->GetVFS();
    VFSAdapter *newVFS = GetVFS();

    RetStatus retStatus = DSTORE_SUCC;
    for (uint16 i = 0; i < walStreamCount; i++) {
        ControlWalStreamPageItemData *pageInfo = walStreams[i];
        if (pageInfo->streamState != static_cast<uint8>(WalStreamState::USING)) {
            continue;
        }
        WalId walId = pageInfo->walId;
        uint64 walFileSize = IsTemplate(srcPdb->GetPdbId()) ?
            TEMPLATE_WAL_FILE_SIZE : static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
        uint32 walFileCnt = 0;
        uint64 walStartPlsn = 0;
        char **fileNames = WalFileManager::GetWalFilesName({srcPdb->GetPdbId(), walId, walFileSize,
                                                            pageInfo->lastWalCheckpoint.diskRecoveryPlsn, false},
                                                           walFileCnt, walStartPlsn);
        for (uint32 j = 0; j < walFileCnt; j++) {
            char walFileName[MAXPGPATH];
            int result = sprintf_s(walFileName, MAXPGPATH, "%08llX_%08X_%016llX", walId, 0, walStartPlsn);
            storage_securec_check_ss(result);
            if (STORAGE_FUNC_FAIL(
                CopyWalFileFromVfs(walFileName, srcVFS, newVFS, walId))) {
                retStatus = DSTORE_FAIL;
                break;
            }
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("copy wal file(fileName = %s) success.", fileNames[j]));
            walStartPlsn += walFileSize;
        }
        for (uint32 j = 0; j < walFileCnt; j++) {
            DstorePfreeExt(fileNames[j]);
        }
        DstorePfreeExt(fileNames);
        if (STORAGE_FUNC_FAIL(retStatus)) {
            break;
        }
    }
    controlFile->FreeWalStreamsInfoArray(walStreams, walStreamCount);
    return retStatus;
}

void StoragePdb::InitWalFileParam(FileParameter &filePara, WalId walId, PdbId pdbId)
{
    filePara.streamId = walId;
    filePara.flag = APPEND_WRITE_FILE;
    filePara.fileSubType = WAL_LOG_FILE_TYPE;
    filePara.rangeSize = CONTROL_FILE_RANGE_SIZE;
    filePara.maxSize = IsTemplate(pdbId) ?
        TEMPLATE_WAL_FILE_SIZE : static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
}

RetStatus StoragePdb::CopyWalFileFromVfs(const char *fileName, VFSAdapter *srcVfs, VFSAdapter *newVfs, WalId walId)
{
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("tenantConfig is null."));
        return DSTORE_FAIL;
    }
    FileParameter filePara;
    InitWalFileParam(filePara, walId, m_pdbId);
    errno_t rc = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                          tenantConfig->storeSpaces[0].storeSpaceName);
    storage_securec_check(rc, "\0", "\0");
    /* new vfs does not need the full path */
    char fullFileName[MAXPGPATH];
    rc = memset_s(fullFileName, MAXPGPATH, 0, MAXPGPATH);
    storage_securec_check(rc, "\0", "\0");
    int result = sprintf_s(fullFileName, MAXPGPATH, "wal/%s", fileName);
    storage_securec_check_ss(result);

    /* Step1: Prepare new wal file in new VFS */
    if (newVfs->FileExists(fullFileName) && STORAGE_FUNC_FAIL(newVfs->RemoveFile(fullFileName))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("file(%s) exists in new pdb, remove it failed.", fullFileName));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(newVfs->CreateFile(fullFileName, filePara, nullptr))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Create file(fileName = %s) failed.", fullFileName));
        return DSTORE_FAIL;
    }

    /* Step2: Check wal file in source vfs */
    FileDescriptor *srcWalFileFd;
    if (STORAGE_FUNC_FAIL(OpenWalFileInVfsIfNeed(srcVfs, fileName, &srcWalFileFd))) {
        return DSTORE_FAIL;
    }
    int64 fileSize = srcVfs->GetSize(srcWalFileFd);
    if (unlikely(fileSize < 0)) {
        (void)srcVfs->CloseFile(srcWalFileFd);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Get file size failed, File(%s).", fileName));
        return DSTORE_FAIL;
    }
    if (unlikely(fileSize == 0)) {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Wal File(%s) size is 0.", fileName));
        (void)srcVfs->CloseFile(srcWalFileFd);
        return DSTORE_SUCC;
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Copy Pdb(%u) Wal File(%s) size(%ld).", m_pdbId, fileName, fileSize));

    /* Step3: Begin copying wal file from source vfs to new vfs */
    FileDescriptor *newWalFileFd;
    if (STORAGE_FUNC_FAIL(newVfs->OpenFile(fullFileName, FILE_READ_AND_WRITE_FLAG, &newWalFileFd))) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Open wal file(%s) in new pdb(%u) vfs failed.", fullFileName, m_pdbId));
        (void)srcVfs->CloseFile(srcWalFileFd);
        return DSTORE_FAIL;
    }
    uint32 readSizeOnce = static_cast<uint32>(BLCKSZ * PS_MAX_READ_BLOCK_COUNT);
    uint32 times = static_cast<uint32>(fileSize) / readSizeOnce;
    int64 offset = 0;
    for (uint32 i = 0; i < times; i++) {
        if (STORAGE_FUNC_FAIL(
            CopySpecifySizeInWalFile(srcVfs, srcWalFileFd, newVfs, newWalFileFd, readSizeOnce, offset))) {
            (void)srcVfs->CloseFile(srcWalFileFd);
            (void)newVfs->CloseFile(newWalFileFd);
            return DSTORE_FAIL;
        }
        offset += static_cast<int64>(readSizeOnce);
    }
    uint32 remainingSize = static_cast<uint32>(fileSize) % readSizeOnce;
    if (remainingSize != 0) {
        if (STORAGE_FUNC_FAIL(
            CopySpecifySizeInWalFile(srcVfs, srcWalFileFd, newVfs, newWalFileFd, remainingSize, offset))) {
            (void)srcVfs->CloseFile(srcWalFileFd);
            (void)newVfs->CloseFile(newWalFileFd);
            return DSTORE_FAIL;
        }
    }

    /* Step4: Sync new wal file to new vfs */
    if (STORAGE_FUNC_FAIL(newVfs->Fsync(newWalFileFd))) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Fsync vfs file failed."));
        return DSTORE_FAIL;
    }
    /* when one walFile has written to max, it should be rewind, otherwise, it does not need. */
    if (filePara.maxSize == static_cast<uint64>(fileSize) && STORAGE_FUNC_FAIL(newVfs->Rewind(newWalFileFd))) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Rewind vfs file failed."));
        return DSTORE_FAIL;
    }

    /* Step5: Close wal file */
    (void)newVfs->CloseFile(newWalFileFd);
    (void)srcVfs->CloseFile(srcWalFileFd);
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("End Pdb(%u)copy Wal File(%s).", m_pdbId, fileName));

    return DSTORE_SUCC;
}

RetStatus StoragePdb::OpenFileInVfsIfNeed(VFSAdapter *vfsAdapter, FileId fileId, const char *fileName)
{
    ::FileDescriptor *fileDesc = vfsAdapter->GetFileDescriptor(fileId);
    if (fileDesc != nullptr) {
        return DSTORE_SUCC;
    }

    RetStatus retStatus = vfsAdapter->OpenFile(fileId, fileName, DSTORE_FILE_OPEN_FLAG);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Open file failed, fileId is %hu, fileName is %s", fileId, fileName));
    }
    return retStatus;
}

RetStatus StoragePdb::OpenWalFileInVfsIfNeed(VFSAdapter *vfsAdapter, const char *fileName, FileDescriptor **walFileFd)
{
    char *pdbPath = StorageInstance::GetPdbWalPath(g_storageInstance->GetGuc()->dataDir, INVALID_PDB_ID);
    char fullFileName[MAX_FILE_NAME_LEN];
    int rc = memset_s(fullFileName, MAX_FILE_NAME_LEN, 0, MAX_FILE_NAME_LEN);
    storage_securec_check(rc, "\0", "\0");
    rc  = sprintf_s(fullFileName, MAX_FILE_NAME_LEN, "%s/%s", pdbPath, fileName);
    storage_securec_check_ss(rc);
    DstorePfreeExt(pdbPath);

    RetStatus retStatus = vfsAdapter->OpenFile(fullFileName, DSTORE_WAL_FILE_OPEN_FLAG, walFileFd);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("[pdbId: %u] Open wal file in template vfs failed, fileName is %s", m_pdbId, fileName));
    }

    return retStatus;
}

RetStatus StoragePdb::CopyFiles(StoragePdb *templatePdb)
{
    if (STORAGE_FUNC_FAIL(CopyControlFile(templatePdb))) {
        return DSTORE_FAIL;
    }

    /* Mark all walStream in control file dropping, for copy using walStream from templatePdb. */
    if (STORAGE_FUNC_FAIL(MarkAllControlWalStreamDropping())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Fail to MarkAllControlWalStreamDropping for pdb %u.", m_pdbId));
        return DSTORE_FAIL;
    }

    /* Copy data files from template pdb to new pdb (all files in each tablespace, including systable files) */
    if (STORAGE_FUNC_FAIL(CopyFilesFromPdb(templatePdb))) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(CopyWalFilesFromPdb(templatePdb))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::MarkAllControlWalStreamDropping()
{
    InitControlFile();
    char *pdbPath = StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, GetPdbId());
    RetStatus ret = m_controlFile->Init(pdbPath);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Init controlFile fail for pdb %u.", m_pdbId));
        DstorePfreeExt(pdbPath);
        DestroyControlFile();
        return DSTORE_FAIL;
    }
    DstorePfreeExt(pdbPath);
    StorageReleasePanic(m_controlFile == nullptr, MODULE_CONTROL, ErrMsg("controlfile is null. "));

    WalId *walIdList = nullptr;
    uint32 streamCount = m_controlFile->GetAllWalStreams(&walIdList);
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Get wal stream count %u from control file. ", streamCount));
    if (streamCount == 0) {
        ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Get 0 wal stream from control file. "));
        DestroyControlFile();
        return DSTORE_SUCC;
    }

    for (uint32 i = 0; i < streamCount; i++) {
        ret = m_controlFile->UpdateWalStreamState(walIdList[i], static_cast<uint8>(WalStreamState::CLOSE_DROPPING));
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("Mark wal stream status to CLOSE_DROPPING for pdb:%u wal:%lu failed.", m_pdbId, walIdList[i]));
            DstorePfreeExt(walIdList);
            DestroyControlFile();
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("Mark wal stream status to CLOSE_DROPPING for pdb:%u wal:%lu success.", m_pdbId, walIdList[i]));
    }

    DstorePfreeExt(walIdList);
    DestroyControlFile();
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CopyControlFile(StoragePdb *templatePdb)
{
    ControlFile *templateControlFile = templatePdb->GetControlFile();
    if (m_controlFile == nullptr) {
        m_controlFile = DstoreNew(g_dstoreCurrentMemoryContext) ControlFile(GetPdbId(), GetVFS());
        if (STORAGE_VAR_NULL(m_controlFile)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc for controlFile."));
            return DSTORE_FAIL;
        }
    }
    ControlFile *newControlFile = m_controlFile;
    if (!newControlFile->IsInitialized()) {
        char *pdbPath =
            StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, static_cast<uint8>(GetPdbId()));
        if (STORAGE_VAR_NULL(pdbPath)) {
            DestroyControlFile();
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to get pdb path."));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(newControlFile->Init(pdbPath))) {
            DstorePfree(pdbPath);
            DestroyControlFile();
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                   ErrMsg("Failed to initialize new control file for pdb %hhu.", GetPdbId()));
            return DSTORE_FAIL;
        }
        DstorePfree(pdbPath);
    }

    while (newControlFile->GetFileSize() < templateControlFile->GetFileSize()) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("CopyControlFile newControlFileSize(%lu) templateControlFileSize(%lu).",
                      newControlFile->GetFileSize(), templateControlFile->GetFileSize()));
        if (STORAGE_FUNC_FAIL(newControlFile->ExtendFile())) {
            DestroyControlFile();
            return DSTORE_FAIL;
        }
    }

    /* copy controlFile from blockNumber 1,
     * we skip blockNumber 0 in templateControlFile because pdbPath of newPdb has inited in blockNumber 0 */
    if (STORAGE_FUNC_FAIL(newControlFile->CopyControlFile(templateControlFile))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Failed to copy new control file for pdb %hhu.", GetPdbId()));
        DestroyControlFile();
        return DSTORE_FAIL;
    }

    /* ControlFile of new pdb need to reload from disk */
    DestroyControlFile();
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CreateSysTablespace(TablespaceId tablespaceId)
{
    TableSpace tablespace(m_controlFile, tablespaceId);

    if (STORAGE_FUNC_FAIL(tablespace.CreateSysTablespace(GetPdbId(), tablespaceId))) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

void StoragePdb::CreateTemplateTablespace()
{
    if (STORAGE_FUNC_FAIL(CreateSysTablespace(static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID))) ||
        STORAGE_FUNC_FAIL(CreateSysTablespace(static_cast<TablespaceId>(TBS_ID::GLOBAL_TABLE_SPACE_ID))) ||
        STORAGE_FUNC_FAIL(CreateSysTablespace(static_cast<TablespaceId>(TBS_ID::CATALOG_TABLE_SPACE_ID))) ||
        STORAGE_FUNC_FAIL(CreateSysTablespace(static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID))) ||
        STORAGE_FUNC_FAIL(CreateSysTablespace(static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID))) ||
        STORAGE_FUNC_FAIL(CreateSysTablespace(static_cast<TablespaceId>(TBS_ID::CATALOG_AUX_TABLE_SPACE_ID)))) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK,
            ErrMsg("Failed to create template tablespace"));
    }
}

void StoragePdb::InitTransactionMgr()
{
    if (m_transactionMgr != nullptr) {
        if (STORAGE_FUNC_FAIL(m_transactionMgr->StartDispatchRollbackTask())) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg(
                "Failed to init transaction mgr for pdb: %hhu", m_pdbId));
        }
        return;
    }
    m_transactionMgr = DstoreNew(g_dstoreCurrentMemoryContext)
        TransactionMgr(GetUndoMgr(), g_storageInstance->GetCsnMgr(), m_pdbId);
    if (unlikely(m_transactionMgr == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed to malloc for transactionMgr."));
        return ;
    }
    if (STORAGE_FUNC_FAIL(m_transactionMgr->StartDispatchRollbackTask())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg(
            "Failed to init transaction mgr for pdb: %hhu", m_pdbId));
    }
}

void StoragePdb::DestoryTablespaceMgr()
{
    if (m_tablespaceMgr) {
        m_tablespaceMgr->Destroy();
        delete m_tablespaceMgr;
    }
    m_tablespaceMgr = nullptr;
}

void StoragePdb::DestroyTransactionMgr()
{
    if (m_transactionMgr) {
        m_transactionMgr->StopDispatchRollbackTask();
        delete m_transactionMgr;
    }
    m_transactionMgr = nullptr;
}

RetStatus StoragePdb::InitUndoMgr()
{
    if (m_undoMgr != nullptr) {
        return DSTORE_SUCC;
    }
    m_undoMgr = DstoreNew(g_dstoreCurrentMemoryContext) UndoMgr(g_storageInstance->GetBufferMgr(), m_pdbId);
    if (STORAGE_VAR_NULL(m_undoMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Alloc UndoMgr Failed."));
        return DSTORE_FAIL;
    }
    RetStatus ret = m_undoMgr->Init(g_dstoreCurrentMemoryContext);
    if (STORAGE_FUNC_FAIL(ret)) {
        DestroyUndoMgr();
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("m_undoMgr init fail."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void StoragePdb::DestroyUndoMgr()
{
    if (m_undoMgr != nullptr) {
        m_undoMgr->DestroyUndoMapSegment();
        m_undoMgr->DestroyUndoTxnInfoCache();
        delete m_undoMgr;
    }
    m_undoMgr = nullptr;
}

RetStatus StoragePdb::InitWalMgr()
{
    if (m_walMgr != nullptr) {
        return DSTORE_SUCC;
    }
    m_walMgr = DstoreNew(g_dstoreCurrentMemoryContext) WalManager(g_dstoreCurrentMemoryContext);
    if (STORAGE_VAR_NULL(m_walMgr)) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed to malloc for walMgr."));
        return DSTORE_FAIL;
    }
    if (m_walMgr->Init(m_controlFile) != DSTORE_SUCC) {
        /* Todo error log here */
        DestroyWalMgr();
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("walMgr init failed."));
        return DSTORE_FAIL;
    }
    m_walMgrhasInitialized = true;
    return DSTORE_SUCC;
}

void StoragePdb::DestroyWalMgr()
{
    if (m_walMgr) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("DestroyWalMgr for pdb %u.", m_pdbId));
        m_walMgr->Destroy();
        delete m_walMgr;
    }
    m_walMgr = nullptr;
    m_walMgrhasInitialized = false;
}

RetStatus StoragePdb::InitLogicalReplicaMgr()
{
    return DSTORE_SUCC;
#ifdef ENABLE_LOGICAL_REPL
    if (m_logicalReplicaMgr != nullptr) {
        return DSTORE_SUCC;
    }
    m_logicalReplicaMgr =
        DstoreNew(g_dstoreCurrentMemoryContext) LogicalReplicaMgr(m_pdbId, g_dstoreCurrentMemoryContext);
    if (m_logicalReplicaMgr == nullptr || STORAGE_FUNC_FAIL(m_logicalReplicaMgr->Init())) {
        /* error log here */
        DestroyLogicalReplicaMgr();
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("Failed to init LogicalReplicaMgr."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
#endif
}

void StoragePdb::DestroyLogicalReplicaMgr()
{
    return;
#ifdef ENABLE_LOGICAL_REPL
    if (m_logicalReplicaMgr) {
        m_logicalReplicaMgr->Destroy();
        delete m_logicalReplicaMgr;
    }
    m_logicalReplicaMgr = nullptr;
#endif
}

RetStatus StoragePdb::InitObjSpaceMgr()
{
    if (m_objSpaceMgr != nullptr) {
        if (m_objSpaceMgr->IsObjSpaceMgrInitialized()) {
            return DSTORE_SUCC;
        } else {
            DestroyObjSpaceMgr();
        }
    }
    m_numObjSpaceMgrWorkers = g_storageInstance->GetGuc()->numObjSpaceMgrWorkers;
    m_objSpaceMgr = DstoreNew(g_dstoreCurrentMemoryContext) ObjSpaceMgr(m_numObjSpaceMgrWorkers);
    if (m_objSpaceMgr == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init ObjSpaceMgr."));
        return DSTORE_FAIL;
    }
    return m_objSpaceMgr->Initialize(g_dstoreCurrentMemoryContext);
}

void StoragePdb::DestroyObjSpaceMgr()
{
    if (m_objSpaceMgr) {
        m_objSpaceMgr->Destroy();
        delete m_objSpaceMgr;
    }
    m_objSpaceMgr = nullptr;
}

void StoragePdb::InitCheckpointMgr()
{
    if (m_checkpointMgr != nullptr) {
        return;
    }
    m_checkpointMgr = DstoreNew(g_dstoreCurrentMemoryContext) CheckpointMgr(m_walMgr, m_pdbId);
}

void StoragePdb::StoreMaxAppendPlsnOfWalStreams(PdbsPlsnRecords &plsnRecords)
{
    if (STORAGE_VAR_NULL(m_walMgr)) {
        return;
    }
    WalStreamManager *walStreamManager = m_walMgr->GetWalStreamManager();
    if (STORAGE_VAR_NULL(walStreamManager)) {
        return;
    }
    dlist_mutable_iter iter = {};
    WalStreamNode* walStreamNode = nullptr;
    WalStream *walStream = nullptr;
    while ((walStreamNode = walStreamManager->GetNextWalStream(&iter, WalStream::IsWalStreamForWrite)) != nullptr) {
        walStream = walStreamNode->walStream;
        WalPlsnRecord record = {
            .pdbId = m_pdbId,
            .walId = walStream->GetWalId(),
            .plsn = walStream->GetMaxAppendedPlsn()
        };
        plsnRecords.Add(record);
    }
}

RetStatus StoragePdb::CreateCheckpoint()
{
    if (STORAGE_VAR_NULL(m_walMgr)) {
        ErrLog(DSTORE_LOG, MODULE_BUFFER, ErrMsg("no wal stream need checkpoint, pdb:%hhu.", m_pdbId));
        return DSTORE_SUCC;
    }
    WalStreamManager *walStreamManager = m_walMgr->GetWalStreamManager();
    if (STORAGE_VAR_NULL(walStreamManager)) {
        ErrLog(DSTORE_LOG, MODULE_BUFFER, ErrMsg("no wal stream need checkpoint, pdb:%hhu.", m_pdbId));
        return DSTORE_SUCC;
    }
    dlist_mutable_iter iter = {};
    WalStreamNode *walStreamNode = nullptr;
    WalStream *walStream = nullptr;
    while ((walStreamNode = walStreamManager->GetNextWalStream(&iter, WalStream::IsWalStreamForWrite)) != nullptr) {
        walStream = walStreamNode->walStream;
        if (STORAGE_FUNC_FAIL(
            CreateCheckpointForWalStream(walStream, walStream->GetMaxAppendedPlsn()))) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CreateCheckpoint(const PdbsPlsnRecords &diskRecoveryPlsnRecords)
{
    if (STORAGE_VAR_NULL(m_walMgr)) {
        ErrLog(DSTORE_LOG, MODULE_BUFFER, ErrMsg("no wal stream need checkpoint, pdb:%hhu.", m_pdbId));
        return DSTORE_SUCC;
    }

    WalStreamManager *walStreamManager = m_walMgr->GetWalStreamManager();
    if (STORAGE_VAR_NULL(walStreamManager)) {
        ErrLog(DSTORE_LOG, MODULE_BUFFER, ErrMsg("no wal stream need checkpoint, pdb:%hhu.", m_pdbId));
        return DSTORE_SUCC;
    }

    WalPlsnRecord record;
    RetStatus ret = DSTORE_SUCC;
    diskRecoveryPlsnRecords.ResetCursor();
    while (diskRecoveryPlsnRecords.GetNext(record)) {
        if (record.pdbId != m_pdbId) {
            continue;
        }
        WalStream *walStream = walStreamManager->GetWalStream(record.walId);
        if (STORAGE_VAR_NULL(walStream)) {
            continue;
        }
        if (STORAGE_FUNC_FAIL(CreateCheckpointForWalStream(walStream, record.plsn))) {
            ret = DSTORE_FAIL;
            /* we don't break, try our best to do the next */
        }
    }

    return ret;
}

RetStatus StoragePdb::CreateCheckpointForWalStream(const WalStream *walStream, uint64 diskRecoveryPlsn)
{
    WalId walId = walStream->GetWalId();
    /* update Control File */
    if (m_controlFile == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("m_controlFile is null."));
        return DSTORE_FAIL;
    }
    ControlWalStreamPageItemData *walStreamInfo = nullptr;
    RetStatus ret = m_controlFile->GetWalStreamInfo(walId, &walStreamInfo);
    if (STORAGE_FUNC_FAIL(ret)) {
        return ret;
    }
    if (walStreamInfo == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("walStreamInfo is null."));
        return DSTORE_FAIL;
    }

    uint64 lastrecoveryPlsn = walStreamInfo->lastWalCheckpoint.diskRecoveryPlsn;
    if (lastrecoveryPlsn >= diskRecoveryPlsn) {
        /* todo: race condition against checkpoint thread to be solved. */
        m_controlFile->FreeWalStreamsInfo(walStreamInfo);
        return DSTORE_SUCC;
    }

    WalCheckPoint checkPoint = {};
    checkPoint.time = time(nullptr);
    checkPoint.diskRecoveryPlsn = diskRecoveryPlsn;
    checkPoint.memoryCheckpoint = walStreamInfo->lastWalCheckpoint.memoryCheckpoint;

    walStreamInfo->lastCheckpointPLsn = 0; /* todo: use the start pos of the checkpoint wal record */
    walStreamInfo->lastWalCheckpoint = checkPoint;
    ret = m_controlFile->UpdateWalStreamForCheckPoint(walId, walStreamInfo->lastCheckpointPLsn,
                                                      walStreamInfo->lastWalCheckpoint);
    if (STORAGE_FUNC_FAIL(ret)) {
        m_controlFile->FreeWalStreamsInfo(walStreamInfo);
        return ret;
    }
    m_controlFile->FreeWalStreamsInfo(walStreamInfo);

    ErrLog(DSTORE_WARNING, MODULE_BUFFER,
           ErrMsg("wal stream %lu complete checkpoint, checkpoint recoveryPlsn %lu, last "
                  "checkpoint recoveryPlsn %lu, current Flush Max Plsn %lu, current Append Max Plsn %lu ",
                  walId, checkPoint.diskRecoveryPlsn, lastrecoveryPlsn,
                  walStream->GetMaxFlushedPlsn(), diskRecoveryPlsn));
    return DSTORE_SUCC;
}

RetStatus StoragePdb::UpdateDatabaseSysTable(HeapTuple *sysDatabaseTuple)
{
    RetStatus ret;
    TablespaceId tblSpcId = static_cast<TablespaceId>(TBS_ID::CATALOG_TABLE_SPACE_ID);
    /* step1: get pageId of systable relation */
    PageId pageId = INVALID_PAGE_ID;
    ret = SystableInterface::GetCoreSystableSegmentId(m_pdbId, SYSTABLE_RELATION_OID, pageId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Failed to get core systable segment id in step 1."));
        return DSTORE_FAIL;
    }
    StorageReleasePanic(pageId == INVALID_PAGE_ID, MODULE_SYSTABLE, ErrMsg("copy systable from TemplateDB failed."));

    /* step2: get sysRel of pg_database */
    TableStorageMgr *sysRelSmgr = StorageTableInterface::CreateTableSmgr(
        m_pdbId, tblSpcId, pageId, DEFAULT_HEAP_FILLFACTOR, nullptr);
    if (STORAGE_VAR_NULL(sysRelSmgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE,
            ErrMsg("Failed to create TableSmgr(%u-%hu-%hu-%u).", m_pdbId, tblSpcId, pageId.m_fileId, pageId.m_blockId));
        return DSTORE_FAIL;
    }
    DstoreMemoryContext ctx = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE);
    StorageRelation sysRel = (StorageRelation) DstoreMemoryContextAllocZero(ctx, sizeof(StorageRelationData));
    sysRel->pgstat_info = NULL;
    if (STORAGE_VAR_NULL(sysRel)) {
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Failed to malloc for storageRelation."));
        return DSTORE_FAIL;
    }
    TupleDesc tupleDesc = CreateTupleDesc(SYS_RELATION_COLS_CNT, true, SYS_RELATION_COLS);
    sysRel->tableSmgr = sysRelSmgr;
    sysRel->attr = tupleDesc;
    sysRel->m_pdbId = m_pdbId;
    auto scantuple = SystableInterface::ScanSysRelation(sysRel, SYS_DATABASE_RELATION_ID);
    if (scantuple == nullptr) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("System table tuple not found due to internal error or user canceling."));
        return DSTORE_FAIL;
    }
    /* step3: get segmentId of pg_database */
    PageId sysDbSegmentId = INVALID_PAGE_ID;
    ret = SystableInterface::GetCoreSystableSegmentId(m_pdbId, SYS_DATABASE_RELATION_ID, sysDbSegmentId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Failed to get core systable segment id in step 3."));
        return DSTORE_FAIL;
    }
    StorageReleasePanic(sysDbSegmentId == INVALID_PAGE_ID, MODULE_SYSTABLE,
                        ErrMsg("get systable pageId of pg_database from pdb %u failed.", m_pdbId));
    TableStorageMgr *sysDbSmgr = StorageTableInterface::CreateTableSmgr(
        m_pdbId, tblSpcId, sysDbSegmentId, DEFAULT_HEAP_FILLFACTOR, nullptr);
    if (STORAGE_VAR_NULL(sysDbSmgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE,
            ErrMsg("Failed to create TableSmgr(%u-%hu-%hu-%u).",
                m_pdbId, tblSpcId, sysDbSegmentId.m_fileId, sysDbSegmentId.m_blockId));
        return DSTORE_FAIL;
    }
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = sysDbSmgr;
    tmpRel.lobTableSmgr = nullptr;
    tmpRel.relOid = SYS_DATABASE_RELATION_ID;
    tmpRel.m_pdbId = m_pdbId;
    tmpRel.pgstat_info = NULL;
    HeapScanHandler *heapScan = HeapInterface::CreateHeapScanHandler(&tmpRel);
    if (unlikely(heapScan == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Failed to create heapscan handler."));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(HeapInterface::BeginScan(heapScan,
        thrd->GetActiveTransaction()->GetSnapshot()))) {
        HeapInterface::DestroyHeapScanHandler(heapScan);
        DstorePfreeExt(sysRel);
        DstorePfree(tupleDesc);
        StorageTableInterface::DestroyTableSmgr(sysDbSmgr);
        StorageTableInterface::DestroyTableSmgr(sysRelSmgr);
        return DSTORE_FAIL;
    }

    HeapTuple *tuple = HeapInterface::SeqScan(heapScan);

    /* step4: update record to pg_database */
    if (tuple != nullptr) {
        HeapDeleteContext deleteContext;
        deleteContext.ctid = *tuple->GetCtid();
        deleteContext.needReturnTup = true;
        deleteContext.snapshot = *thrd->GetActiveTransaction()->GetSnapshot();
        deleteContext.cid = thrd->GetActiveTransaction()->GetCurCid();
        ret = HeapInterface::Delete(&tmpRel, &deleteContext);
        if (ret == DSTORE_FAIL) {
             ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Failed to delete old tuple when creating database."));
        }
        ret = HeapInterface::Insert(&tmpRel, sysDatabaseTuple, thrd->GetActiveTransaction()->GetCurCid());
        if (ret == DSTORE_FAIL) {
            ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Failed to insert new tuple when creating database."));
        }
    } else {
        ErrLog(DSTORE_WARNING, MODULE_SYSTABLE, ErrMsg("internal error, add rootdb record failed"));
        ret = DSTORE_FAIL;
    }
    HeapInterface::EndScan(heapScan);
    HeapInterface::DestroyHeapScanHandler(heapScan);
    DstorePfreeExt(sysRel);
    DstorePfree(tupleDesc);
    StorageTableInterface::DestroyTableSmgr(sysDbSmgr);
    StorageTableInterface::DestroyTableSmgr(sysRelSmgr);
    return ret;
}

void StoragePdb::DestroyCheckpointMgr()
{
    if (m_checkpointMgr) {
        delete m_checkpointMgr;
    }
    m_checkpointMgr = nullptr;
}

void StoragePdb::StartBgThread()
{
    if (g_defaultPdbId == PDB_ROOT_ID && IsTemplate(GetPdbId())) {
        return;
    }
    AutoMemCxtSwitch autoSwitch(g_storageInstance->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    m_stopBgThread = false;
    if (GetPdbRoleMode() == PdbRoleMode::PDB_PRIMARY) {
        StartupBgPageWriterAndCkptThread();
        StartupUndoRecycleThread();
        StartupSpaceMgrWorkerThread();
        StartUpdateBarrierCsnThread();
        StartupBarrierCreatorThread();
        StartupAsyncRecoverUndoThread();
    } else {
        StartupBgPageWriterAndCkptThread();
        StartupStandbyMonitorThread();
        StartupCollectBarrierThread();
    }
}

RetStatus StoragePdb::StartBgThreadWhenPromoting()
{
    if (!m_stopBgThread) {
        ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("m_stopBgThread is false."));
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoSwitch(g_storageInstance->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    m_stopBgThread = false;
    InitTransactionMgr();
    if (!IsTemplate(GetPdbId())) {
        StartupUndoRecycleThread();
        StartupAsyncRecoverUndoThread();
    }
    StartupSpaceMgrWorkerThread();
    StartUpdateBarrierCsnThread();
    StartupBarrierCreatorThread();
    return DSTORE_SUCC;
}

RetStatus StoragePdb::StartBgThreadWhenDemoting()
{
    if (!m_stopBgThread) {
        ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("m_stopBgThread is false."));
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoSwitch(g_storageInstance->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    m_stopBgThread = false;
    InitTransactionMgr();
    StartupStandbyMonitorThread();
    StartupCollectBarrierThread();
    return DSTORE_SUCC;
}

/* Some background threads (e.g. asyn rollback thread, recyle thread, they invoke
 * InitSQLThreadContext to initialize SQL engine context.) may use SQL engine thread variables,
 * can't stop early before PM exit. */
void StoragePdb::StopSQLDependBgThread()
{
    m_stopBgThread = true;
    /* m_asyncRecoverUndoThread depond on m_rollbackTrxTaskMgr, must stop earlier than StopDispatchRollbackTask */
    if (m_asyncRecoverUndoThread) {
        m_asyncRecoverUndoThread->join();
        delete m_asyncRecoverUndoThread;
        m_asyncRecoverUndoThread = nullptr;
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("[StopBgThread] m_asyncRecoverUndoThread stopped, pdbId=%u", m_pdbId));
    }

    if (m_transactionMgr) {
        m_transactionMgr->StopDispatchRollbackTask();
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("[StopBgThread] m_rollbackTrxTaskMgr and workers stopped, pdbId=%u", m_pdbId));
    }

    if (m_objSpaceMgrWorkerList != nullptr) {
        for (uint16 i = 0; i < m_numObjSpaceMgrWorkers; i++) {
            if (m_objSpaceMgrWorkerList[i] != nullptr) {
                m_objSpaceMgrWorkerList[i]->join();
                delete m_objSpaceMgrWorkerList[i];
                m_objSpaceMgrWorkerList[i] = nullptr;
                ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
                    ErrMsg("[StopBgThread] m_objSpaceMgrWorker %hu pdbId %u stopped!", i, m_pdbId));
            }
        }
        DstorePfree(m_objSpaceMgrWorkerList);
        m_objSpaceMgrWorkerList = nullptr;
    }
}

void StoragePdb::StopBgThread(bool needFullCheckpoint, bool usedToDrPdbreplica)
{
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBgThread] Start to stop bg threads, pdbId=%u", m_pdbId));
    m_stopBgThread = true;
    if (m_collectMinBarrierThread != nullptr) {
        m_collectMinBarrierThread->join();
        delete m_collectMinBarrierThread;
        m_collectMinBarrierThread = nullptr;
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("[StopBgThread] m_collectMinBarrierThread stopped, pdbId=%u", m_pdbId));
    }
    if (m_barrierCreatorThread != nullptr) {
        m_barrierCreatorThread->join();
        delete m_barrierCreatorThread;
        m_barrierCreatorThread = nullptr;
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("[StopBgThread] m_barrierCreatorThread stopped, pdbId=%u", m_pdbId));
    }
    StopUpdateBarrierCsnThread();
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBgThread] m_updateBarrierCsnThread stopped, pdbId=%u", m_pdbId));
    if (m_recycleUndoThread != nullptr) {
        m_recycleUndoThread->join();
        delete m_recycleUndoThread;
        m_recycleUndoThread = nullptr;
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBgThread] m_recycleUndoThread stopped, pdbId=%u", m_pdbId));

    TryMarkSelfUndoStopping();
    StopSQLDependBgThread();
    ResetSelfUndoStopping();

    if (needFullCheckpoint) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("[StopBgThread] start requesting full checkpoint, pdbId=%u", m_pdbId));
        BufMgr *bufMgr = static_cast<BufMgr *>(g_storageInstance->GetBufferMgr());
        StorageReleasePanic(bufMgr == nullptr, MODULE_FRAMEWORK, ErrMsg("[StopBgThread] bufMgr is null."));
        RetStatus ret = bufMgr->FlushAll(false, true, m_pdbId);
        ret = STORAGE_FUNC_SUCC(ret) ? StoragePdb::FullCheckpoint() : DSTORE_FAIL;
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("[StopBgThread] failed to request full checkpoint."));
        } else {
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
                ErrMsg("[StopBgThread] finish requesting full checkpoint, pdbId=%u", m_pdbId));
        }
    } else {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("[StopBgThread] skip requesting full checkpoint, pdbId=%u", m_pdbId));
    }

    if (!usedToDrPdbreplica && m_walMgr != nullptr && m_walMgr->GetWalStreamManager() != nullptr) {
        WalStreamManager *walStreamManager = m_walMgr->GetWalStreamManager();
        dlist_mutable_iter iter = {};
        WalStreamNode *walStreamNode = nullptr;
        WalStream *walStream = nullptr;
        while ((walStreamNode = walStreamManager->GetNextWalStream(&iter)) != nullptr) {
            walStream = walStreamNode->walStream;
            WalId walId = walStream->GetWalId();
            walStreamManager->GetWalStream(walId)->GetWalFileManager()->StopRecycleWalFileWorker();
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
                ErrMsg("[StopBgThread] RecycleWalFileWorker of stream %lu pdb %u stopped!", walId, m_pdbId));
        }
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
                ErrMsg("[StopBgThread] RecycleWalFileWorker stopped, pdbId=%u", m_pdbId));

    if (m_standbyMonitorThread != nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[StopBgThread] m_standbyMonitorThread not stopped, pdbId=%u", m_pdbId));
        return;
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBgThread] m_standbyMonitorThread stopped, pdbId=%u", m_pdbId));

    if (usedToDrPdbreplica) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("[StopBgThread] No need stop Ckpter and bgWriter for dr promote or demote, Finished stopping bg "
            "threads of pdb %u",
            m_pdbId));
        return;
    }

    if (m_checkpointMgr != nullptr) {
        m_checkpointMgr->StopCheckpointer();
    }
    if (m_checkpointThread != nullptr) {
        m_checkpointThread->join();
        delete m_checkpointThread;
        m_checkpointThread = nullptr;
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBgThread] m_checkpointThread stopped, pdbId=%u", m_pdbId));

    if (m_bgPageWriterMgr != nullptr) {
        m_bgPageWriterMgr->StopAllBgPageWriter();
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBgThread] All BgPageWriter stopped, pdbId=%u", m_pdbId));
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBgThread] Finished stopping bg threads of pdb %u", m_pdbId));
}

void StoragePdb::InitRecoveryThreadAndWaitDone()
{
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("InitRecoveryThreadAndWaitDone start for pdb %hhu.", m_pdbId));
    std::thread recoveryThread = std::thread(&StoragePdb::RecoveryThreadMain, this);
    recoveryThread.join();

    /* dirty pages produced by redo this pdb wal must succeed to checkpoint this pdb */
    if (STORAGE_FUNC_FAIL(g_storageInstance->GetBufferMgr()->FlushAll(false, false, m_pdbId))) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Checkpoint pdb %hhu fail.", m_pdbId));
    }
}

void StoragePdb::RecycleUndo()
{
    CommitSeqNo recycleMinCsn;
    static constexpr long sleepIntervalInMs = 50;
    while (!m_stopBgThread) {
        /* sleep first */
        for (int i = 0; i < sleepIntervalInMs && !m_stopBgThread; i++) {
            std::this_thread::sleep_for(std::chrono::microseconds(static_cast<long>(STORAGE_USECS_PER_MSEC)));
        }
        /* check for thread exit after sleep */
        if (m_stopBgThread) {
            break;
        }

        /* If enable flashback need to consider the flashback csnmin */
        recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(m_pdbId);
        /* Now do the real recycle */
        m_undoMgr->Recycle(recycleMinCsn);
    }
}

void StoragePdb::RecycleUndoThreadMain()
{
    InitSigForModule("UndoRecycler");
    CreateThreadAndRegister(true, true, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY, "UndoRecycler");
    while (IsPdbInRestoreFromBackup()) {
        GaussUsleep(BACKUP_RESTORE_SLEEP_TIME);
        if (m_stopBgThread) {
            break;
        }
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("pdb %s is in backup restore.", m_pdbName));
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("UndoRecycler m_pdbId:%u start.", m_pdbId));
    RecycleUndo();
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("UndoRecycler m_pdbId:%u stop.", m_pdbId));
    UnregisterThread();
}

void StoragePdb::AsyncRecoverUndo()
{
    m_rollbackEnd.store(false, std::memory_order_release);
    m_undoMgr->RecoverUndoZone();
    m_rollbackEnd.store(true, std::memory_order_release);
}

void StoragePdb::AsyncRecoverUndoThreadMain()
{
    InitSigForModule("AsyncRecover");
    CreateThreadAndRegister(true, true, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY, "AsyncRecover");
    if (IsPdbInRestoreFromBackup()) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("AsyncRecover m_pdbName:%s m_pdbId:%hhu.", m_pdbName, m_pdbId));
    } else {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("AsyncRecover m_pdbId:%hhu start.", m_pdbId));
        AsyncRecoverUndo();
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("AsyncRecover m_pdbId:%hhu stop.", m_pdbId));
    }

    UnregisterThread();
}

void StoragePdb::CheckpointThreadMain()
{
    InitSigForModule("Checkpoint");
    CreateThreadAndRegister(true, true, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY, "Checkpoint");
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("CheckpointThread start, pdb %u.", m_pdbId));
    m_checkpointMgr->Init();
    m_checkpointMgr->CheckpointerMain();
    m_checkpointMgr->Destroy();
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("CheckpointThread stop, pdb %u.", m_pdbId));
    UnregisterThread();
}

void StoragePdb::StandbyMonitorThreadMain()
{
    CreateThreadAndRegister(true, true, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY, "StandbyMonitor");
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("StandbyMonitorThread start, pdb %u.", m_pdbId));
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("StandbyMonitorThread stop, pdb %u.", m_pdbId));
    UnregisterThread();
}

void StoragePdb::BarrierCreatorLoop()
{
    const long CREATE_INTERNAL_SLEEP_US = 800000L; // 800ms
    while (!m_stopBgThread && !m_stopBarrierThread) {
        CommitSeqNo barrierCsn = g_storageInstance->GetCsnMgr()->GetLocalBarrierCsnMin();
        if (barrierCsn == INVALID_CSN) {
            GaussUsleep(CREATE_INTERNAL_SLEEP_US);
            continue;
        }
    }
}

void StoragePdb::BarrierCreatorThreadMain()
{
    InitSigForModule("BarrierCreator");
    CreateThreadAndRegister(true, true, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY, "BarrierCreator");
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("BarrierCreatorThread start, pdbId=%u", m_pdbId));
    BarrierCreatorLoop();
    DeleteAllFromNoSendBarrierList();
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("BarrierCreatorThread stop, pdbId=%u", m_pdbId));
    UnregisterThread();
}

CommitSeqNo StoragePdb::GetCurrentMinBarrierCsn()
{
    return MAX_COMMITSEQNO;
}

void StoragePdb::CollectMinBarrierLoop()
{
    const long CREATE_INTERNAL_SLEEP_US = 800000L; // 800ms
    while (!m_stopBgThread && !m_stopBarrierThread) {
        GaussUsleep(CREATE_INTERNAL_SLEEP_US);
    }
}

void StoragePdb::CollectMinBarrierThreadMain()
{
    InitSigForModule("CollectBarrier");
    CreateThreadAndRegister(true, true, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY, "CollectBarrier");
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("CollectBarrierThread start, pdbId=%u", m_pdbId));
    CollectMinBarrierLoop();
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("CollectBarrierThread stop, pdbId=%u", m_pdbId));
    UnregisterThread();
}

void StoragePdb::UpdateBarrierCsnThreadMain()
{
    InitSigForModule("UpdatBarrierCsn");
    CreateThreadAndRegister(true, true, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY, "UpdatBarrierCsn");

    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("pdb %hhu UpdateBarrierCsn start.", m_pdbId));
    UpdateBarrierCsnThreadLoop();
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("pdb %hhu UpdateBarrierCsn stop.", m_pdbId));

    UnregisterThread();
}

void StoragePdb::UpdateBarrierCsnThreadLoop()
{
    const long CREATE_INTERNAL_SLEEP_US = 10000L; // 10ms
    while (!m_stopPdbUpdateBarrier) {
        GaussUsleep(CREATE_INTERNAL_SLEEP_US);
    }
}

void StoragePdb::RecoveryThreadMain()
{
    DstoreSetMemoryOutOfControl();
    InitSigForModule("Recovery");
    CreateThreadAndRegister(true, true, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY, "Recovery");

    WalUtils::HandleWalThreadCpuBind("RecoveryThrd");

    /* Step 1: Get which wal streams need to be recoveried */
    WalId walId = thrd->m_walWriterContext->GetWalId();
    /* Step 2: Recovery wal */
    if (walId == INVALID_WAL_ID) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("no valid wal for recover and stop recovery thread"));
    } else {
        /* Step 2: Recovery wal */
        if (STORAGE_FUNC_FAIL(m_walMgr->Recovery(walId, m_pdbId))) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("PDB(%hhu) recovery fail.", m_pdbId));
        }
    }
    WalUtils::HandleWalThreadCpuUnbind("RecoveryThrd");
    UnregisterThread();
    DstoreSetMemoryInControl();
}

RetStatus StoragePdb::InitBgWriterThread()
{
    m_bgWriterSlotId = INVALID_BGWRITER_SLOT_ID;
    if (m_bgPageWriterMgr == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("m_bgPageWriterMgr is null."));
        return DSTORE_FAIL;
    }
    dlist_mutable_iter iter = {};
    WalStreamNode *walStreamNode = nullptr;
    WalStream *walStream = nullptr;

    WalStreamFilter filter;
    if (GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY) {
        filter = WalStream::IsWalStreamNeedCkpt;
    } else {
        filter = WalStream::IsWalStreamForWrite;
    }
    bool primarySlot = GetPdbRoleMode() == PdbRoleMode::PDB_PRIMARY;

    while ((walStreamNode = m_walMgr->GetWalStreamManager()->GetNextWalStream(&iter, filter)) != nullptr) {
        walStream = walStreamNode->walStream;
        RetStatus ret = CreateBgWriterThrdForWalStream(walStream, primarySlot);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("walId %lu bgwriter start failed", walStream->GetWalId()));
        }
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CreateBgWriterThrdForWalStream(const class WalStream *walStream, bool primarySlot)
{
    RetStatus ret = m_bgPageWriterMgr->CreateBgPageWriter(walStream, &m_bgWriterSlotId, primarySlot);
    if (STORAGE_FUNC_SUCC(ret)) {
        int64 slotId;
        if (!primarySlot) {
            slotId = m_bgPageWriterMgr->GetBgWriterSlotIdByWalId(walStream->GetWalId());
        } else {
            slotId = m_bgWriterSlotId;
        }
        if (slotId < 0 || slotId >= DIRTY_PAGE_QUEUE_MAX_SIZE) {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg(
                "CreateBgWriterThrdForWalStream, cannot find this slotId %ld primarySlot %d.", slotId, primarySlot));
            return DSTORE_FAIL;
        }
        ret = m_bgPageWriterMgr->StartupBgPageWriter(slotId);
    }
    return ret;
}

int64 StoragePdb::GetBgWriterSlotId(uint64 walId) const
{
    if (m_bgWriterSlotId == INVALID_BGWRITER_SLOT_ID) {
        return m_bgPageWriterMgr->GetBgWriterSlotIdByWalId(walId);
    }
    return m_bgWriterSlotId;
}

void StoragePdb::SetBgWriterSlotId(const int64 slotId)
{
    m_bgWriterSlotId = slotId;
}

BgDiskPageMasterWriter *StoragePdb::GetBgDiskPageMasterWriter() const
{
    if (m_bgPageWriterMgr == nullptr || m_bgWriterSlotId == INVALID_BGWRITER_SLOT_ID) {
        return nullptr;
    }
    return m_bgPageWriterMgr->GetBgPageWriterBySlot(m_bgWriterSlotId);
}

/*
 * A worker thread that monitors the task queue.
 */
void StoragePdb::ObjSpaceMgrWorkerThreadMain(uint32 workerId) noexcept
{
    InitSigForModule("ObjSpaceMgr");
    (void)g_storageInstance->CreateThreadAndRegister(this->GetPdbId(), false, "ObjSpaceMgr", true,
                                                     ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
#ifdef __aarch64__
    thrd->SetNumaId(1);
#endif
    (void)thrd->InitTransactionRuntime(this->GetPdbId(), nullptr, nullptr, false);
    thrd->SetIsObjSpaceThrd(true);
    BtreeRecycleWorker btrRecycleWorker(workerId, this->GetPdbId());
    constexpr uint64 MAX_DURATION_MS = 5 * 1000 * 1000; // 5s
    while (!m_stopBgThread) {
        ObjSpaceMgrTask *task = nullptr;

        /* Pop task from task queue to avoid 2 threads working on the same task */
        task = m_objSpaceMgr->GetObjSpaceMgrTask(workerId);
        if (task != nullptr) {
            ObjSpaceMgrTaskInfo *taskInfo = task->m_taskInfo;
            if (taskInfo == nullptr) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                    ErrMsg("[ObjSpaceMgr (threadId = %lu workerId = %u)] taskInfo is null.",
                        thrd->GetThreadId(), workerId));
                continue;
            }
            ObjSpaceMgrTaskType taskType = taskInfo->GetTaskType();
            ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
                ErrMsg("[ObjSpaceMgr (threadId = %lu workerId = %u)] Start to execute ObjSpaceMgrTask(%u).",
                    thrd->GetThreadId(), workerId, taskType));
            RetStatus taskRet = DSTORE_SUCC;
            uint64 start = GetSystemTimeInMicrosecond();
            if (taskType == RECYCLE_BTREE_TASK) {
                taskRet = btrRecycleWorker.BtreeRecycleExecute(task);
            } else {
                /* Execute the task here */
                taskRet = task->Execute();
            }
            if (unlikely(taskRet == DSTORE_FAIL)) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                    ErrMsg("[ObjSpaceMgr (threadId = %lu workerId = %u)] Failed to execute ObjSpaceMgrTask(%u).",
                        thrd->GetThreadId(), workerId, taskType));
            }
            delete task;
            ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
                ErrMsg("[ObjSpaceMgr (threadId = %lu workerId = %u)] Finish to execute ObjSpaceMgrTask(%u).",
                    thrd->GetThreadId(), workerId, taskType));
            uint64 cost = GetSystemTimeInMicrosecond() - start;
            if (cost >= MAX_DURATION_MS) {
                ErrLog(DSTORE_LOG, MODULE_SEGMENT,
                    ErrMsg("[ObjSpaceMgr (threadId = %lu workerId = %u)] The execution time(%lu) "
                        "of taskType(%u) is too long.", thrd->GetThreadId(), workerId, cost, taskType));
            }
        } else {
            ObjSpaceMgrWorkerSleep();
        }
    }

    btrRecycleWorker.BtreeRecycleWorkerStop();

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
           ErrMsg("[ObjSpaceMgr (threadId = %lu workerId = %u)] Exited.", thrd->GetThreadId(), workerId));
    g_storageInstance->UnregisterThread();
}
void StoragePdb::CreateThreadAndRegister(bool isNeedInitStorageContext, bool needCommBuffer, ThreadMemoryLevel level,
    const char *threadName)
{
    (void)g_storageInstance->CreateThreadAndRegister(m_pdbId, false, threadName, true, level);
    if (isNeedInitStorageContext) {
        (void)thrd->InitStorageContext(m_pdbId);
    }
    thrd->SetNeedCommBuffer(needCommBuffer);
}

void StoragePdb::UnregisterThread()
{
    if (thrd != nullptr) {
        g_storageInstance->RemoveVisibleThread(thrd);
        thrd->Destroy();
        DstorePfreeAligned(thrd);
        thrd = nullptr;
    }
}
void StoragePdb::InitSigForModule(const char *moduleName) const
{
    InitSignalMask();
    (void)pthread_setname_np(pthread_self(), moduleName);
}

BgPageWriterMgr *StoragePdb::GetBgPageWriterMgr()
{
    return m_bgPageWriterMgr;
}

RetStatus StoragePdb::BgPageWriterMgrInit()
{
    if (m_bgPageWriterMgr != nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("m_bgPageWriterMgr is not null."));
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoSwitch(g_storageInstance->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_BUFFER));
    DstoreMemoryContext mcxt = DstoreAllocSetContextCreate(g_dstoreCurrentMemoryContext, "BgPageWriterMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_INITSIZE,
        MemoryContextType::SHARED_CONTEXT);
    m_bgPageWriterMgr = DstoreNew(mcxt) BgPageWriterMgr(mcxt, m_pdbId);
    if (STORAGE_VAR_NULL(m_bgPageWriterMgr)) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed to malloc for pgPageWriter."));
        return DSTORE_FAIL;
    }
    m_bgPageWriterMgr->Init();
    /* get all wal stream and create BgPageWriter for each wal stream */
    return DSTORE_SUCC;
}

void StoragePdb::BgPageWriterMgrDestroy()
{
    if (m_bgPageWriterMgr != nullptr) {
        m_bgPageWriterMgr->StopAllBgPageWriter();
        m_bgPageWriterMgr->Destroy();
        delete m_bgPageWriterMgr;
        m_bgPageWriterMgr = nullptr;
    }
}

RetStatus StoragePdb::CreateWalPath(char *pdbPath) const
{
    char path[MAXPGPATH] = {};
    int rc = memset_s(path, MAXPGPATH, 0, MAXPGPATH);
    storage_securec_check(rc, "", "");
    rc = sprintf_s(path, MAXPGPATH, "%s/wal", pdbPath);
    storage_securec_check_ss(rc);

    if (mkdir(path, S_IRWXU) < 0) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to create path(%s) for wal.", path));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::DropWalPath(char *pdbPath) const
{
    char path[MAXPGPATH] = {};
    int rc = sprintf_s(path, MAXPGPATH, "%s/wal", pdbPath);
    storage_securec_check_ss(rc);

    if (rmdir(path) < 0) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to create path(%s) for wal.", path));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CreateDecodeDictFile(PdbId pdbId, const char *pdbPath)
{
    UNUSE_PARAM RetStatus ret = DecodeDictFile::CreateFile(
        pdbId,
        DECODEDICT_FILE_1_NAME,
        DECODEDICT_FILE_2_NAME,
        pdbPath);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Create DecodeDict File, filename=%s.", DECODEDICT_FILE_1_NAME));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::GetDecodeDictFileSizeVfs(int64 &partSize)
{
    partSize = 0;
    int64 fileSize = 0;
    if (m_vfs->FileExists(DECODEDICT_FILE_1_NAME)) {
        if (GetFileSizeWithOpen(DECODEDICT_FILE_1_NAME, fileSize) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
        partSize += fileSize;
    }

    if (m_vfs->FileExists(DECODEDICT_FILE_2_NAME)) {
        if (GetFileSizeWithOpen(DECODEDICT_FILE_2_NAME, fileSize) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
        partSize += fileSize;
    }
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CopySpecifySizeInFile(VFSAdapter *newVfs, VFSAdapter *srcVfs, FileId fileId,
                                            uint32 readSize, int64 offset)
{
    int64 realReadSize;
    char *buffer = static_cast<char *>(DstorePallocAligned(static_cast<Size>(readSize), BLCKSZ));
    if (STORAGE_VAR_NULL(buffer)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Alloc memory failed."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(srcVfs->Pread(fileId, buffer, static_cast<uint64>(readSize), offset, &realReadSize))) {
        DstorePfreeAligned(buffer);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Pread vfs file failed."));
        return DSTORE_FAIL;
    }
    StorageReleasePanic(readSize != realReadSize, MODULE_WAL, ErrMsg("readSize not equals realReadSize"));
    RetStatus retStatus = newVfs->PwriteSync(fileId, buffer, static_cast<uint64>(readSize),
                                             static_cast<off_t>(offset));
    if (STORAGE_FUNC_FAIL(retStatus)) {
        DstorePfreeAligned(buffer);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("PwriteAsync vfs file failed."));
        return DSTORE_FAIL;
    }
    DstorePfreeAligned(buffer);
    return DSTORE_SUCC;
}

RetStatus StoragePdb::CopySpecifySizeInWalFile(VFSAdapter *srcVfs, FileDescriptor *srcFileFd, VFSAdapter *newVfs,
                                               FileDescriptor *destFileFd, uint32 readSize, int64 offset)
{
    int64 realReadSize;
    if (readSize == 0) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("CopySpecifySizeInWalFile offset:%ld size:%u.",
            offset, readSize));
        return DSTORE_SUCC;
    }
    char *buffer;
    char *copyBuffer;

    if (srcVfs == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("srcVfs is null."));
        return DSTORE_FAIL;
    }
    if (WalReadWriteWithDio()) {
        int64 adaptedOffset = offset - (offset % WAL_DIO_BLOCK_SIZE);
        int32 adaptedDataLen = ((offset + static_cast<int64>(readSize) - 1) / WAL_DIO_BLOCK_SIZE + 1) *
                                WAL_DIO_BLOCK_SIZE - adaptedOffset;
        buffer = static_cast<char *>(DstorePallocAligned(static_cast<Size>(adaptedDataLen), WAL_DIO_BLOCK_SIZE));
        if (STORAGE_VAR_NULL(buffer)) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Alloc memory:%d failed.", adaptedDataLen));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(
            srcVfs->Pread(srcFileFd, buffer, static_cast<uint64>(adaptedDataLen), adaptedOffset, &realReadSize))) {
            DstorePfreeAligned(buffer);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Pread vfs file failed."));
            return DSTORE_FAIL;
        }
        copyBuffer = buffer + (offset % WAL_DIO_BLOCK_SIZE);
    } else {
        buffer = static_cast<char *>(DstorePallocAligned(static_cast<Size>(readSize), WAL_DIO_BLOCK_SIZE));
        if (STORAGE_VAR_NULL(buffer)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Alloc memory:%u failed.", readSize));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(
            srcVfs->Pread(srcFileFd, buffer, static_cast<uint64>(readSize), offset, &realReadSize))) {
            DstorePfreeAligned(buffer);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Pread vfs file failed."));
            return DSTORE_FAIL;
        }
        copyBuffer = buffer;
    }
    StorageReleasePanic(readSize < realReadSize, MODULE_FRAMEWORK, ErrMsg("readSize less than realReadSize."));
    RetStatus retStatus = newVfs->PwriteSync(destFileFd, copyBuffer, static_cast<uint64>(readSize),
                                             static_cast<off_t>(offset));
    if (STORAGE_FUNC_FAIL(retStatus)) {
        DstorePfreeAligned(buffer);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("PwriteAsync vfs file failed."));
        return DSTORE_FAIL;
    }
    DstorePfreeAligned(buffer);
    return DSTORE_SUCC;
}

void StoragePdb::InitTableSpaceMgr()
{
    if (m_tablespaceMgr == nullptr) {
        m_tablespaceMgr = DstoreNew(g_dstoreCurrentMemoryContext) TablespaceMgr(m_pdbId, m_controlFile, m_vfs);
        StorageReleasePanic(m_tablespaceMgr == nullptr, MODULE_FRAMEWORK, ErrMsg("InitTablespaceMgr fail."));
    }
    if (STORAGE_FUNC_FAIL(m_tablespaceMgr->InitTempTbsBitmapPageTable())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("InitTempTbsBitmapPageTable failed."));
    }
}

RelMap *GetRelMapByType(RelMapType type, BuiltinRelMap *builtinMap)
{
    if (type == RelMapType::RELMAP_SHARED) {
        return &builtinMap->sharedRelMap;
    } else if (type == RelMapType::RELMAP_LOCAL) {
        return &builtinMap->localRelMap;
    } else {
        return nullptr;
    }
}

RetStatus StoragePdb::GetBuiltinRelMap(RelMapType type, RelMapNode *nodes, int *count)
{
    if (!nodes || !count || type == RelMapType::RELMAP_MAX) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK,
               ErrMsg("invalid parameter. pdb(%u) type(%d)", m_pdbId, static_cast<int>(type)));
        return DSTORE_FAIL;
    }
    RWLockWrLock(&m_builtinRelMap.relMapRWLock);
    RelMap *relMap = GetRelMapByType(type, &m_builtinRelMap);
    if (!(relMap->isValid.load(std::memory_order_acquire))) {
        /* read relmap from control file */
        relMap->count = RELMAP_NODE_MAX;
        if (m_controlFile->GetAllSysTableItem(static_cast<int>(type), relMap->relmaps, relMap->count)) {
            RWLockWrUnlock(&m_builtinRelMap.relMapRWLock);
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Failed to get built-in relmap pdb(%u)", m_pdbId));
            return DSTORE_FAIL;
        }
        relMap->isValid.store(true, std::memory_order_release);
    }

    if (relMap->count <= 0) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("Failed to get built-in relmap, invalid relmap count(%d), pdb(%u)",
            relMap->count, m_pdbId));
    } else {
        if (relMap->count > RELMAP_NODE_MAX) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK,
                ErrMsg("Failed to get built-in relmap, invalid relmap count(%d), pdb(%u)",
                relMap->count, m_pdbId));
        }
        ErrLog(DSTORE_DEBUG1, MODULE_FRAMEWORK,
            ErrMsg("GetBuiltinRelMap type(%d) count(%d) oid(%u) pageId(%hu, %u) pdbId(%u).",
                static_cast<int>(type), relMap->count, (relMap->relmaps)[0].sysTableOid,
                (relMap->relmaps)[0].segmentId.m_fileId, (relMap->relmaps)[0].segmentId.m_blockId, m_pdbId));
    }

    if (relMap->count > 0) {
        *count = relMap->count;
        for (int i = 0; i < relMap->count; i++) {
            nodes[i].relid = relMap->relmaps[i].sysTableOid;
            nodes[i].segid = relMap->relmaps[i].segmentId;
        }
    }
    RWLockWrUnlock(&m_builtinRelMap.relMapRWLock);
    return DSTORE_SUCC;
}

void StoragePdb::GenerateWriteBuiltinRelMapWal(RelMapType type, RelMapNode *nodes, int count)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_FRAMEWORK, ErrMsg("Wal has not atomic begin."));

    /* record WalRecordSystableWriteBuiltinRelMap */
    uint32 walDataSize = sizeof(WalRecordSystableWriteBuiltinRelMap) + sizeof(RelMapNode) * count;
    WalRecordSystableWriteBuiltinRelMap *walData =
        static_cast<WalRecordSystableWriteBuiltinRelMap *>(DstorePalloc(walDataSize));
    while (STORAGE_VAR_NULL(walData)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("GenerateWriteBuiltinRelMapWal alloc memory for size %u failed, retry it.", walDataSize));
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        walData = static_cast<WalRecordSystableWriteBuiltinRelMap *>(DstorePalloc(walDataSize));
    }
    walData->SetHeader(WAL_SYSTABLE_WRITE_BUILTIN_RELMAP, walDataSize, type, count);
    walData->SetData(static_cast<char *>(static_cast<void *>(nodes)), sizeof(RelMapNode) * count);

    walContext->PutNewWalRecord(walData);
    DstorePfree(walData);
}

RetStatus StoragePdb::WriteBuiltinRelMap(RelMapType type, RelMapNode *nodes, int count)
{
    if (!nodes || count < 0 || count > RELMAP_NODE_MAX || type == RelMapType::RELMAP_MAX) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("invalid parameter. pdb(%u) type(%d) count(%d)", m_pdbId, static_cast<int>(type), count));
        return DSTORE_FAIL;
    }
    RWLockWrLock(&m_builtinRelMap.relMapRWLock);
    RelMap *relMap = GetRelMapByType(type, &m_builtinRelMap);
    StorageReleasePanic(relMap == nullptr, MODULE_FRAMEWORK, ErrMsg("relMap is null."));

    /* Step 1: Generate redo record */
    StorageAssert(thrd->GetActiveTransaction() != nullptr);
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(thrd->GetCurrentXid());
    GenerateWriteBuiltinRelMapWal(type, nodes, count);
    (void)walContext->EndAtomicWal();

    relMap->count = count;
    /* step 1, update cache */
    for (int i = 0; i < count; i++) {
        relMap->relmaps[i].sysTableOid = nodes[i].relid;
        relMap->relmaps[i].segmentId = nodes[i].segid;
    }

    /* step 2, write relmap to contolfile */
    RetStatus ret =
        m_controlFile->WriteAllSysTableItem(static_cast<int>(type), relMap->relmaps, relMap->count);
    relMap->isValid.store(true, std::memory_order_release);
    RWLockWrUnlock(&m_builtinRelMap.relMapRWLock);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Failed to write built-in relmap pdb(%u)", m_pdbId));
        return DSTORE_FAIL;
    }
    return ret;
}

RetStatus StoragePdb::WriteBuiltinRelMapRedo(RelMapType type, RelMapNode *nodes, int count)
{
    if (!nodes || count < 0 || count > RELMAP_NODE_MAX || type == RelMapType::RELMAP_MAX) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("invalid parameter. pdb(%u) type(%d) count(%d)", m_pdbId, static_cast<int>(type), count));
        return DSTORE_FAIL;
    }
    RWLockWrLock(&m_builtinRelMap.relMapRWLock);
    RelMap *relMap = GetRelMapByType(type, &m_builtinRelMap);
    StorageReleasePanic(relMap == nullptr, MODULE_FRAMEWORK, ErrMsg("relMap is null."));

    relMap->count = count;
    /* step 1, update cache */
    for (int i = 0; i < count; i++) {
        relMap->relmaps[i].sysTableOid = nodes[i].relid;
        relMap->relmaps[i].segmentId = nodes[i].segid;
    }

    /* step 2, write relmap to contolfile */
    RetStatus ret =
        m_controlFile->WriteAllSysTableItem(static_cast<int>(type), relMap->relmaps, relMap->count);
    relMap->isValid.store(true, std::memory_order_release);
    RWLockWrUnlock(&m_builtinRelMap.relMapRWLock);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Failed to write built-in relmap pdb(%u)", m_pdbId));
        return DSTORE_FAIL;
    }
    return ret;
}

GsStatMsgBgWriter* StoragePdb::GetGsStatMsgBgWriter()
{
    return &m_statMsgBgWriter;
}

bool StoragePdb::IsPdbInRestoreFromBackup()
{
    StorageReleasePanic(m_vfs == nullptr, MODULE_FRAMEWORK, ErrMsg("m_vfs is null."));
    if (!m_vfs->FileExists(RESTORE_FILE_NAME)) {
        return false;
    }
    return true;
}

void StoragePdb::TryMarkSelfUndoStopping()
{
    if (unlikely((m_undoMgr == nullptr) || (!m_asyncRecoverUndoThread))) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("TryMarkSelfUndoStopping failed for pdb %u, "
                      "no valid undo mgr or undo thread.",
                      m_pdbId));
        return;
    }
    return m_undoMgr->StopRecoverUndoZone();
}

void StoragePdb::ResetSelfUndoStopping()
{
    if (unlikely((m_undoMgr == nullptr) || (m_asyncRecoverUndoThread != nullptr))) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("ResetSelfUndoStopping failed for pdb %u, "
                      "no valid undo mgr, or undo thread is still alive.",
                      m_pdbId));
        return;
    }
    return m_undoMgr->ResetStopRecoverUndoZone();
}

void StoragePdb::SetPdbInfo(ControlPdbInfoPageItemData *item)
{
    SetPdbUuid(item->pdbUuid);
    SetPdbStatus(item->pdbStatus);
    SetPdbRoleMode(item->pdbRoleMode);
}

bool StoragePdb::IsPrimaryOrBeginPromote()
{
    bool isStandbyPdb = false;
    bool isPromoting = false;
    if (g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_MEMORY) {
        return false;
    }

    isStandbyPdb = GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY;
    if (m_pdbId < FIRST_USER_PDB_ID) {
        return !isStandbyPdb;
    }

    ControlPdbInfoPageItemData *pdbInfo =
        g_storageInstance->GetPdb(g_defaultPdbId)->GetControlFile()->GetPdbInfoById(m_pdbId);
    StorageReleasePanic(pdbInfo == nullptr, MODULE_PDBREPLICA, ErrMsg("pdbInfo is nullptr for pdbId %hhu", m_pdbId));
    isPromoting = pdbInfo->pdbSwitchStatus == PdbSwitchStatus::BEGIN_PROMOTE;
    if (!isStandbyPdb || isPromoting) {
        return true;
    }
    return false;
}
PdbRoleMode StoragePdb::GetPdbRoleMode()
{
    PdbRoleMode roleMode = static_cast<PdbRoleMode>(m_pdbRoleMode.load(std::memory_order_acquire));
    /* If roleMode is PDB_INVALID, get it from rootpdb's controlfile */
    if (unlikely(roleMode == PdbRoleMode::PDB_INVALID)) {
        StoragePdb *rootpdb = g_storageInstance->GetPdb(g_defaultPdbId);
        if (STORAGE_VAR_NULL(rootpdb) || STORAGE_VAR_NULL(rootpdb->GetControlFile())) {
            ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("Rootpdb is not alive."));
            return PdbRoleMode::PDB_INVALID;
        }
        RetStatus ret = rootpdb->GetControlFile()->GetPdbRoleMode(GetPdbId(), roleMode);
        if (ret != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("Get pdb role mode failed."));
            return PdbRoleMode::PDB_INVALID;
        }
        SetPdbRoleMode(roleMode);
    }

    return roleMode;
}

CommitSeqNo StoragePdb::GetPdbRecycleCsnMin()
{
    if (m_pdbId < FIRST_USER_PDB_ID) {
        /* only user pdb has primary-standby relation. */
        return INVALID_CSN;
    }

    CommitSeqNo recycleCsnMin = m_pdbRecycleCsnMin.load(std::memory_order_acquire);
    /* If recycleCsnMin is MAX_COMMITSEQNO, get it from rootpdb's controlfile */
    if (recycleCsnMin == MAX_COMMITSEQNO) {
        StoragePdb *rootpdb = g_storageInstance->GetPdb(g_defaultPdbId);
        if (STORAGE_VAR_NULL(rootpdb) || STORAGE_VAR_NULL(rootpdb->GetControlFile())) {
            ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg(
                "[PdbRecycleCsnMin] Rootpdb is not alive, pdb %u.", m_pdbId));
            return INVALID_CSN;
        }
        RetStatus ret = rootpdb->GetControlFile()->GetPdbRecycleCsnMin(GetPdbId(), recycleCsnMin);
        if (ret != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg(
                "[PdbRecycleCsnMin] Get pdb recycleCsnMin failed, pdb %u.", m_pdbId));
            return INVALID_CSN;
        }
        if (recycleCsnMin == MAX_COMMITSEQNO) {
            ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg(
                "[PdbRecycleCsnMin] Get pdb recycleCsnMin MAX_COMMITSEQNO, pdb %u.", m_pdbId));
            recycleCsnMin = INVALID_CSN;
        }
        SetPdbRecycleCsnMin(recycleCsnMin);
    }

    return recycleCsnMin;
}

void StoragePdb::GenerateVfsName(PdbId pdbId, char* vfsName, uint32 len)
{
    const char *tenantName = g_storageInstance->GetGuc()->tenantConfig->tenantName;
    errno_t rc = EOK;
    if (pdbId == PDB_TEMPLATEA_ID) {
        const char *format = "%s.vfs.templatea";
        int ret = sprintf_s(vfsName, len, format, tenantName);
        if (ret < 0) {
            StorageReleasePanic(true, MODULE_PDB, ErrMsg("Generate vfs name failed"));
        }
    } else if (pdbId == PDB_TEMPLATE0_ID) {
        char* template0VfsName = g_storageInstance->GetGuc()->tenantConfig->storageConfig.template0VfsName;
        rc = strcpy_s(vfsName, len, template0VfsName);
        storage_securec_check(rc, "\0", "\0");
    } else if (pdbId == PDB_TEMPLATE1_ID) {
        char* template1VfsName = g_storageInstance->GetGuc()->tenantConfig->storageConfig.template1VfsName;
        rc = strcpy_s(vfsName, len, template1VfsName);
        storage_securec_check(rc, "\0", "\0");
    } else if (pdbId == PDB_ROOT_ID) {
        char* rootVfsName = g_storageInstance->GetGuc()->tenantConfig->storageConfig.rootpdbVfsName;
        rc = strcpy_s(vfsName, len, rootVfsName);
        storage_securec_check(rc, "\0", "\0");
    } else if (pdbId >= FIRST_USER_PDB_ID && pdbId <= PDB_MAX_ID) {
        const char *format = "%s.vfs.PDB_%u";
        int ret = sprintf_s(vfsName, len, format, tenantName, pdbId);
        if (ret < 0) {
            StorageReleasePanic(true, MODULE_PDB, ErrMsg("Generate vfs name failed"));
        }
    } else {
        ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("Generate vfs name failed, invalid pdbId."));
    }
}

void StoragePdb::StopBarrierThread()
{
    m_stopBarrierThread = true;
    if (m_collectMinBarrierThread != nullptr) {
        m_collectMinBarrierThread->join();
        delete m_collectMinBarrierThread;
        m_collectMinBarrierThread = nullptr;
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBarrierThread] m_collectMinBarrierThread stopped!"));
    }
    if (m_barrierCreatorThread != nullptr) {
        m_barrierCreatorThread->join();
        delete m_barrierCreatorThread;
        m_barrierCreatorThread = nullptr;
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBarrierThread] m_barrierCreatorThread stopped!"));
    }
    StopUpdateBarrierCsnThread();
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[StopBarrierThread] m_updateBarrierCsnThread stopped!"));
    m_stopBarrierThread = false;
}

RetStatus StoragePdb::GetAllTransactionsNeedRollbackByBarrier(CommitSeqNo barrierCsn,
    TransactionNeedRollbackInfo **rollbackArray, uint64 &totalRollbackNum)
{
    uint64 totalNum = m_numOfTransactionsNeedRollbackBarrier;
    if (totalNum == 0) {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Noneed rollback."));
        return DSTORE_SUCC;
    }
    *rollbackArray = static_cast<TransactionNeedRollbackInfo *>(
        DstorePalloc(totalNum * sizeof(TransactionNeedRollbackInfo)));
    if (unlikely(*rollbackArray == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
               ErrMsg("[Promote] GetAllTransactionsNeedRollback palloc failed, num: %lu size: %lu", totalNum,
                      totalNum * sizeof(TransactionNeedRollbackInfo)));
        return DSTORE_FAIL;
    }
 
    uint64 curRollbackCnt = 0;
    dlist_iter iter;
    dlist_foreach(iter, &m_transactionsNeedRollbackBarrierListHead) {
        TransactionsNeedRollbackListNode *listNode = dlist_container(TransactionsNeedRollbackListNode,
            dlistNode, iter.cur);
        if (listNode->info.csn != INVALID_CSN && listNode->info.csn > barrierCsn) {
            (*rollbackArray)[curRollbackCnt].csn = listNode->info.csn;
            (*rollbackArray)[curRollbackCnt].xid = listNode->info.xid;
            curRollbackCnt++;
        }
        ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("[Promote] GetAllTransactionsNeedRollbackByBarrier csn:%lu.", listNode->info.csn));
    }
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("[Promote] GetAllTransactionsNeedRollbackByBarrier curRollbackCnt:%lu", curRollbackCnt));
 
    totalRollbackNum = curRollbackCnt;
    return DSTORE_SUCC;
}
 
void StoragePdb::PushTransactionIntoBarrierRollbackList(TransactionsNeedRollbackListNode *node)
{
    pthread_rwlock_wrlock(&m_rollbackBarrierListLock);
    DListPushTail(&m_transactionsNeedRollbackBarrierListHead, &(node->dlistNode));
    m_numOfTransactionsNeedRollbackBarrier++;
    pthread_rwlock_unlock(&m_rollbackBarrierListLock);
}

void StoragePdb::UpdateCsnInBarrierRollbackListByXid(Xid xid, CommitSeqNo csn)
{
    pthread_rwlock_wrlock(&m_rollbackBarrierListLock);
    dlist_iter iter = {};
    dlist_reverse_foreach(iter, &m_transactionsNeedRollbackBarrierListHead) {
        TransactionsNeedRollbackListNode *trxNode =
            dlist_container(TransactionsNeedRollbackListNode, dlistNode, iter.cur);
        if (trxNode->info.csn == MAX_COMMITSEQNO && trxNode->info.xid == xid) {
            trxNode->info.csn = csn;
            ErrLog(DSTORE_DEBUG1, MODULE_WAL,
                   ErrMsg("UpdateTrxNeedRollbackBarrier update csn %lu which xid(%d, %lu).", csn,
                          static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
            break;
        }
    }
    pthread_rwlock_unlock(&m_rollbackBarrierListLock);
}
 
void StoragePdb::DeleteTrxnsFromBarrierRollbackListBeforeCsn(CommitSeqNo csn)
{
    pthread_rwlock_wrlock(&m_rollbackBarrierListLock);
    dlist_mutable_iter iter = {};
    dlist_foreach_modify(iter, &m_transactionsNeedRollbackBarrierListHead) {
        TransactionsNeedRollbackListNode *trxNode =
            dlist_container(TransactionsNeedRollbackListNode, dlistNode, iter.cur);
        if (trxNode->info.csn != INVALID_CSN && trxNode->info.csn <= csn) {
            ErrLog(DSTORE_DEBUG1, MODULE_WAL,
                   ErrMsg("UpdateTrxNeedRollbackBarrier delete commit xlog from queue, csn %lu.", trxNode->info.csn));
            DListDelete(iter.cur);
            m_numOfTransactionsNeedRollbackBarrier--;
            DstorePfreeExt(trxNode);
        }
    }
    if (m_numOfTransactionsNeedRollbackBarrier == 0) {
        DListInit(&m_transactionsNeedRollbackBarrierListHead);
    }
    pthread_rwlock_unlock(&m_rollbackBarrierListLock);
}
 
void StoragePdb::DeleteAllTrxnsFromBarrierRollbackList()
{
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Clear all barrier rollback list."));
    pthread_rwlock_wrlock(&m_rollbackBarrierListLock);
    dlist_mutable_iter iter = {};
    dlist_foreach_modify(iter, &m_transactionsNeedRollbackBarrierListHead) {
        TransactionsNeedRollbackListNode *trxNode =
            dlist_container(TransactionsNeedRollbackListNode, dlistNode, iter.cur);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
               ErrMsg("UpdateTrxNeedRollbackBarrier delete commit xlog from queue, csn %lu.", trxNode->info.csn));
        DListDelete(iter.cur);
        DstorePfreeExt(trxNode);
    }
    m_numOfTransactionsNeedRollbackBarrier = 0;
    DListInit(&m_transactionsNeedRollbackBarrierListHead);
    pthread_rwlock_unlock(&m_rollbackBarrierListLock);
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Clear all barrier rollback list success."));
}

void StoragePdb::DeleteAndUpdateLastBarrierCsnBeforePlsn(uint64 plsn)
{
    CommitSeqNo lastCsn = COMMITSEQNO_FIRST_NORMAL;
    dlist_mutable_iter iter = {};
    dlist_foreach_modify(iter, &m_noSendBarrierListHead) {
        NoSendBarrierListNode *barrierNode = dlist_container(NoSendBarrierListNode, dlistNode, iter.cur);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
               ErrMsg("DeleteAndGetLastBarrierCsnBeforePlsn csn %lu, plsn %lu.", barrierNode->info.barrierCsn,
                      barrierNode->info.plsn));
        if (plsn >= barrierNode->info.plsn) {
            lastCsn = DstoreMax(lastCsn, barrierNode->info.barrierCsn);
            DListDelete(iter.cur);
            DstorePfreeExt(barrierNode);
            m_numOfNoSendBarrier--;
        }
    }
    if (m_numOfTransactionsNeedRollbackBarrier == 0) {
        DListInit(&m_noSendBarrierListHead);
    }
    if (lastCsn > g_storageInstance->GetCsnMgr()->GetSendBarrierCsn(m_pdbId)) {
        g_storageInstance->GetCsnMgr()->SetSendBarrierCsn(lastCsn, m_pdbId);
    }
}

void StoragePdb::DeleteAllFromNoSendBarrierList()
{
    if (m_numOfNoSendBarrier == 0) {
        DListInit(&m_noSendBarrierListHead);
        return;
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Clear barrier list."));
    dlist_mutable_iter iter = {};
    dlist_foreach_modify(iter, &m_noSendBarrierListHead) {
        NoSendBarrierListNode *barrierNode = dlist_container(NoSendBarrierListNode, dlistNode, iter.cur);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
               ErrMsg("DeleteAndGetLastBarrierCsnBeforePlsn csn %lu, plsn %lu.", barrierNode->info.barrierCsn,
                      barrierNode->info.plsn));
        DListDelete(iter.cur);
        DstorePfreeExt(barrierNode);
        m_numOfNoSendBarrier--;
    }
    m_numOfNoSendBarrier = 0;
    DListInit(&m_noSendBarrierListHead);
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Clear barrier list success."));
}

void StoragePdb::PushBarrierIntoNoSendBarrierList(CommitSeqNo barrierCsn, uint64 plsn)
{
    NoSendBarrierListNode *node = static_cast<NoSendBarrierListNode *>(DstorePalloc0(sizeof(NoSendBarrierListNode)));
    if (unlikely(node == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Malloc for NoSendBarrierListNode fail."));
        return;
    }
    node->info.barrierCsn = barrierCsn;
    node->info.plsn = plsn;
    DListPushTail(&m_noSendBarrierListHead, &(node->dlistNode));
    m_numOfNoSendBarrier++;
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
           ErrMsg("PushBarrierIntoNoSendBarrierList barrier csn %lu, plsn %lu.", barrierCsn, plsn));
}

}  // namespace DSTORE
