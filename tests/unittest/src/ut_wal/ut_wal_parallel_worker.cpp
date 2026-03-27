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
 */
#include <gtest/gtest.h>
#include <random>
#include <thread>

#include "securec.h"
#include "tablespace/dstore_tablespace.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "wal/dstore_wal_file_manager.h"
#include "wal/dstore_wal_parallel_redo_worker.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_tablespace/ut_segment.h"
#include "vfs/vfs_interface.h"

namespace DSTORE {

constexpr uint32 WAL_RECORD_ENTRY_NUM = 300;
constexpr uint32 WAL_RECORD_PLSN_SIZE = 64;
constexpr uint32 WAL_REDO_WORKER_NUM = 10;
constexpr uint32 WAL_REDO_BATCH_NUM = 11;
constexpr PageId ONE_PAGE = {5121, 0};

constexpr uint32 PARALLEL_TEST_CNT_NUM = 10000;
struct WalParallerSpscQueueTest {
    sem_t beginSem1;
    sem_t beginSem2;

    RedoWalRecordEntry X[WAL_RECORD_ENTRY_NUM];
    RedoWalRecordEntry Y[WAL_RECORD_ENTRY_NUM];
    BlockSpscQueue *redoQueue;
};
class WalParallelWorkerTest : public WALBASICTEST {
protected:
    void SetUp() override
    {
        SetDefaultPdbId(PDB_TEMPLATE1_ID);
        WALBASICTEST::SetUp();
#ifdef DYNAMIC_LINK_VFS_LIB
        /* Get the config file. */
        memset(m_configFilePath, 0, MAXPGPATH);
        char *execPath = m_configFilePath;
        ret = readlink("/proc/self/exe", execPath, MAXPGPATH);
        ASSERT_GT(ret, 0);
        char *lastSlashPtr = strrchr(execPath, '/');
        ASSERT_NE(lastSlashPtr, nullptr);

        snprintf(lastSlashPtr + 1, MAXPGPATH / 2, "dynamic_vfs.conf.init");
        ASSERT_EQ(access(m_configFilePath, F_OK), 0); /* It must exist. */

        DSTORETEST::m_guc.vfsConfigPath = m_configFilePath;
#endif
        WALBASICTEST::Prepare();
        PrepareControlFileContent();
        NodeId selfNode = 0;
        RetStatus retStatus = m_walStreamManager->Init(m_walControlFile);
        ASSERT_EQ(retStatus, DSTORE_SUCC);
        for (uint32 i = 0; i < WAL_REDO_WORKER_NUM; i++) {
            m_parallelRedoWorker[i] = nullptr;
        }

        m_numOfWorkers = 0;
        uint64 segSize = g_storageInstance->GetGuc()->walFileSize;
        m_walStream = m_walStreamManager->GetWritingWalStream();
        g_storageInstance->GetGuc()->recoveryWorkerNum = 10;
        m_walRecovery = DstoreNew(m_ut_memory_context) WalRecovery(m_ut_memory_context, m_walStream, segSize, 0);
        m_walRecovery->Init(RedoMode::RECOVERY_REDO, PDB_TEMPLATE1_ID);
        m_testBufMgr = g_storageInstance->GetBufferMgr();
        m_fixedPage = m_testBufMgr->Read(g_defaultPdbId, ONE_PAGE, LW_SHARED);
        ASSERT_NE(m_fixedPage, nullptr);
        m_testBufMgr->UnlockContent(m_fixedPage);
    }

    void TearDown() override
    {
        m_testBufMgr->Release(m_fixedPage);
        for (uint32 i = 0; i < WAL_REDO_WORKER_NUM; i++) {
            if (m_parallelRedoWorker[i] != nullptr) {
                delete m_parallelRedoWorker[i];
            }
            m_parallelRedoWorker[i] = nullptr;
        }
        m_walRecovery->Destroy();
        if (m_walRecovery != nullptr) {
            delete m_walRecovery;
        }
        m_walRecovery = nullptr;
        WALBASICTEST::TearDown();
    }

    void InitAndDispatchWalRecordEntry();
    void WaitAllWorkerFinish();
    void InitAndDispatchDDLWalRecordEntry(uint64 &plsn, uint64 preReuseVersion = 0xFFFFFFFFFFFFFFFFU,
        int num = WAL_RECORD_ENTRY_NUM, int tbs = -1);
    void InitAndBatchDispatchWalRecordEntry();

    RedoWalRecordEntry m_recordEntry[MAX_REDO_QUE_BATCH_NUM];
    WalRecovery *m_walRecovery;
    uint32 m_numOfWorkers;
    ParallelRedoWorker *m_parallelRedoWorker[WAL_REDO_WORKER_NUM];
    BufMgrInterface *m_testBufMgr;
    BufferDesc *m_fixedPage;
    WalParallerSpscQueueTest *m_parallerQueueTest;
    WalStream *m_walStream;
};

void WalParallelWorkerTest::InitAndDispatchWalRecordEntry()
{
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
    TableSpace *testTbs = tablespaceMgr->OpenTablespace(
        static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), DSTORE::DSTORE_NO_LOCK);
                
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, testTbs->GetTablespaceId(),
        g_storageInstance->GetBufferMgr(), SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint32 i = 0; i < WAL_RECORD_ENTRY_NUM; i++) {
        m_recordEntry[0].recordEndPlsn = (i + 1) * WAL_RECORD_PLSN_SIZE;
        WalRecordRedoContext redoCtx = {Xid(INVALID_XID), INVALID_WAL_ID, g_defaultPdbId};
        m_recordEntry[0].ctx = redoCtx;
        WalRecord *walRecord = BuildWal(200);
        WalRecordForPage *forPageWal = static_cast<WalRecordForPage *>(walRecord);
        forPageWal->m_pageId = ONE_PAGE;
        m_testBufMgr->LockContent(m_fixedPage, LW_SHARED);
        forPageWal->m_pagePreWalId = m_fixedPage->GetPage()->GetWalId();
        forPageWal->m_pagePrePlsn = m_fixedPage->GetPage()->GetPlsn();
        forPageWal->m_pagePreGlsn = m_fixedPage->GetPage()->GetGlsn();
        forPageWal->m_filePreVersion = WalUtils::GetFileVersion(g_defaultPdbId, forPageWal->m_pageId.m_fileId);
        m_testBufMgr->UnlockContent(m_fixedPage);
        m_recordEntry[0].walRecordNeedFree = true;
        m_recordEntry[0].walRecordInfo.freeRedoEntry =
            m_parallelRedoWorker[i % m_numOfWorkers]->AllocRedoWalRecord(200);
        errno_t rc = memcpy_s(m_recordEntry[0].GetWalRecordInBufFreeEntry(), walRecord->GetSize(),
            walRecord, walRecord->GetSize());
        storage_securec_check(rc, "\0", "\0");
        m_parallelRedoWorker[i % m_numOfWorkers]->AppendWalRecord(&m_recordEntry[0], 1, __FUNCTION__, __LINE__);
        free(walRecord);
    }
    tablespaceMgr->CloseTablespace(testTbs, DSTORE::DSTORE_NO_LOCK);
}

void WalParallelWorkerTest::WaitAllWorkerFinish()
{
    for (uint32 i = 0; i < m_numOfWorkers; i++) {
        m_parallelRedoWorker[i]->WaitRedoFinish();
    }
}

TEST_F(WalParallelWorkerTest, ParallelPageWorkerTest)
{
    ParallelRedoWorkerInitParam initParam = {0, g_defaultPdbId, 0, 0, 16, nullptr, m_ut_memory_context, m_walRecovery};
    m_numOfWorkers = WAL_REDO_WORKER_NUM;
    for (uint32 i = 0; i < m_numOfWorkers; i++) {
        m_parallelRedoWorker[i] = DstoreNew(m_ut_memory_context) ParallelPageRedoWorker(initParam);
        EXPECT_EQ(m_parallelRedoWorker[i]->Init(), DSTORE_SUCC);
        m_walRecovery->InitParallelRedoWorker(m_parallelRedoWorker[i], i);
        m_parallelRedoWorker[i]->Run("ParallelPageRedoWorker");
    }
    InitAndDispatchWalRecordEntry();
    WaitAllWorkerFinish();

    uint64 maxDispatchedPlsn = 0;
    uint64 maxRedoFinishedPlsn = 0;
    for (uint32 i = 0; i < m_numOfWorkers; i++) {
        maxDispatchedPlsn = maxDispatchedPlsn > m_parallelRedoWorker[i]->GetMaxDispatchedPlsn() ?
            maxDispatchedPlsn : m_parallelRedoWorker[i]->GetMaxDispatchedPlsn();
        maxRedoFinishedPlsn = maxRedoFinishedPlsn > m_parallelRedoWorker[i]->GetMaxRedoFinishedPlsn() ?
            maxRedoFinishedPlsn : m_parallelRedoWorker[i]->GetMaxRedoFinishedPlsn();
    }
    EXPECT_EQ(maxDispatchedPlsn, WAL_RECORD_ENTRY_NUM * WAL_RECORD_PLSN_SIZE);
    EXPECT_EQ(maxRedoFinishedPlsn, WAL_RECORD_ENTRY_NUM * WAL_RECORD_PLSN_SIZE);
}

void WalParallelWorkerTest::InitAndDispatchDDLWalRecordEntry(uint64 &plsn, uint64 preReuseVersion, int num, int tbs)
{
    for (uint32 i = 0; i < num; i++) {
        RedoWalRecordEntry recordEntry;
        plsn += WAL_RECORD_PLSN_SIZE;
        recordEntry.recordEndPlsn = plsn;
        WalRecordRedoContext ctx = {Xid(INVALID_XID), INVALID_WAL_ID};
        recordEntry.ctx = ctx;
        WalRecord *walRecord = BuildDDLWal(200);
        WalRecordTbsLogical *walRecordTbsLogical = static_cast<WalRecordTbsLogical *>(walRecord);
        if (tbs == -1) {
            walRecordTbsLogical->tablespaceId = i;
            walRecordTbsLogical->preReuseVersion = WalUtils::GetTbsVersion(g_defaultPdbId, i);
        } else {
            walRecordTbsLogical->tablespaceId = tbs;
            if (preReuseVersion == 0xFFFFFFFFFFFFFFFFU) {
                walRecordTbsLogical->preReuseVersion = WalUtils::GetTbsVersion(g_defaultPdbId, tbs);
            } else {
                walRecordTbsLogical->preReuseVersion = preReuseVersion;
            }
        }
        recordEntry.walRecordNeedFree = true;
        recordEntry.walRecordInfo.freeRedoEntry =
            m_parallelRedoWorker[0]->AllocRedoWalRecord(200);
        errno_t rc = memcpy_s(recordEntry.GetWalRecordInBufFreeEntry(), walRecord->GetSize(),
            walRecord, walRecord->GetSize());
        storage_securec_check(rc, "\0", "\0");
        m_parallelRedoWorker[0]->AppendWalRecord(&recordEntry, 1, __FUNCTION__, __LINE__);
        free(walRecord);
    }
}

TEST_F(WalParallelWorkerTest, DDLWorkerTest)
{
    uint64 plsn = 0;
    ParallelRedoWorkerInitParam initParam = {0, g_defaultPdbId, 0, 0, 16, nullptr, m_ut_memory_context, m_walRecovery};
    ParallelDDLRedoWorker *ddlWorker = DstoreNew(m_ut_memory_context) ParallelDDLRedoWorker(initParam);
    EXPECT_EQ(ddlWorker->Init(), DSTORE_SUCC);
    m_parallelRedoWorker[0] = ddlWorker;
    ddlWorker->Run("ParallelDDLRedoWorker");
    InitAndDispatchDDLWalRecordEntry(plsn);
    m_parallelRedoWorker[0]->WaitRedoFinish();

    uint64 maxDispatchedPlsn = ddlWorker->GetMaxDispatchedPlsn();
    uint64 maxRedoFinishedPlsn = ddlWorker->GetMaxRedoFinishedPlsn();
    EXPECT_EQ(maxDispatchedPlsn, WAL_RECORD_ENTRY_NUM * WAL_RECORD_PLSN_SIZE);
    EXPECT_EQ(maxRedoFinishedPlsn, WAL_RECORD_ENTRY_NUM * WAL_RECORD_PLSN_SIZE);
    delete ddlWorker;
    m_parallelRedoWorker[0] = nullptr;
}


TEST_F(WalParallelWorkerTest, DDLWorkerMaxRedoPlsnTest)
{
    uint32 blockedWalPos = WAL_RECORD_ENTRY_NUM / 2;
    uint32 totalWalNum = 0;
    uint64 plsn = 0;
    TablespaceId blockedWalTbs = 0;
    TablespaceId normalWalTbs = 1;
    errno_t rc;
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();

    ParallelRedoWorkerInitParam initParam = {0, g_defaultPdbId, 0, 0, 16, nullptr, m_ut_memory_context, m_walRecovery};
    ParallelDDLRedoWorker *ddlWorker = DstoreNew(m_ut_memory_context) ParallelDDLRedoWorker(initParam);
    EXPECT_EQ(ddlWorker->Init(), DSTORE_SUCC);
    m_parallelRedoWorker[0] = ddlWorker;
    m_numOfWorkers = 1;
    ddlWorker->Run("ParallelDDLRedoWorker");

    InitAndDispatchDDLWalRecordEntry(plsn, WalUtils::GetTbsVersion(g_defaultPdbId, blockedWalTbs), blockedWalPos, blockedWalTbs);
    totalWalNum += blockedWalPos;
    InitAndDispatchDDLWalRecordEntry(plsn, WalUtils::GetTbsVersion(g_defaultPdbId, blockedWalTbs) + 1, WAL_RECORD_ENTRY_NUM - blockedWalPos, blockedWalTbs);
    totalWalNum += (WAL_RECORD_ENTRY_NUM - blockedWalPos);

    InitAndDispatchDDLWalRecordEntry(plsn, WalUtils::GetTbsVersion(g_defaultPdbId, normalWalTbs), WAL_RECORD_ENTRY_NUM, blockedWalTbs);
    totalWalNum += WAL_RECORD_ENTRY_NUM;
  
    uint64 maxRedoFinishedPlsn = ddlWorker->GetMaxRedoFinishedPlsn();
    EXPECT_EQ(maxRedoFinishedPlsn, blockedWalPos * WAL_RECORD_PLSN_SIZE);

    BlockNumber tbs_blkno;
    ControlTablespacePageItemData *tbsItem = controlFile->GetTbsPageItemPtr(blockedWalTbs, &tbs_blkno);
    tbsItem->reuseVersion++;

    RedoWalRecordEntry syncerEntry;
    syncerEntry.recordEndPlsn = totalWalNum * WAL_RECORD_PLSN_SIZE;
    syncerEntry.walRecordInfo.walRecord = nullptr;
    syncerEntry.walRecordNeedFree = false;
    m_parallelRedoWorker[0]->AppendWalRecord(&syncerEntry, 1, __FUNCTION__, __LINE__);

    WaitAllWorkerFinish();
    maxRedoFinishedPlsn = ddlWorker->GetMaxRedoFinishedPlsn();
    EXPECT_EQ(maxRedoFinishedPlsn, totalWalNum * WAL_RECORD_PLSN_SIZE);
}


void WalParallelWorkerTest::InitAndBatchDispatchWalRecordEntry()
{
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
    TableSpace *testTbs = tablespaceMgr->OpenTablespace(
        static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), DSTORE::DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(testTbs)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to open tablespace %u.",
                                                    static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID)));
        return;
    }
    
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, testTbs->GetTablespaceId(),
        g_storageInstance->GetBufferMgr(), SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));

    uint32 curNum = 0;
    uint32 i = 0;
    for (; i < WAL_RECORD_ENTRY_NUM; i++) {
        m_recordEntry[curNum].recordEndPlsn = (i + 1) * WAL_RECORD_PLSN_SIZE;
        WalRecordRedoContext redoCtx = {Xid(INVALID_XID), INVALID_WAL_ID, g_defaultPdbId};
        m_recordEntry[curNum].ctx = redoCtx;
        WalRecord *walRecord = BuildWal(200);
        WalRecordForPage *forPageWal = static_cast<WalRecordForPage *>(walRecord);
        forPageWal->m_pageId = ONE_PAGE;
        m_testBufMgr->LockContent(m_fixedPage, LW_SHARED);
        forPageWal->m_pagePreWalId = m_fixedPage->GetPage()->GetWalId();
        forPageWal->m_pagePrePlsn = m_fixedPage->GetPage()->GetPlsn();
        forPageWal->m_pagePreGlsn = m_fixedPage->GetPage()->GetGlsn();
        forPageWal->m_filePreVersion = WalUtils::GetFileVersion(g_defaultPdbId, forPageWal->m_pageId.m_fileId);
        m_testBufMgr->UnlockContent(m_fixedPage);
        m_recordEntry[curNum].walRecordNeedFree = true;
        m_recordEntry[curNum].walRecordInfo.freeRedoEntry =
            m_parallelRedoWorker[i % m_numOfWorkers]->AllocRedoWalRecord(200);
        errno_t rc = memcpy_s(m_recordEntry[curNum].GetWalRecordInBufFreeEntry(), walRecord->GetSize(),
            walRecord, walRecord->GetSize());
        storage_securec_check(rc, "\0", "\0");
        curNum++;
        if (curNum == WAL_REDO_BATCH_NUM) {
            m_parallelRedoWorker[i % m_numOfWorkers]->AppendWalRecord(&m_recordEntry[0], curNum, __FUNCTION__, __LINE__);
            curNum = 0;
        }

        free(walRecord);
    }
    if (curNum != 0) {
        m_parallelRedoWorker[i % m_numOfWorkers]->AppendWalRecord(&m_recordEntry[0], curNum, __FUNCTION__, __LINE__);
    }
    tablespaceMgr->CloseTablespace(testTbs, DSTORE::DSTORE_NO_LOCK);
}

TEST_F(WalParallelWorkerTest, ParallelPageRedoWorkerBatchTest)
{
    ParallelRedoWorkerInitParam initParam = {0, g_defaultPdbId, 0, 0, 16, nullptr, m_ut_memory_context, m_walRecovery};
    m_numOfWorkers = WAL_REDO_WORKER_NUM;
    for (uint32 i = 0; i < m_numOfWorkers; i++) {
        m_parallelRedoWorker[i] = DstoreNew(m_ut_memory_context) ParallelPageRedoWorker(initParam);
        EXPECT_EQ(m_parallelRedoWorker[i]->Init(), DSTORE_SUCC);
        m_walRecovery->InitParallelRedoWorker(m_parallelRedoWorker[i], i);
        m_parallelRedoWorker[i]->Run("ParallelPageRedoWorker");
    }
    InitAndBatchDispatchWalRecordEntry();
    WaitAllWorkerFinish();

    uint64 maxDispatchedPlsn = 0;
    uint64 maxRedoFinishedPlsn = 0;
    for (uint32 i = 0; i < m_numOfWorkers; i++) {
        maxDispatchedPlsn = maxDispatchedPlsn > m_parallelRedoWorker[i]->GetMaxDispatchedPlsn() ?
            maxDispatchedPlsn : m_parallelRedoWorker[i]->GetMaxDispatchedPlsn();
        maxRedoFinishedPlsn = maxRedoFinishedPlsn > m_parallelRedoWorker[i]->GetMaxRedoFinishedPlsn() ?
            maxRedoFinishedPlsn : m_parallelRedoWorker[i]->GetMaxRedoFinishedPlsn();
    }
    EXPECT_EQ(maxDispatchedPlsn, WAL_RECORD_ENTRY_NUM * WAL_RECORD_PLSN_SIZE);
    EXPECT_EQ(maxRedoFinishedPlsn, WAL_RECORD_ENTRY_NUM * WAL_RECORD_PLSN_SIZE);
}

void *SpscQueuePutThread(WalParallerSpscQueueTest *para) {
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister();
    constexpr size_t binCpuTarget = 1;
    cpu_set_t cpuSet;
    CPU_ZERO(&cpuSet);
    CPU_SET(binCpuTarget, &cpuSet);
    int rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpuSet);
    if (rc == -1) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("SpscQueuePutThread bind cpu failed, ErrorCode:%d.", rc));
    }
    std::mt19937 mtRand(std::random_device{}());
    sem_wait(&para->beginSem1);
    for (uint64 cnt = 0; cnt < PARALLEL_TEST_CNT_NUM; ++cnt) {
        // random delay
        while (mtRand() % 8 != 0) {
        }
        for (uint64 i = 1; i < WAL_RECORD_ENTRY_NUM; i++) {
            para->redoQueue->Put<RedoWalRecordEntry*>(&para->X[i]);
        }
    }
    g_storageInstance->UnregisterThread();
    return NULL;
}

void *SpscQueuePopThread(WalParallerSpscQueueTest *para) {
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister();
    constexpr size_t binCpuTarget = 2;
    cpu_set_t cpuSet;
    CPU_ZERO(&cpuSet);
    CPU_SET(binCpuTarget, &cpuSet);
    int rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpuSet);
    if (rc == -1) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("SpscQueuePopThread bind cpu failed, ErrorCode:%d.", rc));
    }
    std::mt19937 mtRand(std::random_device{}());
    uint64 i = 1;
    sem_wait(&para->beginSem2);
    for (uint64 cnt = 0; cnt < PARALLEL_TEST_CNT_NUM; ++cnt) {
        // random delay
        while (mtRand() % 8 != 0) {
        }

        for (uint64 i = 1; i < WAL_RECORD_ENTRY_NUM; i++) {
            while (para->redoQueue->Pop<RedoWalRecordEntry*>(&para->Y[i]) == DSTORE_FAIL) {
                continue;
            }

            EXPECT_EQ(para->Y[i].recordEndPlsn, i);
            EXPECT_EQ(para->Y[i].ctx.walId, i);
        }
    }
    g_storageInstance->UnregisterThread();
    return NULL;
}

TEST_F(WalParallelWorkerTest, ParallelWorkerTestConcurrency)
{
    WalParallerSpscQueueTest *parallerQueueTest =
        static_cast<WalParallerSpscQueueTest *>(DstorePalloc0(sizeof(WalParallerSpscQueueTest)));
    parallerQueueTest->redoQueue = static_cast<BlockSpscQueue *>(DstorePalloc0(sizeof(BlockSpscQueue)));
    EXPECT_EQ(parallerQueueTest->redoQueue->Init(1024, sizeof(RedoWalRecordEntry), nullptr), DSTORE_SUCC);

    for (uint64 i= 1; i < WAL_RECORD_ENTRY_NUM; i++) {
        parallerQueueTest->X[i].recordEndPlsn = i;
        parallerQueueTest->X[i].ctx.walId = i;
    }
    GS_MEMORY_BARRIER();
    sem_init(&parallerQueueTest->beginSem1, 0, 0);
    sem_init(&parallerQueueTest->beginSem2, 0, 0);
    std::thread t1(SpscQueuePutThread, parallerQueueTest);
    std::thread t2(SpscQueuePopThread, parallerQueueTest);
    sem_post(&parallerQueueTest->beginSem1);
    sem_post(&parallerQueueTest->beginSem2);
    t1.join();
    t2.join();

    DstorePfreeExt(parallerQueueTest->redoQueue);
    DstorePfreeExt(parallerQueueTest);
}

}
