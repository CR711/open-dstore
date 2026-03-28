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
 * dstore_bg_writer.cpp
 *     This file implements the functionality of bg page writer.
 *
 * IDENTIFICATION
 *        src/buffer/dstore_bg_writer.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <thread>
#include <csignal>
#include "common/log/dstore_log.h"
#include "wal/dstore_wal_logstream.h"
#include "common/memory/dstore_mctx.h"
#include "port/dstore_port.h"
#include "buffer/dstore_buf_mgr.h"
#include "buffer/dstore_checkpointer.h"
#include "framework/dstore_instance.h"
#include "common/instrument/dstore_stat.h"
#include "buffer/dstore_bg_disk_page_writer.h"

namespace DSTORE {

constexpr long NANOS_PER_SEC = 1000000000L;
bool g_enableAsyncIoFlush = false;

BgDiskPageMasterWriter::BgDiskPageMasterWriter(const WalStream *walStream, DirtyPageQueue *dirtyPageQueue,
    PdbId pdbId, int64 slotId)
    : m_slaveWriterArray(nullptr), m_walStream(walStream), m_dirtyPageQueue(dirtyPageQueue), m_recoveryPlsn(0),
      m_slaveNum(1), m_pdbId(pdbId), m_slotId(slotId), m_flushAll(false)
{}

RetStatus BgDiskPageMasterWriter::Init()
{
    if (STORAGE_FUNC_FAIL(BgPageWriterBase::Init())) {
        return DSTORE_FAIL;
    }

    DstoreMemoryContext oldCxt = DstoreMemoryContextSwitchTo(m_memContext);
    RetStatus ret = DSTORE_SUCC;
    do {
        m_slaveNum = static_cast<uint32>(g_storageInstance->GetGuc()->bgDiskWriterSlaveNum);
        m_slaveWriterArray =
            static_cast<BgSlavePageWriterEntry *>(DstorePalloc0(sizeof(BgSlavePageWriterEntry) * m_slaveNum));
        if (STORAGE_VAR_NULL(m_slaveWriterArray)) {
            ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("alloc memory for slaveWriterArray fail!"));
            ret = DSTORE_FAIL;
            break;
        }

        for (uint32 i = 0; i < m_slaveNum; i++) {
            m_slaveWriterArray[i].bgSlaveDiskPageWriter = nullptr;
            m_slaveWriterArray[i].bgSlaveDiskThread = nullptr;
        }
        if (STORAGE_FUNC_FAIL(ret = m_flushCxt.Init(static_cast<uint32>(g_storageInstance->GetGuc()->buffer)))) {
            break;
        }
        if (STORAGE_FUNC_FAIL(ret = m_tmpDirtyPageVec.Init(m_memContext))) {
            break;
        }
        if (STORAGE_FUNC_FAIL(ret = StartSlavePageWriters())) {
            break;
        }
    } while (false);

    (void)DstoreMemoryContextSwitchTo(oldCxt);
    if (STORAGE_FUNC_FAIL(ret)) {
        DstoreMemoryContextDelete(m_memContext);
        m_memContext = nullptr;
    }
    return ret;
}

void BgDiskPageMasterWriter::Run()
{
    AutoMemCxtSwitch autoSwitch{m_memContext};

    /* Decide if start a dirty page flush */
    RefreshNextFlushTime();

    ErrLog(DSTORE_LOG, MODULE_BGPAGEWRITER, ErrMsg("BgPageWriter pdbId %u, %lu start: walId:%lu", m_pdbId,
        thrd->GetCore()->pid, m_walStream->GetWalId()));

    uint64 recoveryPlsn = INVALID_PLSN;
    uint64 maxAppendPlsn = INVALID_PLSN;
    uint64 advanceNum = 0;

    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_BGPAGEWRITER, ErrMsg("pdb %u is nullptr", m_pdbId));

    for (;;) {
        /* exit if get the request */
        if (IsStop()) {
            Destroy();
            BgPageWriterExit();
            break;
        }

        int64 slotId = GetWalStreamSlotId();
        if (slotId != INVALID_BGWRITER_SLOT_ID) {
            maxAppendPlsn = m_walStream->GetMaxAppendedPlsn();
        } else {
            maxAppendPlsn = m_walStream->GetStandbyPdbRecoveryPlsn();
        }

        /* calculate how many dirty page should be flushed */
        m_flushCxt.Set(0);
        advanceNum = 0;
        if (slotId == INVALID_BGWRITER_SLOT_ID) {
            slotId = pdb->GetBgPageWriterMgr()->GetBgWriterSlotIdByWalId(m_walStream->GetWalId());
        }
        if (slotId < 0 || slotId >= DIRTY_PAGE_QUEUE_MAX_SIZE) {
            ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg(
                "BgDiskPageMasterWriter::MainLoop, cannot find this slotId %ld.", slotId));
            continue;
        }
        uint32 dirtyPageNumber = ScanDirtyListForFlush(advanceNum, slotId);
        if (dirtyPageNumber != 0) {
            ErrLog(DSTORE_DEBUG1, MODULE_BGPAGEWRITER,
                ErrMsg("BgPageWriter %lu for pdb %hhu walId %lu will flush %u page, "
                       "max append PLSN is %lu, now recovery PLSN is %lu",
                       thrd->GetCore()->pid, m_walStream->GetPdbId(), m_walStream->GetWalId(),
                       dirtyPageNumber, maxAppendPlsn, GetMinRecoveryPlsn()));
            WakeUpSlaveWriter();
            WaitSlaveWriterFlushFinish();
            if (!g_storageInstance->IsBootstrapping()) {
                dynamic_cast<BufMgr *>(g_storageInstance->GetBufferMgr())->ReportBgwriter(m_pdbId, slotId);
            }
        }

        /* advance recovery Plsn */
        recoveryPlsn = m_dirtyPageQueue->AdvanceHeadAfterFlush(m_tmpDirtyPageVec, advanceNum, slotId);
        m_dirtyPageQueue->m_statisticInfo.actualFlushCnt.store(dirtyPageNumber, std::memory_order_release);
        if (dirtyPageNumber > 0 || advanceNum > 0) {
            ErrLog(DSTORE_DEBUG1, MODULE_BGPAGEWRITER,
                   ErrMsg("BgPageWriter %lu for pdb %hhu walId %lu flush finish, actual flush %u, "
                          "advanceNum %lu, max append PLSN is %lu, now recovery PLSN is %lu, "
                          "queue size %lu total push %lu, total remove %lu",
                          thrd->GetCore()->pid, m_walStream->GetPdbId(), m_walStream->GetWalId(),
                          dirtyPageNumber, advanceNum, maxAppendPlsn, GetMinRecoveryPlsn(),
                          m_dirtyPageQueue->GetPageNum(), m_dirtyPageQueue->GetTotalPushCnt(),
                          m_dirtyPageQueue->GetTotalRemoveCnt()));
        }
        if (advanceNum == 0) {
            if (m_dirtyPageQueue->UpdateHeadWhenEmpty(maxAppendPlsn)) {
                m_recoveryPlsn.store(maxAppendPlsn, std::memory_order_release);
            }
        } else {
            StorageReleasePanic(recoveryPlsn < m_recoveryPlsn.load(std::memory_order_acquire), MODULE_BGPAGEWRITER,
                                ErrMsg("invalid recoveryPlsn:%lu", recoveryPlsn));
            m_recoveryPlsn.store(recoveryPlsn, std::memory_order_release);
        }

        /* sleep for next turn flush */
        SmartSleep();
        RefreshNextFlushTime();
    }
    ErrLog(DSTORE_LOG, MODULE_BGPAGEWRITER, ErrMsg("BgDiskPageMasterWriter pdbId %u, %lu exited MainLoop", m_pdbId,
        thrd->GetCore()->pid));
}

void BgDiskPageMasterWriter::FlushAllDirtyPages()
{
    while (!IsReady()) {
        GaussUsleep(m_waitStep);
    }

    std::unique_lock<std::mutex> lock(m_mtx);
    m_flushAll.store(true, std::memory_order_release);

    uint64 maxAppendedPlsn;
    StoragePdb *pdb = g_storageInstance->GetPdb(m_walStream->GetPdbId());
    StorageReleasePanic(pdb == nullptr, MODULE_BGPAGEWRITER, ErrMsg("pdb %u is nullptr", m_walStream->GetPdbId()));
    if (pdb->GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY) {
        maxAppendedPlsn = m_walStream->GetStandbyPdbRedoFinishedPlsn();
    } else {
        maxAppendedPlsn = m_walStream->GetMaxAppendedPlsn();
    }

    uint64 loopCount = 0;
    const uint64 LOOP_REPORT_COUNT = 100;
    while (GetMinRecoveryPlsn() < maxAppendedPlsn) {
        GaussUsleep(m_waitStep);
        if (++loopCount % LOOP_REPORT_COUNT == 0) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("[Flush dirty pages]MinRecoveryPlsn %lu, MaxAppendPlsn %lu, walId: %lu, pdbId: %u.",
                GetMinRecoveryPlsn(), maxAppendedPlsn, m_walStream->GetWalId(), m_pdbId));
        }
        if (pdb->GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY) {
            ErrLog(DSTORE_WARNING, MODULE_PDBREPLICA,
                ErrMsg("Currently skip flush dirty pages when closing standby pdb."));
            break;
        }
    }

    m_flushAll.store(false, std::memory_order_release);
    ErrLog(DSTORE_LOG, MODULE_BGPAGEWRITER, ErrMsg("flush all dirty pages finish, pdbId %u, "
        "now recovery PLSN is %lu, queue size %lu, total push %lu, total remove %lu", m_pdbId, GetMinRecoveryPlsn(),
        m_dirtyPageQueue->GetPageNum(), m_dirtyPageQueue->GetTotalPushCnt(), m_dirtyPageQueue->GetTotalRemoveCnt()));
}

void BgDiskPageMasterWriter::Destroy()
{
    /* destroy slave threads and slave data object resources that master manage and malloc */
    if (m_slaveWriterArray != nullptr) {
        for (uint32 i = 0; i < m_slaveNum; i++) {
            if (m_slaveWriterArray[i].bgSlaveDiskPageWriter) {
                delete m_slaveWriterArray[i].bgSlaveDiskPageWriter;
                m_slaveWriterArray[i].bgSlaveDiskPageWriter = nullptr;
            }
            if (m_slaveWriterArray[i].bgSlaveDiskThread != nullptr) {
                delete m_slaveWriterArray[i].bgSlaveDiskThread;
                m_slaveWriterArray[i].bgSlaveDiskThread = nullptr;
            }
        }
        DstorePfreeExt(m_slaveWriterArray);
    }

    if (m_flushCxt.magicNumberHead != nullptr) {
        m_flushCxt.Destroy();
    }
    m_tmpDirtyPageVec.Destroy();
}

void BgDiskPageMasterWriter::Stop()
{   /* firstly exit slave thread */
    StopSlavePageWriters();

    /* secondly exit master thread resources */
    BgPageWriterBase::Stop();
}

RetStatus BgDiskPageMasterWriter::StartSlavePageWriters()
{
    for (uint32 i = 0; i < m_slaveNum; i++) {
        m_slaveWriterArray[i].bgSlaveDiskPageWriter = DstoreNew(m_memContext) BgDiskPageSlaveWriter(&m_flushCxt, this);
        if (STORAGE_VAR_NULL(m_slaveWriterArray[i].bgSlaveDiskPageWriter)) {
            ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER,
                   ErrMsg("alloc memory for m_slaveWriterArray[%u].bgSlaveDiskPageWriter fail!", i));
            StopSlavePageWriters();
            return DSTORE_FAIL;
        }
        RetStatus ret = DSTORE_SUCC;
        m_slaveWriterArray[i].bgSlaveDiskThread =
            new std::thread(SlavePageWriterMain, m_pdbId, m_slaveWriterArray[i].bgSlaveDiskPageWriter, std::ref(ret));
        /* wait until BgPageWriter thread is ready */
        while (!m_slaveWriterArray[i].bgSlaveDiskPageWriter->IsReady()) {
            GaussUsleep(MICRO_PER_MILLI_SEC);
        }
        if (STORAGE_FUNC_FAIL(ret)) {
            StopSlavePageWriters();
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

void BgDiskPageMasterWriter::SlavePageWriterMain(PdbId pdbId, BgDiskPageSlaveWriter *slaveWriter, RetStatus &ret)
{
    InitSignalMask();

    /* create and register thread */
    StorageReleasePanic(slaveWriter == nullptr, MODULE_BGPAGEWRITER, ErrMsg("slaveWriter is nullptr"));
    StorageAssert(thrd == nullptr);
    ret = g_storageInstance->CreateThreadAndRegister(pdbId, false, "DiskSlaveWriter", true,
                                                     ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);

    ret = STORAGE_FUNC_SUCC(ret) ? slaveWriter->Init() : ret;
    /* we must record ret before call SetReady, after SetReady ret maybe will be reuse becuase it's stack memory */
    volatile RetStatus retStatus = ret;
    ErrLog(DSTORE_LOG, MODULE_BUFFER, ErrMsg("Disk SlavePageWriterMain retStatus = %d.", retStatus));
    slaveWriter->SetReady();
    if (STORAGE_FUNC_SUCC(retStatus)) {
        slaveWriter->Run();
    }

    slaveWriter->m_isFlushing.store(false, std::memory_order_release);
    /* unregister thread */
    g_storageInstance->UnregisterThread();
}

void BgDiskPageMasterWriter::StopSlavePageWriters()
{
    if (m_slaveWriterArray == nullptr) {
        return;
    }
    for (uint32 i = 0; i < m_slaveNum; i++) {
        BgSlavePageWriterEntry *entry = &m_slaveWriterArray[i];
        if (entry != nullptr && entry->bgSlaveDiskPageWriter != nullptr && !entry->bgSlaveDiskPageWriter->IsStop()) {
            entry->bgSlaveDiskPageWriter->Stop();
        }
    }
    WakeUpSlaveWriter();
    for (uint32 i = 0; i < m_slaveNum; i++) {
        BgSlavePageWriterEntry *entry = &m_slaveWriterArray[i];
        if (entry != nullptr && entry->bgSlaveDiskThread != nullptr) {
            entry->bgSlaveDiskThread->join();
        }
    }
}

void BgDiskPageMasterWriter::GetSlavePageWriters(AioCompleterInfo *infos, uint32 startIndex)
{
    for (uint32 i = 0; i < m_slaveNum; i++) {
        BgSlavePageWriterEntry *entry = &m_slaveWriterArray[i];
        if (entry != nullptr && entry->bgSlaveDiskPageWriter != nullptr && entry->bgSlaveDiskThread != nullptr) {
            BgDiskPageSlaveWriter* slaveWriter = entry->bgSlaveDiskPageWriter;
            BatchBufferAioContextMgr* batchCtxMgr = slaveWriter->GetBatchBufferAioContextMgr();
            infos[startIndex+i].nodeId = g_storageInstance->GetGuc()->selfNodeId;
            infos[startIndex+i].threadId = i;
            infos[startIndex+i].inProcessCnt = (batchCtxMgr == nullptr ? 0 :
                                               static_cast<uint64_t>(batchCtxMgr->GetInProgressPages()));
            infos[startIndex+i].totalFlushedCnt = (batchCtxMgr == nullptr ? 0 :
                                                  static_cast<uint64_t>(batchCtxMgr->GetFlushedPagesNum()));
            infos[startIndex+i].needFlushCnt = slaveWriter->m_needFlushCnt;
        }
    }
}

uint32 BgDiskPageMasterWriter::GetSlaveSlotInfo(AioSlotUsageInfo *infos, uint32 writerId, uint32 startIndex)
{
    uint32 currentIndex = startIndex;

    for (uint32 i = 0; i < m_slaveNum; ++i) {
        BgSlavePageWriterEntry& entry = m_slaveWriterArray[i];
        if (entry.bgSlaveDiskPageWriter == nullptr) {
            continue;
        }

        BgDiskPageSlaveWriter& slaveWriter = *entry.bgSlaveDiskPageWriter;
        BatchBufferAioContextMgr* batchCtxMgr = slaveWriter.GetBatchBufferAioContextMgr();

        if (batchCtxMgr == nullptr) {
            continue;
        }
        const BufferAioContext *bufferAioContextArr = batchCtxMgr->GetBufferAioContext();
        for (int j = 0; j < BATCH_AIO_SIZE; ++j) {
            BufferAioContext context = bufferAioContextArr[j];
            if (context.submittedTime > 0) {
                AioSlotUsageInfo &currentInfo = infos[currentIndex];
                currentInfo.nodeId = g_storageInstance->GetGuc()->selfNodeId;
                currentInfo.slotId = writerId;
                currentInfo.fileId = context.bufferDesc->bufTag.pageId.m_fileId;
                currentInfo.blockId = context.bufferDesc->bufTag.pageId.m_blockId;
                currentInfo.submittedTime = context.submittedTime;
                currentInfo.elapsedTime = static_cast<uint64>(GetSystemTimeInMicrosecond()) - context.submittedTime;
                currentIndex++;
            }
        }
    }
    return currentIndex;
}

uint32 BgDiskPageMasterWriter::ScanDirtyListForFlush(uint64 &advanceNum, const int64 slotId)
{
    StorageReleasePanic(slotId >= DIRTY_PAGE_QUEUE_MAX_SIZE,
                        MODULE_BGPAGEWRITER, ErrMsg("The slot is invalid(%ld).", slotId));
    uint64 dirtyPageQueSize = m_dirtyPageQueue->GetPageNum();
    GS_MEMORY_BARRIER();

    /* scan tmp dirty page vector first */
    uint32 dirtyPageNum = 0;
    uint32 loc = m_flushCxt.GetValidSize();
    uint32 originLoc = loc;
    BufferDesc *current = nullptr;
    uint32 tmpDirtyPageNumber = m_tmpDirtyPageVec.Size();
    for (uint32 i = 0; i < tmpDirtyPageNumber; ++i) {
        current = m_tmpDirtyPageVec[i];
        uint64 state = current->LockHdr();
        if ((state & (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY)) != 0U) {
            m_flushCxt.candidateFlushArray[loc++] = current;
            dirtyPageNum++;
        }
        StorageReleasePanic(current->nextDirtyPagePtr[slotId].load(std::memory_order_acquire) != INVALID_BUFFER_DESC,
                            MODULE_BGPAGEWRITER, ErrMsg("The next ptr must be null here."));
        current->recoveryPlsn[slotId].store(INVALID_PLSN, std::memory_order_release);
        current->UnlockHdr(state);
    }
    m_tmpDirtyPageVec.Clear();

    /* scan dirty page queue */
    constexpr int bytePerKb = 1024;
    uint64 maxDirtyPageFlushNum = static_cast<uint64>(DstoreMin(
        dirtyPageQueSize, static_cast<uint64>(g_storageInstance->GetGuc()->maxIoCapacityKb) * bytePerKb / BLCKSZ));
    current = m_dirtyPageQueue->GetHead();
    while (maxDirtyPageFlushNum-- > 0) {
        if (unlikely(current == INVALID_BUFFER_DESC)) {
            break;
        }
        if (current->IsPageDirty()) {
            m_flushCxt.candidateFlushArray[loc++] = current;
            dirtyPageNum++;
            StorageAssert(current->GetPageId().IsValid());
            /* The bufferDesc in dirty page queue must belong to this dirty queue, unless pdbid of this buffer
             * is invalid.
             */
            StorageReleasePanic((StoragePdb::IsValidPdbId(current->GetPdbId()) && current->GetPdbId() != m_pdbId),
                MODULE_BGPAGEWRITER, ErrMsg("Invalid dirty page."));
        }
        advanceNum++;
        current = DirtyPageQueue::GetNext(current, slotId);
    }

    if (dirtyPageNum != 0) {
        m_flushCxt.SetValidSize(loc, originLoc);
    }

    return dirtyPageNum;
}

void BgDiskPageMasterWriter::RefreshNextFlushTime()
{
    std::chrono::time_point<std::chrono::steady_clock, std::chrono::milliseconds> now =
            std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now());
    m_nextFlushTime = now + std::chrono::milliseconds(g_storageInstance->GetGuc()->bgPageWriterSleepMilliSecond);
}

void BgDiskPageMasterWriter::SmartSleep() const
{
    std::chrono::time_point<std::chrono::steady_clock, std::chrono::milliseconds> now =
            std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now());

    long durationUs = (m_nextFlushTime - now).count() *1000;
    if (durationUs < 0) {
        /* No time for sleeping. */
        return;
    }
    long sleepTime = 0;
    while (durationUs > 0 && !m_flushAll.load(std::memory_order_acquire)) {
        sleepTime = DstoreMin(durationUs, m_waitStep);
        durationUs -= m_waitStep;
        GaussUsleep(sleepTime);
    }
}

void BgDiskPageMasterWriter::WakeUpSlaveWriter()
{
    if (m_slaveWriterArray == nullptr) {
        return;
    }
    for (uint32 i = 0; i < m_slaveNum; i++) {
        if (m_slaveWriterArray[i].bgSlaveDiskPageWriter == nullptr) {
            continue;
        }
        if (m_slaveWriterArray[i].bgSlaveDiskPageWriter->IsStop()) {
            m_slaveWriterArray[i].bgSlaveDiskPageWriter->WakeupIfStopping();
        } else {
            m_slaveWriterArray[i].bgSlaveDiskPageWriter->WakeupIfSleeping();
        }
    }
}

void BgDiskPageMasterWriter::WaitSlaveWriterFlushFinish() const
{
    for (uint32 i = 0; i < m_slaveNum; i++) {
        while (m_slaveWriterArray[i].bgSlaveDiskPageWriter->IsFlushing()) {
            GaussUsleep(MICRO_PER_MILLI_SEC);
        }
    }
}

uint64 BgDiskPageMasterWriter::GetMinRecoveryPlsn() const
{
    return m_recoveryPlsn.load(std::memory_order_acquire);
}

bool BgDiskPageMasterWriter::WaitNowAllDirtyPageFlushed() const
{
    /* Step1: fitst get now all dirty page count, assert last page is PageX, which is last page we wait flush */
    uint64 nowDirtyPageCnt = m_dirtyPageQueue->GetPageNum();
    if (nowDirtyPageCnt == 0) {
        return true;
    }

    /*
     * Step2: then get all removed dirtyPage count, notice some page might flushed during step1 and step2.
     */
    uint64 start = m_dirtyPageQueue->GetTotalRemoveCnt();

    /*
     * Step3: wait all dirty page corresponding to nowDirtyPageCnt flushed. judge by two condition:
     *
     * condition1: m_dirtyPageQueue->GetTotalRemoveCnt() - start < nowDirtyPageCnt
     * 1.1: no dirty page fluhsed during step1 and step2, conditon1 match accurately at PageX
     * 1.2: Y dirty page fluhsed during step1 and step2, condition 1 match at page after PageX + Y
     *
     * conditon2: m_dirtyPageQueue->GetPageNum() != 0
     * on condition 1.2, if not enough Y pages after PageX, then just need to wait DirtypageQueue empty
     */
    while ((m_dirtyPageQueue->GetTotalRemoveCnt() - start < nowDirtyPageCnt) && m_dirtyPageQueue->GetPageNum() != 0) {
        GaussUsleep(MICRO_PER_MILLI_SEC);
    }
    return true;
}

bool BgDiskPageMasterWriter::NeedFlushAll()
{
    return m_flushAll.load(std::memory_order_acquire);
}

char *BgDiskPageMasterWriter::Dump()
{
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_BGPAGEWRITER, ErrMsg("pdb %u is nullptr", m_pdbId));
    int64 slotId = GetWalStreamSlotId();
    if (slotId == INVALID_BGWRITER_SLOT_ID) {
        slotId = pdb->GetBgPageWriterMgr()->GetBgWriterSlotIdByWalId(m_walStream->GetWalId());
    }
    if (slotId < 0 || slotId >= DIRTY_PAGE_QUEUE_MAX_SIZE) {
        ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("Dump, cannot find this slotId %ld.", slotId));
        return nullptr;
    }

    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("cannot allocate memory for dirtyPageQueue dump info."));
        return nullptr;
    }
    if (m_dirtyPageQueue != nullptr) {
        char bufferInfo[BUFFER_DESC_FORMAT_SIZE];
        BufferDesc *current = m_dirtyPageQueue->GetHead();
        while (!DirtyPageQueue::IsEnd(current)) {
            if (current->IsPageDirty()) {
                current->PrintBufferDesc(bufferInfo, BUFFER_DESC_FORMAT_SIZE);
                dumpInfo.AppendString(bufferInfo);
                dumpInfo.AppendString("\n");
            }
            current = DirtyPageQueue::GetNext(current, slotId);
        }
    }
    dumpInfo.append("(Note that the dirty page infos are estimates only)\n");
    return dumpInfo.data;
}

BgDiskPageSlaveWriter::BgDiskPageSlaveWriter(CandidateFlushCxt *flushCxt, BgDiskPageMasterWriter* master)
    : BgPageSlaveWriter(flushCxt), m_useAio(USE_VFS_AIO), m_master(master), batchCtxMgr(nullptr)
{}

void BgDiskPageSlaveWriter::SeizeDirtyPageListForFlush()
{
    uint32 maxBatchFlush = 1000;
    uint32 totalFlushCnt = m_flushCxt->GetValidSize();
    m_startFlushLoc = m_flushCxt->ScrambleLoc(maxBatchFlush);
    if (m_startFlushLoc >= totalFlushCnt) {
        m_needFlushCnt = 0;
        return;
    }

    m_needFlushCnt =
        (m_startFlushLoc + maxBatchFlush) > totalFlushCnt ? (totalFlushCnt - m_startFlushLoc) : maxBatchFlush;
    ErrLog(DSTORE_DEBUG1, MODULE_BGPAGEWRITER,
        ErrMsg("BgSlavePageWriter %lu for pdb %hhu walId %lu will Flush startLoc %u, cnt %u",
               thrd->GetCore()->pid, m_master->GetWalStream()->GetPdbId(), m_master->GetWalStream()->GetWalId(),
               m_startFlushLoc, m_needFlushCnt));
}

BatchBufferAioContextMgr* BgDiskPageSlaveWriter::GetBatchBufferAioContextMgr()
{
    return batchCtxMgr;
}

RetStatus BgDiskPageSlaveWriter::Init()
{
    if (STORAGE_FUNC_FAIL(BgPageWriterBase::Init())) {
        return DSTORE_FAIL;
    }
    batchCtxMgr = nullptr;
    if (m_useAio) {
        batchCtxMgr = DstoreNew(g_dstoreCurrentMemoryContext) BatchBufferAioContextMgr();
        if (STORAGE_VAR_NULL(batchCtxMgr)) {
            ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("alloc batchCtxMgr fail"));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(batchCtxMgr->InitBatch(false, g_storageInstance->GetBufferMgr()))) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

void BgDiskPageSlaveWriter::Run()
{
    ErrLog(DSTORE_LOG, MODULE_BGPAGEWRITER, ErrMsg("BgSlavePageWriter pdbId %u, %lu start", m_master->GetPdbId(),
        thrd->GetCore()->pid));

    while (true) {
        thrd->RefreshWorkingVersionNum();
        WaitNextFlush();
        if (IsStop()) {
            BgPageWriterExit();
            break;
        }
    LOOP:
        SeizeDirtyPageListForFlush();
        if (m_needFlushCnt == 0) {
            continue;
        }
        FlushCandidateDirtyPage(batchCtxMgr);
        if (m_useAio) {
            batchCtxMgr->FsyncBatch();
        }
        if (m_flushCxt->GetStartFlushLoc() < m_flushCxt->GetValidSize()) {
            goto LOOP;
        }
    }
    if (m_useAio) {
        batchCtxMgr->FsyncBatch();
        batchCtxMgr->DestoryBatch();
        delete batchCtxMgr;
    }
    ErrLog(DSTORE_LOG, MODULE_BGPAGEWRITER, ErrMsg("BgDiskPageSlaveWriter pdbId %u, %lu exited MainLoop",
        m_master->GetPdbId(), thrd->GetCore()->pid));
}

void BgDiskPageSlaveWriter::FlushCandidateDirtyPage(BatchBufferAioContextMgr *batchBufferCtxMgr)
{
    RetStatus ret = DSTORE_SUCC;
    uint32 currentFlushLoc = m_startFlushLoc;
    uint32 needFlushCnt = m_needFlushCnt;
    do {
        BufferDesc *bufferDesc = m_flushCxt->candidateFlushArray[currentFlushLoc];
        BufferTag bufTag = bufferDesc->GetBufferTag();
        if (m_useAio) {
            ret = batchBufferCtxMgr->AsyncFlushPage(bufTag);
        } else {
            ret = g_storageInstance->GetBufferMgr()->Flush(bufTag);
        }
        StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_BGPAGEWRITER, ErrMsg("Flush bufTag:(%hhu, %hu, %u) failed.",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
        currentFlushLoc++;
    } while (--needFlushCnt > 0);

    ErrLog(DSTORE_DEBUG1, MODULE_BGPAGEWRITER, ErrMsg(
        "BgSlavePageWriter %lu Flushed cnt %u", thrd->GetCore()->pid, m_needFlushCnt));
}

}  // namespace DSTORE
