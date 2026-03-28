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
 *        src/buffer/dstore_bg_writer.cppp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <thread>
#include <csignal>
#include "buffer/dstore_bg_disk_page_writer.h"
#include "errorcode/dstore_buf_error_code.h"
#include "common/log/dstore_log.h"
#include "wal/dstore_wal_logstream.h"
#include "common/memory/dstore_mctx.h"
#include "errorcode/dstore_buf_error_code.h"
#include "framework/dstore_instance.h"
#include "common/concurrent/dstore_atomic.h"
#include "buffer/dstore_bg_page_writer_mgr.h"

namespace DSTORE {

void BgPageWriterMgr::Init()
{
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; i++) {
        m_bgPageWriterArray[i].walId = INVALID_WAL_ID;
        m_bgPageWriterArray[i].walStream = nullptr;
        m_bgPageWriterArray[i].dirtyPageQueue = nullptr;
        m_bgPageWriterArray[i].bgDiskPageWriter = nullptr;
        m_bgPageWriterArray[i].bgDiskThread = nullptr;
    }
}

void BgPageWriterMgr::Destroy()
{
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; i++) {
        if (m_bgPageWriterArray[i].walId != INVALID_WAL_ID) {
            m_bgPageWriterArray[i].dirtyPageQueue->Destroy();
            m_bgPageWriterArray[i].dirtyPageQueue = nullptr;
        }
    }
}

RetStatus BgPageWriterMgr::CreateBgPageWriter(const WalStream *walStream, int64 *slot, bool primarySlot)
{
    WalId walId = walStream->GetWalId();
    std::lock_guard<std::mutex> lockGuard(m_arrayMutex);
    int32 freeSlot = -1;
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; i++) {
        if (m_bgPageWriterArray[i].walId == walId) {
            ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("BgPageWriter of wal %lu is already created", walId));
            return DSTORE_FAIL;
        }

        if ((m_bgPageWriterArray[i].walId == INVALID_WAL_ID) && (freeSlot == -1)) {
            /* find a free slot */
            freeSlot = static_cast<int32>(i);
        }
    }

    if (freeSlot == -1) {
        /* no free slot to create */
        storage_set_error(BGPAGEWRITER_ERROR_NO_AVAILABLE_SLOT);
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR, ErrMsg("Don't have available slot to create new BgPageWriter"));
        return DSTORE_FAIL;
    }

    m_bgPageWriterArray[freeSlot].walId = walId;
    m_bgPageWriterArray[freeSlot].walStream = walStream;
    m_bgPageWriterArray[freeSlot].dirtyPageQueue = DstoreNew(m_mcxt) DirtyPageQueue();
    if (m_bgPageWriterArray[freeSlot].dirtyPageQueue == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
            ErrMsg("alloc memory for m_bgPageWriterArray[%d].dirtyPageQueue fail", freeSlot));
        return DSTORE_FAIL;
    }
    m_bgPageWriterArray[freeSlot].dirtyPageQueue->Init(walStream);
    m_bgPageWriterArray[freeSlot].bgDiskThread = nullptr;
    m_bgPageWriterArray[freeSlot].bgDiskPageWriter =
        DstoreNew(m_mcxt) BgDiskPageMasterWriter(walStream, m_bgPageWriterArray[freeSlot].dirtyPageQueue, m_pdbId,
        (primarySlot ? freeSlot : INVALID_BGWRITER_SLOT_ID));
    if (m_bgPageWriterArray[freeSlot].bgDiskPageWriter == nullptr) {
        DstorePfreeExt(m_bgPageWriterArray[freeSlot].dirtyPageQueue);
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
            ErrMsg("alloc memory for m_bgPageWriterArray[%d].bgDiskPageWriter fail", freeSlot));
        return DSTORE_FAIL;
    }
    StorageReleasePanic(g_storageInstance->GetPdb(m_pdbId) == nullptr, MODULE_BUFMGR,
        ErrMsg("pdb %u is nullptr", m_pdbId));
    if (primarySlot) {
        *slot = freeSlot;
    } else {
        *slot = INVALID_BGWRITER_SLOT_ID;
    }
    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("CreateBgPageWriter pdbId %u, freeSlot %d, walId %lu, primarySlot %d",
        m_pdbId, freeSlot, walId, primarySlot));
    return DSTORE_SUCC;
}

BgPageWriterEntry *BgPageWriterMgr::GetBgPageWriterEntry(const int64 slotId)
{
    return &m_bgPageWriterArray[slotId];
}

int64 BgPageWriterMgr::GetBgWriterSlotIdByWalId(const WalId walId) const
{
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; i++) {
        if (m_bgPageWriterArray[i].walId == walId) {
            return i;
        }
    }
    return INVALID_BGWRITER_SLOT_ID;
}

template BgDiskPageMasterWriter *BgPageWriterMgr::GetBgPageWriter<BgDiskPageMasterWriter>(WalId walId);

template <typename T>
T *BgPageWriterMgr::GetBgPageWriter(WalId walId)
{
    T *bgPageWriter = nullptr;
    /*
     * In general, the number of WalStream will not be changed frequently.
     * The m_arrayLwLock will not become a point of contention.
     */
    std::lock_guard<std::mutex> lockGuard(m_arrayMutex);
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; i++) {
        if (m_bgPageWriterArray[i].walId == walId) {
            if (std::is_same<T, BgDiskPageMasterWriter>::value) {
                bgPageWriter = reinterpret_cast<T *>(m_bgPageWriterArray[i].bgDiskPageWriter);
            }
            break;
        }
    }
    return bgPageWriter;
}

RetStatus BgPageWriterMgr::PushDirtyPageToQueue(BufferDesc *bufferDesc, bool needUpdateRecoveryPlsn, const int64 slotId)
{
    DirtyPageQueue *dirtyPageQueue = m_bgPageWriterArray[slotId].dirtyPageQueue;
    BgDiskPageMasterWriter *bgPageWriter = m_bgPageWriterArray[slotId].bgDiskPageWriter;
    if (STORAGE_VAR_NULL(bgPageWriter) || !bgPageWriter->IsReady()) {
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
               ErrMsg("Fail to push dirty page bufTag:(%hhu, %hu, %u) into queue because of the"
                      "bgWriter is not ready.",
                      bufferDesc->GetPdbId(), bufferDesc->GetPageId().m_fileId, bufferDesc->GetPageId().m_blockId));
        storage_set_error(BGPAGEWRITER_WARNING_PAGEWRITER_NOT_READY);
        return DSTORE_FAIL;
    }
    StorageReleasePanic(bufferDesc->GetPdbId() != m_pdbId, MODULE_BGPAGEWRITER,
                        ErrMsg("Cannot put other PDB pages into the queue!"));
    dirtyPageQueue->Push(bufferDesc, needUpdateRecoveryPlsn, slotId, bgPageWriter->GetMinRecoveryPlsn(),
        bgPageWriter->GetWalStreamSlotId() != INVALID_BGWRITER_SLOT_ID);
    return DSTORE_SUCC;
}

void BgPageWriterMgr::FlushAllDirtyPages()
{
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; ++i) {
        if (m_bgPageWriterArray[i].bgDiskPageWriter == nullptr) {
            continue;
        }
        m_bgPageWriterArray[i].bgDiskPageWriter->FlushAllDirtyPages();
    }
}

BgDiskPageMasterWriter *BgPageWriterMgr::GetBgPageWriterBySlot(const int64 slotId)
{
    return m_bgPageWriterArray[slotId].bgDiskPageWriter;
}

/*
 * The method will be called when other thread is stopped, and the number of WalStream will not be changed.
 * So there is no protection of m_arrayLwLock in this method.
 */
void BgPageWriterMgr::StopAllBgPageWriter()
{
    std::lock_guard<std::mutex> lockGuard(m_arrayMutex);
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; i++) {
        BgPageWriterEntry *entry = &m_bgPageWriterArray[i];
        if (entry->bgDiskPageWriter != nullptr && !entry->bgDiskPageWriter->IsStop()) {
            entry->bgDiskPageWriter->Stop();
            if (entry->bgDiskThread != nullptr) {
                entry->bgDiskThread->join();
                delete entry->bgDiskThread;
                entry->bgDiskThread = nullptr;
            }
            delete entry->bgDiskPageWriter;
            entry->bgDiskPageWriter = nullptr;
        }
        entry->walId = INVALID_WAL_ID;
    }
}

void BgPageWriterMgr::StopOneBgPageWriter(WalId walId)
{
    std::lock_guard<std::mutex> lockGuard(m_arrayMutex);
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; i++) {
        BgPageWriterEntry *entry = &m_bgPageWriterArray[i];
        if (entry->walId != walId) {
            continue;
        }
        if (entry->bgDiskPageWriter != nullptr && !entry->bgDiskPageWriter->IsStop()) {
            entry->bgDiskPageWriter->Stop();
            if (entry->bgDiskThread != nullptr) {
                entry->bgDiskThread->join();
                delete entry->bgDiskThread;
                entry->bgDiskThread = nullptr;
            }
            delete entry->bgDiskPageWriter;
            entry->bgDiskPageWriter = nullptr;
        }
        entry->walId = INVALID_WAL_ID;
    }
}

RetStatus BgPageWriterMgr::StartupBgPageWriter(int64 slotId)
{
    RetStatus diskRet = DSTORE_SUCC;
    constexpr uint32 waitReadyMicrosec = 1000;

    BgPageWriterEntry *entry = GetBgPageWriterEntry(slotId);
    entry->bgDiskThread =
        new std::thread(BgPageWriterMain<BgDiskPageMasterWriter>, entry->bgDiskPageWriter, m_pdbId, std::ref(diskRet));
    /* wait until BgPageWriter thread is ready */
    while (!entry->bgDiskPageWriter->IsReady()) {
        GaussUsleep(waitReadyMicrosec);
    }
    if (STORAGE_FUNC_FAIL(diskRet)) {
        ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("filed to start diskWriter!"));
        return diskRet;
    }
    return DSTORE_SUCC;
}

template void BgPageWriterMgr::BgPageWriterMain<BgDiskPageMasterWriter>(BgDiskPageMasterWriter *bgPageWriter,
                                                                        PdbId pdbId, RetStatus &ret);
template <typename T>
void BgPageWriterMgr::BgPageWriterMain(T *bgPageWriter, PdbId pdbId, RetStatus &ret)
{
    InitSignalMask();

    /* create and register thread */
    StorageReleasePanic(bgPageWriter == nullptr, MODULE_BGPAGEWRITER, ErrMsg("BgPageWriter is nullptr"));
    StorageAssert(thrd == nullptr);
    if (std::is_same<T, BgDiskPageMasterWriter>::value) {
        ret = g_storageInstance->CreateThreadAndRegister(pdbId, false, "DiskMstrWriter", true,
                                                         ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    } else {
        ret = g_storageInstance->CreateThreadAndRegister(pdbId, false, "MemMstrWriter", true,
                                                         ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    }

    ret = STORAGE_FUNC_SUCC(ret) ? bgPageWriter->Init() : ret;

    bgPageWriter->SetReady();

    if (STORAGE_FUNC_SUCC(ret)) {
        bgPageWriter->Run();
    }

    /* unregister thread */
    g_storageInstance->UnregisterThread();
}

uint32 BgPageWriterMgr::GetBgPageWriterArraySize() const
{
    return BG_PAGE_WRITER_ARRAY_SIZE;
}

uint64 BgPageWriterMgr::GetTotalDirtyPageCnt() const
{
    uint64 totalDirtyCount = 0;
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; i++) {
        if (m_bgPageWriterArray[i].bgDiskPageWriter == nullptr) {
            continue;
        }
        totalDirtyCount += m_bgPageWriterArray[i].dirtyPageQueue->GetPageNum();
    }
    return totalDirtyCount;
}

char *BgPageWriterMgr::DumpSummaryInfo()
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("cannot allocate memory for bgPageWriter dump info."));
        return nullptr;
    }
    uint64 totalDirtyPage = 0;
    for (uint32 i = 0; i < GetBgPageWriterArraySize(); i++) {
        dumpInfo.append("SlotId:%3u", i);
        uint64 dirtyPage = 0;
        if (m_bgPageWriterArray[i].walId == INVALID_WAL_ID) {
            dumpInfo.append(" WalId: -1");
        } else {
            dumpInfo.append(" WalId:%lu", m_bgPageWriterArray[i].walId);
        }
        BgDiskPageMasterWriter *bgpageWriter = m_bgPageWriterArray[i].bgDiskPageWriter;
        if (bgpageWriter == nullptr) {
            dumpInfo.append(" BgPageWriter:        nullptr");
        } else {
            dumpInfo.append(" BgPageWriter::%p", bgpageWriter);
            dumpInfo.append(" IsReady::%s", (bgpageWriter->IsReady() ? "true" : "false"));
            dumpInfo.append(" IsStop::%s", (bgpageWriter->IsStop() ? "true" : "false"));
            dirtyPage = GetTotalDirtyPageCnt();
            dumpInfo.append(" dirtyPage::%lu", dirtyPage);
        }
        totalDirtyPage += dirtyPage;
        dumpInfo.append("\n");
    }
    dumpInfo.append("-----------------------------------------------------\n");
    dumpInfo.append("BgPageWriter array size:%u\n", GetBgPageWriterArraySize());
    dumpInfo.append("Number of dirty pages in total:%lu (blocks)", totalDirtyPage);
    return dumpInfo.data;
}

uint32 BgPageWriterMgr::GetFlushInfo(AioCompleterInfo **infos)
{
    uint32 length = 0;
    for (uint32 i = 0; i < GetBgPageWriterArraySize(); i++) {
        BgDiskPageMasterWriter *bgpageWriter = m_bgPageWriterArray[i].bgDiskPageWriter;
        if (bgpageWriter != nullptr) {
            length += bgpageWriter->GetSlaveNum();
        }
    }
    *infos = static_cast<AioCompleterInfo *>(DstorePalloc(length * sizeof(AioCompleterInfo)));
    if (STORAGE_VAR_NULL(*infos)) {
        ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("no memory for AioCompleterInfo"));
        return 0;
    }
    uint32 currentIndex = 0;
    for (uint32 i = 0; i < GetBgPageWriterArraySize(); i++) {
        BgDiskPageMasterWriter *bgpageWriter = m_bgPageWriterArray[i].bgDiskPageWriter;
        if (bgpageWriter == nullptr) {
            continue;
        }
        uint32 slaveNum = bgpageWriter->GetSlaveNum();
        bgpageWriter->GetSlavePageWriters(*infos, currentIndex);
        currentIndex += slaveNum;
    }
    return length;
}

uint32 BgPageWriterMgr::GetSlotUsageInfo(AioSlotUsageInfo **infos)
{
    uint32 length = 0;
    for (uint32 i = 0; i < GetBgPageWriterArraySize(); i++) {
        BgDiskPageMasterWriter *bgpageWriter = m_bgPageWriterArray[i].bgDiskPageWriter;
        if (bgpageWriter != nullptr) {
            length += bgpageWriter->GetSlaveNum();
        }
    }
    *infos = static_cast<AioSlotUsageInfo *>(DstorePalloc(length * BATCH_AIO_SIZE * sizeof(AioSlotUsageInfo)));
    if (STORAGE_VAR_NULL(*infos)) {
        ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("no memory for AioSlotUsageInfo"));
        return 0;
    }
    uint32 currentIndex = 0;
    for (uint32 i = 0; i < GetBgPageWriterArraySize(); i++) {
        BgDiskPageMasterWriter *bgpageWriter = m_bgPageWriterArray[i].bgDiskPageWriter;
        if (bgpageWriter == nullptr) {
            continue;
        }
        currentIndex += bgpageWriter->GetSlaveSlotInfo(*infos, i, currentIndex);
    }
    return currentIndex;
}

uint32 BgPageWriterMgr::GetPagewriterInfo(PagewriterInfo **infos)
{
    uint32 length = 0;
    for (uint32 i = 0; i < GetBgPageWriterArraySize(); i++) {
        BgDiskPageMasterWriter *bgpageWriter = m_bgPageWriterArray[i].bgDiskPageWriter;
        if (bgpageWriter != nullptr) {
            length++;
        }
    }
    *infos = static_cast<PagewriterInfo *>(DstorePalloc(length * sizeof(PagewriterInfo)));
    if (STORAGE_VAR_NULL(*infos)) {
        ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("no memory for PagewriterInfo"));
        return 0;
    }
    uint32 j = 0;
    for (uint32 i = 0; i < GetBgPageWriterArraySize(); i++) {
        DirtyPageQueue *dirtyPageQueue = m_bgPageWriterArray[i].dirtyPageQueue;
        BgDiskPageMasterWriter *bgpageWriter = m_bgPageWriterArray[i].bgDiskPageWriter;
        if (dirtyPageQueue == nullptr || bgpageWriter == nullptr) {
            continue;
        }
        (*infos)[j].nodeId = g_storageInstance->GetGuc()->selfNodeId;
        (*infos)[j].actualFlushCnt = dirtyPageQueue->m_statisticInfo.actualFlushCnt.load(std::memory_order_acquire);
        (*infos)[j].lastRemoveCnt = dirtyPageQueue->m_statisticInfo.lastRemoveCnt;
        (*infos)[j].pushQueueTotalCnt = dirtyPageQueue->m_statisticInfo.pushQueueTotalCnt.load(std::memory_order_acquire);
        (*infos)[j].removeTotalCnt = GsAtomicReadU64(&dirtyPageQueue->m_statisticInfo.removeTotalCnt);
        (*infos)[j].recoveryPlsn = bgpageWriter->GetMinRecoveryPlsn();
        j++;
    }
    return length;
}

void BgPageWriterMgr::FreeAioCompleterInfoArr(AioCompleterInfo *infos)
{
    DstorePfreeExt(infos);
}

void BgPageWriterMgr::FreeAioSlotUsageInfoArr(AioSlotUsageInfo *infos)
{
    DstorePfreeExt(infos);
}

void BgPageWriterMgr::FreePagewriterInfoArr(PagewriterInfo *infos)
{
    DstorePfreeExt(infos);
}

bool BgPageWriterMgr::IsContains(BgDiskPageMasterWriter *bgPageWriter)
{
    if (bgPageWriter == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lockGuard(m_arrayMutex);
    for (uint32 i = 0; i < BG_PAGE_WRITER_ARRAY_SIZE; i++) {
        if (m_bgPageWriterArray[i].bgDiskPageWriter == bgPageWriter) {
            return true;
        }
    }
    return false;
}

DirtyPageQueue::DirtyPageQueue() : m_statisticInfo{}, m_queueInfoPtr(nullptr), m_walStream{nullptr}
{
    for (int i = 0; i < HEX_ALIGN; i++) {
        m_queueInfo.pad[i] = 0;
    }
    m_queueInfo.head = nullptr;
    m_queueInfo.tail = nullptr;
}

void DirtyPageQueue::Init(const WalStream *walStream)
{
    m_queueInfoPtr = &m_queueInfo;
    PointerToAddress queueInfo = {.address = TYPEALIGN(HEX_ALIGN, m_queueInfoPtr)};
    m_queueInfoPtr = static_cast<QueueInfo *>(queueInfo.pointer);
    m_queueInfoPtr->head = nullptr;
    m_queueInfoPtr->tail = nullptr;
    m_queueInfoPtr->dirtyPageCnt = 0;
    m_walStream = walStream;
}

void DirtyPageQueue::Destroy()
{
    m_queueInfoPtr = nullptr;
    m_walStream = nullptr;
}

/*
 * Push Dirty Page To Queue
 *
 * Pushes the page indicated by the provided BufferDesc *bufferDesc
 * to the first free slot in the DirtyPageQueue.
 *
 * Returns rc = DSTORE_SUCC for success, DSTORE_FAIL for failure (the page could not be
 * inserted in the current DirtyPageQueue).
 */
void DirtyPageQueue::Push(BufferDesc *bufferDesc, bool needUpdateRecoveryPlsn, const int64 slotId,
                          uint64 minRecoveryPlsn, bool isWritingWalStream)
{
    StorageAssert(bufferDesc->GetState(false) & Buffer::BUF_LOCKED);
    if (slotId < 0 || slotId >= DIRTY_PAGE_QUEUE_MAX_SIZE) {
        ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("Push, cannot find this slotId %ld.", slotId));
        return;
    }
    /* Return directly if this buffer_desc has been enqueued already. */
    if (bufferDesc->IsInDirtyPageQueue(slotId)) {
        return;
    }

    StorageAssert(bufferDesc->nextDirtyPagePtr[slotId].load(std::memory_order_acquire) == INVALID_BUFFER_DESC);
    GS_READ_BARRIER();
    /*
     * the dirty page is not in the dirty list
     * Use CAS method to update the contents at the tailPointer
     */
    /* The tail index has been successfully iterated forwards at this point, the old index is now safely ours to use. */
    BufferDesc *newQueueTailPointer = bufferDesc;
    BufferDesc *oldQueueTailPointer = nullptr;

    uint64 recoveryPlsn = INVALID_PLSN;
    PreOccupyTail(newQueueTailPointer, oldQueueTailPointer, recoveryPlsn, isWritingWalStream);
    if (needUpdateRecoveryPlsn) {
        newQueueTailPointer->recoveryPlsn[slotId].store(recoveryPlsn, std::memory_order_release);
        StorageReleasePanic(recoveryPlsn < minRecoveryPlsn, MODULE_BGPAGEWRITER, ErrMsg("Invalid recoveryPlsn!"));
    }

    GS_WRITE_BARRIER();
    if (oldQueueTailPointer != nullptr) {
        StorageReleasePanic(oldQueueTailPointer->recoveryPlsn[slotId].load(std::memory_order_acquire) > recoveryPlsn,
                            MODULE_BGPAGEWRITER, ErrMsg("Invalid recoveryPlsn!"));
        oldQueueTailPointer->nextDirtyPagePtr[slotId].store(newQueueTailPointer, std::memory_order_release);
    }

    (void)GsAtomicFetchAddU64(&m_queueInfoPtr->dirtyPageCnt, 1);
    (void)++m_statisticInfo.pushQueueTotalCnt;
}

/*
 * make sure the buffer header is locked,
 * so no one can push/remove this buffer from the dirty page queue
 */
void DirtyPageQueue::Remove(BufferDesc *bufferDesc, const int64 slotId)
{
    uint64 state = bufferDesc->GetState(false);
    StorageAssert(state & Buffer::BUF_LOCKED);

    StorageReleasePanic(!bufferDesc->IsInDirtyPageQueue(slotId), MODULE_BGPAGEWRITER,
        ErrMsg("It must be in the queue"));

    bufferDesc->nextDirtyPagePtr[slotId].store(INVALID_BUFFER_DESC, std::memory_order_seq_cst);

    if ((state & (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY)) == 0U) {
        bufferDesc->recoveryPlsn[slotId].store(INVALID_PLSN, std::memory_order_seq_cst);
    }

    (void)GsAtomicFetchSubU64(&m_queueInfoPtr->dirtyPageCnt, 1);
}

uint64 DirtyPageQueue::GetMinRecoveryPlsn() const
{
    BufferDesc *head = m_queueInfoPtr->head;
    StoragePdb *pdb = g_storageInstance->GetPdb(m_walStream->GetPdbId());
    StorageReleasePanic(pdb == nullptr, MODULE_BGPAGEWRITER, ErrMsg("pdb %u is nullptr", m_walStream->GetPdbId()));
    int64 slot = pdb->GetBgWriterSlotId(m_walStream->GetWalId());
    if (slot < 0 || slot >= DIRTY_PAGE_QUEUE_MAX_SIZE) {
        ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("GetMinRecoveryPlsn, cannot find this slotId %ld.", slot));
        return INVALID_PLSN;
    }
    if (head) {
        TsAnnotateHappensAfter(&head->recoveryPlsn);
        return head->recoveryPlsn[slot].load(std::memory_order_acquire);
    }
    return INVALID_PLSN;
}

uint64 DirtyPageQueue::GetPageNum() const
{
    return GsAtomicReadU64(&m_queueInfoPtr->dirtyPageCnt);
}

void DirtyPageQueue::PreOccupyTail(BufferDesc *newQueueTailPointer, BufferDesc *&oldQueueTailPointer,
                                   uint64 &recoveryPlsn, bool isWritingWalStream)
{
    uint128_u compare;
    uint128_u exchange;
    uint128_u current;
    bool isnull = false;

    compare = atomic_compare_and_swap_u128(static_cast<volatile uint128_u *>(static_cast<void *>(m_queueInfoPtr)));
    StorageAssert(sizeof(m_queueInfoPtr->head) == SIZEOF_VOID_P);
    StorageAssert(sizeof(m_queueInfoPtr->tail) == SIZEOF_VOID_P);

LOOP:
    if (reinterpret_cast<BufferDesc *>(compare.u64[0]) == nullptr) {
        exchange.u64[0] = reinterpret_cast<uint64>(newQueueTailPointer);
        exchange.u64[1] = reinterpret_cast<uint64>(newQueueTailPointer);
        isnull = true;
    } else {
        exchange.u64[0] = compare.u64[0];
        exchange.u64[1] = reinterpret_cast<uint64>(newQueueTailPointer);
    }
    if (isWritingWalStream) {
        recoveryPlsn = m_walStream->GetMaxAppendedPlsn();
    } else {
        recoveryPlsn = m_walStream->GetStandbyPdbRecoveryPlsn();
    }

    recoveryPlsn = (recoveryPlsn == INVALID_PLSN) ? 1 : recoveryPlsn;
    TsAnnotateHappensBefore(&newQueueTailPointer->recoveryPlsn);
    current = atomic_compare_and_swap_u128(static_cast<volatile uint128_u *>(static_cast<void *>(m_queueInfoPtr)),
                                           compare, exchange);
    if (compare.u128 != current.u128) {
        compare.u128 = current.u128;
        isnull = false;
        goto LOOP;
    }

    if (!isnull) {
        oldQueueTailPointer = reinterpret_cast<BufferDesc *>(compare.u64[1]);
    }
}

void DirtyPageQueue::UpdateHead(BufferDesc *entry, const int64 slotId)
{
    uint128_u exchange, current;
    uint128_u compare =
        atomic_compare_and_swap_u128(static_cast<volatile uint128_u *>(static_cast<void *>(m_queueInfoPtr)));

    BufferDesc *nextEntry = nullptr;
LOOP:
    GS_READ_BARRIER();
    nextEntry = entry->nextDirtyPagePtr[slotId].load(std::memory_order_acquire);
    if (nextEntry == nullptr) {
        if (reinterpret_cast<uint64>(entry) != compare.u64[1]) {
            goto LOOP;
        }
        exchange.u64[0] = static_cast<uint64>(0);
        exchange.u64[1] = static_cast<uint64>(entry->recoveryPlsn[slotId]);
    } else {
        exchange.u64[0] = reinterpret_cast<uint64>(nextEntry);
        exchange.u64[1] = compare.u64[1];
    }
    current = atomic_compare_and_swap_u128(static_cast<volatile uint128_u *>(static_cast<void *>(m_queueInfoPtr)),
                                           compare, exchange);
    if (compare.u128 != current.u128) {
        compare.u128 = current.u128;
        goto LOOP;
    }
}

uint64 DirtyPageQueue::AdvanceHeadAfterFlush(BufDescVector &tmpDirtyPageVec, uint64 advanceNum, const int64 slotId)
{
    StorageAssert(advanceNum <= GetPageNum());
    uint64 removeCnt = 0;
    uint64 recoveryPlsn = INVALID_PLSN;
    if (advanceNum == 0) {
        /*
         * Update the recovery plsn with the tail plsn of the wal stream, if the queue is empty or is all dirty.
         *
         * We should advance the recovery plsn of the queue ASAP. if the queue is empty, the recovery plsn of the
         * queue can be updated with the tail plsn of the wal stream. Because of that we get the tail plsn first
         * and then get the size of the queue. So if the queue is empty, it means that all the dirty page which is
         * generated before the tail plsn of the wal stream have been flushed. So this make sense.
         */
        m_statisticInfo.lastRemoveCnt = 0;
        return recoveryPlsn;
    }

    TsAnnotateBenignRaceSized(&m_queueInfoPtr->head, sizeof(m_queueInfoPtr->head));
    BufferDesc *currentEntry = m_queueInfoPtr->head;
    BufferDesc *nextEntry = nullptr;
    while (advanceNum-- > 0) {
        StorageAssert(currentEntry != INVALID_BUFFER_DESC);
        nextEntry = currentEntry->nextDirtyPagePtr[slotId].load(std::memory_order_acquire);
        StorageReleasePanic(advanceNum > 0 && nextEntry == INVALID_BUFFER_DESC, MODULE_BGPAGEWRITER,
                            ErrMsg("nextEntry must not be null"));
        uint64 state = currentEntry->LockHdr();
        if (state & (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY)) {
            if (STORAGE_FUNC_FAIL(tmpDirtyPageVec.Push(currentEntry))) {
                recoveryPlsn = currentEntry->recoveryPlsn[slotId].load(std::memory_order_acquire);
                currentEntry->UnlockHdr(state);
                break;
            }
        }
        recoveryPlsn = currentEntry->recoveryPlsn[slotId].load(std::memory_order_acquire);
        UpdateHead(currentEntry, slotId);
        Remove(currentEntry, slotId);
        currentEntry->UnlockHdr(state);
        removeCnt++;
        currentEntry = nextEntry;
    }

    m_statisticInfo.lastRemoveCnt = removeCnt;
    (void)GsAtomicAddFetchU64(&m_statisticInfo.removeTotalCnt, removeCnt);

    return recoveryPlsn;
}

bool DirtyPageQueue::UpdateHeadWhenEmpty(uint64 recoveryplsn)
{
    uint128_u exchange;
    uint128_u current;
    uint128_u compare =
        atomic_compare_and_swap_u128(static_cast<volatile uint128_u *>(static_cast<void *>(m_queueInfoPtr)));
    if (reinterpret_cast<BufferDesc *>(compare.u64[0]) == nullptr) {
        exchange.u64[0] = static_cast<uint64>(0);
        exchange.u64[1] = static_cast<uint64>(recoveryplsn);
    } else {
        return false;
    }
    current = atomic_compare_and_swap_u128(static_cast<volatile uint128_u *>(static_cast<void *>(m_queueInfoPtr)),
                                           compare, exchange);
    if (compare.u128 != current.u128) {
        return false;
    }
    return true;
}

BgPageSlaveWriter::BgPageSlaveWriter(CandidateFlushCxt *flushCxt)
    : BgPageWriterBase(), m_startFlushLoc(0), m_needFlushCnt(0),
      m_isFlushing(false), m_flushCxt(flushCxt)
{}

bool BgPageSlaveWriter::IsFlushing()
{
    std::unique_lock<std::mutex> waitLock(m_mutex);
    return m_isFlushing.load(std::memory_order_acquire);
}

void BgPageSlaveWriter::SeizeDirtyPageListForFlush()
{
    constexpr uint32 maxBatchFlush = 10000;
    uint32 totalFlushCnt = m_flushCxt->GetValidSize();
    m_startFlushLoc = m_flushCxt->ScrambleLoc(maxBatchFlush);
    if (m_startFlushLoc >= totalFlushCnt) {
        m_needFlushCnt = 0;
        return;
    }

    m_needFlushCnt =
        (m_startFlushLoc + maxBatchFlush) > totalFlushCnt ? (totalFlushCnt - m_startFlushLoc) : maxBatchFlush;
    ErrLog(DSTORE_DEBUG1, MODULE_BGPAGEWRITER,
           ErrMsg("BgSlavePageWriter %lu will Flush startLoc %u, cnt %u", thrd->GetCore()->pid, m_startFlushLoc,
                  m_needFlushCnt));
}

void BgPageSlaveWriter::WakeupIfStopping()
{
    if (IsStop()) {
        /* sleeping until slaver work finished and enter WaitNextFlush */
        while (m_isFlushing.load(std::memory_order_acquire)) {
            GaussUsleep(MICRO_PER_MILLI_SEC);
        }
        /* avoid hardly race that slave thread in WaitNextFlush set m_isFlushing false but not enter wait */
        std::unique_lock<std::mutex> waitLock(m_mutex);
        m_cv.notify_all();
    }
}
} /* namespace DSTORE */
