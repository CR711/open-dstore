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
 * dstore_bg_page_writer_mgr.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/buffer/dstore_bg_page_writer_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_BG_PAGE_WRITER_MGR_H
#define DSTORE_BG_PAGE_WRITER_MGR_H

#include <mutex>
#include <thread>
#include "errorcode/dstore_buf_error_code.h"
#include "buffer/dstore_buf.h"
#include "buffer/dstore_bg_page_writer_mgr.h"

namespace DSTORE {
class WalStream;

constexpr int64 INVALID_BGWRITER_SLOT_ID = -1;
constexpr int64 DEFAULT_BGWRITER_SLOT_ID = 0;
constexpr long MICRO_PER_MILLI_SEC = 1000L;
constexpr long NANO_PER_MILLI_SEC = 1000L * 1000L;

constexpr uint32 BG_PAGE_WRITER_ARRAY_SIZE = 5;
constexpr uint32 BG_PAGE_WRITER_MAX_SLAVE_NUM = 16;
constexpr uint64 INVALID_TIMESTAMP = 0;
struct AioCompleterInfo {
    uint32 nodeId;
    uint64 threadId;
    uint64 inProcessCnt;
    uint64 needFlushCnt;
    uint64 totalFlushedCnt;
};

struct AioSlotUsageInfo {
    uint32 nodeId;
    uint64 slotId;
    uint16 fileId;
    uint32 blockId;
    uint64 submittedTime;
    uint64 elapsedTime;
};

struct PagewriterInfo {
    uint32 nodeId;
    uint64 pushQueueTotalCnt;
    uint64 actualFlushCnt;
    uint64 removeTotalCnt;
    uint64 lastRemoveCnt;
    uint64 recoveryPlsn;
};
/*
 * DirtyPageQueue is a queue that records the order in which dirty pages are generated. When a dirty page generate,
 * it has a PLSN which represents a position which the WalStream is inserting at, the PLSN is also named recovery PLSN.
 * We always get the recovery PLSN from the WalStream first, and then push the dirty page in the queue,
 * at last flush the wal record of the dirty page into WalStream. So we can say
 *   recovery PLSN < PLSN of the page (1)
 * And because of the WAL rules, after we flush a dirty page, the wal record must be flushed exceed the recovery PLSN.
 * So we can say, the dirty pages before recovery PLSN must be flushed.
 *
 * The proof is as follows:
 * Base on (1), we have that
 * 'Get recovery PLSN from WalStream'     happens before
 * 'Push dirty page in queue'              happens before
 * 'Insert wal record in the WalStream'
 * So when we get the recovery PLSN, the the dirty page which insert the wal record into the WalStream and the PLSN is
 * equal to the recovery PLSN we get must be ahead of the dirty page we will push in the dirty page queue.
 * So when we flush the dirty page from queue head, the dirty page before the recovery PLSN must be flushed already.
 *
 * The DirtyPageQueue is a bounded MPSC queue. Multi thread can push in the queue simultaneously but only one thread can
 * pop from the queue.
 * In general, we start a thread to consume dirty pages in the queue, after flush the dirty page, we advance queue head.
 * And the size of DirtyPageQueue should be greater than or equal to the size of buffer pool.
 */
class DirtyPageQueue : public BaseObject {
public:
    DirtyPageQueue();
    ~DirtyPageQueue() = default;

    DISALLOW_COPY_AND_MOVE(DirtyPageQueue);

    void Init(const WalStream *walStream);

    void Destroy();

    /*
     * Push a buffer into the queue.
     *
     * @param buffer the buffer will be pushed
     * @return
     */
    void Push(BufferDesc *bufferDesc, bool needUpdateRecoveryPlsn, const int64 slotId, uint64 minRecoveryPlsn,
        bool isWritingWalStream = true);
    void UpdateHead(BufferDesc *entry, const int64 slotId);
    bool UpdateHeadWhenEmpty(uint64 recoveryplsn);
    /*
     * Remove the buffer from the slot to indicate the buffer is flushed.
     *
     * The buffer is flushed and remove it from the queue to avoid flush again in BgPageWriter thread.
     */
    void Remove(BufferDesc *bufferDesc, const int64 slotId);

    /*
     * Get the min recovery PLSN of the dirty page queue.
     */
    uint64 GetMinRecoveryPlsn() const;

    /*
     * Advance queue head after flush the dirty page in the queue.
     *
     * @return the last removed PLSN.
     * NOTE: the method should be called only in BgPageWriter thread.
     */
    uint64 AdvanceHeadAfterFlush(BufDescVector &tmpDirtyPageVec, uint64 advanceNum, const int64 slotId);

    /*
     * GetPageNum
     *
     * Returns the size of this DirtyPageQueue between the head and tail.
     * Some slots between head and tail may be removed but the slot
     * cannot be reused until they have been flushed and the head has advanced.
     *
     * Note: The guarantee we can provide is that returned value for the
     * current number of dirty pages in the queue will be equal to or an
     * overestimate of the true number of dirty pages that exist in
     * the queue at the time of the atomic read of the queue tail.
     *
     * The head and tail index of the queue can be obtained by passing a pointer.
     */
    uint64 GetPageNum() const;

    /*
     * Check if the iterator is reach the tail of the queue.
     */
    static bool IsEnd(BufferDesc *current)
    {
        return current == nullptr;
    }

    static BufferDesc *GetNext(BufferDesc *current, const int64 slotId)
    {
        return current->nextDirtyPagePtr[slotId].load(std::memory_order_acquire);
    }

    uint64 GetTotalPushCnt() const
    {
        return m_statisticInfo.pushQueueTotalCnt;
    }

    uint64 GetTotalRemoveCnt() const
    {
        return GsAtomicReadU64(const_cast<uint64 *>(&m_statisticInfo.removeTotalCnt));
    }

    uint64 GetRemoveCnt() const
    {
        return m_statisticInfo.lastRemoveCnt;
    }

    BufferDesc *GetHead()
    {
        return m_queueInfoPtr->head;
    }

    BufferDesc *GetTail()
    {
        return m_queueInfoPtr->tail;
    }

    struct QueueInfo {
        BufferDesc *head;
        BufferDesc *tail;
        gs_atomic_uint64 dirtyPageCnt;
        char pad[HEX_ALIGN];
    };

    struct {
        std::atomic_uint64_t pushQueueTotalCnt;
        std::atomic_uint64_t actualFlushCnt;
        gs_atomic_uint64 removeTotalCnt;
        uint64 lastRemoveCnt;
    } m_statisticInfo;

protected:
    void PreOccupyTail(BufferDesc *newQueueTailPointer, BufferDesc *&oldQueueTailPointer, uint64 &recoveryPlsn,
        bool isWritingWalStream = true);
    QueueInfo m_queueInfo;
    QueueInfo *m_queueInfoPtr;

    const WalStream *m_walStream;
};

/*
 * The data structure to record the relation between walId and BgPageWriter thread.
 * Each WalStream will start a BgPageWriter thread.
 *
 * NOTE: we chose array instead of hash table to store the relation between walId and BgPageWriter is because
 * the number of WalStream in one cluster node will not be too large.
 */
struct BgPageWriterEntry {
    WalId walId;
    const WalStream *walStream;
    DirtyPageQueue *dirtyPageQueue;
    class BgDiskPageMasterWriter *bgDiskPageWriter;
    std::thread *bgDiskThread;
};

/*
 * BgPageWriterMgr is a manager to handle Init/Destroy, Startup/Shutdown of a BgPageWriter backend for a WalStream.
 * It should be initialized after Wal and BufferMgr module, and start a BgPageWriter for each WalStream.
 */
class BgPageWriterMgr : public BaseObject {
public:
    explicit BgPageWriterMgr(DstoreMemoryContext mcxt, PdbId pdbId)
        : m_mcxt(mcxt),
          m_pdbId(pdbId),
          m_arrayMutex()
    {}
    ~BgPageWriterMgr() = default;

    void Init();
    /*
     * Destroy the BgPageWriterMgr.
     *
     * NOTE: this method should be called after all the BgPageWriter has stoped.
     */
    void Destroy();

    /*
     * Create a new BgPageWriter for a WalStream.
     *
     * @param walStream the wal stream.
     * @param slot the slot id of new BgPageWriter.
     * @return DSTORE_SUCC if the thread start success, otherwise return DSTORE_FAIL.
     */
    RetStatus CreateBgPageWriter(const WalStream *walStream, int64 *slot, bool primarySlot = true);

    RetStatus StartupBgPageWriter(int64 slotId);

    void StopAllBgPageWriter();

    void StopOneBgPageWriter(WalId walId);

    RetStatus PushDirtyPageToQueue(BufferDesc *bufferDesc, bool needUpdateRecoveryPlsn, const int64 slotId);

    void FlushAllDirtyPages();

    template <typename T>
    T *GetBgPageWriter(WalId walId);

    BgDiskPageMasterWriter *GetBgPageWriterBySlot(const int64 slotId);

    /*
     * Get the BgPageWriterEnrty according to the slot id.
     */
    struct BgPageWriterEntry *GetBgPageWriterEntry(const int64 slotId);

    int64 GetBgWriterSlotIdByWalId(const WalId walId) const;

    template <typename T>
    static void BgPageWriterMain(T *bgPageWriter, PdbId pdbId, RetStatus &ret);

    uint32 GetBgPageWriterArraySize() const;

    uint64 GetTotalDirtyPageCnt() const;

    char *DumpSummaryInfo();

    uint32 GetFlushInfo(AioCompleterInfo **aioCompleterInfo);

    uint32 GetPagewriterInfo(PagewriterInfo **PagewriterInfo);

    uint32 GetSlotUsageInfo(AioSlotUsageInfo **aioSlotUsageInfo);

    void FreeAioCompleterInfoArr(AioCompleterInfo *infos);

    void FreeAioSlotUsageInfoArr(AioSlotUsageInfo *infos);

    void FreePagewriterInfoArr(PagewriterInfo *infos);

    bool IsContains(BgDiskPageMasterWriter *bgPageWriter);

    friend class PastImageSenderMgr;

protected:
    DstoreMemoryContext m_mcxt;
    PdbId m_pdbId;
    BgPageWriterEntry m_bgPageWriterArray[BG_PAGE_WRITER_ARRAY_SIZE];
    std::mutex m_arrayMutex;
};

} /* namespace DSTORE */

#endif /* STORAGE_BG_PAGE_WRITER_MGR_H */
