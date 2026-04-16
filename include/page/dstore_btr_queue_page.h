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
 * dstore_btr_queue_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_btr_queue_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_BTR_QUEUE_PAGE_H
#define SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_BTR_QUEUE_PAGE_H

#include "page/dstore_page.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "buffer/dstore_buf.h"
#include "framework/dstore_instance.h"
#include "transaction/dstore_csn_mgr.h"

namespace DSTORE {

struct __attribute__((packed)) RecyclablePage {
    CommitSeqNo csn;
    PageId pageId;

    bool operator==(const RecyclablePage &recyclablePage) const
    {
        return csn == recyclablePage.csn;
    }

    bool operator!=(const RecyclablePage &recyclablePage) const
    {
        return !(*this == recyclablePage);
    }

    bool operator<(const RecyclablePage &recyclablePage) const
    {
        return csn < recyclablePage.csn;
    }

    inline bool IsValid() const
    {
        return pageId.IsValid() && csn != INVALID_CSN;
    }
};
struct RecyclablePageQueue {
/*
 * DO NOT inherit this class, to ensure that recyclablePages
 * is the last member field of the class.
 */
public:
    uint16 head;
    uint16 tail;
    uint16 capacity;
    uint16 size;
    RecyclablePage recyclablePages[1];

    RecyclablePageQueue() = default;
    ~RecyclablePageQueue() {}

    inline void Init(uint16 freeSpace)
    {
        freeSpace -= offsetof(RecyclablePageQueue, recyclablePages);
        head = 0;
        tail = 0;
        capacity = freeSpace / sizeof(RecyclablePage);
        size = 0;
    }

    inline uint16 GetCapacity() const
    {
        return capacity;
    }

    inline uint16 GetSize() const
    {
        return size;
    }

    inline uint16 GetHead() const
    {
        return head;
    }

    inline uint16 GetTail() const
    {
        return tail;
    }

    inline bool IsEmpty(PdbId pdbId) const
    {
        (void)pdbId;
        return size == 0;
    }

    inline bool IsFull() const
    {
        return size == capacity;
    }

    inline void Push(const RecyclablePage recyclablePage)
    {
        assert(!IsFull());
        recyclablePages[tail] = recyclablePage;
        tail = static_cast<uint16>(tail + 1) % capacity;
        size++;
    }

    /* Pop a RecyclablePage at idx of the queue. */
    inline PageId Pop(int16 idx)
    {
        if (idx < 0 || idx >= GetCapacity()) {
            return INVALID_PAGE_ID;
        }
        assert(!IsEmpty(INVALID_PDB_ID));
        uint16 pos = static_cast<uint16>(idx);
        PageId recyclablePageId = recyclablePages[pos].pageId;
        recyclablePages[pos] = recyclablePages[head];
        head = static_cast<uint16>(head + 1) % capacity;
        size--;
        return recyclablePageId;
    }

    /* Look for a RecyclablePage whose CSN is less than minCsn */
    inline int16 Peek(CommitSeqNo minCsn, uint64 *numSkippedPage = nullptr)
    {
        for (uint16 i = 0; i < size; i++) {
            uint16 idx = static_cast<uint16>(head + i) % capacity;
            RecyclablePage recyclablePage = recyclablePages[idx];
            if (recyclablePage.csn < minCsn || recyclablePage.csn == INVALID_CSN) {
                return static_cast<int16>(idx);
            }
            if (likely(numSkippedPage != nullptr)) {
                (*numSkippedPage)++;
            }
        }
        return -1;
    }

    inline void DumpPageQueue(StringInfo str)
    {
        str->append("RecyclablePageQueue: \n");
        str->append("Head of page queue: %hu \n", GetHead());
        str->append("Tail of page queue: %hu \n", GetTail());
        str->append("Capacity of page queue: %hu \n", GetCapacity());
        str->append("Size of page queue: %hu \n", GetSize());

        for (uint16 i = 0; i < GetSize(); i++) {
            uint16 pos = static_cast<uint16>(head + i) % capacity;
            RecyclablePage page = recyclablePages[pos];
            str->append("Recyclable Page [%d]: [csn, page id] [%lu, (%hu, %u)] \n", pos, page.csn,
                        page.pageId.m_fileId, page.pageId.m_blockId);
        }
    }
};

using ReusablePage = RecyclablePage;

struct __attribute__((packed)) ReusablePageSlot {
    ReusablePage reusablePage;
    uint8 isUsed : 1;
    uint8 reserved : 7;

    bool operator==(const ReusablePageSlot &pageSlot) const
    {
        return reusablePage.pageId == pageSlot.reusablePage.pageId && isUsed == pageSlot.isUsed;
    }
};

struct ReusablePageQueue {
public:
    uint16 head;
    uint16 tail;
    uint16 capacity;
    uint16 size;
    uint16 numAllocatedSlots;
    ReusablePageSlot reusablePages[1];

    ReusablePageQueue() = default;
    ~ReusablePageQueue() {}

    inline void Init(uint16 freeSpace)
    {
        freeSpace -= offsetof(ReusablePageQueue, reusablePages);
        head = 0;
        tail = 0;
        capacity = freeSpace / sizeof(ReusablePageSlot);
        size = 0;
        numAllocatedSlots = 0;
    }

    inline uint16 GetSize() const
    {
        return size;
    }

    inline uint16 GetCapacity() const
    {
        return capacity;
    }

    inline bool IsFull() const
    {
        return size == capacity;
    }

    inline bool IsEmpty(PdbId pdbId) const
    {
        if (numAllocatedSlots == 0) {
            return size == 0;
        }

        CommitSeqNo recycleCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(pdbId);
        for (uint16 i = 0; i < size; i++) {
            uint16 idx = static_cast<uint16>(head + i) % capacity;
            if (reusablePages[idx].isUsed || reusablePages[idx].reusablePage.csn >= recycleCsn) {
                return false;
            }
        }
        return true;
    }

    inline uint16 GetHead() const
    {
        return head;
    }

    inline uint16 GetTail() const
    {
        return tail;
    }

    inline bool IsIndexInQueue(int16 index) const
    {
        if (head <= tail) {
            return head <= index && index < tail;
        } else {
            return !(tail <= index && index < head);
        }
    }

    inline int16 AllocSlot(const ReusablePage reusablePage)
    {
        int16 pos = static_cast<int16>(tail);
        ReusablePageSlot reusablePageSlot;
        reusablePageSlot.reusablePage = reusablePage;
        reusablePageSlot.isUsed = 0;
        reusablePages[tail] = reusablePageSlot;
        tail = static_cast<uint16>(tail + 1) % capacity;
        size++;
        numAllocatedSlots++;
        return pos;
    }

    inline int16 WriteSlot(int16 pos, const ReusablePage reusablePage)
    {
        ReusablePageSlot *reusablePageSlot = &reusablePages[pos];
        if (IsIndexInQueue(pos) && reusablePageSlot->reusablePage.pageId == reusablePage.pageId) {
            reusablePageSlot->reusablePage = reusablePage;
            reusablePageSlot->isUsed = 1;
            numAllocatedSlots--;
            return pos;
        }

        int16 allocatedPos = -1;
        for (uint16 i = 0; i < size; i++) {
            uint16 idx = static_cast<uint16>(head + i) % capacity;
            reusablePageSlot = &reusablePages[idx];
            if (!reusablePageSlot->isUsed && reusablePageSlot->reusablePage.pageId == reusablePage.pageId) {
                allocatedPos = static_cast<int16>(idx);
                break;
            }
        }

        if (allocatedPos == -1) {
            return -1;
        }
        reusablePageSlot = &reusablePages[allocatedPos];
        reusablePageSlot->reusablePage = reusablePage;
        reusablePageSlot->isUsed = 1;
        numAllocatedSlots--;
        return allocatedPos;
    }

    inline void Push(const ReusablePage reusablePage)
    {
        ReusablePageSlot reusablePageSlot;
        reusablePageSlot.reusablePage = reusablePage;
        reusablePageSlot.isUsed = 1;
        reusablePages[tail] = reusablePageSlot;
        tail = static_cast<uint16>(tail + 1) % capacity;
        size++;
    }

    // Look for a ReusablePage whose CSN is less than minCsn
    inline int16 Peek(CommitSeqNo minCsn)
    {
        for (uint16 i = 0; i < size; i++) {
            uint16 idx = static_cast<uint16>(head + i) % capacity;
            ReusablePageSlot reusablePageSlot = reusablePages[idx];
            if (!reusablePageSlot.isUsed) {
                continue;
            }
            if (reusablePageSlot.reusablePage.csn < minCsn || reusablePageSlot.reusablePage.csn == INVALID_CSN) {
                return static_cast<int16>(idx);
            }
        }
        return -1;
    }

    // Pop a ReusablePage at idx of the queue.
    inline PageId Pop(int16 idx)
    {
        if (idx < 0 || idx >= GetCapacity()) {
            return INVALID_PAGE_ID;
        }
        uint16 pos = static_cast<uint16>(idx);
        PageId reusablePageId = reusablePages[pos].reusablePage.pageId;
        reusablePages[pos].isUsed = 0;
        reusablePages[pos] = reusablePages[head];
        head = static_cast<uint16>(head + 1) % capacity;
        size--;
        return reusablePageId;
    }

    inline void DumpPageQueue(StringInfo str)
    {
        str->append("ReusablePageQueue: \n");
        str->append("Head of page queue: %hu \n", GetHead());
        str->append("Tail of page queue: %hu \n", GetTail());
        str->append("Capacity of page queue: %hu \n", GetCapacity());
        str->append("Size of page queue: %hu \n", GetSize());
        str->append("Number of allocated slots: %hu \n", numAllocatedSlots);

        for (uint16 i = 0; i < GetSize(); i++) {
            uint16 pos = static_cast<uint16>(head + i) % capacity;
            ReusablePageSlot slot = reusablePages[pos];
            str->append("Reusable Page Slot [%d]: csn = %lu, page id = (%hu, %u), isUsed = %hhu \n",
                        pos, slot.reusablePage.csn, slot.reusablePage.pageId.m_fileId,
                        slot.reusablePage.pageId.m_blockId, slot.isUsed);
        }
    }
};

struct FreeQueueSlot {
    PageId qPage;
    int16 pos;

    inline bool IsValid() const
    {
        return qPage != INVALID_PAGE_ID && pos != -1;
    }
};

/*
 * BtrQueuePage
 *
 * The disk format of a BtrQueuePage.
 * +------------+-----------------------------------------+
 * | PageHeader |                   ...                   |
 * +------------+-----------------------------------------+
 * |                        ......                        |
 * +------------------------------------------------------+
 * |                 ... Queue Object ...                 |
 * +------------------------------------------------------+
 * |                        ......                        |
 * +-----------------------------------+------------------+
 * |              ......               | BtrQueuePageMeta |
 * +-----------------------------------+------------------+
 *                                     ^
 *                             "special offset"
 *
 * A BtrQueuePage stores a queue object that holds PageIds and other auxiliary
 * data used for Btree page recycling. Since a single page does not hold too
 * much PageIds, the BtrQueuePageMeta has a pointer to the next BtrQueuePage at
 * the end of the page. QueuePages can be chained into a linked list of pages,
 * and thus can grow and shrink dynamically.
 */
struct BtrQueuePageMeta {
    uint32 versionNumber;
    Xid createdXid;
    PageId next;
    BtrRecycleQueueType type;

    template <typename T>
    inline void Init(Xid btrCreatedXid)
    {
        createdXid = btrCreatedXid;
        next = INVALID_PAGE_ID;
        if (std::is_same<T, RecyclablePageQueue>::value) {
            type = BtrRecycleQueueType::RECYCLE;
        } else {
            type = BtrRecycleQueueType::FREE;
        }
    }

    inline PageId GetNext()
    {
        return next;
    }

    inline void SetNext(const PageId nextQPage)
    {
        next = nextQPage;
    }

    inline bool IsTail() const
    {
        return next == INVALID_PAGE_ID;
    }

    inline BtrRecycleQueueType GetType() const
    {
        return type;
    }

    inline void DumpQueuePageMeta(StringInfo str)
    {
        str->append("BtrQueuePageMeta: \n");
        str->append("Next Queue PageId: (%hu, %u) \n", GetNext().m_fileId, GetNext().m_blockId);
    }
};

struct BtrQueuePage : public Page {
public:
    template <typename T>
    void Init(const PageId selfPageId, Xid createdXid)
    {
        uint16 specialSize = MAXALIGN(sizeof(BtrQueuePageMeta));
        Page::Init(specialSize, PageType::BTR_QUEUE_PAGE_TYPE, selfPageId);
        GetMeta()->Init<T>(createdXid);
        uint16 freeSpace = static_cast<uint16>(m_header.m_upper - m_header.m_lower);
        GetQueue<T>()->Init(freeSpace);
    }

    template <typename T>
    void Reset(const PageId selfPageId, Xid createXid)
    {
        uint64 glsn = m_header.m_glsn;
        uint64 plsn = m_header.m_plsn;
        WalId walId = m_header.m_walId;
        Init<T>(selfPageId, createXid);
        m_header.m_glsn = glsn;
        m_header.m_plsn = plsn;
        m_header.m_walId = walId;
    }

    static bool isQueuePageValid(BufferDesc *qBuf, Xid checkingXid)
    {
        if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
            return false;
        }
        Page* page = qBuf->GetPage();
        if (unlikely(!page->TestType(PageType::BTR_QUEUE_PAGE_TYPE))) {
            return false;
        }
        BtrQueuePage *qPage = static_cast<BtrQueuePage *>(page);
        return qPage->GetCreatedXid() == checkingXid;
    }

    inline Xid GetCreatedXid()
    {
        return GetMeta()->createdXid;
    }

    template <typename T>
    inline T *GetQueue()
    {
        char *queue = static_cast<char *>(static_cast<void *>(this)) + GetDataBeginOffset();
        return static_cast<T *>(static_cast<void *>(queue));
    }

    template <typename T>
    inline const T *GetQueue() const
    {
        const char *queue = static_cast<const char *>(static_cast<const void *>(this)) + GetDataBeginOffset();
        return static_cast<const T *>(static_cast<const void *>(queue));
    }

    inline bool IsTail()
    {
        return GetMeta()->IsTail();
    }

    inline PageId GetNext()
    {
        return GetMeta()->GetNext();
    }

    inline void SetNext(const PageId page)
    {
        GetMeta()->SetNext(page);
    }

    inline char* Dump()
    {
        StringInfoData str;
        str.init();

        str.append("Page Header: \n");
        Page::DumpHeader(&str);
        str.append("\n");

        GetMeta()->DumpQueuePageMeta(&str);
        BtrRecycleQueueType type = GetMeta()->GetType();
        if (type == BtrRecycleQueueType::RECYCLE) {
            GetQueue<RecyclablePageQueue>()->DumpPageQueue(&str);
        } else {
            GetQueue<ReusablePageQueue>()->DumpPageQueue(&str);
        }
        str.append("CreatedXid:(%d, %lu)\n",
                   static_cast<int32>(GetCreatedXid().m_zoneId), GetCreatedXid().m_logicSlotId);

        return str.data;
    }

private:
    inline BtrQueuePageMeta *GetMeta()
    {
        char *qPageMeta = static_cast<char *>(static_cast<void *>(this)) + GetSpecialOffset();
        return static_cast<BtrQueuePageMeta *>(static_cast<void *>(qPageMeta));
    }
};
STATIC_ASSERT_TRIVIAL(BtrQueuePage);

}
#endif /* SRC_GAUSSKERNEL_INCLUDE_PAGE_STORAGE_BTR_QUEUE_PAGE_H */