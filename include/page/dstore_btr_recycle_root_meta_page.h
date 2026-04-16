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
 * dstore_btr_recycle_root_meta_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_btr_recycle_root_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_BTR_RECYCLE_ROOT_META_PAGE_H
#define SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_BTR_RECYCLE_ROOT_META_PAGE_H

#include "buffer/dstore_buf_mgr.h"
#include "tablespace/dstore_index_segment.h"

namespace DSTORE {
/*
 * BtrRecycleRootMetaPage
 *
 * The disk format of a BtrRecycleRootMetaPage.
 *
 * +------------+------------+-------------------------+-------------------------+
 * | PageHeader | createdXid | recyclePartitionPageId0 | recyclePartitionPageId1 |
 * +-------------------------+---------------------------------------------------+
 * | recyclePartitionPageId2 |                         ......                    |
 * +-------------------------+---------------------------------------------------+
 * |                                     ......                                  |
 * +---------------------------------------------------+-------------------------+
 * |                         ......                    | recyclePartitionPageIdN |
 * +---------------------------------------------------+-------------------------+
 *
 * The BtrRecycleRootMetaPage holds a fixed number of MAX_BTR_RECYCLE_PARTITION
 * BtrRecyclePartitionMetaPage in any index relation. The PageId of each
 * BtrRecyclePartitionMetaPage is kept.
 *
 * Ideally, the number of BtrRecyclePartitionMetaPage in an index should be
 * configured to be more or less equal to the number of concurrent accesses
 * by different physical machines.
 *
 * The BtrRecycleRootMetaPage is written to only when a new
 * BtrRecyclePartitionMetaPageId is added onto the page.
 *
 * The use of a BtrRecycleRootMetaPage ensures that any meta data changes to
 * the btree page recycling data structures are localized. It avoids sharing
 * of pages of that keep track of the BtrRecyclePartitionMetaPage, so that
 * pages do not jump between bufferpool of different physical machines.
 *
 * The high level hierarchy of Btree Page Recycle is as depicted below:
 *
 *                           RecycleRootMeta
 *                 +--------------------------------+--------------------+ ...
 *                 |                                |                    |
 *       RecyclePartitionMeta             RecyclePartitionMeta          ...
 *         +-------------+                   +------------+
 *         |             |                   |            |
 *      Recycle        Free               Recycle       Free
 *         |             |                   |            |
 *         v             v                   v            v
 *     QueuePage    QueuePage            QueuePage    QueuePage
 *         |             |                   |            |
 *         v             v                   v            v
 *     QueuePage    QueuePage                X           ...
 *         |
 *         v
 *        ...
 */
struct BtrRecycleRootMetaPageHeader {
    uint32 versionNum;
    Xid createdXid;
};
/* Max partitions a root page can hold. See BtrRecycleRootMetaPage */
constexpr uint32 MAX_BTR_RECYCLE_PARTITION =
    (BLCKSZ - MAXALIGN(sizeof(Page::PageHeader)) - MAXALIGN(sizeof(BtrRecycleRootMetaPageHeader))) /
    MAXALIGN(sizeof(PageId));
struct BtrRecycleRootMetaPage : public Page {
/*
 * DO NOT inherit this class, to ensure that recyclePartitionMeta
 * is the last member field of the class.
 */
public:
    BtrRecycleRootMetaPageHeader metaPageHeader;
    PageId recyclePartitionMeta[DSTORE_FLEXIBLE_ARRAY_MEMBER];

    void InitRecycleRootMetaPage(const PageId selfPageId, Xid currXid)
    {
        Page::Init(0, PageType::BTR_RECYCLE_ROOT_META_PAGE_TYPE, selfPageId);
        for (uint16 i = 0; i < MAX_BTR_RECYCLE_PARTITION; i++) {
            recyclePartitionMeta[i] = INVALID_PAGE_ID;
        }
        metaPageHeader.createdXid = currXid;
    }

    inline Xid GetCreatedXid() const
    {
        return metaPageHeader.createdXid;
    }
    PageId GetRecyclePartitionMetaPageId(uint16 id)
    {
        if (unlikely(id >= MAX_BTR_RECYCLE_PARTITION)) {
            return INVALID_PAGE_ID;
        }
        return recyclePartitionMeta[id];
    }

    PageId GetRecyclePartitionMetaPageId(uint16 id) const
    {
        if (unlikely(id >= MAX_BTR_RECYCLE_PARTITION)) {
            return INVALID_PAGE_ID;
        }
        return recyclePartitionMeta[id];
    }

    void SetRecyclePartitionMeta(uint16 id, const PageId recyclePartitionMetaPage)
    {
        recyclePartitionMeta[id] = recyclePartitionMetaPage;
    }

    char* Dump();
};
STATIC_ASSERT_TRIVIAL(BtrRecycleRootMetaPage);

struct BtrRecycleRootMeta {
public:
    PdbId pdbId;
    IndexSegment *segment{nullptr};
    PageId recycleRootMeta{INVALID_PAGE_ID};

    explicit BtrRecycleRootMeta(IndexSegment *indexSegment, BufMgrInterface *bufMgrInterface) : segment(indexSegment),
        bufMgr(bufMgrInterface)
    {
        // RecycleRootMetaPage is always the page following BtrMetaPage.
        // BtrMetaPage always proceeds the SegmentMetaPage.
        PageId recycleRootMetaPage = indexSegment->GetSegmentMetaPageId();
        const uint32 numSegmentMetaPage = 1;
        const uint32 numBtrMetaPage = 1;
        uint32 numStaticAllocPages = (numSegmentMetaPage + numBtrMetaPage);
        recycleRootMetaPage.m_blockId += numStaticAllocPages;
        recycleRootMeta = recycleRootMetaPage;
        pdbId = indexSegment->GetPdbId();
    }

    ~BtrRecycleRootMeta()
    {
        bufMgr = nullptr;
        segment = nullptr;
        recycleRootMetaBuf = nullptr;
    }

    inline PdbId GetPdbId()
    {
        return pdbId;
    }

    RetStatus GetRecyclePartitionMetaPageId(NodeId nodeId, PageId &pageId, Xid &createdXid);

    RetStatus InitRecyclePartitionMeta(NodeId nodeId, PageId &pageId);

    BufMgrInterface *bufMgr{nullptr};
    BufferDesc *recycleRootMetaBuf{nullptr};

    BufMgrInterface *GetBufMgr();

    RetStatus AcquireRecycleRootMetaBuf(LWLockMode access, Xid &createdXid);

    void ReleaseRecycleRootMetaBuf();

    BtrRecycleRootMetaPage *GetRecycleRootMetaPage();

private:
    void GenerateWalForRecycleRootMetaSetPartitionMeta(uint16 recyclePartitionId, const PageId recyclePartitionMeta);
};

}
#endif /* SRC_GAUSSKERNEL_INCLUDE_PAGE_STORAGE_BTR_RECYCLE_ROOT_META_PAGE_H */
