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
 * dstore_fsm_page.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/page/dstore_fsm_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_FSM_PAGE_H
#define DSTORE_DSTORE_FSM_PAGE_H

#include "page/dstore_page.h"
#include "page/dstore_segment_meta_page.h"
#include "tablespace/dstore_tablespace_utils_internal.h"

namespace DSTORE {

struct FsmList {
    uint16 count;
    uint16 first;
};

struct FsmIndex {
    PageId page;
    uint16 slot;    /* fsm slot */
};

struct FsmNode {
    PageId page; /* pointer to next level page(fsm page or heap page) */
    uint16 listId; /* list id of page */
    uint16 prev; /* prev slot id, INVALID_FSM_SLOT_NUM if current node is the first node in fsm list */
    uint16 next; /* next slot id, INVALID_FSM_SLOT_NUM if current node is the last node in fsm list */
};

/* Since our largest EXTENT has 8K pages at most, we could add 8K pages to this FSM in one extension */
constexpr uint16 MAX_FSM_EXTEND_COEFFICIENT = 5;
constexpr uint16 MIN_FSM_EXTEND_COEFFICIENT = 1;
struct FreeSpaceMapMetaPage : public Page {
public:
    uint8 version;
    ExtentRange fsmExtents; /* linked list of fsm extents */

    /* FSM related info (used in Heapegment) */
    uint8 numFsmLevels; /* total number of levels in current fsm */
    uint16 listRange[FSM_FREE_LIST_COUNT]; /* fsm maximum range boundary value in each fsm list */
    uint64 mapCount[HEAP_MAX_MAP_LEVEL]; /* number of pages in each fsm level */
    PageId currMap[HEAP_MAX_MAP_LEVEL]; /* right-most pageId in each fsm level */
    PageId usedFsmPage; /* PageId has used in current fsm extent */
    PageId lastFsmPage; /* Last PageId in current fsm extent */
    PageId curFsmExtMetaPageId; /* Extent meta page id in current fsm extent */
    PageId firstFsmRootPageId; /* PageId of the first ever FSM root page for tracking purpose */

    uint64 numTotalPages; /* total number of pages in current fsm */
    uint64 numUsedPages; /* number of used pages in current fsm, not an exact value */

    uint16 extendCoefficient; /* how many thousands pages we should extend each time */
    TimestampTz accessTimestamp; /* local timestamp (in second) of last access from assigned Node */

    void InitFreeSpaceMapMetaPage(const PageId& myselfPageId, const PageId& rootFsmPageId,
                                  TimestampTz accessTimestampInput)
    {
        Page::Init(0, PageType::FSM_META_PAGE_TYPE, myselfPageId);
        fsmExtents.count = 0;
        fsmExtents.first = fsmExtents.last = INVALID_PAGE_ID;

        numFsmLevels = 0;
        for (int i = 0; i < FSM_FREE_LIST_COUNT; ++i) {
            listRange[i] = FSM_SPACE_LINE[i];
        }
        mapCount[0] = 1;
        currMap[0] = rootFsmPageId;
        for (int i = 1; i < HEAP_MAX_MAP_LEVEL; ++i) {
            mapCount[i] = 0;
            currMap[i] = INVALID_PAGE_ID;
        }
        usedFsmPage = lastFsmPage = INVALID_PAGE_ID;
        firstFsmRootPageId = rootFsmPageId;

        numTotalPages = 0;
        numUsedPages = 0;
        curFsmExtMetaPageId = INVALID_PAGE_ID;
        extendCoefficient = MIN_FSM_EXTEND_COEFFICIENT;

        /* Update Access Timestamp at the initialization of FSM */
        accessTimestamp = accessTimestampInput;
    }

    uint16 GetFsmRootLevel() const
    {
        return numFsmLevels;
    }

    PageId GetFsmRoot() const
    {
        return currMap[numFsmLevels];
    }

    PageId GetfirstFsmRoot() const
    {
        return firstFsmRootPageId;
    }

    inline uint64 GetNumUnusedPages() const
    {
        StorageReleasePanic(numTotalPages < numUsedPages, MODULE_SEGMENT,
            ErrMsg("numTotalPages(%lu) is less than numUsedPages(%lu).", numTotalPages, numUsedPages));
        return numTotalPages - numUsedPages;
    }

    inline uint64 GetNumTotalPages() const
    {
        return numTotalPages;
    }

    inline uint64 GetNumUsedPages() const
    {
        return numUsedPages;
    }

    inline uint16 GetExtendCoefficient() const
    {
        return extendCoefficient;
    }

    uint16 GetFreeFsmPageCount() const
    {
        if (usedFsmPage == INVALID_PAGE_ID) {
            return 0;
        }
        uint16 freeFsmPageCount = static_cast<uint16>(lastFsmPage.m_blockId - usedFsmPage.m_blockId);
        if (fsmExtents.last != curFsmExtMetaPageId) {
            freeFsmPageCount += static_cast<uint16>(FSM_EXT_SIZE);
        }
        return freeFsmPageCount;
    }
    void AddFsmExtent(const PageId &extMetaPageId)
    {
        StorageReleasePanic(extMetaPageId.IsInvalid(), MODULE_SEGMENT,
            ErrMsg("Failed to add fsm extent: pageid{%u, %u}.",
            extMetaPageId.m_fileId, extMetaPageId.m_blockId));
        if (fsmExtents.count == 0) {
            fsmExtents.first = extMetaPageId;
            usedFsmPage = extMetaPageId;
            lastFsmPage = {extMetaPageId.m_fileId, extMetaPageId.m_blockId + static_cast<uint32>(FSM_EXT_SIZE) - 1U};
            curFsmExtMetaPageId = extMetaPageId;
        }
        fsmExtents.count += 1;
        fsmExtents.last = extMetaPageId;
    }

    void UnlinkExtent(const PageId &newNextExtMetaPageId)
    {
        fsmExtents.count -= 1;
        if (fsmExtents.count == 0) {
            fsmExtents.first = fsmExtents.last = INVALID_PAGE_ID;
        } else {
            fsmExtents.first = newNextExtMetaPageId;
        }
    }

    void UpdateFsmTree(uint16 needExtendPageCount, PageId *pageIdList)
    {
        /* The root node of the tree needs to be adjusted. */
        if (needExtendPageCount == GetFsmRootLevel() + 2) {  /* 2 is not magic */
            numFsmLevels += 1;
        }
        for (uint16 i = 0; i < needExtendPageCount; ++i) {
            uint16 modifyFsmLevel = needExtendPageCount - (i + 1U);
            mapCount[modifyFsmLevel] += 1;
            currMap[modifyFsmLevel] = pageIdList[i];
        }
    }

    bool IsFreeSpaceMapExpired(const TimestampTz currentTimestamp, const TimestampTz recycleTimeThreshold) const
    {
        return (accessTimestamp != 0 && (currentTimestamp - accessTimestamp >= recycleTimeThreshold));
    }

    char *Dump()
    {
        StringInfoData str;
        str.init();
        /* Data segment meta info */
        str.append("Total number of fsm extents in current segment = %lu\n", fsmExtents.count);
        str.append("First fsm extent meta pageId = (%hu, %u)\n",
                   fsmExtents.first.m_fileId, fsmExtents.first.m_blockId);
        str.append("Last fsm extent meta pageId = (%hu, %u)\n",
                   fsmExtents.last.m_fileId, fsmExtents.last.m_blockId);
        str.append("Total number of levels in current fsm tree = %hhu\n", numFsmLevels);
        str.append("Current segment fsm list upper bound\n\n");
        for (uint16 i = 0; i < FSM_FREE_LIST_COUNT; ++i) {
            str.append("Fsm list id %hu upper space bound = %hu\n", i, listRange[i]);
        }
        str.append("Current fsm page info for each level\n");
        for (uint16 i = 0; i < HEAP_MAX_MAP_LEVEL; ++i) {
            str.append("Fsm level %hu has %lu fsm pages, rightmost fsm page id in this level = (%hu, %u)\n",
                       i, mapCount[i], currMap[i].m_fileId, currMap[i].m_blockId);
        }
        str.append("PageId (%hu, %u) is the highest used page\n",
                   usedFsmPage.m_fileId, usedFsmPage.m_blockId);
        str.append("PageId (%hu, %u) is last page in fsm last extent\n",
                   lastFsmPage.m_fileId, lastFsmPage.m_blockId);

        str.append("PageId (%hu, %u) is first root page in fsm\n",
                   firstFsmRootPageId.m_fileId, firstFsmRootPageId.m_blockId);

        str.append("Page statistics of the current FSM:\n");
        str.append("    Total number of pages is %lu\n", numTotalPages);
        str.append("    The number of used pages is %lu\n", numUsedPages);
        str.append("    The number of unused pages is %lu\n", GetNumUnusedPages());

        str.append("The extend coefficient of the current FSM is %hu\n", extendCoefficient);
        str.append("Last accessTimestamp of the current FSM is %ld\n", accessTimestamp);
        return str.data;
    }
};
STATIC_ASSERT_TRIVIAL(FreeSpaceMapMetaPage);

static_assert(sizeof(FreeSpaceMapMetaPage) <= BLCKSZ, "Fsm Meta Page cannot exceed BLCKSZ");

/*
 * FsmPage
 *
 * The disk format of a FsmPage.
 * +------------+-----------------------------------------+
 * | PageHeader | FsmPageHeader |             ...         |
 * +------------+-----------------------------------------+
 * |                        ......                        |
 * +------------------------------------------------------+
 * |                 ... FsmNode Object ...               |
 * +------------------------------------------------------+
 * |                        ......                        |
 * +-----------------------------------+------------------+
 * |              ......               | m_searchSeed[]   |
 * +-----------------------------------+------------------+
 *                                     ^
 *                             "special offset"
 **/
struct FsmSearchSeed {
    uint16 m_searchSeed[FSM_FREE_LIST_COUNT];
};

struct FsmPage : public Page {
public:
    struct FsmPageHeader {
        uint8 version;
        PageId fsmMetaPageId; /* which FSM this page belongs to */
        FsmIndex upperIndex; /* upper fsm index */
        FsmList lists[FSM_FREE_LIST_COUNT];
        uint16 hwm; /* In fsm page, [0,hwm) slot id is used, maximum valid slot is FSM_MAX_HWM in one page */
        uint8 reserved[32];
    };
    FsmPageHeader fsmPageHeader;
    /* can modify in share lock, there is no guarantee of flushing disks. */
    char data[(BLCKSZ - sizeof(FsmPageHeader)) - sizeof(PageHeader)];

    void InitFsmPage(const PageId &selfPageId, const PageId &fsmMetaPageId, const FsmIndex &upperFsmIndex)
    {
        /* specialSize need align with 4 bytes */
        uint16 specialSize = static_cast<uint16>(DstoreRoundUp<Size>(sizeof(FsmSearchSeed), sizeof(uint32)));
        Page::Init(specialSize, PageType::FSM_PAGE_TYPE, selfPageId);
        for (uint16 i = 0; i < FSM_FREE_LIST_COUNT; i++) {
            *GetSearchSeeds(i) = 0;
        }
        fsmPageHeader.fsmMetaPageId = fsmMetaPageId;
        fsmPageHeader.upperIndex = {INVALID_PAGE_ID, INVALID_FSM_SLOT_NUM};
        for (auto& fsmList : fsmPageHeader.lists) {
            fsmList.count = 0;
            fsmList.first = INVALID_FSM_SLOT_NUM;
        }
        fsmPageHeader.hwm = 0;
        fsmPageHeader.upperIndex = upperFsmIndex;
    }
    bool HasFreeNode() const
    {
        return fsmPageHeader.hwm < FSM_MAX_HWM;
    }
    uint16 GetFreeNodeCount() const
    {
        StorageAssert(fsmPageHeader.hwm <= FSM_MAX_HWM);
        return static_cast<uint16>(FSM_MAX_HWM - fsmPageHeader.hwm);
    }
    FsmNode* FsmNodePtr(uint16 slotId)
    {
        char *slotPtr = data + sizeof(FsmNode) * slotId;
        FsmNode *fsmNodePtr = reinterpret_cast<FsmNode *>(slotPtr);
        return fsmNodePtr;
    }
    const FsmNode* FsmNodePtr(uint16 slotId) const
    {
        const char *slotPtr = data + sizeof(FsmNode) * slotId;
        const FsmNode *fsmNodePtr = reinterpret_cast<const FsmNode *>(slotPtr);
        return fsmNodePtr;
    }
    FsmList* FsmListPtr(uint16 listId)
    {
        StorageAssert(listId != FSM_FREE_LIST_COUNT);
        return &(fsmPageHeader.lists[listId]);
    }
    const FsmList* FsmListPtr(uint16 listId) const
    {
        StorageAssert(listId != FSM_FREE_LIST_COUNT);
        return &(fsmPageHeader.lists[listId]);
    }
    PageId GetUpperFsmPageId() const
    {
        return fsmPageHeader.upperIndex.page;
    }
    uint16 GetUpperSlot() const
    {
        return fsmPageHeader.upperIndex.slot;
    }
    uint16 AddNode(const PageId& pageId, uint16 targetList)
    {
        /* Get an empty slot and add to targetList */
        uint16 targetSlot = fsmPageHeader.hwm;
        fsmPageHeader.hwm += 1;
        StorageReleasePanic(fsmPageHeader.hwm > FSM_MAX_HWM, MODULE_SEGMENT,
            ErrMsg("Fsm page high water mark is invalid, targetList = %d, hwm = %u.", targetList, fsmPageHeader.hwm));
        FsmNodePtr(targetSlot)->page = pageId;
        FsmNodePtr(targetSlot)->listId = targetList;
        FsmNodePtr(targetSlot)->prev = INVALID_FSM_SLOT_NUM;
        FsmNodePtr(targetSlot)->next = FsmListPtr(targetList)->first;
        if (FsmListPtr(targetList)->first != INVALID_FSM_SLOT_NUM) {
            FsmNodePtr(FsmListPtr(targetList)->first)->prev = targetSlot;
        }
        FsmListPtr(targetList)->first = targetSlot;
        FsmListPtr(targetList)->count += 1;
        return targetSlot;
    }

    /* Add multiple data pages to current fsm page */
    uint16 AddMultiNode(PageId *pageIdList, uint16 pageCount, uint16 targetList)
    {
        uint16 firstAddSlot = fsmPageHeader.hwm;

        /* Get empty slots from current hwm and add to targetList */
        fsmPageHeader.hwm += pageCount;
        StorageReleasePanic(fsmPageHeader.hwm > FSM_MAX_HWM, MODULE_SEGMENT,
            ErrMsg("Fsm page high water mark is invalid, pageCount = %d, targetList = %d, hwm = %u.",
                pageCount, targetList, fsmPageHeader.hwm));
        for (uint16 i = 0; i < pageCount; ++i) {
            uint16 curSlotId = firstAddSlot + i;
            FsmNodePtr(curSlotId)->page = pageIdList[i];
            FsmNodePtr(curSlotId)->listId = targetList;
            if (i != pageCount - 1) {
                FsmNodePtr(curSlotId)->prev = curSlotId + 1;
            } else {
                FsmNodePtr(curSlotId)->prev = INVALID_FSM_SLOT_NUM;
            }
            if (i == 0) {
                FsmNodePtr(curSlotId)->next = FsmListPtr(targetList)->first;
            } else {
                FsmNodePtr(curSlotId)->next = static_cast<uint16>(curSlotId - 1);
            }
        }
        if (FsmListPtr(targetList)->first != INVALID_FSM_SLOT_NUM) {
            FsmNodePtr(FsmListPtr(targetList)->first)->prev = firstAddSlot;
        }
        FsmListPtr(targetList)->first = static_cast<uint16>(firstAddSlot + pageCount - 1);
        FsmListPtr(targetList)->count += pageCount;
        return firstAddSlot;
    }

    void MoveNode(uint16 slot, uint16 newList)
    {
        uint16 oldList = FsmNodePtr(slot)->listId;

        FsmNodePtr(slot)->listId = newList;

        /* Remove fsm node from old fsm list */
        if (FsmNodePtr(slot)->prev == INVALID_FSM_SLOT_NUM) {
            FsmListPtr(oldList)->first = FsmNodePtr(slot)->next;
        } else {
            FsmNodePtr(FsmNodePtr(slot)->prev)->next = FsmNodePtr(slot)->next;
        }
        if (FsmNodePtr(slot)->next != INVALID_FSM_SLOT_NUM) {
            FsmNodePtr(FsmNodePtr(slot)->next)->prev = FsmNodePtr(slot)->prev;
        }
        FsmListPtr(oldList)->count -= 1;

        /* Add fsm node to new fsm list, always add to first entry */
        FsmNodePtr(slot)->next = FsmListPtr(newList)->first;
        FsmNodePtr(slot)->prev = INVALID_FSM_SLOT_NUM;
        if (FsmListPtr(newList)->first != INVALID_FSM_SLOT_NUM) {
            FsmNodePtr(FsmListPtr(newList)->first)->prev = slot;
        }
        FsmListPtr(newList)->first = slot;
        FsmListPtr(newList)->count += 1;
    }
    uint16 GetFsmMaxListId() const
    {
        uint16 resListId = 0;
        for (uint16 i = 0; i < FSM_FREE_LIST_COUNT; ++i) {
            if (fsmPageHeader.lists[i].count > 0) {
                resListId = i;
            }
        }
        return resListId;
    }

    void ResetSearchSeed(uint16 listId)
    {
        StorageAssert(listId < FSM_FREE_LIST_COUNT);
        *GetSearchSeeds(listId) = 0;
    }

    char* Dump()
    {
        StringInfoData str;

        str.init();
        /* Fsm page info */
        str.append("Fsm page data\n");
        str.append("Upper Fsm PageId = (%d, %u)\n",
                   fsmPageHeader.upperIndex.page.m_fileId, fsmPageHeader.upperIndex.page.m_blockId);
        str.append("Upper Fsm Slot Id = %d\n", fsmPageHeader.upperIndex.slot);
        str.append("Current Fsm List Info\n");
        for (int i = 0; i < FSM_FREE_LIST_COUNT; ++i) {
            str.append("ListId %d has %d slots and first slot is %d\n",
                       i, fsmPageHeader.lists[i].count, fsmPageHeader.lists[i].first);
        }
        str.append("Current Fsm Page has %hu valid slots\n", fsmPageHeader.hwm);
        for (uint16 i = 0; i < fsmPageHeader.hwm; ++i) {
            str.append("SlotId %d info: PageId (%hu, %u) belongs to list id %hu, prev slot = %hu, next slot = %hu\n",
                       i, FsmNodePtr(i)->page.m_fileId, FsmNodePtr(i)->page.m_blockId, FsmNodePtr(i)->listId,
                       FsmNodePtr(i)->prev, FsmNodePtr(i)->next);
        }
        return str.data;
    }

    inline uint16 *GetSearchSeeds(uint16 index)
    {
        char *qPageSearchSeeds = static_cast<char *>(static_cast<void *>(this)) + GetSpecialOffset();
        FsmSearchSeed *fsmSearchSeed = static_cast<FsmSearchSeed *>(static_cast<void *>(qPageSearchSeeds));
        return &(fsmSearchSeed->m_searchSeed[index]);
    }
};
STATIC_ASSERT_TRIVIAL(FsmPage);

static_assert(sizeof(FsmPage) <= BLCKSZ, "Fsm Page cannot exceed BLCKSZ");

}  // namespace DSTORE

#endif  // DSTORE_STORAGE_FSM_PAGE_H
