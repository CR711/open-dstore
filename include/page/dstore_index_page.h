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
 * dstore_index_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_index_page.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_GAUSSKERNEL_INCLUDE_PAGE_STORAGE_BTPAGE
#define SRC_GAUSSKERNEL_INCLUDE_PAGE_STORAGE_BTPAGE

#include "framework/dstore_instance_interface.h"
#include "page/dstore_data_page.h"
#include "buffer/dstore_buf.h"
#include "tuple/dstore_index_tuple.h"

#define BTR_PAGE_HEADER_FMT PAGE_HEADER_FMT ", tdCount:%hhu, createdXid:(%d, %lu) "
#define BTR_PAGE_HEADER_VAL(page) PAGE_HEADER_VAL(page), (page)->dataHeader.tdCount, \
    (static_cast<int32>(((BtrPage *)page)->GetBtrMetaCreateXid().m_zoneId)), \
    (((BtrPage *)page)->GetBtrMetaCreateXid().m_logicSlotId)

#define BTR_PAGE_LINK_AND_STATUS_FMT ", prevFileId:%hu, prevBlockId:%u, nextFileId:%hu, nextBlockId:%u, level:%u, " \
    "btrMetaPageId:(%hu, %u), status:%hu/%hu/%hu/%hu"
#define BTR_PAGE_LINK_AND_STATUS_VAL(btrStatus) \
    (btrStatus)->prev.m_fileId, (btrStatus)->prev.m_blockId, (btrStatus)->next.m_fileId, (btrStatus)->next.m_blockId, \
    (btrStatus)->level, (btrStatus)->btrMetaPageId.m_fileId, (btrStatus)->btrMetaPageId.m_blockId, \
    (btrStatus)->status.bitVal.type, \
    (btrStatus)->status.bitVal.isRoot, (btrStatus)->status.bitVal.liveStat, (btrStatus)->status.bitVal.splitStat

namespace DSTORE {

constexpr uint16 BTREE_PAGE_HIKEY = 1;
constexpr uint16 BTREE_PAGE_FIRSTKEY = 2;
constexpr int BTREE_MIN_FILLFACTOR = 10;
constexpr int BTREE_DEFAULT_FILLFACTOR = 90;
constexpr int BTREE_NONLEAF_FILLFACTOR = 70;
constexpr int BTREE_SINGLEVAL_FILLFACTOR = 96;
constexpr uint64 BTREE_MAGIC = 0xEBEBEBEBEBEBEBEB; /* magic number for btree class/extended class sentinel check */

constexpr uint32 BTREE_HIGHEST_LEVEL = 32;
constexpr uint32 BTREE_META_STAT_INIT_MAGIC_NUM = 0xEAEAEAEA;
enum class BtreeOperType {
    BTR_OPER_SPLIT_WHEN_BUILD = 0,
    BTR_OPER_SPLIT_WHEN_BUILD_CONCURRENTLY,
    BTR_OPER_SPLIT_WHEN_INSERT,
    BTR_OPER_MARK_RECYCLABLE,
    BTR_OPER_RECYCLED,
    BTR_OPER_MAX
};

enum class BtrPageType {
    INVALID_BTR_PAGE = 0,
    LEAF_PAGE,  /* leaf page, i.e. not internal page */
    INTERNAL_PAGE,  /* internal page */
    META_PAGE       /* Btree meta-page */
};

/*
 * BtrPageLiveStatus
 *
 * -- EMPTY_HAS_PARENT_HAS_SIB:
 *    Before deleting the last tuple on a page, we change status of the page to BTP_EMPTY_HAS_PARENT_HAS_SIB, meaning
 *    this page is about to be empty but is still on the btree. Be careful when being assigned a page with a
 *    BTP_EMPTY_HAS_PARENT_HAS_SIB flag from FSM, because this flag doesn't mean the page is really empty. Insertions
 *    might have been taken after the last-tuple-deletion committed. Always check emptiness and make sure the page is
 *    still recyclable before trying to recycle and reuse a BTP_EMPTY_HAS_PARENT_HAS_SIB page.
 *
 * -- EMPTY_NO_PARENT_HAS_SIB
 *    During recycling process, a BTP_EMPTY_NO_PARENT_HAS_SIB page is unlinked from its parent but is still on the
 *    btree with links to siblings. This kind of pages will no longer be accessed by a regular searching since there's
 *    no downlink leading to the page.
 *
 * -- BTP_EMPTY_NO_PARENT_NO_SIB
 *    A BTP_EMPTY_NO_PARENT_NO_SIB page is an empty page that has already been completely deleted from btree and can be
 *    reused immediately.
 */
enum class BtrPageLiveStatus {
    EMPTY_NO_PARENT_NO_SIB = 0,
    NORMAL_USING,
    EMPTY_HAS_PARENT_HAS_SIB,
    EMPTY_NO_PARENT_HAS_SIB,
};

enum class BtrPageSplitStatus {
    SPLIT_COMPLETE = 0,
    SPLIT_INCOMPLETE    /* right sibling's downlink is missing */
};

struct BtrPageLinkAndStatus {
    PageId btrMetaPageId;     /* Block ID of Btree meta page */
    PageId prev;      /* left sibling, or INVALID_PAGE_ID if leftmost */
    PageId next;      /* right sibling, or INVALID_PAGE_ID if rightmost */
    uint16 level;     /* tree level --- zero for leaf pages */
    union {
        uint32 stat;
        struct {
            uint32 type       : 2;    /* leaf/root/meta page */
            uint32 isRoot    : 1;    /* page has a parent or not */
            uint32 liveStat  : 2;    /* live status */
            uint32 splitStat : 2;    /* split status */
            uint32 reserved   : 25;
        } bitVal;
    } status;

    inline void InitPageMeta(const PageId &btrMetaPageIdInput, uint32 initLevel, bool isRoot)
    {
        btrMetaPageId = btrMetaPageIdInput;
        prev = INVALID_PAGE_ID;
        next = INVALID_PAGE_ID;
        level = static_cast<uint16>(initLevel);
        status.bitVal.type = static_cast<uint16>(initLevel == 0 ? BtrPageType::LEAF_PAGE : BtrPageType::INTERNAL_PAGE);
        status.bitVal.isRoot = isRoot ? 1 : 0;
        status.bitVal.liveStat = static_cast<uint16>(BtrPageLiveStatus::NORMAL_USING);
        status.bitVal.splitStat = static_cast<uint16>(BtrPageSplitStatus::SPLIT_COMPLETE);
    }

    inline void SetLevel(uint32 slevel)
    {
        level = static_cast<uint16>(slevel);
    }

    inline uint32 GetLevel() const
    {
        return static_cast<uint32>(level);
    }

    inline PageId GetLeft() const
    {
        return prev;
    }

    inline PageId GetRight() const
    {
        return next;
    }

    inline void SetLeft(const PageId left)
    {
        prev = left;
    }

    inline void SetRight(const PageId right)
    {
        next = right;
    }

    inline bool IsRightmost() const
    {
        return next == INVALID_PAGE_ID;
    }

    inline bool IsLeftmost() const
    {
        return prev == INVALID_PAGE_ID;
    }

    inline void SetType(BtrPageType type)
    {
        status.bitVal.type = static_cast<uint16>(type);
    }

    inline bool TestType(BtrPageType type) const
    {
        return (status.bitVal.type == static_cast<uint16>(type));
    }

    inline uint16 GetType() const
    {
        return status.bitVal.type;
    }

    inline void SetLiveStatus(BtrPageLiveStatus stat)
    {
        status.bitVal.liveStat = static_cast<uint16>(stat);
    }

    inline bool TestLiveStatus(BtrPageLiveStatus stat) const
    {
        return (status.bitVal.liveStat == static_cast<uint16>(stat));
    }

    inline bool IsUnlinked() const
    {
        return (TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB) ||
                TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB));
    }

    inline void SetSplitStatus(BtrPageSplitStatus stat)
    {
        status.bitVal.splitStat = static_cast<uint16>(stat);
    }

    inline bool IsSplitComplete() const
    {
        return (status.bitVal.splitStat == static_cast<uint16>(BtrPageSplitStatus::SPLIT_COMPLETE));
    }

    inline uint16 GetLiveStatus() const
    {
        return status.bitVal.liveStat;
    }

    inline void SetRoot(bool isRoot)
    {
        status.bitVal.isRoot = static_cast<uint16>(isRoot);
    }

    inline bool IsRoot() const
    {
        return (status.bitVal.isRoot == 1);
    }

    inline uint16 GetFirstDataOffset() const
    {
        /* remove branch */
        return IsRightmost() ? BTREE_PAGE_HIKEY : BTREE_PAGE_FIRSTKEY;
    }
} PACKED;

struct BtreeTdSplitInfo {
    uint8 origId;
    uint8 newId;
    TD *td;
};

struct SplitContext {
    OffsetNumber insertOff;
    OffsetNumber firstRightOff;
    bool insertOnLeft;
};

class BtreeUndoContext;
struct BtrPage: public DataPage {
public:
    BtrPageHeader m_btrPageHeader;
    char m_data[BLCKSZ - sizeof(Page) - sizeof(DataPageHeader) - sizeof(BtrPageHeader)];

    void InitBtrPageInner(const PageId &selfPageId);
    void Reset(const PageId &selfPageId);

    /* BtrPage init callback function use in GetNewPage() of DataSegment */
    static void InitBtrPage(BufferDesc *bufDesc, const PageId &selfPageId,
                            const FsmIndex &fsmIndex = {INVALID_PAGE_ID, 0});

    static bool IsBtrPageValid(Page *page, Xid checkingXid);
    static bool IsBtrPageValid(Page *page, Xid checkingXid, BtrPageType checkingType, PageId btrMetaPageId);

    void InitNewPageForBuild(const PageId &btrMetaPageId, Xid createdXid, uint32 level, bool isRoot, uint8 tdCount);
    TdId AllocTd(TDAllocContext &context);

    OffsetNumber AddTuple(IndexTuple *tuple, OffsetNumber offset, uint8 tdID = INVALID_TD_SLOT);
    OffsetNumber AddTupleWhenSplit(const ItemId *itemId, const IndexTuple *tuple, OffsetNumber offset);

    /* pageSize is always PAGE_SIZE except when ExtendCrPage. */
    OffsetNumber AddTupleData(const IndexTuple *indexTuple, OffsetNumber offset, uint16 pageSize = PAGE_SIZE);
    RetStatus CopyItemsFromSplitPage(OffsetNumber &targetOff, BtrPage *newPage, const IndexTuple *insertTuple,
                                    bool isLeft, SplitContext &splitContext, bool isSameWithLastLeft = false);
    RetStatus OverwriteTuple(OffsetNumber offnum, IndexTuple *newtup);
    RetStatus RemoveItemId(OffsetNumber offset);

    /* Undo with btree indexinfo */
    RetStatus UndoBtree(UndoRecord *undoRec, BtreeUndoContext *btrUndoContext, TD *tdOnPage = nullptr);
    /* Rollback for Recovery: redo a WalRecordRollbackForBtree log */
    RetStatus RollbackBtrForRecovery(UndoRecord *undoRec);

    IndexInfo *GetIndexInfoFromMetaPage(BufferDesc *bufDesc);

    RetStatus ConstructCR(Transaction *transaction, CRContext *crCtx, BtreeUndoContext *btrUndoContext,
                          BufMgrInterface *bufMgr);

    OffsetNumber BinarySearch(IndexTuple *tuple, IndexInfo *indexInfo, bool *isEqual);

    void InitMemLeftForSplit(BtrPage *splitPage);
    void InitNewRightForSplit(BtrPage *splitPage, BtrPage *leftPage);

    inline char* GetData()
    {
        return m_data;
    }

    inline BtrPageLinkAndStatus *GetLinkAndStatus()
    {
        return static_cast<BtrPageLinkAndStatus*>(static_cast<void *>(
            static_cast<char *>(static_cast<void *>(this)) + GetSpecialOffset()));
    }

    inline const BtrPageLinkAndStatus *GetLinkAndStatus() const
    {
        return reinterpret_cast<const BtrPageLinkAndStatus *>(
            reinterpret_cast<const char *>(this) + GetSpecialOffset());
    }

    inline PageId GetRight()
    {
        return GetLinkAndStatus()->GetRight();
    }

    inline PageId GetLeft()
    {
        return GetLinkAndStatus()->GetLeft();
    }

    inline bool IsRightmost()
    {
        return GetLinkAndStatus()->IsRightmost();
    }

    inline bool IsLeftmost()
    {
        return GetLinkAndStatus()->IsLeftmost();
    }

    inline uint32 GetLevel()
    {
        return GetLinkAndStatus()->GetLevel();
    }

    inline IndexTuple *GetIndexTuple(OffsetNumber offset)
    {
        return static_cast<IndexTuple *>(this->DataPage::GetRowData(offset));
    }

    inline const IndexTuple *GetIndexTuple(OffsetNumber offset) const
    {
        return static_cast<const IndexTuple *>(this->DataPage::GetRowData(offset));
    }

    inline IndexTuple *GetIndexTuple(ItemId *itemId)
    {
        return static_cast<IndexTuple *>(this->DataPage::GetRowData(itemId));
    }

    inline bool IsInitialized()
    {
        BtrPageLinkAndStatus *meta = GetLinkAndStatus();
        return (GetSpecialOffset() == static_cast<uint16>((BLCKSZ - MAXALIGN(sizeof(BtrPageLinkAndStatus))))) &&
               (meta->btrMetaPageId.IsValid());
    }

    inline bool IsDamaged() const
    {
        uint16 linkStatSize = static_cast<uint16>(MAXALIGN(sizeof(BtrPageLinkAndStatus)));
        return !TestType(PageType::INDEX_PAGE_TYPE) || GetUpper() == 0 || GetLower() == 0 ||
               GetSpecialOffset() + linkStatSize != BLCKSZ;
    }

    /*
     * Copy the whole memory page to buffer page.
     * We should remain some initialized values for segment drop.
     */
    inline void Clone(BtrPage *src)
    {
        uint16 lower = src->m_header.m_lower;
        uint16 upper = src->m_header.m_upper;
        uint8 tdcount = src->GetTdCount();
        Size dataSize = sizeof(BtrPage) - DataHeaderSize();
        errno_t rc = memcpy_s(GetData(), dataSize, src->GetData(), dataSize);
        storage_securec_check(rc, "\0", "\0");
        m_header.m_lower = lower;
        m_header.m_upper = upper;
        dataHeader.tdCount = tdcount;
        SetBtrMetaCreateXid(src->GetBtrMetaCreateXid());
    }

    /*
     * future we can optimize to strip unnecessary td undo chain to rebuild td info.
     * as one td chain separate to left and right page.
     * */
    inline void CopyTd(BtreeTdSplitInfo* info)
    {
        TD *td = GetTd(info->newId);
        errno_t rc = memcpy_s(td, sizeof(TD), info->td, sizeof(TD));
        storage_securec_check(rc, "\0", "\0");
    }

    TdId GetTupleTdId(OffsetNumber offset)
    {
        return DataPage::GetTupleTdId<IndexTuple>(offset);
    }

    bool TestTupleTdStatus(TupleTdStatus currentStatus, TupleTdStatus status)
    {
        return currentStatus == status;
    }

    bool TestTupleTdStatus(OffsetNumber offset, TupleTdStatus status)
    {
        return DataPage::TestTupleTdStatus<IndexTuple>(offset, status);
    }

    template <bool needFillCSN>
    bool JudgeTupCommitBeforeSpecCsn(
        PdbId pdbId, OffsetNumber offset, CommitSeqNo specCsn, bool &isDirty, CommitSeqNo *tupleCsn = nullptr)
    {
        return DataPage::JudgeTupCommitBeforeSpecCsn<IndexTuple, needFillCSN>(
            pdbId, offset, specCsn, isDirty, tupleCsn);
    }

    inline PageId GetBtrMetaPageId()
    {
        if (!IsInitialized() || GetLinkAndStatus()->TestType(BtrPageType::META_PAGE)) {
            return INVALID_PAGE_ID;
        }
        return GetLinkAndStatus()->btrMetaPageId;
    }

    inline Xid GetBtrMetaCreateXid()
    {
        return GetSegmentCreateXid();
    }

    inline void SetBtrMetaCreateXid(Xid xid)
    {
        SetSegmentCreateXid(xid);
    }

    inline void EmptyPage()
    {
        /* Clear all ItemIds, Tuples and TD space */
        dataHeader.tdCount = 0;
        SetUpper(GetSpecialOffset());
        SetLower(DataHeaderSize());
        StorageAssert(CheckSanity());
    }

    inline uint16 GetNonDeletedTupleNum()
    {
        uint16 nonDeletedNum = 0;
        for (uint16 off = GetLinkAndStatus()->GetFirstDataOffset(); off <= GetMaxOffset(); off++) {
            ItemId *id = GetItemIdPtr(off);
            if (!id->IsNormal()) {
                continue;
            }
            IndexTuple *tuple = GetIndexTuple(id);
            nonDeletedNum += static_cast<uint16>((!tuple->IsDeleted()));
        }
        return nonDeletedNum;
    }

    /* For dstore internal dfx logs */
    char *DumpLeafPageForLogs();
    /* For pagedump tool only */
    char *Dump(Page *metaPage, bool showData = true);

    bool HasGarbageSpace();
private:
    RetStatus UndoBtreeInsert(UndoRecord *undoRec, BtreeUndoContext *btrUndoContext, TD *tdOnPage);
    RetStatus UndoBtreeDelete(UndoRecord *undoRec, BtreeUndoContext *btrUndoContext, TD *tdOnPage);
    OffsetNumber UndoInsertPrunedTupleBack(IndexTuple *tuple, uint32 tupleSize, bool needRecycleTd,
        IndexInfo *indexInfo);
    RetStatus PruneNonItemIdTuples(uint32 &spacePruned, uint32 spaceNeeded, bool needRecycleTd);

    void DumpForPageMeta(StringInfo str);
    void DumpForLeaf(StringInfo str, Page *metaPage, bool showData);
    void DumpForPivot(StringInfo str, Page *metaPage, bool showData);
    void DumpForMeta(StringInfo str);
    void CollectTupleKeys(IndexTuple *tuple, TupleDesc tupleDesc, StringInfo str);
    void AppendAttrToStrByType(Oid typeOid, Datum attr, StringInfo str);
    void AppendArrAttrToStr(Datum attr, StringInfo str);
};
static_assert(sizeof(BtrPage) == BLCKSZ, "BtrPage size must be equal to BLCKSZ");

/* btree meta info, after page header */
struct BtrMeta {
    PageId rootPage;
    uint32 rootLevel;
    /*
     * After massive deletions we might have a scenario in which the
     * tree is "skinny",with several single-page levels below the root.
     * Operations will still be correct in this case, but we'd waste cycles
     * descending through the single-page levels.  To handle this we use an idea
     * from Lanin and Shasha: we keep track of the "fast root" level, which is
     * the lowest single-page level.All ordinary operations initiate their
     * searches at the fast root not the true root.
     * original name is fast_root, i change that to a straightforward word.
     */
    PageId lowestSinglePage;
    uint32 lowestSinglePageLevel;

    /* Index info, only used for undo */
    uint16 nkeyAtts;
    uint16 natts;
    int16 indexOption[INDEX_MAX_KEY_NUM];
    Oid attTypeIds[INDEX_MAX_KEY_NUM];
    int16 attlen[INDEX_MAX_KEY_NUM];
    bool attbyval[INDEX_MAX_KEY_NUM];
    char attalign[INDEX_MAX_KEY_NUM];
    Oid opcinTypes[INDEX_MAX_KEY_NUM]; /* Save opcintype to keep the rollback comparison funcs consistent with building
                                          and scanning. */
    Oid functionOids[INDEX_MAX_KEY_NUM * BTREE_SUPPORT_FUNC_NUM]; /* Save functionOids to
                                                                     keep the rollback comparison funcs. */
    int16_t numSupportProc; /* Save numSupportProc to keep the rollback compare. */
    Xid createXid; /* Xid when meta page created, used to verify when undo */
    char relKind;
    int16_t tableOidAtt;

    uint32 initializedtMagicNum;       /* mark if initialized, set to BTREE_META_STAT_INIT_MAGIC_NUM if initialized */

    /* operCount[BtreeOperType::BTR_OPER_SPLIT_WHEN_BUILD]: count splitting times of each level during build */
    /* operCount[BtreeOperType::BTR_OPER_SPLIT_WHEN_INSERT]: count splitting times of each level during insert */
    /* operCount[BtreeOperType::BTR_OPER_MARK_RECYCLABLE]: count marking recyclable times of each level */
    /* operCount[BtreeOperType::BTR_OPER_RECYCLE]: count recycle times of each level */
    uint64 operCount[static_cast<int>(BtreeOperType::BTR_OPER_MAX)][BTREE_HIGHEST_LEVEL];

    Oid attColIds[INDEX_MAX_KEY_NUM];

    inline void InitStatisticsInfo()
    {
        initializedtMagicNum = BTREE_META_STAT_INIT_MAGIC_NUM;
        if (likely((static_cast<uint64>(g_traceSwitch) & BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL) == 0)) {
            return;
        }
        for (int type = 0; type < static_cast<int>(BtreeOperType::BTR_OPER_MAX); type++) {
            for (uint32 level = 0; level < BTREE_HIGHEST_LEVEL; level++) {
                operCount[type][level] = 0U;
            }
        }
    }

    TupleDesc ConstructTupleDesc();

    inline PageId GetRootPageId() const
    {
        return rootPage;
    }

    inline uint32 GetRootLevel() const
    {
        return rootLevel;
    }

    inline void SetLowestSinglePage(const PageId dst)
    {
        lowestSinglePage = dst;
    }

    inline void SetLowestSinglePageLevel(uint32 level)
    {
        lowestSinglePageLevel = level;
    }

    inline PageId GetLowestSinglePage() const
    {
        return lowestSinglePage;
    }

    inline uint32 GetLowestSinglePageLevel() const
    {
        return lowestSinglePageLevel;
    }

    inline void SetBtreeMetaInfo(const PageId rootPageId, const PageId fastRootId, uint32 level, uint32 fastLevel)
    {
        rootPage = rootPageId;
        rootLevel = level;

        lowestSinglePage = fastRootId;
        lowestSinglePageLevel = fastLevel;
    }

    inline uint16 GetNkeyatts() const
    {
        return nkeyAtts;
    }

    inline uint16 GetNatts() const
    {
        return natts;
    }

    inline int16 GetIndoption(int i) const
    {
        return indexOption[i];
    }

    inline Oid GetAttTypids(int i) const
    {
        return attTypeIds[i];
    }

    inline int16 GetAttlen(int i) const
    {
        return attlen[i];
    }

    inline bool GetAttbyval(int i) const
    {
        return attbyval[i];
    }

    inline char GetAttalign(int i) const
    {
        return attalign[i];
    }

    inline Oid GetAttCollation(int i) const
    {
        return attColIds[i];
    }

    inline Xid GetCreateXid()
    {
        return createXid;
    }

    inline char GetRelKind() const
    {
        return relKind;
    }

    inline Oid GetTableOidAtt() const
    {
        return tableOidAtt;
    }
    inline int16_t GetNumSupportProc() const
    {
        return numSupportProc;
    }
};

STATIC_ASSERT_TRIVIAL(BtrPage);

constexpr uint32 BTR_META_SIZE = static_cast<uint32>(sizeof(BtrMeta));
constexpr uint32 DATA_SIZE_ON_BTREE_PAGE = static_cast<uint32>(MaxAlignDown((((BLCKSZ -
    MAXALIGN(RESERVED_BTR_PAGE_HEADER_SIZE)) - DEFAULT_TD_COUNT * sizeof(TD)) -
    MAXALIGN(sizeof(BtrPageLinkAndStatus)))));

constexpr int NUMBER_HIKEY_PER_BTREE_PAGE = 1;
constexpr int MIN_CTID_PER_BTREE_PAGE = 2; /* Need at least 2 index tuple with ctid per btree page */
constexpr int MAX_CTID_PER_BTREE_PAGE =
    static_cast<int>(DATA_SIZE_ON_BTREE_PAGE) / static_cast<int>((sizeof(IndexTuple) + sizeof(ItemId)));
constexpr uint32 MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE = DATA_SIZE_ON_BTREE_PAGE / (NUMBER_HIKEY_PER_BTREE_PAGE +
    MIN_CTID_PER_BTREE_PAGE) - static_cast<uint32>(sizeof(ItemId) + sizeof(ItemPointerData));
}  // namespace DSTORE

#endif /* SRC_GAUSSKERNEL_INCLUDE_PAGE_STORAGE_BTPAGE */
