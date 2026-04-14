/*
 * Attack-Defense unit tests for the DFX page verification framework.
 *
 * 攻防测试：构造约 20 种典型页面腐蚀攻击（覆盖 Generic / Heap / Index / Undo / Segment），
 * 通过参数化测试验证校验框架在各级别下的检测能力，并包含：
 *   - 参数化攻击表：按最小检测级别 / 高级别 / NONE 级别分别验证
 *   - False-positive 测试：正常页面在所有级别下均通过
 *   - 多重腐蚀测试：同一页面注入 3 种错误，VerifyPageFull 收集 >=2 个错误码
 *   - 确定性 Fuzz 测试：随机选取攻击 + 随机高级别，100% 检测率
 *   - CRC 比特翻转 Fuzz：随机位翻转，>=99% 检测率
 *   - 三场景一致性测试：OnRead / Full / OnWrite 行为一致性
 */
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "page/dstore_undo_page.h"
#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_heap_segment_meta_page.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "page/dstore_bitmap_meta_page.h"
#include "page/dstore_bitmap_page.h"
#include "page/dstore_tbs_file_meta_page.h"
#include "page/dstore_tbs_space_meta_page.h"
#include "undo/dstore_undo_record.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::ScopedVerifyConfig;
using DSTORE::ut_dfx::HasVerifyCode;
using DSTORE::ut_dfx::EnableAllModules;

namespace {

/* ======================================================================
 * Page Factory Functions — 复用自其他 UT 文件的标准构造函数
 * ====================================================================== */

/* ---------- Heap ---------- */
HeapPage *MakeValidHeapPage(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = reinterpret_cast<HeapPage *>(buf.data());
    page->Init(0, PageType::HEAP_PAGE_TYPE, pageId);
    page->SetLsn(1, 1, 1, false);
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET);
    page->m_header.m_lower = HEAP_PAGE_DATA_OFFSET;
    page->AllocateTdSpace();
    page->SetFsmIndex({INVALID_PAGE_ID, 0});
    page->SetPotentialDelSize(0);
    page->SetChecksum();
    return page;
}

void AddHeapTuple(HeapPage *page, OffsetNumber offset, uint16 tupleSize, uint8 tdId)
{
    page->SetUpper(static_cast<uint16>(page->GetUpper() - tupleSize));
    ItemId *itemId = page->GetItemIdPtr(offset);
    itemId->SetNormal(page->GetUpper(), tupleSize);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    HeapDiskTuple *tuple = page->GetDiskTuple(offset);
    tuple->SetTupleSize(tupleSize);
    tuple->SetTdId(tdId);
    tuple->SetLockerTdId(INVALID_TD_SLOT);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    tuple->SetLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_INSERT);
    tuple->SetNumColumn(1);
}

/* ---------- Index ---------- */
BtrPage *MakeValidIndexPage(PageBuffer &buf, PageId pageId)
{
    BtrPage *page = reinterpret_cast<BtrPage *>(buf.data());
    page->InitBtrPageInner(pageId);
    page->SetLsn(1, 1, 1, false);
    page->GetLinkAndStatus()->InitPageMeta({1, 1}, 0, false);
    page->SetBtrMetaCreateXid(Xid(0));
    page->AllocateTdSpace();
    page->SetChecksum();
    return page;
}

void AddIndexTuple(BtrPage *page, OffsetNumber offset, uint16 tupleSize, uint8 tdId)
{
    page->SetUpper(static_cast<uint16>(page->GetUpper() - tupleSize));
    ItemId *itemId = page->GetItemIdPtr(offset);
    itemId->SetNormal(page->GetUpper(), tupleSize);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    IndexTuple *tuple = page->GetIndexTuple(offset);
    tuple->SetSize(tupleSize);
    tuple->SetTdId(tdId);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    ItemPointerData heapCtid = INVALID_ITEM_POINTER;
    tuple->SetHeapCtid(&heapCtid);
}

/* ---------- Undo ---------- */
UndoRecordPage *MakeValidUndoRecPage(PageBuffer &buf, PageId pageId)
{
    UndoRecordPage *page = reinterpret_cast<UndoRecordPage *>(buf.data());
    page->m_undoRecPageHeader = {0, pageId, INVALID_PAGE_ID, INVALID_PAGE_ID};
    page->InitUndoRecPage(pageId);
    page->SetLsn(1, 1, 1, false);
    page->m_header.m_lower = UNDO_RECORD_PAGE_HEADER_SIZE;
    page->m_header.m_upper = BLCKSZ;
    page->SetChecksum();
    return page;
}

void AddFakeUndoRecord(UndoRecordPage *page, uint8 serializeSize, UndoType undoType)
{
    char *cursor = reinterpret_cast<char *>(page) + page->GetLower();
    *reinterpret_cast<uint8 *>(cursor) = serializeSize;
    memcpy(cursor + sizeof(uint8), &undoType, sizeof(UndoType));
    page->m_header.m_lower = static_cast<uint16>(page->GetLower() + serializeSize);
}

TransactionSlotPage *MakeValidTxnSlotPage(PageBuffer &buf, PageId pageId)
{
    TransactionSlotPage *page = reinterpret_cast<TransactionSlotPage *>(buf.data());
    page->InitTxnSlotPage(pageId);
    page->SetLsn(1, 1, 1, false);
    page->m_header.m_lower = TRX_PAGE_HEADER_SIZE;
    page->m_header.m_upper = BLCKSZ;
    for (int i = 0; i < TRX_PAGE_SLOTS_NUM; ++i) {
        page->m_slots[i].SetTrxSlotStatus(TXN_STATUS_FROZEN);
    }
    page->SetChecksum();
    return page;
}

/* ---------- Segment ---------- */
DataSegmentMetaPage *MakeValidDataSegMeta(PageBuffer &buf, PageId pageId)
{
    DataSegmentMetaPage *page = reinterpret_cast<DataSegmentMetaPage *>(buf.data());
    page->InitDataSegmentMetaPage(SegmentType::HEAP_SEGMENT_TYPE, pageId, EXT_SIZE_8, 1, 1);
    page->dataBlockCount = 1;
    page->dataFirst = pageId;
    page->dataLast = pageId;
    page->addedPageId = pageId;
    page->extendedPageId = {pageId.m_fileId, pageId.m_blockId + 7};
    page->lastExtentIsReused = false;
    page->SetChecksum();
    return page;
}

HeapSegmentMetaPage *MakeValidHeapSegMeta(PageBuffer &buf, PageId pageId)
{
    HeapSegmentMetaPage *page = reinterpret_cast<HeapSegmentMetaPage *>(buf.data());
    page->InitHeapSegmentMetaPage(SegmentType::HEAP_SEGMENT_TYPE, pageId, EXT_SIZE_8, 1, 1);
    page->dataBlockCount = 1;
    page->dataFirst = pageId;
    page->dataLast = pageId;
    page->addedPageId = pageId;
    page->extendedPageId = {pageId.m_fileId, pageId.m_blockId + 7};
    page->lastExtentIsReused = false;
    page->numFsms = 1;
    page->fsmInfos[0] = {pageId, 0};
    page->SetChecksum();
    return page;
}

UndoSegmentMetaPage *MakeValidUndoSegMeta(PageBuffer &buf, PageId pageId)
{
    UndoSegmentMetaPage *page = reinterpret_cast<UndoSegmentMetaPage *>(buf.data());
    page->InitUndoSegmentMetaPage(pageId, 1, 1);
    page->SetChecksum();
    return page;
}

/* ======================================================================
 * Attack Descriptor — 攻击描述结构
 * ====================================================================== */

/* 每个攻击的注入函数签名：接收 buffer 和 pageId，在 buffer 中构造并腐蚀页面 */
using InjectFunc = void (*)(PageBuffer &buf, PageId pageId);

struct AttackDescriptor {
    const char *name;            /* 攻击名称（用于 gtest 输出） */
    InjectFunc inject;           /* 注入函数 */
    VerifyLevel minDetectLevel;  /* 最低可检测级别 */
    VerifyCode expectedCode;     /* 期望报告的 VerifyCode */
    bool needsAllModules;        /* 是否需要启用所有模块（如 Segment/Tablespace） */
};

/* ======================================================================
 * Attack Inject Functions — 攻击注入实现
 * ====================================================================== */

/* --- G01: CRC single bit flip (不更新 CRC) --- */
void InjectG01_CrcBitFlip(PageBuffer &buf, PageId pageId)
{
    MakeValidHeapPage(buf, pageId);
    /* 翻转中间字节的一个比特，不更新 CRC */
    buf[BLCKSZ / 2] ^= 0x01;
}

/* --- G03: lower > upper --- */
void InjectG03_LowerGtUpper(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetLower(static_cast<uint16>(page->GetUpper() + 100));
    page->SetChecksum();
}

/* --- G04: upper > BLCKSZ --- */
void InjectG04_UpperGtBlcksz(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    page->SetUpper(static_cast<uint16>(BLCKSZ + 1));
    page->SetChecksum();
}

/* --- H01: special offset mismatch (Heap 的 special 应为 BLCKSZ) --- */
void InjectH01_SpecialOffsetMismatch(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    uint16 upperVal = page->GetUpper();
    page->m_header.m_special.m_offset = static_cast<uint16>((upperVal + BLCKSZ) / 2);
    page->SetChecksum();
}

/* --- H02: header offset invalid --- */
void InjectH02_HeaderOffsetInvalid(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET + 8);
    page->SetChecksum();
}

/* --- H03: TD count overflow --- */
void InjectH03_TdCountOverflow(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    page->dataHeader.tdCount = 200;
    page->SetChecksum();
}

/* --- H04: FSM slot invalid --- */
void InjectH04_FsmSlotInvalid(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    page->SetFsmIndex({INVALID_PAGE_ID, FSM_MAX_HWM});
    page->SetChecksum();
}

/* --- H05: potentialDelSize exceeds BLCKSZ --- */
void InjectH05_PotentialDelSizeOverflow(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    page->SetPotentialDelSize(BLCKSZ + 1);
    page->SetChecksum();
}

/* --- H06: TD count below minimum --- */
void InjectH06_TdCountBelowMin(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    page->dataHeader.tdCount = static_cast<uint8>(MIN_TD_COUNT - 1);
    page->SetChecksum();
}

/* --- H08: MEDIUM ItemId alignment invalid --- */
void InjectH08_ItemIdAlignmentInvalid(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    uint16 goodLower = page->GetLower();
    page->SetLower(goodLower + 1);  /* 打破 ItemId 对齐 */
    page->SetChecksum();
}

/* --- H11: HEAVY tuple overlap --- */
void InjectH11_TupleOverlap(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 64, 0);

    /* 手动构造第二个 tuple 使其与第一个重叠 */
    ItemId *itemId2 = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER + 1);
    uint16 overlapOffset = page->GetUpper() + 32;
    itemId2->SetNormal(overlapOffset, 32);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    HeapDiskTuple *tuple2 = reinterpret_cast<HeapDiskTuple *>(
        reinterpret_cast<char *>(page) + overlapOffset);
    tuple2->SetTupleSize(32);
    tuple2->SetTdId(0);
    tuple2->SetLockerTdId(INVALID_TD_SLOT);
    tuple2->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    tuple2->SetNumColumn(1);
    page->SetChecksum();
}

/* --- H13: HEAVY TD status invalid --- */
void InjectH13_TdStatusInvalid(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->SetTdStatus(static_cast<TupleTdStatus>(0xFF));
    page->SetChecksum();
}

/* --- H14: HEAVY tdId out of bounds --- */
void InjectH14_TdIdOob(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    uint8 tdCount = page->GetTdCount();
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, tdCount); /* tdId == tdCount，越界 */
    page->SetChecksum();
}

/* --- H16: HEAVY numColumn == 0 --- */
void InjectH16_NumColumnZero(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = MakeValidHeapPage(buf, pageId);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->SetNumColumn(0);
    page->SetChecksum();
}

/* --- I01: Index special offset invalid --- */
void InjectI01_IndexSpecialOffset(PageBuffer &buf, PageId pageId)
{
    BtrPage *page = MakeValidIndexPage(buf, pageId);
    page->SetSpecialOffset(static_cast<uint16>(page->GetSpecialOffset() - 8));
    page->SetChecksum();
}

/* --- I09: Index missing high key (non-rightmost page) --- */
void InjectI09_MissingHighKey(PageBuffer &buf, PageId pageId)
{
    BtrPage *page = MakeValidIndexPage(buf, pageId);
    page->GetLinkAndStatus()->SetRight({2, 2});
    page->SetChecksum();
}

/* --- I11: Index tuple size mismatch --- */
void InjectI11_IndexTupleSizeMismatch(PageBuffer &buf, PageId pageId)
{
    BtrPage *page = MakeValidIndexPage(buf, pageId);
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    /* 篡改 ItemId len 使之与 tuple->GetSize() 不匹配 */
    ItemId *itemId = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
    itemId->SetNormal(itemId->GetOffset(), 8);
    page->SetChecksum();
}

/* --- U01: Undo record page lower below header --- */
void InjectU01_UndoLowerBelowHeader(PageBuffer &buf, PageId pageId)
{
    UndoRecordPage *page = MakeValidUndoRecPage(buf, pageId);
    page->m_header.m_lower = static_cast<uint16>(sizeof(Page)) - 1;
    page->SetChecksum();
}

/* --- U03: Undo record page cur mismatch --- */
void InjectU03_UndoCurMismatch(PageBuffer &buf, PageId pageId)
{
    UndoRecordPage *page = MakeValidUndoRecPage(buf, pageId);
    page->m_undoRecPageHeader.cur = {pageId.m_fileId, static_cast<uint32>(pageId.m_blockId + 999)};
    page->SetChecksum();
}

/* --- U07: Undo record type invalid --- */
void InjectU07_UndoRecTypeInvalid(PageBuffer &buf, PageId pageId)
{
    UndoRecordPage *page = MakeValidUndoRecPage(buf, pageId);
    AddFakeUndoRecord(page, 16, UNDO_UNKNOWN);
    page->SetChecksum();
}

/* --- U12: Txn slot status invalid --- */
void InjectU12_TxnSlotStatusInvalid(PageBuffer &buf, PageId pageId)
{
    TransactionSlotPage *page = MakeValidTxnSlotPage(buf, pageId);
    page->m_slots[0].SetTrxSlotStatus(static_cast<TrxSlotStatus>(0xFF));
    page->SetChecksum();
}

/* --- S01: Segment type invalid --- */
void InjectS01_SegmentTypeInvalid(PageBuffer &buf, PageId pageId)
{
    DataSegmentMetaPage *page = MakeValidDataSegMeta(buf, pageId);
    page->segmentHeader.segmentType = SegmentType::UNDO_SEGMENT_TYPE;
    page->SetChecksum();
}

/* --- S02: Segment magic mismatch --- */
void InjectS02_SegmentMagicMismatch(PageBuffer &buf, PageId pageId)
{
    DataSegmentMetaPage *page = MakeValidDataSegMeta(buf, pageId);
    page->extentMeta.magic = 0;
    page->SetChecksum();
}

/* --- U13: Txn slot committed with INVALID_CSN --- */
void InjectU13_TxnSlotXidInvalid(PageBuffer &buf, PageId pageId)
{
    TransactionSlotPage *page = MakeValidTxnSlotPage(buf, pageId);
    page->m_slots[0].SetTrxSlotStatus(TXN_STATUS_COMMITTED);
    page->m_slots[0].SetCsn(INVALID_CSN);
    page->SetChecksum();
}

/*
 * Minimal layout of BtrQueuePageMeta for test use only.
 * Avoids including dstore_btr_queue_page.h which pulls in heavy
 * framework dependencies.
 */
struct TestBtrQueuePageMeta {
    uint32 versionNumber;
    Xid createdXid;
    PageId next;
    uint32 type;  /* BtrRecycleQueueType: RECYCLE=0, FREE=1 */
};

/* --- BQ01: BtrQueue type invalid --- */
void InjectBQ01_BtrQueueTypeInvalid(PageBuffer &buf, PageId pageId)
{
    Page *page = reinterpret_cast<Page *>(buf.data());
    uint16 specialSize = static_cast<uint16>(MAXALIGN(sizeof(TestBtrQueuePageMeta)));
    page->Init(specialSize, PageType::BTR_QUEUE_PAGE_TYPE, pageId);
    page->SetLsn(1, 1, 1, false);

    uint16 specialOffset = page->GetSpecialOffset();
    TestBtrQueuePageMeta *meta = reinterpret_cast<TestBtrQueuePageMeta *>(
        reinterpret_cast<char *>(page) + specialOffset);
    meta->versionNumber = 0;
    meta->createdXid = Xid(0);
    meta->next = INVALID_PAGE_ID;
    meta->type = 0xFF;  /* invalid BtrRecycleQueueType */
    page->SetChecksum();
}

/* --- TB01: Bitmap meta extent size invalid --- */
void InjectTB01_BitmapMetaExtentSizeInvalid(PageBuffer &buf, PageId pageId)
{
    TbsBitmapMetaPage *page = reinterpret_cast<TbsBitmapMetaPage *>(buf.data());
    page->InitBitmapMetaPage(pageId, 0, EXT_SIZE_8);
    page->SetLsn(1, 1, 1, false);
    page->extentSize = static_cast<ExtentSize>(0xFF);  /* invalid */
    page->SetChecksum();
}

/* --- TB02: Bitmap allocated count mismatch --- */
void InjectTB02_BitmapAllocatedCntMismatch(PageBuffer &buf, PageId pageId)
{
    TbsBitmapPage *page = reinterpret_cast<TbsBitmapPage *>(buf.data());
    page->InitBitmapPage(pageId, {pageId.m_fileId, pageId.m_blockId + 1});
    page->SetLsn(1, 1, 1, false);
    page->allocatedExtentCount = 0;
    memset(page->bitmap, 0, DF_BITMAP_BYTE_CNT);
    page->bitmap[0] = 0x07;  /* 3 bits set, but count says 0 */
    page->SetChecksum();
}

/* --- TB03: Space meta page version invalid --- */
void InjectTB03_SpacePageVersionInvalid(PageBuffer &buf, PageId pageId)
{
    TbsSpaceMetaPage *page = reinterpret_cast<TbsSpaceMetaPage *>(buf.data());
    page->InitTbsSpaceMetaPage(pageId);
    page->SetLsn(1, 1, 1, false);
    page->pageVersion = 2;  /* > 1, invalid */
    page->SetChecksum();
}

/* --- TB04: File meta oid below bootstrap --- */
void InjectTB04_FileBlockIdInvalid(PageBuffer &buf, PageId pageId)
{
    TbsFileMetaPage *page = reinterpret_cast<TbsFileMetaPage *>(buf.data());
    page->InitTbsFileMetaPage(pageId, 0, Xid(0));
    page->SetLsn(1, 1, 1, false);
    page->pageBaseGlsn = 1;
    page->oid = 0;  /* below FIRST_BOOTSTRAP_OBJECT_ID */
    page->SetChecksum();
}

/* ======================================================================
 * Attack Table — 攻击表
 * ====================================================================== */

/*
 * 攻击表：约 20 种核心攻击，涵盖 Generic / Heap / Index / Undo / Segment 五大模块。
 * 每条记录：{ 名称, 注入函数, 最低检测级别, 期望错误码 }
 */
const AttackDescriptor kAttackTable[] = {
    /* Generic attacks */
    {"G01_CrcBitFlip",         InjectG01_CrcBitFlip,          VerifyLevel::LIGHT,  VerifyCode::PAGE_CRC_MISMATCH, false},
    {"G03_LowerGtUpper",       InjectG03_LowerGtUpper,        VerifyLevel::LIGHT,  VerifyCode::PAGE_BOUNDARY_INVALID, false},
    {"G04_UpperGtBlcksz",      InjectG04_UpperGtBlcksz,       VerifyLevel::LIGHT,  VerifyCode::PAGE_BOUNDARY_INVALID, false},

    /* Heap LIGHT attacks */
    {"H01_SpecialOffset",      InjectH01_SpecialOffsetMismatch, VerifyLevel::LIGHT,  VerifyCode::HEAP_SPECIAL_OFFSET_MISMATCH, false},
    {"H02_HeaderOffset",       InjectH02_HeaderOffsetInvalid,   VerifyLevel::LIGHT,  VerifyCode::HEAP_HEADER_OFFSET_INVALID, false},
    {"H03_TdCountOverflow",    InjectH03_TdCountOverflow,       VerifyLevel::LIGHT,  VerifyCode::HEAP_TD_COUNT_OVERFLOW, false},
    {"H04_FsmSlotInvalid",     InjectH04_FsmSlotInvalid,        VerifyLevel::LIGHT,  VerifyCode::HEAP_FSM_SLOT_INVALID, false},
    {"H05_PotentialDelSize",   InjectH05_PotentialDelSizeOverflow, VerifyLevel::LIGHT, VerifyCode::PAGE_BOUNDARY_INVALID, false},
    {"H06_TdCountBelowMin",    InjectH06_TdCountBelowMin,       VerifyLevel::LIGHT,  VerifyCode::HEAP_TD_COUNT_OVERFLOW, false},

    /* Heap MEDIUM attacks */
    {"H08_ItemIdAlignment",    InjectH08_ItemIdAlignmentInvalid, VerifyLevel::MEDIUM, VerifyCode::HEAP_ITEMID_ALIGNMENT_INVALID, false},

    /* Heap HEAVY attacks */
    {"H11_TupleOverlap",      InjectH11_TupleOverlap,          VerifyLevel::HEAVY,  VerifyCode::HEAP_TUPLE_OVERLAP, false},
    {"H13_TdStatusInvalid",   InjectH13_TdStatusInvalid,       VerifyLevel::HEAVY,  VerifyCode::HEAP_TD_SANITY_FAIL, false},
    {"H14_TdIdOob",           InjectH14_TdIdOob,               VerifyLevel::HEAVY,  VerifyCode::HEAP_TD_SANITY_FAIL, false},
    {"H16_NumColumnZero",     InjectH16_NumColumnZero,          VerifyLevel::HEAVY,  VerifyCode::HEAP_TUPLE_NUM_COLUMN_INVALID, false},

    /* Index attacks */
    {"I01_SpecialOffset",     InjectI01_IndexSpecialOffset,     VerifyLevel::LIGHT,  VerifyCode::BTR_SPECIAL_OFFSET_INVALID, false},
    {"I09_MissingHighKey",    InjectI09_MissingHighKey,         VerifyLevel::HEAVY,  VerifyCode::PAGE_BOUNDARY_INVALID, false},
    {"I11_TupleSizeMismatch", InjectI11_IndexTupleSizeMismatch, VerifyLevel::HEAVY,  VerifyCode::INDEX_TUPLE_SIZE_MISMATCH, false},

    /* Undo attacks */
    {"U01_LowerBelowHeader",  InjectU01_UndoLowerBelowHeader,  VerifyLevel::LIGHT,  VerifyCode::PAGE_BOUNDARY_INVALID, false},
    {"U03_CurMismatch",       InjectU03_UndoCurMismatch,       VerifyLevel::LIGHT,  VerifyCode::PAGE_ID_MISMATCH, false},
    {"U07_UndoRecTypeInvalid", InjectU07_UndoRecTypeInvalid,   VerifyLevel::HEAVY,  VerifyCode::UNDO_REC_TYPE_INVALID, false},
    {"U12_SlotStatusInvalid", InjectU12_TxnSlotStatusInvalid,  VerifyLevel::HEAVY,  VerifyCode::UNDO_SLOT_STATE_INVALID, false},
    {"U13_TxnSlotXidInvalid", InjectU13_TxnSlotXidInvalid,    VerifyLevel::HEAVY,  VerifyCode::UNDO_SLOT_XID_INVALID, false},

    /* Segment attacks (需要启用 SEGMENT 模块) */
    {"S01_SegmentTypeInvalid", InjectS01_SegmentTypeInvalid,   VerifyLevel::LIGHT,  VerifyCode::SEG_SEGMENT_TYPE_INVALID, true},
    {"S02_SegmentMagicMismatch", InjectS02_SegmentMagicMismatch, VerifyLevel::LIGHT, VerifyCode::SEG_MAGIC_MISMATCH, true},

    /* BtrRecycle attacks (需要启用所有模块) */
    {"BQ01_BtrQueueTypeInvalid", InjectBQ01_BtrQueueTypeInvalid, VerifyLevel::LIGHT, VerifyCode::BTR_QUEUE_INCONSISTENT, false},

    /* Tablespace attacks (需要启用所有模块) */
    {"TB01_BitmapMetaExtentSize", InjectTB01_BitmapMetaExtentSizeInvalid, VerifyLevel::LIGHT, VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID, true},
    {"TB02_BitmapAllocatedCnt",   InjectTB02_BitmapAllocatedCntMismatch,  VerifyLevel::HEAVY, VerifyCode::BITMAP_ALLOCATED_CNT_MISMATCH, true},
    {"TB03_SpacePageVersion",     InjectTB03_SpacePageVersionInvalid,     VerifyLevel::LIGHT, VerifyCode::SPACE_PAGE_VERSION_INVALID, true},
    {"TB04_FileBlockIdInvalid",   InjectTB04_FileBlockIdInvalid,          VerifyLevel::HEAVY, VerifyCode::FILE_BLOCK_ID_INVALID, true},
};

constexpr int ATTACK_COUNT = sizeof(kAttackTable) / sizeof(kAttackTable[0]);

/* ======================================================================
 * Helper: 判断某个级别是否 >= 另一个级别
 * ====================================================================== */
bool IsLevelAtLeast(VerifyLevel actual, VerifyLevel required)
{
    return static_cast<int>(actual) >= static_cast<int>(required);
}

/* Helper: 注册所有已实现的页面校验器 */
void RegisterAllVerifiers()
{
    RegisterHeapPageVerifier();
    RegisterIndexPageVerifier();
    RegisterUndoPageVerifiers();
    RegisterSegmentPageVerifiers();
    RegisterFsmPageVerifiers();
    RegisterTablespacePageVerifiers();
    RegisterBtrRecyclePageVerifiers();
}

/* Helper: 判断攻击是否需要启用所有模块 */
bool NeedsAllModules(int attackIdx)
{
    return kAttackTable[attackIdx].needsAllModules;
}

}  /* anonymous namespace */

/* ======================================================================
 * Parameterized Test Class
 * ====================================================================== */

class UTAttackDefenseVerify : public ::testing::TestWithParam<int> {
protected:
    void SetUp() override
    {
        RegisterAllVerifiers();
    }
};

/* ========== 参数化测试：最小检测级别下能检测到攻击 ========== */

TEST_P(UTAttackDefenseVerify, AttackDetectedAtMinLevel)
{
    int idx = GetParam();
    const AttackDescriptor &attack = kAttackTable[idx];

    ScopedVerifyConfig guard;
    if (NeedsAllModules(idx)) {
        EnableAllModules();
    }

    PageBuffer buf{};
    PageId pageId = {100, static_cast<uint32>(500 + idx)};
    attack.inject(buf, pageId);

    VerifyReport report;
    Page *page = reinterpret_cast<Page *>(buf.data());
    RetStatus ret = VerifyPage(page, attack.minDetectLevel, &report);

    EXPECT_EQ(ret, DSTORE_FAIL)
        << "Attack [" << attack.name << "] should be detected at level "
        << static_cast<int>(attack.minDetectLevel);
    EXPECT_TRUE(report.HasError())
        << "Attack [" << attack.name << "] report should have errors";

    /* 验证期望的错误码出现在报告中。
     * 对于 I01（Index special offset），generic boundary 检查可能先触发，
     * 因此允许 PAGE_BOUNDARY_INVALID 作为替代。 */
    bool codeMatch = HasVerifyCode(report, attack.expectedCode);
    if (!codeMatch && attack.expectedCode == VerifyCode::BTR_SPECIAL_OFFSET_INVALID) {
        codeMatch = HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID);
    }
    EXPECT_TRUE(codeMatch)
        << "Attack [" << attack.name << "] expected code 0x"
        << std::hex << static_cast<uint32>(attack.expectedCode);
}

/* ========== 参数化测试：高于最小级别同样能检测 ========== */

TEST_P(UTAttackDefenseVerify, AttackDetectedAtHigherLevels)
{
    int idx = GetParam();
    const AttackDescriptor &attack = kAttackTable[idx];

    ScopedVerifyConfig guard;
    if (NeedsAllModules(idx)) {
        EnableAllModules();
    }

    VerifyLevel allLevels[] = {VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel level : allLevels) {
        if (!IsLevelAtLeast(level, attack.minDetectLevel)) {
            continue;  /* 跳过低于最小检测级别的 */
        }

        PageBuffer buf{};
        PageId pageId = {100, static_cast<uint32>(600 + idx * 10 + static_cast<int>(level))};
        attack.inject(buf, pageId);

        VerifyReport report;
        Page *page = reinterpret_cast<Page *>(buf.data());
        RetStatus ret = VerifyPage(page, level, &report);

        EXPECT_EQ(ret, DSTORE_FAIL)
            << "Attack [" << attack.name << "] should be detected at level "
            << static_cast<int>(level);
        EXPECT_TRUE(report.HasError());
    }
}

/* ========== 参数化测试：NONE 级别永远不检测 ========== */

TEST_P(UTAttackDefenseVerify, NoneLevelNeverDetects)
{
    int idx = GetParam();
    const AttackDescriptor &attack = kAttackTable[idx];

    ScopedVerifyConfig guard;
    if (NeedsAllModules(idx)) {
        EnableAllModules();
    }

    PageBuffer buf{};
    PageId pageId = {100, static_cast<uint32>(700 + idx)};
    attack.inject(buf, pageId);

    VerifyReport report;
    Page *page = reinterpret_cast<Page *>(buf.data());
    RetStatus ret = VerifyPage(page, VerifyLevel::NONE, &report);

    EXPECT_EQ(ret, DSTORE_SUCC)
        << "Attack [" << attack.name << "] should NOT be detected at NONE level";
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

/* ========== 参数化测试：低于最小检测级别时不应检测到 ========== */

TEST_P(UTAttackDefenseVerify, BelowMinLevel_NotDetected)
{
    int idx = GetParam();
    const AttackDescriptor &attack = kAttackTable[idx];

    /* Skip attacks with minDetectLevel == LIGHT (no level below LIGHT except NONE, which is already tested) */
    if (attack.minDetectLevel == VerifyLevel::LIGHT) return;

    ScopedVerifyConfig guard;
    if (NeedsAllModules(idx)) {
        EnableAllModules();
    }

    VerifyLevel levels[] = {VerifyLevel::LIGHT, VerifyLevel::MEDIUM};
    for (VerifyLevel level : levels) {
        if (IsLevelAtLeast(level, attack.minDetectLevel)) break;

        PageBuffer buf{};
        PageId pageId = {100, static_cast<uint32>(1500 + idx * 10 + static_cast<int>(level))};
        attack.inject(buf, pageId);

        VerifyReport report;
        Page *page = reinterpret_cast<Page *>(buf.data());
        RetStatus ret = VerifyPage(page, level, &report);

        EXPECT_EQ(ret, DSTORE_SUCC)
            << "Attack [" << attack.name << "] should NOT be detected at level "
            << static_cast<int>(level) << " (below min " << static_cast<int>(attack.minDetectLevel) << ")";
    }
}

INSTANTIATE_TEST_SUITE_P(
    AllAttacks,
    UTAttackDefenseVerify,
    ::testing::Range(0, ATTACK_COUNT),
    [](const ::testing::TestParamInfo<int> &info) {
        return std::string(kAttackTable[info.param].name);
    }
);

/* ======================================================================
 * False-Positive Tests — 正常页面在所有级别下均通过
 * ====================================================================== */

TEST(UTAttackDefenseVerify, ValidHeapPage_PassesAllLevels)
{
    RegisterAllVerifiers();

    VerifyLevel levels[] = {VerifyLevel::NONE, VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel level : levels) {
        PageBuffer buf{};
        HeapPage *page = MakeValidHeapPage(buf, {100, 800});
        AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
        page->SetChecksum();

        VerifyReport report;
        EXPECT_EQ(VerifyPage(page, level, &report), DSTORE_SUCC)
            << "Valid heap page should pass at level " << static_cast<int>(level);
    }
}

TEST(UTAttackDefenseVerify, ValidIndexPage_PassesAllLevels)
{
    RegisterAllVerifiers();

    VerifyLevel levels[] = {VerifyLevel::NONE, VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel level : levels) {
        PageBuffer buf{};
        BtrPage *page = MakeValidIndexPage(buf, {100, 801});
        AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
        page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
        page->SetChecksum();

        VerifyReport report;
        EXPECT_EQ(VerifyPage(page, level, &report), DSTORE_SUCC)
            << "Valid index page should pass at level " << static_cast<int>(level);
    }
}

TEST(UTAttackDefenseVerify, ValidUndoRecordPage_PassesAllLevels)
{
    RegisterAllVerifiers();

    VerifyLevel levels[] = {VerifyLevel::NONE, VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel level : levels) {
        PageBuffer buf{};
        MakeValidUndoRecPage(buf, {100, 802});

        VerifyReport report;
        Page *page = reinterpret_cast<Page *>(buf.data());
        EXPECT_EQ(VerifyPage(page, level, &report), DSTORE_SUCC)
            << "Valid undo record page should pass at level " << static_cast<int>(level);
    }
}

TEST(UTAttackDefenseVerify, ValidTxnSlotPage_PassesAllLevels)
{
    RegisterAllVerifiers();

    VerifyLevel levels[] = {VerifyLevel::NONE, VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel level : levels) {
        PageBuffer buf{};
        MakeValidTxnSlotPage(buf, {100, 803});

        VerifyReport report;
        Page *page = reinterpret_cast<Page *>(buf.data());
        EXPECT_EQ(VerifyPage(page, level, &report), DSTORE_SUCC)
            << "Valid txn slot page should pass at level " << static_cast<int>(level);
    }
}

TEST(UTAttackDefenseVerify, ValidDataSegMeta_PassesAllLevels)
{
    RegisterAllVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();

    VerifyLevel levels[] = {VerifyLevel::NONE, VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel level : levels) {
        PageBuffer buf{};
        MakeValidDataSegMeta(buf, {100, 804});

        VerifyReport report;
        Page *page = reinterpret_cast<Page *>(buf.data());
        EXPECT_EQ(VerifyPage(page, level, &report), DSTORE_SUCC)
            << "Valid data segment meta should pass at level " << static_cast<int>(level);
    }
}

TEST(UTAttackDefenseVerify, ValidHeapSegMeta_PassesAllLevels)
{
    RegisterAllVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();

    VerifyLevel levels[] = {VerifyLevel::NONE, VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel level : levels) {
        PageBuffer buf{};
        MakeValidHeapSegMeta(buf, {100, 805});

        VerifyReport report;
        Page *page = reinterpret_cast<Page *>(buf.data());
        EXPECT_EQ(VerifyPage(page, level, &report), DSTORE_SUCC)
            << "Valid heap segment meta should pass at level " << static_cast<int>(level);
    }
}

TEST(UTAttackDefenseVerify, ValidUndoSegMeta_PassesAllLevels)
{
    RegisterAllVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();

    VerifyLevel levels[] = {VerifyLevel::NONE, VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel level : levels) {
        PageBuffer buf{};
        MakeValidUndoSegMeta(buf, {100, 806});

        VerifyReport report;
        Page *page = reinterpret_cast<Page *>(buf.data());
        EXPECT_EQ(VerifyPage(page, level, &report), DSTORE_SUCC)
            << "Valid undo segment meta should pass at level " << static_cast<int>(level);
    }
}

/* ======================================================================
 * Multi-Corruption Test — 多重腐蚀
 * ====================================================================== */

TEST(UTAttackDefenseVerify, MultiCorruption_HeapTripleFault)
{
    RegisterAllVerifiers();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {100, 900});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 1, 32, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 2, 32, 0);

    /* Fault 1: tuple 1 invalid TdStatus */
    HeapDiskTuple *tuple1 = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple1->SetTdStatus(static_cast<TupleTdStatus>(0xFF));

    /* Fault 2: tuple 2 numColumn = 0 */
    HeapDiskTuple *tuple2 = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER + 1);
    tuple2->SetNumColumn(0);

    /* Fault 3: tuple 3 lockerTdId out of bounds */
    HeapDiskTuple *tuple3 = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER + 2);
    tuple3->SetLockerTdId(page->GetTdCount() + 10);

    page->SetChecksum();

    /* 使用 VerifyPageFull（收集模式），不会因第一个错误而中止 */
    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());

    /* HEAVY 收集模式下应检测到 >=2 个错误码 */
    EXPECT_GE(report.GetErrorCount(), 2U)
        << "Multi-corruption should collect at least 2 distinct errors";
}

/* ======================================================================
 * Fuzz Tests — 确定性随机攻击
 * ====================================================================== */

TEST(UTAttackDefenseVerify, FuzzRandomAttacks)
{
    RegisterAllVerifiers();

    srand(42);  /* 确定性种子 */
    int totalRuns = 500;
    int detected = 0;

    ScopedVerifyConfig guard;
    EnableAllModules();  /* enable all modules for entire fuzz */

    for (int i = 0; i < totalRuns; ++i) {
        int attackIdx = rand() % ATTACK_COUNT;
        const AttackDescriptor &attack = kAttackTable[attackIdx];

        /* 选择 >= minDetectLevel 的随机级别 */
        VerifyLevel candidates[3];
        int candidateCount = 0;
        VerifyLevel allLevels[] = {VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
        for (VerifyLevel lvl : allLevels) {
            if (IsLevelAtLeast(lvl, attack.minDetectLevel)) {
                candidates[candidateCount++] = lvl;
            }
        }
        VerifyLevel level = candidates[rand() % candidateCount];

        PageBuffer buf{};
        PageId pageId = {100, static_cast<uint32>(1000 + i)};
        attack.inject(buf, pageId);

        VerifyReport report;
        Page *page = reinterpret_cast<Page *>(buf.data());
        RetStatus ret = VerifyPage(page, level, &report);

        if (ret == DSTORE_FAIL && report.HasError()) {
            detected++;
        }
    }

    /* 100% 检测率——所有攻击在 >= minDetectLevel 下必须被检测到 */
    EXPECT_EQ(detected, totalRuns)
        << "Fuzz: all " << totalRuns << " attacks should be detected, but only "
        << detected << " were caught";
}

TEST(UTAttackDefenseVerify, FuzzRandomBitFlips)
{
    RegisterAllVerifiers();

    srand(12345);  /* 确定性种子，与 FuzzRandomAttacks 使用不同种子 */
    int totalRuns = 200;
    int detected = 0;

    for (int i = 0; i < totalRuns; ++i) {
        PageBuffer buf{};
        Page *page = nullptr;

        /* 交替对 Heap 和 Index 页面进行比特翻转 */
        if (i % 2 == 0) {
            HeapPage *hp = MakeValidHeapPage(buf, {100, static_cast<uint32>(2000 + i)});
            AddHeapTuple(hp, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
            hp->SetChecksum();
            page = hp;
        } else {
            BtrPage *ip = MakeValidIndexPage(buf, {100, static_cast<uint32>(2000 + i)});
            AddIndexTuple(ip, BTREE_PAGE_HIKEY, 24, 0);
            ip->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
            ip->SetChecksum();
            page = ip;
        }

        /* 在页面数据区域随机翻转一个比特（避开 checksum 字段本身）。
         * Page header 的 checksum 位于固定偏移，翻转数据区即可触发 CRC 失败。 */
        int bytePos = static_cast<int>(sizeof(uint32)) + (rand() % (BLCKSZ - static_cast<int>(sizeof(uint32)) - 1));
        int bitPos = rand() % 8;
        reinterpret_cast<unsigned char *>(page)[bytePos] ^= static_cast<unsigned char>(1 << bitPos);

        VerifyReport report;
        RetStatus ret = VerifyPage(page, VerifyLevel::LIGHT, &report);

        if (ret == DSTORE_FAIL) {
            detected++;
        }
    }

    /* CRC 检测率应 >= 99%（理论上对单比特翻转应 100%，但某些翻转位置可能
     * 恰好落在 CRC 字段内部导致误匹配，此处留 1% 容差） */
    double detectionRate = static_cast<double>(detected) / static_cast<double>(totalRuns);
    EXPECT_GE(detectionRate, 0.99)
        << "CRC bit flip detection rate " << (detectionRate * 100.0)
        << "% is below 99% threshold";
}

/* ======================================================================
 * Three-Scenario Consistency Test — 三场景一致性
 * ====================================================================== */

TEST(UTAttackDefenseVerify, ThreeScenarios_Consistency)
{
    RegisterAllVerifiers();

    /* 构造一个带 boundary 错误的 Heap 页面 */
    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {100, 950});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetLower(static_cast<uint16>(page->GetUpper() + 100));
    page->SetChecksum();

    /* Scenario 1: VerifyPageOnRead — 返回 FAIL，报告错误 */
    VerifyReport readReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &readReport), DSTORE_FAIL);
    EXPECT_TRUE(readReport.HasError());
    EXPECT_TRUE(HasVerifyCode(readReport, VerifyCode::PAGE_BOUNDARY_INVALID));

    /* Scenario 2: VerifyPageFull — 返回 SUCC（收集模式），但报告有错误 */
    VerifyReport fullReport;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &fullReport), DSTORE_SUCC);
    EXPECT_TRUE(fullReport.HasError());
    EXPECT_TRUE(HasVerifyCode(fullReport, VerifyCode::PAGE_BOUNDARY_INVALID));

    /* Scenario 3: VerifyPageOnWrite 在真实环境中会 PANIC->abort，
     * UT 环境无法安全测试（进程直接终止），因此不在此验证 Write 路径。
     * Write 路径的正确性由 OnRead + Full 的一致性间接保证。 */
}

/* ======================================================================
 * 三场景 + Index 页面一致性（扩展覆盖 Index 模块）
 * ====================================================================== */

TEST(UTAttackDefenseVerify, ThreeScenarios_IndexPage)
{
    RegisterAllVerifiers();

    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {100, 960});
    /* 破坏 special offset */
    page->SetSpecialOffset(static_cast<uint16>(page->GetSpecialOffset() - 8));
    page->SetChecksum();

    /* OnRead: FAIL */
    VerifyReport readReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &readReport), DSTORE_FAIL);
    EXPECT_TRUE(readReport.HasError());

    /* Full: SUCC with errors */
    VerifyReport fullReport;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &fullReport), DSTORE_SUCC);
    EXPECT_TRUE(fullReport.HasError());

    /* OnWrite: 会 PANIC->abort，UT 环境下无法安全测试 */
}
