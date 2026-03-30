#include "dfx/dstore_page_verify.h"

#include "page/dstore_btr_queue_page.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "page/dstore_btr_recycle_root_meta_page.h"

namespace DSTORE {

namespace {

RetStatus ReportBtrRecycleError(VerifyReport *report, const Page *page, const char *checkName, uint64 expected,
    uint64 actual, const char *message)
{
    if (report != nullptr) {
        report->AddResult(VerifySeverity::ERROR_LEVEL, "page", page->GetSelfPageId(), checkName, expected, actual, "%s",
            message);
    }
    return DSTORE_FAIL;
}

bool IsValidQueueType(BtrRecycleQueueType type)
{
    return type == BtrRecycleQueueType::RECYCLE || type == BtrRecycleQueueType::FREE;
}

const BtrQueuePageMeta *GetQueuePageMeta(const BtrQueuePage *queuePage)
{
    const char *meta = reinterpret_cast<const char *>(queuePage) + queuePage->GetSpecialOffset();
    return reinterpret_cast<const BtrQueuePageMeta *>(meta);
}

RetStatus VerifyBtrQueuePageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    BtrQueuePage *queuePage = const_cast<BtrQueuePage *>(static_cast<const BtrQueuePage *>(page));
    const uint16 expectedSpecialOffset = static_cast<uint16>(BLCKSZ - MAXALIGN(sizeof(BtrQueuePageMeta)));

    if (queuePage->GetSpecialOffset() != expectedSpecialOffset) {
        return ReportBtrRecycleError(report, queuePage, "btr_queue_special_offset_invalid", expectedSpecialOffset,
            queuePage->GetSpecialOffset(), "Btree recycle queue page special offset is invalid");
    }

    const BtrQueuePageMeta *meta = GetQueuePageMeta(queuePage);
    if (!IsValidQueueType(meta->GetType())) {
        return ReportBtrRecycleError(report, queuePage, "btr_queue_type_invalid",
            static_cast<uint64>(BtrRecycleQueueType::FREE), static_cast<uint64>(meta->GetType()),
            "Btree recycle queue page type is invalid");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyBtrQueuePageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyBtrQueuePageLightweight(page, level, report);
    BtrQueuePage *queuePage = const_cast<BtrQueuePage *>(static_cast<const BtrQueuePage *>(page));

    const BtrQueuePageMeta *meta = GetQueuePageMeta(queuePage);
    if (meta->GetType() == BtrRecycleQueueType::RECYCLE) {
        RecyclablePageQueue *queue = queuePage->GetQueue<RecyclablePageQueue>();
        if (queue->GetSize() > queue->GetCapacity()) {
            ret = ReportBtrRecycleError(report, queuePage, "btr_recycle_queue_size_invalid", queue->GetCapacity(),
                queue->GetSize(), "Btree recycle queue page size exceeds queue capacity");
        }
    } else {
        ReusablePageQueue *queue = queuePage->GetQueue<ReusablePageQueue>();
        if (queue->GetSize() > queue->GetCapacity()) {
            ret = ReportBtrRecycleError(report, queuePage, "btr_free_queue_size_invalid", queue->GetCapacity(),
                queue->GetSize(), "Btree free queue page size exceeds queue capacity");
        }
        if (queue->numAllocatedSlots > queue->GetCapacity()) {
            ret = ReportBtrRecycleError(report, queuePage, "btr_free_queue_slots_invalid", queue->GetCapacity(),
                queue->numAllocatedSlots, "Btree free queue page allocated slot count exceeds queue capacity");
        }
    }

    return ret;
}

RetStatus VerifyBtrRecyclePartitionMetaLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const BtrRecyclePartitionMetaPage *metaPage = static_cast<const BtrRecyclePartitionMetaPage *>(page);

    if (metaPage->createdXid == INVALID_XID) {
        return ReportBtrRecycleError(report, metaPage, "btr_recycle_partition_xid_invalid", 1, 0,
            "Btree recycle partition meta page must keep a valid created xid");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyBtrRecyclePartitionMetaHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyBtrRecyclePartitionMetaLightweight(page, level, report);
    const BtrRecyclePartitionMetaPage *metaPage = static_cast<const BtrRecyclePartitionMetaPage *>(page);

    if (metaPage->accessTimestamp == 0) {
        ret = ReportBtrRecycleError(report, metaPage, "btr_recycle_partition_timestamp_invalid", 1, 0,
            "Btree recycle partition meta page access timestamp must be initialized");
    }

    if (metaPage->recycleQueueHead == metaPage->GetSelfPageId() || metaPage->freeQueueHead == metaPage->GetSelfPageId()) {
        ret = ReportBtrRecycleError(report, metaPage, "btr_recycle_partition_self_link_invalid", 0, 1,
            "Btree recycle partition meta page queue heads must not point to itself");
    }

    return ret;
}

RetStatus VerifyBtrRecycleRootMetaLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const BtrRecycleRootMetaPage *metaPage = static_cast<const BtrRecycleRootMetaPage *>(page);

    if (metaPage->GetCreatedXid() == INVALID_XID) {
        return ReportBtrRecycleError(report, metaPage, "btr_recycle_root_xid_invalid", 1, 0,
            "Btree recycle root meta page must keep a valid created xid");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyBtrRecycleRootMetaHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyBtrRecycleRootMetaLightweight(page, level, report);
    BtrRecycleRootMetaPage *metaPage = const_cast<BtrRecycleRootMetaPage *>(static_cast<const BtrRecycleRootMetaPage *>(page));

    for (uint16 i = 0; i < MAX_BTR_RECYCLE_PARTITION; ++i) {
        const PageId partMetaPageId = metaPage->GetRecyclePartitionMetaPageId(i);
        if (partMetaPageId == metaPage->GetSelfPageId()) {
            ret = ReportBtrRecycleError(report, metaPage, "btr_recycle_root_partition_self_link_invalid", 0, i + 1,
                "Btree recycle root meta page partition entry must not point to itself");
            break;
        }
    }

    return ret;
}

}  // namespace

void RegisterBtrRecyclePageVerifiers()
{
    (void)RegisterPageVerifier(PageType::BTR_QUEUE_PAGE_TYPE, "BtrQueuePage",
        VerifyBtrQueuePageLightweight, VerifyBtrQueuePageHeavyweight);
    (void)RegisterPageVerifier(PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE, "BtrRecyclePartitionMetaPage",
        VerifyBtrRecyclePartitionMetaLightweight, VerifyBtrRecyclePartitionMetaHeavyweight);
    (void)RegisterPageVerifier(PageType::BTR_RECYCLE_ROOT_META_PAGE_TYPE, "BtrRecycleRootMetaPage",
        VerifyBtrRecycleRootMetaLightweight, VerifyBtrRecycleRootMetaHeavyweight);
}

}  // namespace DSTORE
