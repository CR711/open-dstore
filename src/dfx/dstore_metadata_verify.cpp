#include "dfx/dstore_metadata_verify.h"

#include <cstdarg>
#include <cstdio>

#include "buffer/dstore_buf_mgr.h"
#include "control/dstore_control_file.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_pdb.h"
#include "dfx/dstore_page_verify.h"

namespace DSTORE {

namespace {

constexpr const char *METADATA_VERIFY_TARGET = "metadata";

class BufferMetadataVerifyPageSource : public MetadataVerifyPageSource {
public:
    BufferMetadataVerifyPageSource(BufMgrInterface *bufMgr, PdbId pdbId)
        : m_bufMgr(bufMgr), m_pdbId(pdbId == INVALID_PDB_ID ? g_defaultPdbId : pdbId)
    {}

    SegmentMetaPage *ReadSegmentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return static_cast<SegmentMetaPage *>(ReadPage(pageId, bufferDesc));
    }

    BtrPage *ReadBtreeMetaPage(const PageId &segmentMetaPageId, BufferDesc **bufferDesc) override
    {
        return static_cast<BtrPage *>(ReadPage(
            {segmentMetaPageId.m_fileId, static_cast<BlockNumber>(segmentMetaPageId.m_blockId + 1)}, bufferDesc));
    }

    RetStatus GetTablespaceId(FileId fileId, TablespaceId *tablespaceId) override
    {
        if (tablespaceId == nullptr || g_storageInstance == nullptr) {
            return DSTORE_FAIL;
        }

        StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
        if (pdb == nullptr || pdb->GetControlFile() == nullptr) {
            return DSTORE_FAIL;
        }

        ControlDataFilePageItemData fileItem;
        if (pdb->GetControlFile()->GetDataFilePageItemData(fileId, &fileItem) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }

        *tablespaceId = fileItem.tablespaceId;
        return DSTORE_SUCC;
    }

    void ReleasePage(BufferDesc *bufferDesc) override
    {
        if (bufferDesc != INVALID_BUFFER_DESC && m_bufMgr != nullptr) {
            m_bufMgr->UnlockAndRelease(bufferDesc);
        }
    }

private:
    Page *ReadPage(const PageId &pageId, BufferDesc **bufferDesc)
    {
        if (bufferDesc == nullptr || m_bufMgr == nullptr) {
            return nullptr;
        }

        *bufferDesc = m_bufMgr->Read(m_pdbId, pageId, LW_SHARED);
        if (*bufferDesc == INVALID_BUFFER_DESC) {
            return nullptr;
        }
        return static_cast<Page *>((*bufferDesc)->GetPage());
    }

    BufMgrInterface *m_bufMgr{nullptr};
    PdbId m_pdbId{INVALID_PDB_ID};
};

}  // namespace

MetadataVerifier::MetadataVerifier(
    MetadataVerifyPageSource *pageSource, const MetadataInputStruct &input, VerifyContext *context)
    : m_pageSource(pageSource), m_input(input), m_context(context)
{}

RetStatus MetadataVerifier::Verify()
{
    if (m_pageSource == nullptr || m_context == nullptr || m_context->GetReport() == nullptr) {
        return DSTORE_FAIL;
    }

    SegmentType heapSegmentType = SegmentType::HEAP_SEGMENT_TYPE;
    SegmentType indexSegmentType = SegmentType::INDEX_SEGMENT_TYPE;
    if (!ResolveExpectedSegmentTypes(&heapSegmentType, &indexSegmentType)) {
        ReportResult(VerifySeverity::ERROR_LEVEL, INVALID_PAGE_ID, "metadata_segment_type_invalid", 1, 0,
            "Failed to resolve expected segment types from metadata input");
        return DSTORE_FAIL;
    }

    if (VerifySegmentMetadata(m_input.heapSegmentPageId, heapSegmentType, "heap_segment") != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    if (m_input.lobSegmentPageId.IsValid() &&
        VerifySegmentMetadata(m_input.lobSegmentPageId, heapSegmentType, "lob_segment") != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    for (const IndexMetaEntry &entry : m_input.indexEntries) {
        if (VerifyIndexMetadata(entry) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
    }

    return m_context->GetReport()->GetRetStatus();
}

RetStatus MetadataVerifier::VerifySegmentMetadata(
    const PageId &segmentMetaPageId, SegmentType expectedSegmentType, const char *checkPrefix)
{
    if (!segmentMetaPageId.IsValid()) {
        ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPageId, "segment_missing", 1, 0,
            "%s is invalid", checkPrefix);
        return DSTORE_FAIL;
    }

    BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
    SegmentMetaPage *segmentMetaPage = m_pageSource->ReadSegmentMetaPage(segmentMetaPageId, &bufferDesc);
    if (segmentMetaPage == nullptr) {
        ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPageId, "segment_missing", 1, 0,
            "Failed to read %s metadata page (%hu,%u)", checkPrefix, segmentMetaPageId.m_fileId,
            segmentMetaPageId.m_blockId);
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_SUCC;
    TablespaceId actualTablespaceId = INVALID_TABLESPACE_ID;
    if (VerifyPage(segmentMetaPage, VerifyLevel::HEAVYWEIGHT, m_context->GetReport()) != DSTORE_SUCC) {
        ret = DSTORE_FAIL;
        goto out;
    }

    if (segmentMetaPage->segmentHeader.segmentType != expectedSegmentType) {
        ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPageId, "segment_type_mismatch",
            static_cast<uint64>(expectedSegmentType), static_cast<uint64>(segmentMetaPage->segmentHeader.segmentType),
            "%s type %u mismatches expected type %u", checkPrefix,
            static_cast<uint8>(segmentMetaPage->segmentHeader.segmentType), static_cast<uint8>(expectedSegmentType));
        ret = DSTORE_FAIL;
        goto out;
    }

    if (m_input.tablespaceId != INVALID_TABLESPACE_ID &&
        m_pageSource->GetTablespaceId(segmentMetaPageId.m_fileId, &actualTablespaceId) == DSTORE_SUCC &&
        actualTablespaceId != m_input.tablespaceId) {
        ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPageId, "tablespace_mismatch", m_input.tablespaceId,
            actualTablespaceId, "%s file %hu belongs to tablespace %hu, expected %hu", checkPrefix,
            segmentMetaPageId.m_fileId, actualTablespaceId, m_input.tablespaceId);
        ret = DSTORE_FAIL;
    }

out:
    m_pageSource->ReleasePage(bufferDesc);
    return ret;
}

RetStatus MetadataVerifier::VerifyIndexMetadata(const IndexMetaEntry &indexEntry)
{
    SegmentType heapSegmentType = SegmentType::HEAP_SEGMENT_TYPE;
    SegmentType expectedIndexSegmentType = SegmentType::INDEX_SEGMENT_TYPE;
    if (!ResolveExpectedSegmentTypes(&heapSegmentType, &expectedIndexSegmentType)) {
        return DSTORE_FAIL;
    }

    if (VerifySegmentMetadata(indexEntry.segmentMetaPageId, expectedIndexSegmentType, "index_segment") != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
    BtrPage *metaPage = m_pageSource->ReadBtreeMetaPage(indexEntry.segmentMetaPageId, &bufferDesc);
    if (metaPage == nullptr) {
        ReportResult(VerifySeverity::ERROR_LEVEL, indexEntry.segmentMetaPageId, "index_meta_missing", 1, 0,
            "Failed to read btree meta page for index segment (%hu,%u)", indexEntry.segmentMetaPageId.m_fileId,
            indexEntry.segmentMetaPageId.m_blockId);
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_SUCC;
    BtrMeta *btrMeta = nullptr;
    if (VerifyPage(metaPage, VerifyLevel::HEAVYWEIGHT, m_context->GetReport()) != DSTORE_SUCC) {
        ret = DSTORE_FAIL;
        goto out;
    }

    btrMeta = static_cast<BtrMeta *>(static_cast<void *>(metaPage->GetData()));
    if (btrMeta->GetNkeyatts() != indexEntry.nKeyAtts) {
        ReportResult(VerifySeverity::ERROR_LEVEL, metaPage->GetSelfPageId(), "index_key_attr_count_mismatch",
            indexEntry.nKeyAtts, btrMeta->GetNkeyatts(),
            "Btree meta key attr count %hu mismatches metadata input %hu", btrMeta->GetNkeyatts(),
            indexEntry.nKeyAtts);
        ret = DSTORE_FAIL;
        goto out;
    }

    if (indexEntry.attTypeIds.size() != indexEntry.nKeyAtts) {
        ReportResult(VerifySeverity::ERROR_LEVEL, metaPage->GetSelfPageId(), "index_attr_type_count_mismatch",
            indexEntry.nKeyAtts, indexEntry.attTypeIds.size(),
            "Metadata input provides %lu attribute types for %hu key attributes",
            indexEntry.attTypeIds.size(), indexEntry.nKeyAtts);
        ret = DSTORE_FAIL;
        goto out;
    }

    for (uint16 i = 0; i < indexEntry.nKeyAtts; ++i) {
        if (btrMeta->GetAttTypids(i) != indexEntry.attTypeIds[i]) {
            ReportResult(VerifySeverity::ERROR_LEVEL, metaPage->GetSelfPageId(), "index_attr_type_mismatch",
                indexEntry.attTypeIds[i], btrMeta->GetAttTypids(i),
                "Btree meta attribute type at key %hu mismatches metadata input", i);
            ret = DSTORE_FAIL;
            goto out;
        }
    }

out:
    m_pageSource->ReleasePage(bufferDesc);
    return ret;
}

bool MetadataVerifier::ResolveExpectedSegmentTypes(SegmentType *heapSegmentType, SegmentType *indexSegmentType) const
{
    if (heapSegmentType == nullptr || indexSegmentType == nullptr) {
        return false;
    }

    if (m_input.isTempRelation) {
        *heapSegmentType = SegmentType::HEAP_TEMP_SEGMENT_TYPE;
        *indexSegmentType = SegmentType::INDEX_TEMP_SEGMENT_TYPE;
    } else {
        *heapSegmentType = SegmentType::HEAP_SEGMENT_TYPE;
        *indexSegmentType = SegmentType::INDEX_SEGMENT_TYPE;
    }
    return true;
}

void MetadataVerifier::ReportResult(VerifySeverity severity, const PageId &pageId, const char *checkName,
    uint64 expected, uint64 actual, const char *format, ...)
{
    if (m_context == nullptr || m_context->GetReport() == nullptr) {
        return;
    }

    char message[256] = {0};
    va_list args;
    va_start(args, format);
    vsnprintf(message, sizeof(message), format, args);
    va_end(args);

    m_context->GetReport()->AddResult(severity, METADATA_VERIFY_TARGET, pageId, checkName, expected, actual, "%s",
        message);
}

RetStatus VerifyMetadataConsistency(BufMgrInterface *bufMgr, const MetadataInputStruct &input, VerifyReport *report)
{
    if (bufMgr == nullptr || report == nullptr) {
        return DSTORE_FAIL;
    }

    BufferMetadataVerifyPageSource pageSource(bufMgr, input.pdbId);
    VerifyContext context(report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);
    return verifier.Verify();
}

}  // namespace DSTORE
