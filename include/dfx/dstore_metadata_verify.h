#ifndef DSTORE_METADATA_VERIFY_H
#define DSTORE_METADATA_VERIFY_H

#include <vector>

#include "dfx/dstore_verify_context.h"
#include "page/dstore_index_page.h"
#include "page/dstore_segment_meta_page.h"

namespace DSTORE {

class BufferDesc;
class BufMgrInterface;

struct IndexMetaEntry {
    PageId segmentMetaPageId{INVALID_PAGE_ID};
    uint16 nKeyAtts{0};
    std::vector<Oid> attTypeIds;
};

struct MetadataInputStruct {
    PdbId pdbId{INVALID_PDB_ID};
    TablespaceId tablespaceId{INVALID_TABLESPACE_ID};
    PageId heapSegmentPageId{INVALID_PAGE_ID};
    PageId lobSegmentPageId{INVALID_PAGE_ID};
    std::vector<IndexMetaEntry> indexEntries;
    bool isTempRelation{false};
    uint32 maxErrors{1000};
};

class MetadataVerifyPageSource {
public:
    virtual ~MetadataVerifyPageSource() = default;

    virtual SegmentMetaPage *ReadSegmentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) = 0;
    virtual BtrPage *ReadBtreeMetaPage(const PageId &segmentMetaPageId, BufferDesc **bufferDesc) = 0;
    virtual RetStatus GetTablespaceId(FileId fileId, TablespaceId *tablespaceId) = 0;
    virtual void ReleasePage(BufferDesc *bufferDesc) = 0;
};

class MetadataVerifier {
public:
    MetadataVerifier(MetadataVerifyPageSource *pageSource, const MetadataInputStruct &input, VerifyContext *context);

    RetStatus Verify();

private:
    RetStatus VerifySegmentMetadata(
        const PageId &segmentMetaPageId, SegmentType expectedSegmentType, const char *checkPrefix);
    RetStatus VerifyIndexMetadata(const IndexMetaEntry &indexEntry);
    bool ResolveExpectedSegmentTypes(SegmentType *heapSegmentType, SegmentType *indexSegmentType) const;
    void ReportResult(VerifySeverity severity, const PageId &pageId, const char *checkName, uint64 expected,
        uint64 actual, const char *format, ...) __attribute__((format(printf, 7, 8)));

    MetadataVerifyPageSource *m_pageSource{nullptr};
    MetadataInputStruct m_input;
    VerifyContext *m_context{nullptr};
};

RetStatus VerifyMetadataConsistency(
    BufMgrInterface *bufMgr, const MetadataInputStruct &input, VerifyReport *report);

}  // namespace DSTORE

#endif
