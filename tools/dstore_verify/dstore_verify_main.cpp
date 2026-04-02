#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>

#include "cjson/cJSON.h"
#include "dfx/dstore_metadata_verify.h"
#include "dfx/dstore_page_verify.h"
#include "dfx/dstore_segment_verify.h"
#include "tablespace/dstore_tablespace_internal.h"

namespace DSTORE {

namespace {

enum class CliFormat : uint8 {
    TEXT = 0,
    JSON
};

enum class CliExitCode : int {
    SUCCESS = 0,
    ERROR_FOUND = 1,
    WARNING_FOUND = 2,
    EXECUTION_ERROR = 3
};

struct CliOptions {
    std::string dataDir;
    std::string metadataFile;
    PageId pageId{INVALID_PAGE_ID};
    PageId segmentId{INVALID_PAGE_ID};
    VerifyLevel level{VerifyLevel::HEAVYWEIGHT};
    CliFormat format{CliFormat::TEXT};
    float sampleRatio{1.0F};
    uint32 maxErrors{1000};
    Oid tableOid{DSTORE_INVALID_OID};
    bool hasPage{false};
    bool hasSegment{false};
    bool hasTable{false};
    bool checkBtree{false};
    bool checkHeap{false};
    bool checkSegment{false};
    bool checkExtent{false};
    bool runAll{false};
};

class OfflinePageReader {
public:
    explicit OfflinePageReader(std::string dataDir) : m_dataDir(std::move(dataDir)) {}

    ~OfflinePageReader()
    {
        for (const auto &entry : m_fdCache) {
            if (entry.second >= 0) {
                close(entry.second);
            }
        }
    }

    const Page *ReadPage(const PageId &pageId, std::string *errorMessage = nullptr)
    {
        if (!pageId.IsValid()) {
            SetError(errorMessage, "PageId is invalid");
            return nullptr;
        }

        const uint64 cacheKey = PageIdToUint64(pageId);
        auto cacheIt = m_pageCache.find(cacheKey);
        if (cacheIt != m_pageCache.end()) {
            return reinterpret_cast<const Page *>(cacheIt->second.data());
        }

        int fd = OpenFile(pageId.m_fileId, errorMessage);
        if (fd < 0) {
            return nullptr;
        }

        std::vector<char> pageData(BLCKSZ, 0);
        const off_t offset = static_cast<off_t>(pageId.m_blockId) * BLCKSZ;
        ssize_t bytesRead = pread(fd, pageData.data(), BLCKSZ, offset);
        if (bytesRead != BLCKSZ) {
            SetError(errorMessage, "Failed to read page (" + PageIdToString(pageId) + "), bytesRead=" +
                std::to_string(bytesRead) + ", errno=" + std::to_string(errno));
            return nullptr;
        }

        auto inserted = m_pageCache.emplace(cacheKey, std::move(pageData));
        return reinterpret_cast<const Page *>(inserted.first->second.data());
    }

private:
    static uint64 PageIdToUint64(const PageId &pageId)
    {
        return (static_cast<uint64>(pageId.m_fileId) << 32) | pageId.m_blockId;
    }

    static std::string PageIdToString(const PageId &pageId)
    {
        char buf[64] = {0};
        std::snprintf(buf, sizeof(buf), "%hu:%u", pageId.m_fileId, pageId.m_blockId);
        return std::string(buf);
    }

    static void SetError(std::string *errorMessage, const std::string &message)
    {
        if (errorMessage != nullptr) {
            *errorMessage = message;
        }
    }

    int OpenFile(FileId fileId, std::string *errorMessage)
    {
        auto fdIt = m_fdCache.find(fileId);
        if (fdIt != m_fdCache.end()) {
            return fdIt->second;
        }

        std::string filePath = m_dataDir;
        if (!filePath.empty() && filePath.back() != '/') {
            filePath.push_back('/');
        }
        filePath += std::to_string(fileId);

        int fd = open(filePath.c_str(), O_RDONLY);
        if (fd < 0) {
            SetError(errorMessage, "Failed to open data file " + filePath + ", errno=" + std::to_string(errno));
            return -1;
        }

        m_fdCache.emplace(fileId, fd);
        return fd;
    }

    std::string m_dataDir;
    std::unordered_map<FileId, int> m_fdCache;
    std::unordered_map<uint64, std::vector<char>> m_pageCache;
};

class OfflineSegmentVerifyPageSource : public SegmentVerifyPageSource {
public:
    explicit OfflineSegmentVerifyPageSource(OfflinePageReader *reader) : m_reader(reader) {}

    SegmentMetaPage *ReadSegmentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return reinterpret_cast<SegmentMetaPage *>(const_cast<Page *>(ReadPageInternal(pageId, bufferDesc)));
    }

    SegExtentMetaPage *ReadExtentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return reinterpret_cast<SegExtentMetaPage *>(const_cast<Page *>(ReadPageInternal(pageId, bufferDesc)));
    }

    Page *ReadPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return const_cast<Page *>(ReadPageInternal(pageId, bufferDesc));
    }

    TbsBitmapMetaPage *ReadBitmapMetaPage(FileId fileId, BufferDesc **bufferDesc) override
    {
        const PageId bitmapMetaPageId{fileId, TBS_BITMAP_META_PAGE};
        return reinterpret_cast<TbsBitmapMetaPage *>(
            const_cast<Page *>(ReadPageInternal(bitmapMetaPageId, bufferDesc)));
    }

    TbsBitmapPage *ReadBitmapPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return reinterpret_cast<TbsBitmapPage *>(const_cast<Page *>(ReadPageInternal(pageId, bufferDesc)));
    }

    bool GetIndexRootInfo(const PageId &segmentMetaPageId, PageId *rootPageId, uint32 *rootLevel) override
    {
        if (rootPageId == nullptr || rootLevel == nullptr) {
            return false;
        }

        BufferDesc *bufferDesc = nullptr;
        BtrPage *metaPage = reinterpret_cast<BtrPage *>(
            const_cast<Page *>(ReadPageInternal({segmentMetaPageId.m_fileId,
                static_cast<BlockNumber>(segmentMetaPageId.m_blockId + 1)}, &bufferDesc)));
        if (metaPage == nullptr) {
            return false;
        }

        BtrMeta *meta = static_cast<BtrMeta *>(static_cast<void *>(metaPage->GetData()));
        *rootPageId = meta->GetRootPageId();
        *rootLevel = meta->GetRootLevel();
        return rootPageId->IsValid();
    }

    void ReleasePage(BufferDesc *bufferDesc) override
    {
        (void)bufferDesc;
    }

    const std::string &GetLastError() const
    {
        return m_lastError;
    }

private:
    const Page *ReadPageInternal(const PageId &pageId, BufferDesc **bufferDesc)
    {
        if (bufferDesc != nullptr) {
            *bufferDesc = nullptr;
        }
        if (m_reader == nullptr) {
            m_lastError = "Offline page reader is null";
            return nullptr;
        }
        return m_reader->ReadPage(pageId, &m_lastError);
    }

    OfflinePageReader *m_reader{nullptr};
    std::string m_lastError;
};

class OfflineMetadataVerifyPageSource : public MetadataVerifyPageSource {
public:
    OfflineMetadataVerifyPageSource(OfflinePageReader *reader, std::unordered_map<FileId, TablespaceId> fileMap)
        : m_reader(reader), m_fileTablespace(std::move(fileMap))
    {}

    SegmentMetaPage *ReadSegmentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return reinterpret_cast<SegmentMetaPage *>(const_cast<Page *>(ReadPageInternal(pageId, bufferDesc)));
    }

    BtrPage *ReadBtreeMetaPage(const PageId &segmentMetaPageId, BufferDesc **bufferDesc) override
    {
        return reinterpret_cast<BtrPage *>(const_cast<Page *>(ReadPageInternal(
            {segmentMetaPageId.m_fileId, static_cast<BlockNumber>(segmentMetaPageId.m_blockId + 1)}, bufferDesc)));
    }

    RetStatus GetTablespaceId(FileId fileId, TablespaceId *tablespaceId) override
    {
        if (tablespaceId == nullptr) {
            return DSTORE_FAIL;
        }
        auto it = m_fileTablespace.find(fileId);
        if (it == m_fileTablespace.end()) {
            return DSTORE_FAIL;
        }
        *tablespaceId = it->second;
        return DSTORE_SUCC;
    }

    void ReleasePage(BufferDesc *bufferDesc) override
    {
        (void)bufferDesc;
    }

    const std::string &GetLastError() const
    {
        return m_lastError;
    }

private:
    const Page *ReadPageInternal(const PageId &pageId, BufferDesc **bufferDesc)
    {
        if (bufferDesc != nullptr) {
            *bufferDesc = nullptr;
        }
        if (m_reader == nullptr) {
            m_lastError = "Offline page reader is null";
            return nullptr;
        }
        return m_reader->ReadPage(pageId, &m_lastError);
    }

    OfflinePageReader *m_reader{nullptr};
    std::unordered_map<FileId, TablespaceId> m_fileTablespace;
    std::string m_lastError;
};

void PrintUsage()
{
    std::fputs(
        "Usage: dstore_verify [OPTIONS] <datadir>\n"
        "\n"
        "Options:\n"
        "  --table <oid>           Verify a table by OID (not supported in offline mode yet)\n"
        "  --segment <file:block>  Verify a segment meta page\n"
        "  --page <file:block>     Verify a single page\n"
        "  --level <lw|hw>         Verification level (default hw)\n"
        "  --check-btree           Request btree verification (not supported in offline mode yet)\n"
        "  --check-heap            Request heap verification (not supported in offline mode yet)\n"
        "  --check-segment         Run segment metadata verification\n"
        "  --check-extent          Run extent and bitmap consistency verification\n"
        "  --sample-ratio <0-100>  Sampling ratio for data consistency checks\n"
        "  --max-errors <N>        Maximum error count before stop\n"
        "  --format <text|json>    Output format (default text)\n"
        "  --all                   Run all available offline checks for the chosen target\n"
        "  --metadata-file <path>  Metadata JSON input for metadata consistency verification\n",
        stdout);
}

bool ParseUnsigned(const char *input, uint64 *value)
{
    if (input == nullptr || value == nullptr || *input == '\0') {
        return false;
    }

    char *endPtr = nullptr;
    errno = 0;
    unsigned long long parsed = std::strtoull(input, &endPtr, 10);
    if (errno != 0 || endPtr == input || *endPtr != '\0') {
        return false;
    }

    *value = static_cast<uint64>(parsed);
    return true;
}

bool ParsePageId(const char *input, PageId *pageId)
{
    if (input == nullptr || pageId == nullptr) {
        return false;
    }

    const char *colon = std::strchr(input, ':');
    if (colon == nullptr) {
        return false;
    }

    std::string filePart(input, static_cast<size_t>(colon - input));
    std::string blockPart(colon + 1);
    uint64 fileId = 0;
    uint64 blockId = 0;
    if (!ParseUnsigned(filePart.c_str(), &fileId) || !ParseUnsigned(blockPart.c_str(), &blockId)) {
        return false;
    }

    *pageId = {static_cast<FileId>(fileId), static_cast<BlockNumber>(blockId)};
    return pageId->IsValid();
}

bool ParseFormat(const char *input, CliFormat *format)
{
    if (input == nullptr || format == nullptr) {
        return false;
    }
    if (std::strcmp(input, "text") == 0) {
        *format = CliFormat::TEXT;
        return true;
    }
    if (std::strcmp(input, "json") == 0) {
        *format = CliFormat::JSON;
        return true;
    }
    return false;
}

bool ParseLevel(const char *input, VerifyLevel *level)
{
    if (input == nullptr || level == nullptr) {
        return false;
    }
    if (std::strcmp(input, "lw") == 0) {
        *level = VerifyLevel::LIGHTWEIGHT;
        return true;
    }
    if (std::strcmp(input, "hw") == 0) {
        *level = VerifyLevel::HEAVYWEIGHT;
        return true;
    }
    return false;
}

bool ParseFloatPercent(const char *input, float *value)
{
    if (input == nullptr || value == nullptr) {
        return false;
    }

    char *endPtr = nullptr;
    errno = 0;
    float parsed = std::strtof(input, &endPtr);
    if (errno != 0 || endPtr == input || *endPtr != '\0' || parsed < 0.0F || parsed > 100.0F) {
        return false;
    }
    *value = parsed / 100.0F;
    return true;
}

bool ParseCliArgs(int argc, char **argv, CliOptions *options, std::string *errorMessage)
{
    if (options == nullptr) {
        return false;
    }

    for (int i = 1; i < argc; ++i) {
        const char *arg = argv[i];
        auto requireValue = [&](const char *name) -> const char * {
            if (i + 1 >= argc) {
                if (errorMessage != nullptr) {
                    *errorMessage = std::string("Missing value for ") + name;
                }
                return nullptr;
            }
            return argv[++i];
        };

        if (std::strcmp(arg, "--table") == 0) {
            const char *value = requireValue(arg);
            uint64 parsed = 0;
            if (value == nullptr || !ParseUnsigned(value, &parsed)) {
                if (errorMessage != nullptr && errorMessage->empty()) {
                    *errorMessage = "Invalid table oid";
                }
                return false;
            }
            options->tableOid = static_cast<Oid>(parsed);
            options->hasTable = true;
        } else if (std::strcmp(arg, "--segment") == 0) {
            const char *value = requireValue(arg);
            if (value == nullptr || !ParsePageId(value, &options->segmentId)) {
                if (errorMessage != nullptr && errorMessage->empty()) {
                    *errorMessage = "Invalid segment page id";
                }
                return false;
            }
            options->hasSegment = true;
        } else if (std::strcmp(arg, "--page") == 0) {
            const char *value = requireValue(arg);
            if (value == nullptr || !ParsePageId(value, &options->pageId)) {
                if (errorMessage != nullptr && errorMessage->empty()) {
                    *errorMessage = "Invalid page id";
                }
                return false;
            }
            options->hasPage = true;
        } else if (std::strcmp(arg, "--level") == 0) {
            const char *value = requireValue(arg);
            if (value == nullptr || !ParseLevel(value, &options->level)) {
                if (errorMessage != nullptr && errorMessage->empty()) {
                    *errorMessage = "Invalid verification level";
                }
                return false;
            }
        } else if (std::strcmp(arg, "--check-btree") == 0) {
            options->checkBtree = true;
        } else if (std::strcmp(arg, "--check-heap") == 0) {
            options->checkHeap = true;
        } else if (std::strcmp(arg, "--check-segment") == 0) {
            options->checkSegment = true;
        } else if (std::strcmp(arg, "--check-extent") == 0) {
            options->checkExtent = true;
        } else if (std::strcmp(arg, "--sample-ratio") == 0) {
            const char *value = requireValue(arg);
            if (value == nullptr || !ParseFloatPercent(value, &options->sampleRatio)) {
                if (errorMessage != nullptr && errorMessage->empty()) {
                    *errorMessage = "Invalid sample ratio";
                }
                return false;
            }
        } else if (std::strcmp(arg, "--max-errors") == 0) {
            const char *value = requireValue(arg);
            uint64 parsed = 0;
            if (value == nullptr || !ParseUnsigned(value, &parsed)) {
                if (errorMessage != nullptr && errorMessage->empty()) {
                    *errorMessage = "Invalid max-errors";
                }
                return false;
            }
            options->maxErrors = static_cast<uint32>(parsed);
        } else if (std::strcmp(arg, "--format") == 0) {
            const char *value = requireValue(arg);
            if (value == nullptr || !ParseFormat(value, &options->format)) {
                if (errorMessage != nullptr && errorMessage->empty()) {
                    *errorMessage = "Invalid format";
                }
                return false;
            }
        } else if (std::strcmp(arg, "--all") == 0) {
            options->runAll = true;
        } else if (std::strcmp(arg, "--metadata-file") == 0) {
            const char *value = requireValue(arg);
            if (value == nullptr) {
                return false;
            }
            options->metadataFile = value;
        } else if (std::strcmp(arg, "--help") == 0 || std::strcmp(arg, "-h") == 0) {
            PrintUsage();
            std::exit(static_cast<int>(CliExitCode::SUCCESS));
        } else if (arg[0] == '-') {
            if (errorMessage != nullptr) {
                *errorMessage = std::string("Unknown option: ") + arg;
            }
            return false;
        } else if (options->dataDir.empty()) {
            options->dataDir = arg;
        } else {
            if (errorMessage != nullptr) {
                *errorMessage = "Only one datadir positional argument is allowed";
            }
            return false;
        }
    }

    if (options->dataDir.empty()) {
        if (errorMessage != nullptr) {
            *errorMessage = "Datadir is required";
        }
        return false;
    }

    return true;
}

bool ReadFileContents(const std::string &path, std::string *content)
{
    if (content == nullptr) {
        return false;
    }
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return false;
    }

    std::string data;
    char buffer[4096];
    while (true) {
        ssize_t bytesRead = read(fd, buffer, sizeof(buffer));
        if (bytesRead < 0) {
            close(fd);
            return false;
        }
        if (bytesRead == 0) {
            break;
        }
        data.append(buffer, static_cast<size_t>(bytesRead));
    }
    close(fd);
    *content = std::move(data);
    return true;
}

bool ParseJsonPageId(cJSON *node, PageId *pageId)
{
    if (node == nullptr || pageId == nullptr || !cJSON_IsString(node) || node->valuestring == nullptr) {
        return false;
    }
    return ParsePageId(node->valuestring, pageId);
}

bool ParseMetadataFile(const std::string &path, MetadataInputStruct *input,
    std::unordered_map<FileId, TablespaceId> *fileTablespace, std::string *errorMessage)
{
    if (input == nullptr || fileTablespace == nullptr) {
        return false;
    }

    std::string jsonText;
    if (!ReadFileContents(path, &jsonText)) {
        if (errorMessage != nullptr) {
            *errorMessage = "Failed to read metadata file: " + path;
        }
        return false;
    }

    cJSON *root = cJSON_Parse(jsonText.c_str());
    if (root == nullptr) {
        if (errorMessage != nullptr) {
            *errorMessage = "Failed to parse metadata json";
        }
        return false;
    }

    auto fail = [&](const char *message) {
        if (errorMessage != nullptr) {
            *errorMessage = message;
        }
        cJSON_Delete(root);
        return false;
    };

    cJSON *heapSegment = cJSON_GetObjectItemCaseSensitive(root, "heapSegmentPageId");
    if (!ParseJsonPageId(heapSegment, &input->heapSegmentPageId)) {
        return fail("metadata.heapSegmentPageId is invalid");
    }

    cJSON *lobSegment = cJSON_GetObjectItemCaseSensitive(root, "lobSegmentPageId");
    if (lobSegment != nullptr && !cJSON_IsNull(lobSegment) && !ParseJsonPageId(lobSegment, &input->lobSegmentPageId)) {
        return fail("metadata.lobSegmentPageId is invalid");
    }

    cJSON *pdbId = cJSON_GetObjectItemCaseSensitive(root, "pdbId");
    if (cJSON_IsNumber(pdbId)) {
        input->pdbId = static_cast<PdbId>(pdbId->valuedouble);
    }

    cJSON *tablespaceId = cJSON_GetObjectItemCaseSensitive(root, "tablespaceId");
    if (cJSON_IsNumber(tablespaceId)) {
        input->tablespaceId = static_cast<TablespaceId>(tablespaceId->valuedouble);
    }

    cJSON *isTemp = cJSON_GetObjectItemCaseSensitive(root, "isTempRelation");
    input->isTempRelation = cJSON_IsTrue(isTemp);

    cJSON *maxErrors = cJSON_GetObjectItemCaseSensitive(root, "maxErrors");
    if (cJSON_IsNumber(maxErrors)) {
        input->maxErrors = static_cast<uint32>(maxErrors->valuedouble);
    }

    cJSON *fileMap = cJSON_GetObjectItemCaseSensitive(root, "fileTablespace");
    if (fileMap != nullptr && cJSON_IsObject(fileMap)) {
        cJSON *entry = nullptr;
        cJSON_ArrayForEach(entry, fileMap)
        {
            uint64 fileId = 0;
            if (!ParseUnsigned(entry->string, &fileId) || !cJSON_IsNumber(entry)) {
                return fail("metadata.fileTablespace contains invalid entry");
            }
            (*fileTablespace)[static_cast<FileId>(fileId)] = static_cast<TablespaceId>(entry->valuedouble);
        }
    }

    cJSON *indexes = cJSON_GetObjectItemCaseSensitive(root, "indexEntries");
    if (indexes != nullptr && cJSON_IsArray(indexes)) {
        cJSON *indexEntry = nullptr;
        cJSON_ArrayForEach(indexEntry, indexes)
        {
            IndexMetaEntry entry;
            if (!ParseJsonPageId(cJSON_GetObjectItemCaseSensitive(indexEntry, "segmentMetaPageId"), &entry.segmentMetaPageId)) {
                return fail("metadata.indexEntries[].segmentMetaPageId is invalid");
            }

            cJSON *nKeyAtts = cJSON_GetObjectItemCaseSensitive(indexEntry, "nKeyAtts");
            if (!cJSON_IsNumber(nKeyAtts)) {
                return fail("metadata.indexEntries[].nKeyAtts is invalid");
            }
            entry.nKeyAtts = static_cast<uint16>(nKeyAtts->valuedouble);

            cJSON *attTypes = cJSON_GetObjectItemCaseSensitive(indexEntry, "attTypeIds");
            if (!cJSON_IsArray(attTypes)) {
                return fail("metadata.indexEntries[].attTypeIds is invalid");
            }
            cJSON *attType = nullptr;
            cJSON_ArrayForEach(attType, attTypes)
            {
                if (!cJSON_IsNumber(attType)) {
                    return fail("metadata.indexEntries[].attTypeIds[] is invalid");
                }
                entry.attTypeIds.push_back(static_cast<Oid>(attType->valuedouble));
            }

            input->indexEntries.push_back(entry);
        }
    }

    cJSON_Delete(root);
    return true;
}

int ReportExitCode(const VerifyReport &report, RetStatus status)
{
    if (status != DSTORE_SUCC || report.HasError()) {
        return static_cast<int>(CliExitCode::ERROR_FOUND);
    }
    if (report.GetWarningCount() > 0) {
        return static_cast<int>(CliExitCode::WARNING_FOUND);
    }
    return static_cast<int>(CliExitCode::SUCCESS);
}

void PrintReport(const VerifyReport &report, CliFormat format)
{
    const std::string output = (format == CliFormat::JSON) ? report.FormatJson() : report.FormatText();
    std::fputs(output.c_str(), stdout);
    std::fputc('\n', stdout);
}

int RunPageVerification(const CliOptions &options, VerifyReport *report)
{
    OfflinePageReader reader(options.dataDir);
    std::string errorMessage;
    const Page *page = reader.ReadPage(options.pageId, &errorMessage);
    if (page == nullptr) {
        std::fprintf(stderr, "%s\n", errorMessage.c_str());
        return static_cast<int>(CliExitCode::EXECUTION_ERROR);
    }

    RetStatus status = VerifyPage(page, options.level, report);
    PrintReport(*report, options.format);
    return ReportExitCode(*report, status);
}

int RunSegmentVerification(const CliOptions &options, VerifyReport *report)
{
    OfflinePageReader reader(options.dataDir);
    OfflineSegmentVerifyPageSource pageSource(&reader);
    SegmentVerifyOptions segmentOptions;
    segmentOptions.checkExtentChain = true;
    segmentOptions.checkExtentBitmap = options.runAll || options.checkExtent || options.checkSegment;
    segmentOptions.checkPageCounts = true;
    segmentOptions.maxErrors = options.maxErrors;

    VerifyContext context(report, nullptr, 1.0F, false, segmentOptions.maxErrors);
    SegmentVerifier verifier(&pageSource, options.segmentId, segmentOptions, &context);
    RetStatus status = verifier.Verify();
    if (status != DSTORE_SUCC && !pageSource.GetLastError().empty()) {
        std::fprintf(stderr, "%s\n", pageSource.GetLastError().c_str());
    }
    PrintReport(*report, options.format);
    return ReportExitCode(*report, status);
}

int RunMetadataVerification(const CliOptions &options, VerifyReport *report)
{
    MetadataInputStruct input;
    std::unordered_map<FileId, TablespaceId> fileTablespace;
    std::string errorMessage;
    if (!ParseMetadataFile(options.metadataFile, &input, &fileTablespace, &errorMessage)) {
        std::fprintf(stderr, "%s\n", errorMessage.c_str());
        return static_cast<int>(CliExitCode::EXECUTION_ERROR);
    }

    input.maxErrors = options.maxErrors;

    OfflinePageReader reader(options.dataDir);
    OfflineMetadataVerifyPageSource pageSource(&reader, std::move(fileTablespace));
    VerifyContext context(report, nullptr, 1.0F, false, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);
    RetStatus status = verifier.Verify();
    if (status != DSTORE_SUCC && !pageSource.GetLastError().empty()) {
        std::fprintf(stderr, "%s\n", pageSource.GetLastError().c_str());
    }
    PrintReport(*report, options.format);
    return ReportExitCode(*report, status);
}

}  // namespace

int DstoreVerifyMain(int argc, char **argv)
{
    CliOptions options;
    std::string errorMessage;
    if (!ParseCliArgs(argc, argv, &options, &errorMessage)) {
        if (!errorMessage.empty()) {
            std::fprintf(stderr, "%s\n", errorMessage.c_str());
        }
        PrintUsage();
        return static_cast<int>(CliExitCode::EXECUTION_ERROR);
    }

    if (options.runAll && options.hasTable) {
        std::fprintf(stderr, "Offline table verification is not implemented yet.\n");
        return static_cast<int>(CliExitCode::EXECUTION_ERROR);
    }
    if (options.hasTable || options.checkHeap || options.checkBtree) {
        std::fprintf(stderr,
            "Requested verification requires online relation/catalog resolution and is not supported by the offline CLI yet.\n");
        return static_cast<int>(CliExitCode::EXECUTION_ERROR);
    }

    VerifyReport report;
    if (options.hasPage) {
        return RunPageVerification(options, &report);
    }
    if (options.hasSegment || options.checkSegment || options.checkExtent) {
        if (!options.hasSegment) {
            std::fprintf(stderr, "--segment <file:block> is required for segment verification in offline mode.\n");
            return static_cast<int>(CliExitCode::EXECUTION_ERROR);
        }
        return RunSegmentVerification(options, &report);
    }
    if (!options.metadataFile.empty()) {
        return RunMetadataVerification(options, &report);
    }

    std::fprintf(stderr, "No actionable verification target was provided.\n");
    PrintUsage();
    return static_cast<int>(CliExitCode::EXECUTION_ERROR);
}

}  // namespace DSTORE

int main(int argc, char **argv)
{
    return DSTORE::DstoreVerifyMain(argc, argv);
}
