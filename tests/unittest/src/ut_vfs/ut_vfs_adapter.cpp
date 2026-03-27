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
 */

#include "ut_utilities/ut_dstore_framework.h"

#include <libgen.h>
#include "framework/dstore_vfs_adapter.h"
#include "framework/dstore_vfs_interface.h"
#include "framework/dstore_config_interface.h"
#include "vfs/vfs_interface.h"

#include <string>

extern char g_utDataDir[MAXPGPATH];
namespace DSTORE { struct TenantConfig; }
class VfsAdapterTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        SetDefaultPdbId(PDB_TEMPLATE1_ID);
        DSTORETEST::SetUp();

        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        m_vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        if (m_vfs->FileExists(UT_VFS_TEST_FILE_ID, UT_VFS_FILENAME)) {
            m_vfs->RemoveFile(UT_VFS_TEST_FILE_ID, UT_VFS_FILENAME);
        }

        FileParameter filePara;
        filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
        filePara.flag = IN_PLACE_WRITE_FILE;
        filePara.fileSubType = DATA_FILE_TYPE;
        filePara.rangeSize = (64 << 10);        /* 64KB */
        filePara.maxSize = (uint64) DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
        filePara.recycleTtl = 0;
        filePara.mode = FILE_READ_AND_WRITE_MODE;
        filePara.isReplayWrite = false;
        errno_t ret = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, UT_DEFAULT_STORESPACE_NAME);
        storage_securec_check(ret, "\0", "\0");

        m_vfs->CreateFile(UT_VFS_TEST_FILE_ID, UT_VFS_FILENAME, filePara);
        ASSERT_TRUE(m_vfs->FileExists(UT_VFS_TEST_FILE_ID, UT_VFS_FILENAME));

        errno_t rc = memset_s(m_fakePage, BLCKSZ, 0, BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
    }

    void TearDown() override
    {
        if (m_vfs->FileExists(UT_VFS_TEST_FILE_ID, UT_VFS_FILENAME)) {
            m_vfs->Close(UT_VFS_TEST_FILE_ID); /* Close it anyway. */
            m_vfs->RemoveFile(UT_VFS_TEST_FILE_ID, UT_VFS_FILENAME);
        }

        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    static constexpr char UT_VFS_FILENAME[] = "UtVfsAdapterFile.data";
    static constexpr FileId UT_VFS_TEST_FILE_ID = 101;
    static constexpr uint32 TEST_BUFFER_LENGTH = 64;

    VFSAdapter *m_vfs;
    char m_fakePage[BLCKSZ];
    char m_configFilePath[MAXPGPATH];
};

constexpr char VfsAdapterTest::UT_VFS_FILENAME[];


TEST_F(VfsAdapterTest, CreateVfsTest)
{
    const char *datadir = "./";
    const char *vfsName = "VFS_NAME_01";
    ASSERT_EQ(m_vfs->CreateVfs(datadir, vfsName, true), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->DestroyDataForce(vfsName, g_defaultPdbId), DSTORE_SUCC);
}

TEST_F(VfsAdapterTest, OpenCloseFileTest) 
{   
    char path[MAXPGPATH] = {0};
    snprintf(path, MAXPGPATH, "%s", "test_openclose.data");
    VFSAdapter *vfs = DstoreNew(m_ut_memory_context)VFSAdapter(2);
    auto tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    vfs->Initialize(tenantConfig->storageConfig.rootpdbVfsName);

    FileParameter filePara;
    filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    filePara.flag = IN_PLACE_WRITE_FILE;
    filePara.fileSubType = DATA_FILE_TYPE;
    filePara.rangeSize = (64 << 10);        /* 64KB */
    filePara.maxSize = (uint64) DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
    errno_t ret = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, UT_DEFAULT_STORESPACE_NAME);
    storage_securec_check(ret, "\0", "\0");

    FileId fileId = 1;
    ASSERT_EQ(vfs->CreateFile(fileId, path, filePara), DSTORE_SUCC);
    ASSERT_EQ(vfs->OpenFile(fileId, path, DSTORE_FILE_OPEN_FLAG), DSTORE_SUCC);
    ASSERT_EQ(vfs->Close(fileId), DSTORE_SUCC);
    ASSERT_EQ(vfs->OpenFile(fileId, path, DSTORE_FILE_OPEN_FLAG), DSTORE_SUCC);

    fileId = 2;
    char *filename = "test_openclose.data2";
    ASSERT_EQ(vfs->CreateFile(fileId, filename, filePara), DSTORE_SUCC);
    ASSERT_EQ(vfs->OpenFile(fileId, filename, DSTORE_FILE_OPEN_FLAG), DSTORE_SUCC);

    ASSERT_EQ(vfs->CloseAllFiles(), DSTORE_SUCC);
    vfs->Destroy(tenantConfig->storageConfig.rootpdbVfsName);
    delete vfs;
}

TEST_F(VfsAdapterTest, RemoveFileTest)
{
    FileParameter filePara;
    filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    filePara.flag = IN_PLACE_WRITE_FILE;
    filePara.fileSubType = DATA_FILE_TYPE;
    filePara.rangeSize = (64 << 10);        /* 64KB */
    filePara.maxSize = (uint64) DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
    errno_t ret = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, UT_DEFAULT_STORESPACE_NAME);
    storage_securec_check(ret, "\0", "\0");
        
    char* filename = "remove.file";
    const FileId fileId = 1;

    ASSERT_EQ(m_vfs->CreateFile(fileId, filename, filePara), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->OpenFile(fileId, filename, DSTORE_FILE_OPEN_FLAG), DSTORE_SUCC);

    ASSERT_EQ(m_vfs->Close(fileId), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->RemoveFile(fileId, filename), DSTORE_SUCC);
}

TEST_F(VfsAdapterTest, ExtendTruncateTest)
{
    const FileId fileId = UT_VFS_TEST_FILE_ID;
    const int64 extendSize = 1024;
    const int64 truncateSize = 512;

    // failed Extend case 
    const FileId notExistsFileId = 100;
    ASSERT_EQ(m_vfs->Extend(notExistsFileId, extendSize), DSTORE_FAIL);

    // successful Extend case
    ASSERT_EQ(m_vfs->Extend(fileId, extendSize), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->GetSize(fileId), extendSize);

    // failed Truncate case
    ASSERT_EQ(m_vfs->Truncate(notExistsFileId, truncateSize), DSTORE_FAIL);

    // successful Truncate case
    ASSERT_EQ(m_vfs->Truncate(fileId, truncateSize), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->GetSize(fileId), truncateSize);
}

TEST_F(VfsAdapterTest, ReadWriteSyncTest)
{
    std::string data = "May the lights be with you.";
    size_t dataLength = data.length() + 1;
    const FileId fileId = UT_VFS_TEST_FILE_ID;
    PageId pageId = {.m_fileId = fileId, .m_blockId = 0};

    snprintf(m_fakePage, dataLength, "%s", data.c_str());
    ASSERT_EQ(m_vfs->GetSize(fileId), 0);
    ASSERT_EQ(m_vfs->Extend(fileId, BLCKSZ), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->GetSize(fileId), BLCKSZ);

    RetStatus ret = m_vfs->WritePageSync(pageId, m_fakePage);
    ASSERT_EQ(ret, DSTORE_SUCC);

    ret = m_vfs->ReadPageSync(pageId, m_fakePage);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(strncmp(m_fakePage, data.c_str(), dataLength), 0);

    // failed WritePageSync case
    const FileId notExistsFileId = 100;
    pageId = {.m_fileId = notExistsFileId, .m_blockId = 0};
    ret = m_vfs->WritePageSync(pageId, m_fakePage);
    ASSERT_EQ(ret, DSTORE_FAIL);
}

TEST_F(VfsAdapterTest, AnotherAdapterTest)
{
    char path[MAXPGPATH] = {0};
    snprintf(path, MAXPGPATH, "%s", "yet_another.data");
    VFSAdapter *vfs = DstoreNew(m_ut_memory_context)VFSAdapter(2);
    auto tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    vfs->Initialize(tenantConfig->storageConfig.rootpdbVfsName);

    FileParameter filePara;
    filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    filePara.flag = IN_PLACE_WRITE_FILE;
    filePara.fileSubType = DATA_FILE_TYPE;
    filePara.rangeSize = (64 << 10);        /* 64KB */
    filePara.maxSize = (uint64) DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
    errno_t ret = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, UT_DEFAULT_STORESPACE_NAME);
    storage_securec_check(ret, "\0", "\0");

    ASSERT_EQ(vfs->CreateFile(1, path, filePara), DSTORE_SUCC);
    ASSERT_EQ(vfs->OpenFile(1, path, DSTORE_FILE_OPEN_FLAG), DSTORE_SUCC);
    vfs->Close(1);

    vfs->Destroy(tenantConfig->storageConfig.rootpdbVfsName);
    delete vfs;
}

TEST_F(VfsAdapterTest, GetTenantTest001)
{
    /* Get the start config file. */
    char startConfFilePath[MAXPGPATH];
    errno_t rc = memset_s(startConfFilePath, MAXPGPATH, 0, MAXPGPATH);
    storage_securec_check(rc, "\0", "\0");
    char *execPath4start = startConfFilePath;
    int ret = readlink("/proc/self/exe", execPath4start, MAXPGPATH);
    ASSERT_GT(ret, 0);
    char *lastSlashPtr4start = strrchr(execPath4start, '/');
    ASSERT_NE(lastSlashPtr4start, nullptr);
    ret = snprintf(lastSlashPtr4start + 1, MAXPGPATH / 2, "start_config.json");
    ASSERT_GT(ret, 0);
    ASSERT_EQ(access(startConfFilePath, F_OK), 0); /* It must exist. */
    DSTORE::TenantConfig config;
    DSTORE::RetStatus err = TenantConfigInterface::GetTenantConfig(startConfFilePath, &config);
    ASSERT_EQ(err, DSTORE_SUCC);
    ret = strncmp(config.clusterName, "rgnCluster1", VFS_NAME_MAX_LEN);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(config.tenantId, 3);
    ASSERT_EQ(config.nodeId, 1);
    ASSERT_EQ(config.storageConfig.type, StorageType::PAGESTORE);
    ret = strncmp(config.storageConfig.clientLibPath,
                  "xxx/libsalofflinefileisdata.so", FILE_PATH_MAX_LEN);
    ASSERT_EQ(ret, 0);
    ret = strncmp(config.storageConfig.serverProtocolType, "TCP_TYPE", MAXTYPELEN);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(config.communicationConfig.clusterId, 1);
    ret = strncmp(config.votingConfig.votingFilePath, "", FILE_PATH_MAX_LEN);
    ASSERT_EQ(ret, 0);
}

TEST_F(VfsAdapterTest, FileDescriptorHashTest)
{
    char testFile[MAXPGPATH] = {0};
    snprintf(testFile, MAXPGPATH, "%s", "fd_hash_1.data");
    char testFile2[MAXPGPATH] = {0};
    snprintf(testFile2, MAXPGPATH, "%s", "fd_hash_2.data");

    FileParameter filePara;
    filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    filePara.flag = IN_PLACE_WRITE_FILE;
    filePara.fileSubType = DATA_FILE_TYPE;
    filePara.rangeSize = (64 << 10);        /* 64KB */
    filePara.maxSize = (uint64) DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
    errno_t ret = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, UT_DEFAULT_STORESPACE_NAME);
    storage_securec_check(ret, "\0", "\0");

    /* Open and close test */
    FileDescriptor *fd1;
    ASSERT_EQ(m_vfs->CreateFile(testFile, filePara, &fd1), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->CloseFile(fd1), DSTORE_SUCC);

    FileDescriptor *fd2;
    ASSERT_EQ(m_vfs->OpenFile(testFile, DSTORE_FILE_OPEN_FLAG, &fd2), DSTORE_SUCC);
    FileDescriptor *fd3;
    ASSERT_EQ(m_vfs->OpenFile(testFile, DSTORE_FILE_OPEN_FLAG, &fd3), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->CloseFile(fd2), DSTORE_SUCC);

    /* Remove test */
    ASSERT_EQ(m_vfs->RemoveFile(testFile), DSTORE_FAIL);
    ASSERT_EQ(m_vfs->CloseFile(fd3), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->RemoveFile(testFile), DSTORE_SUCC);
    ASSERT_FALSE(m_vfs->FileExists(testFile));

    /* Rename test */
    ASSERT_EQ(m_vfs->CreateFile(testFile2, filePara, nullptr), DSTORE_SUCC);
    FileDescriptor *fd4;
    ASSERT_EQ(m_vfs->OpenFile(testFile2, DSTORE_FILE_OPEN_FLAG, &fd4), DSTORE_SUCC);
    ASSERT_TRUE(fd4 != nullptr);

    ASSERT_EQ(m_vfs->RenameFile(testFile2, "fd_hash_2_new.data"), DSTORE_FAIL);
    ASSERT_EQ(m_vfs->CloseFile(fd4), DSTORE_SUCC);
    ASSERT_EQ(m_vfs->RenameFile(testFile2, "fd_hash_2_new.data"), DSTORE_SUCC);
    ASSERT_FALSE(m_vfs->FileExists(testFile2));
    ASSERT_TRUE(m_vfs->FileExists("fd_hash_2_new.data"));
}