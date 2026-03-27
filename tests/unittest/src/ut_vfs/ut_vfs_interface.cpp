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
class VfsInterfaceTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        ASSERT_EQ(::GetStaticLocalVfsInstance(&m_testStaticVfs), 0);
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    ::VirtualFileSystem *m_testStaticVfs;
};

TEST_F(VfsInterfaceTest, OpenCloseVfsTest) 
{
    TenantConfig* tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    ASSERT_EQ(VfsInterface::OpenVfs(tenantConfig->tenantName,
              tenantConfig->storageConfig.template0VfsName, &m_testStaticVfs), DSTORE_SUCC);

    ASSERT_EQ(VfsInterface::CloseVfs(tenantConfig->storageConfig.template0VfsName, m_testStaticVfs), DSTORE_SUCC);
}

TEST_F(VfsInterfaceTest, SetupPageStoreTest) 
{
    ASSERT_EQ(VfsInterface::SetupPageStore(nullptr), nullptr);

    TenantConfig* tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    // current type is TENANT_ISOLATION
    ASSERT_EQ(VfsInterface::SetupPageStore(tenantConfig), nullptr);
}

TEST_F(VfsInterfaceTest, AddDrClusterIdTest) {
    static constexpr uint32 MAX_SIZE = 5;
    uint32 clusterIdArr[MAX_SIZE] = {};
    uint32 realSize = 0;

    // Case 1: Add a new unique clusterId
    EXPECT_TRUE(VfsInterface::AddDrClusterId(clusterIdArr, 1001, MAX_SIZE, &realSize));
    EXPECT_EQ(realSize, 1);
    EXPECT_EQ(clusterIdArr[0], 1001);

    // Case 2: Add the same clusterId again (should not increase realSize)
    EXPECT_TRUE(VfsInterface::AddDrClusterId(clusterIdArr, 1001, MAX_SIZE, &realSize));
    EXPECT_EQ(realSize, 1);  // Should remain unchanged

    // Case 3: Add more unique clusterIds until reaching maximum capacity
    EXPECT_TRUE(VfsInterface::AddDrClusterId(clusterIdArr, 1002, MAX_SIZE, &realSize));
    EXPECT_TRUE(VfsInterface::AddDrClusterId(clusterIdArr, 1003, MAX_SIZE, &realSize));
    EXPECT_TRUE(VfsInterface::AddDrClusterId(clusterIdArr, 1004, MAX_SIZE, &realSize));
    EXPECT_TRUE(VfsInterface::AddDrClusterId(clusterIdArr, 1005, MAX_SIZE, &realSize));
    EXPECT_EQ(realSize, 5);  // Should reach max size

    // Case 4: Attempt to add when array is full (should fail)
    EXPECT_FALSE(VfsInterface::AddDrClusterId(clusterIdArr, 1006, MAX_SIZE, &realSize));
    EXPECT_EQ(realSize, 5);  // Should not change

    // Case 5: Add a duplicate again after reaching full capacity (should be ignored, no error)
    EXPECT_FALSE(VfsInterface::AddDrClusterId(clusterIdArr, 1003, MAX_SIZE, &realSize));
    EXPECT_EQ(realSize, 5);  // Still no change
}

TEST_F(VfsInterfaceTest, GetVfsClientIdTest)
{
    uint64 adapterClientId = 0;
    EXPECT_EQ(VfsInterface::GetVfsClientId(&adapterClientId), DSTORE_SUCC);
}

TEST_F(VfsInterfaceTest, CopyAllPssConfigFileToLocalTest)
{
    VFSAdapter *localVfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    
    VFSAdapter *remoteVfs = DstoreNew(m_ut_memory_context)VFSAdapter(2);
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    remoteVfs->Initialize(tenantConfig->storageConfig.rootpdbVfsName);
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
    EXPECT_EQ(localVfs->CreateFile(101, "101", filePara), DSTORE_SUCC);
    EXPECT_EQ(localVfs->CreateFile(102, "102", filePara), DSTORE_SUCC);

    DSTORE::StoreSpaceConfig *store_space_config = VfsInterface::ChooseTheBestStoreSpaceConfig(
        tenantConfig->storeSpaces, tenantConfig->storeSpaceCnt, VfsInterface::DISK_PERF_DESC_HIGH);
    EXPECT_EQ(VfsInterface::CopyAllPssConfigFileToLocal(remoteVfs->GetStaticVfs(), "",
              store_space_config->storeSpaceName), DSTORE_FAIL);
}
