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

#include <cassert>
#include <cstring>
#include <iostream>
#include <unistd.h>

#include "common/dstore_datatype.h"
#include "config/dstore_vfs_config.h"
#include "securec.h"
#include "sysbench_client.h"
#include "sysbench_server.h"
#include "table_handler.h"

int main(int argc, char *argv[])
{
    (void)argc;
    (void)argv;

    /* Step 1: Get current working directory as base for data directory */
    char utTopDir[VFS_FILE_PATH_MAX_LEN] = {0};
    getcwd(utTopDir, VFS_FILE_PATH_MAX_LEN);

    char dataDir[VFS_FILE_PATH_MAX_LEN] = {0};
    __attribute__((__unused__)) int rc =
        sprintf_s(dataDir, VFS_FILE_PATH_MAX_LEN, "%s/sysbenchdir", utTopDir);
    storage_securec_check_ss(rc);

    /* Step 2: Load config to determine command */
    SYSBENCH::SysbenchStorage storage;
    storage.Init(1);
    SYSBENCH::SysbenchConfig cfg = storage.GetConfig();

    /* Step 3: Bootstrap or start storage engine */
    uint32_t allocedMaxRelOid = 1;

    if (cfg.command == SYSBENCH::CMD_PREPARE || cfg.command == SYSBENCH::CMD_ALL) {
        /* Prepare needs a fresh initdb: bootstrap, then transition to normal mode */
        DSTORE::SysbenchStorageInstance::Init(utTopDir);
        DSTORE::SysbenchStorageInstance::InitFinished();
        chdir(dataDir);
        DSTORE::InitVfsClientHandles();
        DSTORE::SysbenchStorageInstance::Start(allocedMaxRelOid, dataDir);
    } else {
        /* Run / Cleanup reuse existing data directory */
        chdir(dataDir);
        DSTORE::InitVfsClientHandles();
        DSTORE::SysbenchStorageInstance::Start(1, dataDir);
    }

    /* Step 4: Dispatch to the appropriate phase(s) */
    switch (cfg.command) {
        case SYSBENCH::CMD_PREPARE:
            storage.CreateTables(&allocedMaxRelOid);
            storage.LoadData();
            storage.CreateIndexes(&allocedMaxRelOid);
            break;

        case SYSBENCH::CMD_RUN:
            storage.Execute();
            break;

        case SYSBENCH::CMD_CLEANUP:
            storage.DropTables();
            break;

        case SYSBENCH::CMD_ALL:
            storage.CreateTables(&allocedMaxRelOid);
            storage.LoadData();
            storage.CreateIndexes(&allocedMaxRelOid);
            storage.Execute();
            storage.DropTables();
            break;

        default:
            std::cout << "Unknown command" << std::endl;
            break;
    }

    /* Step 5: Shutdown */
    DSTORE::SysbenchStorageInstance::Stop();

    std::cout << "sysbenchtest done." << std::endl;
    return 0;
}
