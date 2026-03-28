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

#ifndef SYSBENCH_SERVER_H
#define SYSBENCH_SERVER_H

#include "framework/dstore_instance_interface.h"
#include "common/dstore_common_utils.h"

namespace DSTORE {

class SysbenchStorageInstance : public StorageInstanceInterface {
public:
    /* Create data directory, init VFS, bootstrap storage engine */
    static void Init(const char *dataDirBase);
    /* Flush pages and tear down the bootstrap instance */
    static void InitFinished();
    /* Startup storage engine from existing data directory */
    static void Start(Oid allocMaxRelOid, const char *dataDir);
    /* Flush and destroy storage engine */
    static void Stop();

    void InitWorkingVersionNum(const uint32_t *workingGrandVersionNum) {}
    uint32_t GetWorkingVersionNum() { return 0; }
};

} /* namespace DSTORE */

constexpr uint32_t SYSBENCH_GRAND_VERSION_NUM = 97039;

#endif /* SYSBENCH_SERVER_H */
