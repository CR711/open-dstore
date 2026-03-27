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
 * Description: CloudNativeDatabase UTTableOperationTest(ut table Operate test class)
 */

#ifndef DSTORE_UT_RELATION_H
#define DSTORE_UT_RELATION_H

#include "ut_utilities/ut_dstore_framework.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "ut_tablehandler/ut_table_handler.h"

#include "table_handler.h"
#include "table_data_generator.h"
#include "transaction/dstore_transaction_interface.h"
#include "common/datatype/dstore_varlena_utils.h"

#include "ut_tablehandler/ut_table.h"
#include "table/dstore_table_interface.h"


#include <gtest/gtest.h>
#include <gmock/gmock.h>



using namespace DSTORE;

class UTRelation : virtual public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context) UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context, true);

        segment = dynamic_cast<HeapNormalSegment *>(m_utTableHandler->GetHeapTabSmgr()->GetSegment());
        m_storageRelationData = new StorageRelationData();
        m_storageRelationData->tableSmgr = DstoreNew(m_ut_memory_context)
            TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);

        m_storageRelationData->lobTableSmgr = DstoreNew(m_ut_memory_context)
            TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);

        m_storageRelationData->SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));

        m_storageRelationData->btreeSmgr = DstoreNew(m_ut_memory_context)
            BtreeStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, SYS_RELPERSISTENCE_PERMANENT);

        tupleDesc = m_utTableHandler->GetHeapTupDesc();
    }

    void TearDown() override
    {
        delete m_storageRelationData;
        m_storageRelationData = nullptr;

        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;

        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    UTTableHandler *m_utTableHandler;
    StorageRelationData* m_storageRelationData;
    TableStorageMgr* m_tableSmgr;
    TableStorageMgr* m_lobTableSmgr;
    HeapNormalSegment *segment;
    UTRelation *GenerateRecyclableSubtree(int numLeaves, int rootLevel);

    SysClassTupDef* sysClassTupDef;
    TupleDesc tupleDesc;

    SysClassTupDef *BuildSysRelationTuple(const TableInfo &tableInfo, Oid tablespaceOid,
                                          PageId segmentId)
    {
        /* Step1: Allocates memory for a SysClassTupDef object. */
        SysClassTupDef *relation = static_cast<SysClassTupDef *>(DstorePalloc0(sizeof(SysClassTupDef)));
        StorageAssert(relation != nullptr);

        /* Step2: Generates data for the SysClassTupDef object. */
        errno_t rc = memcpy_s(relation->relname.data, strlen(tableInfo.relation.name), tableInfo.relation.name,
                              strlen(tableInfo.relation.name));
        storage_securec_check(rc, "\0", "\0");
        relation->relnamespace = 11;
        relation->reltablespace = tablespaceOid;
        relation->reltype = DSTORE_INVALID_OID;
        relation->reloftype = DSTORE_INVALID_OID;
        relation->relowner = 10;
        relation->relam = 0;
        relation->reltoastrelid = DSTORE_INVALID_OID;
        relation->reltoastidxid = DSTORE_INVALID_OID;

        relation->reldeltarelid = DSTORE_INVALID_OID;
        relation->reldeltaidx = DSTORE_INVALID_OID;
        relation->relcudescrelid = DSTORE_INVALID_OID;
        relation->relcudescidx = DSTORE_INVALID_OID;
        relation->relhasindex = false;
        relation->relisshared = false;
        relation->relpersistence = tableInfo.relation.persistenceLevel;
        relation->relkind = tableInfo.relation.relKind;
        relation->relnatts = static_cast<int16_t>(tableInfo.relation.attrNum);
        relation->relchecks = 0;
        relation->relhasoids = false;
        relation->relhaspkey = false;
        relation->relhasrules = false;
        relation->relhastriggers = false;
        relation->relhassubclass = false;
        relation->relcmprs = 0;
        relation->relhasclusterkey = false;
        relation->relrowmovement = false;
        relation->parttype = tableInfo.relation.partType;
        relation->relfileid = segmentId.m_fileId;
        relation->relblknum = segmentId.m_blockId;
        relation->rellobfileid = INVALID_VFS_FILE_ID;
        relation->rellobblknum = DSTORE_INVALID_BLOCK_NUMBER;
        return relation;
    }

};

#endif
