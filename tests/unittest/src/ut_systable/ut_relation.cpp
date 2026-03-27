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

#include "ut_tablespace/ut_data_segment.h"
#include "ut_systable/ut_relation.h"
#include "systable/dstore_relation.h"
#include "tablespace/dstore_heap_normal_segment.h"
#include "tablespace/dstore_heap_segment.h"
#include "index/dstore_btree.h"

/**
* @tc.name  : SetTableSmgrSegment_ShouldSetSegment_WhenSegmentIsValid
* @tc.number: SetTableSmgrSegment_Test_001
* @tc.desc  : 验证当传入有效的HeapSegment指针时，函数能正确设置tableSmgr的m_segment成员
 */
TEST_F(UTRelation, SetTableSmgrSegment_level0) {
    m_storageRelationData->SetTableSmgrSegment(segment);
}


/**
* @tc.name  : SetBtreeSmgrSegment_ShouldSetSegment_WhenSegmentIsValid
* @tc.number: SetBtreeSmgrSegment_Test_001
* @tc.desc  : 测试当传入有效的IndexSegment指针时，能够正确设置btreeSmgr的m_segment成员
 */
TEST_F(UTRelation, SetBtreeSmgrSegment_level0) {
    IndexSegment* testSegment = reinterpret_cast<IndexSegment*>(0x1234);
    m_storageRelationData->SetBtreeSmgrSegment(testSegment);

    EXPECT_EQ(m_storageRelationData->btreeSmgr->m_segment, testSegment);
}

/**
* @tc.name  : SetBtreeSmgrSegment_ShouldSetNull_WhenSegmentIsNull
* @tc.number: SetBtreeSmgrSegment_Test_002
* @tc.desc  : 测试当传入nullptr时，能够正确设置btreeSmgr的m_segment为nullptr
 */
TEST_F(UTRelation, SetBtreeSmgrSegment_isNull_level0) {
    m_storageRelationData->SetBtreeSmgrSegment(nullptr);

    EXPECT_EQ(m_storageRelationData->btreeSmgr->m_segment, nullptr);
}

/**
* @tc.name  : SetBtreeSmgrSegment_ShouldOverwriteExistingSegment_WhenCalledMultipleTimes
* @tc.number: SetBtreeSmgrSegment_Test_003
* @tc.desc  : 测试多次调用函数时，能够正确覆盖之前的m_segment值
 */
TEST_F(UTRelation, SetBtreeSmgrSegment_WhenCalledMultipleTimes_level0) {
    IndexSegment* firstSegment = reinterpret_cast<IndexSegment*>(0x1234);
    IndexSegment* secondSegment = reinterpret_cast<IndexSegment*>(0x5678);

    m_storageRelationData->SetBtreeSmgrSegment(firstSegment);
    m_storageRelationData->SetBtreeSmgrSegment(secondSegment);

    EXPECT_EQ(m_storageRelationData->btreeSmgr->m_segment, secondSegment);
}

/**
* @tc.name  : GetTableSmgrSegment_ShouldReturnSegment_WhenTableSmgrIsValid
* @tc.number: StorageRelationData_GetTableSmgrSegment_001
* @tc.desc  : 测试当tableSmgr有效时，能正确返回m_segment指针
 */
TEST_F(UTRelation, GetTableSmgrSegment_ShouldReturnSegment_WhenTableSmgrIsValid_level0) {
    SegmentInterface* result = m_storageRelationData->GetTableSmgrSegment();
    EXPECT_EQ(result, segment);
}

/**
* @tc.name  : GetTableSmgrSegment_ShouldAssertFail_WhenTableSmgrIsNull
* @tc.number: StorageRelationData_GetTableSmgrSegment_002
* @tc.desc  : 测试当tableSmgr为空时，断言失败
 */
TEST_F(UTRelation, GetTableSmgrSegment_ShouldAssertFail_WhenTableSmgrIsNull_level0) {
    StorageRelationData relationData;
    relationData.tableSmgr = nullptr;

    EXPECT_DEATH(relationData.GetTableSmgrSegment(), "");
}

/**
* @tc.name  : GetBtreeSmgrSegment_ShouldReturnCorrectSegment_WhenBtreeSmgrIsSet_level0
* @tc.number: StorageRelationData_GetBtreeSmgrSegment_001
* @tc.desc  : 测试当btreeSmgr已设置时，GetBtreeSmgrSegment应返回正确的segment指针
 */
TEST_F(UTRelation, GetBtreeSmgrSegment_ShouldReturnCorrectSegment_WhenBtreeSmgrIsSet_level0) {
    IndexSegment* firstSegment = reinterpret_cast<IndexSegment*>(0x1234);
    m_storageRelationData->SetBtreeSmgrSegment(firstSegment);
    SegmentInterface* result = m_storageRelationData->GetBtreeSmgrSegment();
    EXPECT_EQ(result, firstSegment);
}

/**
* @tc.name  : GetBtreeSmgrSegment_ShouldReturnNull_WhenBtreeSmgrIsNull_level0
* @tc.number: StorageRelationData_GetBtreeSmgrSegment_002
* @tc.desc  : 测试当btreeSmgr为null时，GetBtreeSmgrSegment应返回null
 */
TEST_F(UTRelation, GetBtreeSmgrSegment_ShouldReturnNull_WhenBtreeSmgrIsNull_level0) {
    m_storageRelationData->SetBtreeSmgrSegment(nullptr);
    SegmentInterface* result = m_storageRelationData->GetBtreeSmgrSegment();
    EXPECT_EQ(result, nullptr);
}

/**
* @tc.name  : Construct_ShouldReturnSuccess_WhenViewType
* @tc.number: StorageRelationDataTest_001
* @tc.desc  : 测试当relkind为视图类型时，构造函数应成功返回
 */
TEST_F(UTRelation, Construct_ShouldReturnSuccess_WhenViewType_level0) {
    /* Build class tuple. */
    TableInfo tableInfo = TABLE_CACHE[1];
    PageId segmentId = TableSpace_Interface::AllocSegment(g_defaultPdbId,
            static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), SegmentType::HEAP_SEGMENT_TYPE);
    TablespaceId tabelSpaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    SysClassTupDef *classTuple = BuildSysRelationTuple(tableInfo, static_cast<Oid>(tabelSpaceId), segmentId);
    /* Test Build */
    classTuple->relkind = SYS_RELKIND_VIEW;

    TablespaceId tablespaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    Oid relid = 1;
    RetStatus ret = m_storageRelationData->Construct(g_defaultPdbId, relid, classTuple, tupleDesc,
                                                     DEFAULT_HEAP_FILLFACTOR, tablespaceId, false, false);
    EXPECT_EQ(ret, DSTORE_SUCC);
}

/**
* @tc.name  : Construct_ShouldReturnSuccess_WhenIndexType_level0
* @tc.number: StorageRelationDataTest_001
* @tc.desc  : 测试当relkind为索引类型时，构造函数应成功返回
 */
TEST_F(UTRelation, Construct_ShouldReturnSuccess_WhenIndexType_level0) {

    /* Build class tuple. */
    TableInfo tableInfo = TABLE_CACHE[1];
    PageId segmentId = TableSpace_Interface::AllocSegment(g_defaultPdbId,
                                                          static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), SegmentType::HEAP_SEGMENT_TYPE);
    TablespaceId tabelSpaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    SysClassTupDef *classTuple = BuildSysRelationTuple(tableInfo, static_cast<Oid>(tabelSpaceId), segmentId);
    /* Test Build */
    classTuple->relkind = SYS_RELKIND_INDEX;
    classTuple->relfileid = segmentId.m_fileId;
    classTuple->relblknum = segmentId.m_blockId;

    segment->InitSegment();
    storage_set_error(INDEX_ERROR_FAIL_CREATE_BTREE_SMGR, segmentId.m_fileId, segmentId.m_blockId);

    Oid relid = 1;
    RetStatus ret = m_storageRelationData->Construct(g_defaultPdbId, relid, classTuple, tupleDesc,
                                                     DEFAULT_HEAP_FILLFACTOR, tabelSpaceId, false, true);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(m_storageRelationData->btreeSmgr, nullptr);
}

/**
* @tc.name  : Construct_ShouldReturnSuccess_WhenIndexType_level0
* @tc.number: StorageRelationDataTest_002
* @tc.desc  : 测试当relkind为索引类型时，构造函数应失败返回
 */
TEST_F(UTRelation, Construct_ShouldReturnFail_WhenIndexType_level0) {

    /* Build class tuple. */
    TableInfo tableInfo = TABLE_CACHE[1];
    PageId segmentId = TableSpace_Interface::AllocSegment(g_defaultPdbId,
                                                          static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), SegmentType::HEAP_SEGMENT_TYPE);
    TablespaceId tabelSpaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    SysClassTupDef *classTuple = BuildSysRelationTuple(tableInfo, static_cast<Oid>(tabelSpaceId), segmentId);
    /* Test Build */
    classTuple->relkind = SYS_RELKIND_INDEX;
    classTuple->relfileid = segmentId.m_fileId;
    classTuple->relblknum = segmentId.m_blockId;

    segment->InitSegment();

    Oid relid = 1;
    RetStatus ret = m_storageRelationData->Construct(g_defaultPdbId, relid, classTuple, tupleDesc,
                                                     DEFAULT_HEAP_FILLFACTOR, tabelSpaceId, false, false);
    EXPECT_EQ(ret, DSTORE_FAIL);
    EXPECT_EQ(m_storageRelationData->btreeSmgr, nullptr);
}

/**
* @tc.name  : Construct_ShouldReturnSuccess_WhenContviewType_level0
* @tc.number: StorageRelationDataTest_003
* @tc.desc  : 测试当relkind为序列类型时，构造函数应成功返回
 */
TEST_F(UTRelation, Construct_ShouldReturnSuccess_WhenContviewType_level0) {

    /* Build class tuple. */
    TableInfo tableInfo = TABLE_CACHE[1];
    PageId segmentId = TableSpace_Interface::AllocSegment(g_defaultPdbId,
                                                          static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), SegmentType::HEAP_SEGMENT_TYPE);
    TablespaceId tabelSpaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    SysClassTupDef *classTuple = BuildSysRelationTuple(tableInfo, static_cast<Oid>(tabelSpaceId), segmentId);
    /* Test Build */
    classTuple->relkind = SYS_RELKIND_CONTQUERY;
    classTuple->relpersistence = SYS_RELKIND_CONTQUERY;

    Oid relid = 1;
    RetStatus ret = m_storageRelationData->Construct(g_defaultPdbId, relid, classTuple, nullptr,
                                                     DEFAULT_HEAP_FILLFACTOR, tabelSpaceId, false, true);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(m_storageRelationData->btreeSmgr, nullptr);
}

/**
* @tc.name  : Construct_ShouldReturnSuccess_WhenSequenceType_level0
* @tc.number: StorageRelationDataTest_003
* @tc.desc  : 测试当relkind为序列类型时，构造函数应成功返回
 */
TEST_F(UTRelation, Construct_ShouldReturnSuccess_WhenSequenceType_level0) {

    /* Build class tuple. */
    TableInfo tableInfo = TABLE_CACHE[1];
    PageId segmentId = TableSpace_Interface::AllocSegment(g_defaultPdbId,
                                                          static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), SegmentType::HEAP_SEGMENT_TYPE);
    TablespaceId tabelSpaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    SysClassTupDef *classTuple = BuildSysRelationTuple(tableInfo, static_cast<Oid>(tabelSpaceId), segmentId);
    /* Test Build */
    classTuple->relkind = SYS_RELKIND_SEQUENCE;
    classTuple->relpersistence = SYS_RELKIND_SEQUENCE;

    Oid relid = SEQUENCE_RELATION_ID;
    RetStatus ret = m_storageRelationData->Construct(g_defaultPdbId, relid, classTuple, nullptr,
                                                     DEFAULT_HEAP_FILLFACTOR, tabelSpaceId, false, true);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(m_storageRelationData->btreeSmgr, nullptr);
}

/**
* @tc.name  : Construct_ShouldReturnSuccess_WhenSequenceIndexType_level0
* @tc.number: StorageRelationDataTest_004
* @tc.desc  : 测试当relkind为序列索引类型时，构造函数应成功返回
 */
TEST_F(UTRelation, Construct_ShouldReturnSuccess_WhenSequenceIndexType_level0) {

    /* Build class tuple. */
    TableInfo tableInfo = TABLE_CACHE[1];
    PageId segmentId = TableSpace_Interface::AllocSegment(g_defaultPdbId,
                                                          static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), SegmentType::HEAP_SEGMENT_TYPE);
    TablespaceId tabelSpaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    SysClassTupDef *classTuple = BuildSysRelationTuple(tableInfo, static_cast<Oid>(tabelSpaceId), segmentId);
    /* Test Build */
    classTuple->relkind = SYS_RELKIND_LARGE_SEQUENCE;
    classTuple->relpersistence = SYS_RELKIND_LARGE_SEQUENCE;

    Oid relid = SEQUENCE_INDEX_RELATION_ID;
    RetStatus ret = m_storageRelationData->Construct(g_defaultPdbId, relid, classTuple, nullptr,
                                                     DEFAULT_HEAP_FILLFACTOR, tabelSpaceId, false, true);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(m_storageRelationData->btreeSmgr, nullptr);
}

/**
* @tc.name  : Construct_ShouldReturnSuccess_WhenRelationType_level0
* @tc.number: StorageRelationDataTest_004
* @tc.desc  : 测试当relkind为序列索引类型时，构造函数应成功返回
 */
TEST_F(UTRelation, Construct_ShouldReturnSuccess_WhenRelationType_level0) {

    /* Build class tuple. */
    TableInfo tableInfo = TABLE_CACHE[1];
    PageId segmentId = TableSpace_Interface::AllocSegment(g_defaultPdbId,
                                                          static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), SegmentType::HEAP_SEGMENT_TYPE);
    TablespaceId tabelSpaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    SysClassTupDef *classTuple = BuildSysRelationTuple(tableInfo, static_cast<Oid>(tabelSpaceId), segmentId);
    /* Test Build */
    classTuple->relkind = SYS_RELKIND_RELATION;
    classTuple->relpersistence = SYS_RELKIND_RELATION;

    Oid relid = SEQUENCE_INDEX_RELATION_ID;
    RetStatus ret = m_storageRelationData->Construct(g_defaultPdbId, relid, classTuple, tupleDesc,
                                                     DEFAULT_HEAP_FILLFACTOR, tabelSpaceId, false, false);
    EXPECT_EQ(ret, DSTORE_SUCC);
}

/**
* @tc.name  : Construct_ShouldReturnSuccess_WhenMatviewType_level0
* @tc.number: StorageRelationDataTest_005
* @tc.desc  : 测试当relkind为序列索引类型时，构造函数应成功返回
 */
TEST_F(UTRelation, Construct_ShouldReturnSuccess_WhenMatviewType_level0) {

    /* Build class tuple. */
    TableInfo tableInfo = TABLE_CACHE[1];
    PageId segmentId = TableSpace_Interface::AllocSegment(g_defaultPdbId,
                                                          static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), SegmentType::HEAP_SEGMENT_TYPE);
    TablespaceId tabelSpaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    SysClassTupDef *classTuple = BuildSysRelationTuple(tableInfo, static_cast<Oid>(tabelSpaceId), segmentId);
    /* Test Build */
    classTuple->relkind = SYS_RELKIND_MATVIEW;
    classTuple->relpersistence = SYS_RELKIND_MATVIEW;


    Oid relid = SEQUENCE_INDEX_RELATION_ID;
    RetStatus ret = m_storageRelationData->Construct(0, relid, classTuple, tupleDesc,
                                                     DEFAULT_HEAP_FILLFACTOR, tabelSpaceId, true, true);
    EXPECT_EQ(ret, DSTORE_SUCC);
}

/**
* @tc.name  : Construct_ShouldReturnFail_WhenMatviewType_level0
* @tc.number: StorageRelationDataTest_006
* @tc.desc  : 测试当relkind为GTT类型时，构造函数应成功失败
 */
TEST_F(UTRelation, Construct_ShouldReturnFail_WhenMatviewType_level0) {

    /* Build class tuple. */
    TableInfo tableInfo = TABLE_CACHE[1];
    PageId segmentId = TableSpace_Interface::AllocSegment(g_defaultPdbId,
                                                          static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID), SegmentType::HEAP_SEGMENT_TYPE);
    TablespaceId tabelSpaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    SysClassTupDef *classTuple = BuildSysRelationTuple(tableInfo, static_cast<Oid>(tabelSpaceId), segmentId);
    /* Test Build */
    classTuple->relkind = SYS_RELPERSISTENCE_GLOBAL_TEMP;
    classTuple->relpersistence = SYS_RELPERSISTENCE_GLOBAL_TEMP;


    Oid relid = SEQUENCE_INDEX_RELATION_ID;
    RetStatus ret = m_storageRelationData->Construct(0, relid, classTuple, tupleDesc,
                                                     DEFAULT_HEAP_FILLFACTOR, tabelSpaceId, true, true);
    EXPECT_EQ(ret, DSTORE_FAIL);
}