// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>
#include <fstream>
#include "../src/util/File.h"
#include "../src/index/IndexMetaData.h"

// Let rmd be meta-data for a relation:
// 10 20
// 10 21
// 10 22
// 15 20
// 16 20
// 17 20
// Split into two blocks for lhs 10+15 and 16+17
// --> 6 elements, 2 blocks, FI has 6 * 2 * sizeof(Id) bytes
// LHS list has size 4 * sizeof(Id) + sizeof(off_t)
// RHS list size depends on compression, assume trivial 6 * sizeof(Id)

TEST(RelationMetaDataTest, getBlockStartAndNofBytesForLhsTest) {
  vector<BlockMetaData> bs;
  off_t afterFI =  6 * 2 * sizeof(Id);
  off_t afterLhs = afterFI + 4 * (sizeof(Id) + sizeof(off_t));
  off_t afterRhs = afterLhs + 6 * sizeof(Id);
  bs.push_back(BlockMetaData(10, afterFI));
  bs.push_back(BlockMetaData(16, afterFI + 2 * (sizeof(Id) + sizeof(off_t))));
  RelationMetaData rmd(1, 0, afterLhs, afterRhs, 6, 2, bs);

  auto rv = rmd.getBlockStartAndNofBytesForLhs(10);
  ASSERT_EQ(afterFI, rv.first);
  ASSERT_EQ(2 * (sizeof(Id) + sizeof(off_t)), rv.second);
  rv = rmd.getBlockStartAndNofBytesForLhs(15);
  ASSERT_EQ(afterFI, rv.first);
  ASSERT_EQ(2 * (sizeof(Id) + sizeof(off_t)), rv.second);
  rv = rmd.getBlockStartAndNofBytesForLhs(16);
  ASSERT_EQ(afterFI + 2 * (sizeof(Id) + sizeof(off_t)), rv.first);
  ASSERT_EQ(2 * (sizeof(Id) + sizeof(off_t)), rv.second);
  rv = rmd.getBlockStartAndNofBytesForLhs(17);
  ASSERT_EQ(afterFI + 2 * (sizeof(Id) + sizeof(off_t)), rv.first);
  ASSERT_EQ(2 * (sizeof(Id) + sizeof(off_t)), rv.second);
}

TEST(RelationMetaDataTest, writeReadTest) {
  vector<BlockMetaData> bs;
  off_t afterFI =  6 * 2 * sizeof(Id);
  off_t afterLhs = afterFI + 4 * (sizeof(Id) + sizeof(off_t));
  off_t afterRhs = afterLhs + 6 * sizeof(Id);
  bs.push_back(BlockMetaData(10, afterFI));
  bs.push_back(BlockMetaData(16, afterFI + 2 * (sizeof(Id) + sizeof(off_t))));
  RelationMetaData rmd(1, 0, afterLhs, afterRhs, 6, 2, bs);

  ad_utility::File f("_testtmp.rmd", "w");
  f << rmd;
  f.close();

  ad_utility::File in("_testtmp.rmd", "r");
  ASSERT_EQ(3 * sizeof(Id) + 5 * sizeof(off_t) + 2 * sizeof(size_t),
      rmd.bytesRequired());
  unsigned char* buf = new unsigned char[rmd.bytesRequired()];
  in.read(buf, rmd.bytesRequired());
  RelationMetaData rmd2;
  rmd2.createFromByteBuffer(buf);

  delete[] buf;
  remove("_testtmp.rmd");

  ASSERT_EQ(rmd._relId, rmd2._relId);
  ASSERT_EQ(rmd._startFullIndex, rmd2._startFullIndex);
  ASSERT_EQ(rmd._startRhs, rmd2._startRhs);
  ASSERT_EQ(rmd._offsetAfter, rmd2._offsetAfter);
  ASSERT_EQ(rmd._nofElements, rmd2._nofElements);
  ASSERT_EQ(rmd._nofBlocks, rmd2._nofBlocks);
  ASSERT_EQ(rmd._blocks.size(), rmd2._blocks.size());
  ASSERT_EQ(rmd._blocks[0]._firstLhs, rmd2._blocks[0]._firstLhs);
  ASSERT_EQ(rmd._blocks[0]._startOffset, rmd2._blocks[0]._startOffset);
  ASSERT_EQ(rmd._blocks[1]._firstLhs, rmd2._blocks[1]._firstLhs);
  ASSERT_EQ(rmd._blocks[1]._startOffset, rmd2._blocks[1]._startOffset);
}

TEST(IndexMetaDataTest, writeReadTest) {
  vector<BlockMetaData> bs;
  off_t afterFI =  6 * 2 * sizeof(Id);
  off_t afterLhs = afterFI + 4 * (sizeof(Id) + sizeof(off_t));
  off_t afterRhs = afterLhs + 6 * sizeof(Id);
  bs.push_back(BlockMetaData(10, afterFI));
  bs.push_back(BlockMetaData(16, afterFI + 2 * (sizeof(Id) + sizeof(off_t))));
  RelationMetaData rmd(1, 0, afterLhs, afterRhs, 6, 2, bs);
  RelationMetaData rmd2(rmd);
  rmd2._relId = 2;
  IndexMetaData imd;
  imd.add(rmd);
  imd.add(rmd2);

  ad_utility::File f("_testtmp.imd", "w");
  f << imd;
  f.close();

  ad_utility::File in("_testtmp.imd", "r");
  size_t imdBytes = sizeof(size_t) + rmd.bytesRequired() * 2;
  unsigned char* buf = new unsigned char[imdBytes];
  in.read(buf, imdBytes);
  IndexMetaData imd2;
  imd2.createFromByteBuffer(buf);
  delete[] buf;
  remove("_testtmp.rmd");

  ASSERT_EQ(rmd._relId, imd2.getRmd(1)._relId);
  ASSERT_EQ(rmd._startFullIndex, imd2.getRmd(1)._startFullIndex);
  ASSERT_EQ(rmd._startRhs, imd2.getRmd(1)._startRhs);
  ASSERT_EQ(rmd._offsetAfter, imd2.getRmd(1)._offsetAfter);
  ASSERT_EQ(rmd._nofElements, imd2.getRmd(1)._nofElements);
  ASSERT_EQ(rmd._nofBlocks, imd2.getRmd(1)._nofBlocks);
  ASSERT_EQ(rmd._blocks.size(), imd2.getRmd(1)._blocks.size());
  ASSERT_EQ(rmd._blocks[0]._firstLhs, imd2.getRmd(1)._blocks[0]._firstLhs);
  ASSERT_EQ(rmd._blocks[0]._startOffset, imd2.getRmd(1)._blocks[0]._startOffset);
  ASSERT_EQ(rmd._blocks[1]._firstLhs, imd2.getRmd(1)._blocks[1]._firstLhs);
  ASSERT_EQ(rmd._blocks[1]._startOffset, imd2.getRmd(1)._blocks[1]._startOffset);

  ASSERT_EQ(rmd2._relId, imd2.getRmd(2)._relId);
  ASSERT_EQ(rmd2._startFullIndex, imd2.getRmd(2)._startFullIndex);
  ASSERT_EQ(rmd2._startRhs, imd2.getRmd(2)._startRhs);
  ASSERT_EQ(rmd2._offsetAfter, imd2.getRmd(2)._offsetAfter);
  ASSERT_EQ(rmd2._nofElements, imd2.getRmd(2)._nofElements);
  ASSERT_EQ(rmd2._nofBlocks, imd2.getRmd(2)._nofBlocks);
  ASSERT_EQ(rmd2._blocks.size(), imd2.getRmd(2)._blocks.size());
  ASSERT_EQ(rmd2._blocks[0]._firstLhs, imd2.getRmd(2)._blocks[0]._firstLhs);
  ASSERT_EQ(rmd2._blocks[0]._startOffset, imd2.getRmd(2)._blocks[0]._startOffset);
  ASSERT_EQ(rmd2._blocks[1]._firstLhs, imd2.getRmd(2)._blocks[1]._firstLhs);
  ASSERT_EQ(rmd2._blocks[1]._startOffset, imd2.getRmd(2)._blocks[1]._startOffset);
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}