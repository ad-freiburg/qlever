// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <fstream>

#include "../src/index/IndexMetaData.h"
#include "../src/util/File.h"

TEST(FullRelationMetaDataTest, testFunctionAndBlockFlagging) {
  FullRelationMetaData rmd(0, 0, 5, 1, 1, false, false);
  ASSERT_EQ(5u, rmd.getNofElements());
  ASSERT_FALSE(rmd.hasBlocks());
  ASSERT_FALSE(rmd.isFunctional());

  rmd.setHasBlocks(true);
  ASSERT_EQ(5u, rmd.getNofElements());
  ASSERT_TRUE(rmd.hasBlocks());
  ASSERT_FALSE(rmd.isFunctional());

  rmd.setIsFunctional(true);
  ASSERT_EQ(5u, rmd.getNofElements());
  ASSERT_TRUE(rmd.hasBlocks());
  ASSERT_TRUE(rmd.isFunctional());

  rmd.setIsFunctional(false);
  ASSERT_EQ(5u, rmd.getNofElements());
  ASSERT_TRUE(rmd.hasBlocks());
  ASSERT_FALSE(rmd.isFunctional());

  rmd.setHasBlocks(false);
  ASSERT_EQ(5u, rmd.getNofElements());
  ASSERT_FALSE(rmd.hasBlocks());
  ASSERT_FALSE(rmd.isFunctional());

  rmd.setIsFunctional(true);
  ASSERT_EQ(5u, rmd.getNofElements());
  ASSERT_FALSE(rmd.hasBlocks());
  ASSERT_TRUE(rmd.isFunctional());

  rmd.setCol1LogMultiplicity(1);
  ASSERT_EQ(5u, rmd.getNofElements());
  ASSERT_FALSE(rmd.hasBlocks());
  ASSERT_TRUE(rmd.isFunctional());
  ASSERT_EQ(1u, rmd.getCol1LogMultiplicity());

  rmd.setCol2LogMultiplicity(200);
  ASSERT_EQ(5u, rmd.getNofElements());
  ASSERT_FALSE(rmd.hasBlocks());
  ASSERT_TRUE(rmd.isFunctional());
  ASSERT_EQ(1u, rmd.getCol1LogMultiplicity());
  ASSERT_EQ(200u, rmd.getCol2LogMultiplicity());

  FullRelationMetaData rmd2(0, 0, 5, 1, 1, true, true);
  rmd2.setIsFunctional(true);
  ASSERT_EQ(5u, rmd2.getNofElements());
  ASSERT_TRUE(rmd2.hasBlocks());
  ASSERT_TRUE(rmd2.isFunctional());
}

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
  off_t afterFI = 6 * 2 * sizeof(Id);
  off_t afterLhs = afterFI + 4 * (sizeof(Id) + sizeof(off_t));
  off_t afterRhs = afterLhs + 6 * sizeof(Id);
  bs.push_back(BlockMetaData(10, afterFI));
  bs.push_back(BlockMetaData(16, afterFI + 2 * (sizeof(Id) + sizeof(off_t))));
  BlockBasedRelationMetaData rmd(afterLhs, afterRhs, bs);

  auto rv = rmd.getBlockStartAndNofBytesForLhs(10);
  ASSERT_EQ(afterFI, rv.first);
  ASSERT_EQ(2 * (sizeof(Id) + sizeof(off_t)), rv.second);
  rv = rmd.getBlockStartAndNofBytesForLhs(15);
  ASSERT_EQ(afterFI, rv.first);
  ASSERT_EQ(2 * (sizeof(Id) + sizeof(off_t)), rv.second);
  rv = rmd.getBlockStartAndNofBytesForLhs(16);
  ASSERT_EQ(afterFI + 2 * (sizeof(Id) + sizeof(off_t)),
            static_cast<size_t>(rv.first));
  ASSERT_EQ(2 * (sizeof(Id) + sizeof(off_t)), rv.second);
  rv = rmd.getBlockStartAndNofBytesForLhs(17);
  ASSERT_EQ(afterFI + 2 * (sizeof(Id) + sizeof(off_t)),
            static_cast<size_t>(rv.first));
  ASSERT_EQ(2 * (sizeof(Id) + sizeof(off_t)), rv.second);
}

TEST(RelationMetaDataTest, writeReadTest) {
  try {
    vector<BlockMetaData> bs;
    off_t afterFI = 6 * 2 * sizeof(Id);
    off_t afterLhs = afterFI + 4 * (sizeof(Id) + sizeof(off_t));
    off_t afterRhs = afterLhs + 6 * sizeof(Id);
    bs.push_back(BlockMetaData(10, afterFI));
    bs.push_back(BlockMetaData(16, afterFI + 2 * (sizeof(Id) + sizeof(off_t))));
    FullRelationMetaData rmdF(1, 0, 6, 1, 1, false, true);
    BlockBasedRelationMetaData rmdB(afterLhs, afterRhs, bs);

    ad_utility::File f("_testtmp.rmd", "w");
    f << rmdF << rmdB;
    f.close();

    ad_utility::File in("_testtmp.rmd", "r");
    unsigned char* buf =
        new unsigned char[rmdF.bytesRequired() + rmdB.bytesRequired()];
    in.read(buf, rmdF.bytesRequired() + rmdB.bytesRequired());
    FullRelationMetaData rmdF2;
    rmdF2.createFromByteBuffer(buf);
    BlockBasedRelationMetaData rmdB2;
    rmdB2.createFromByteBuffer(buf + rmdF.bytesRequired());

    delete[] buf;
    remove("_testtmp.rmd");

    ASSERT_EQ(rmdF._relId, rmdF2._relId);
    ASSERT_EQ(rmdF._startFullIndex, rmdF2._startFullIndex);
    ASSERT_EQ(rmdB._startRhs, rmdB2._startRhs);
    ASSERT_EQ(rmdB._offsetAfter, rmdB2._offsetAfter);
    ASSERT_EQ(rmdF.getNofElements(), rmdF2.getNofElements());
    ASSERT_EQ(rmdB._blocks.size(), rmdB2._blocks.size());
    ASSERT_EQ(rmdB._blocks[0]._firstLhs, rmdB2._blocks[0]._firstLhs);
    ASSERT_EQ(rmdB._blocks[0]._startOffset, rmdB2._blocks[0]._startOffset);
    ASSERT_EQ(rmdB._blocks[1]._firstLhs, rmdB2._blocks[1]._firstLhs);
    ASSERT_EQ(rmdB._blocks[1]._startOffset, rmdB2._blocks[1]._startOffset);
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(IndexMetaDataTest, writeReadTest2Hmap) {
  try {
    vector<BlockMetaData> bs;
    off_t afterFI = 6 * 2 * sizeof(Id);
    off_t afterLhs = afterFI + 4 * (sizeof(Id) + sizeof(off_t));
    off_t afterRhs = afterLhs + 6 * sizeof(Id);
    bs.push_back(BlockMetaData(10, afterFI));
    bs.push_back(BlockMetaData(16, afterFI + 2 * (sizeof(Id) + sizeof(off_t))));
    FullRelationMetaData rmdF(1, 0, 6, 1, 1, false, true);
    BlockBasedRelationMetaData rmdB(afterLhs, afterRhs, bs);
    FullRelationMetaData rmdF2(rmdF);
    rmdF2._relId = 2;
    IndexMetaDataHmap imd;
    imd.add(rmdF, rmdB);
    imd.add(rmdF2, rmdB);

    ad_utility::File f("_testtmp.imd", "w");
    f << imd;
    f.close();

    ad_utility::File in("_testtmp.imd", "r");
    IndexMetaDataHmap imd2;
    imd2.readFromFile(&in);
    remove("_testtmp.rmd");

    ASSERT_EQ(rmdF._relId, imd2.getRmd(1)._rmdPairs._relId);
    ASSERT_EQ(rmdF._startFullIndex, imd2.getRmd(1)._rmdPairs._startFullIndex);
    ASSERT_EQ(rmdB._startRhs, imd2.getRmd(1)._rmdBlocks->_startRhs);
    ASSERT_EQ(rmdB._offsetAfter, imd2.getRmd(1)._rmdBlocks->_offsetAfter);
    ASSERT_EQ(rmdF.getNofElements(), imd2.getRmd(1)._rmdPairs.getNofElements());
    ASSERT_EQ(rmdB._blocks.size(), imd2.getRmd(1)._rmdBlocks->_blocks.size());
    ASSERT_EQ(rmdB._blocks[0]._firstLhs,
              imd2.getRmd(1)._rmdBlocks->_blocks[0]._firstLhs);
    ASSERT_EQ(rmdB._blocks[0]._startOffset,
              imd2.getRmd(1)._rmdBlocks->_blocks[0]._startOffset);
    ASSERT_EQ(rmdB._blocks[1]._firstLhs,
              imd2.getRmd(1)._rmdBlocks->_blocks[1]._firstLhs);
    ASSERT_EQ(rmdB._blocks[1]._startOffset,
              imd2.getRmd(1)._rmdBlocks->_blocks[1]._startOffset);

    ASSERT_EQ(rmdF2._relId, imd2.getRmd(2)._rmdPairs._relId);
    ASSERT_EQ(rmdF2._startFullIndex, imd2.getRmd(2)._rmdPairs._startFullIndex);
    ASSERT_EQ(rmdB._startRhs, imd2.getRmd(2)._rmdBlocks->_startRhs);
    ASSERT_EQ(rmdB._offsetAfter, imd2.getRmd(2)._rmdBlocks->_offsetAfter);
    ASSERT_EQ(rmdF2.getNofElements(),
              imd2.getRmd(2)._rmdPairs.getNofElements());
    ASSERT_EQ(rmdB._blocks.size(), imd2.getRmd(2)._rmdBlocks->_blocks.size());
    ASSERT_EQ(rmdB._blocks[0]._firstLhs,
              imd2.getRmd(2)._rmdBlocks->_blocks[0]._firstLhs);
    ASSERT_EQ(rmdB._blocks[0]._startOffset,
              imd2.getRmd(2)._rmdBlocks->_blocks[0]._startOffset);
    ASSERT_EQ(rmdB._blocks[1]._firstLhs,
              imd2.getRmd(2)._rmdBlocks->_blocks[1]._firstLhs);
    ASSERT_EQ(rmdB._blocks[1]._startOffset,
              imd2.getRmd(2)._rmdBlocks->_blocks[1]._startOffset);
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(IndexMetaDataTest, writeReadTest2Mmap) {
  try {
    vector<BlockMetaData> bs;
    off_t afterFI = 6 * 2 * sizeof(Id);
    off_t afterLhs = afterFI + 4 * (sizeof(Id) + sizeof(off_t));
    off_t afterRhs = afterLhs + 6 * sizeof(Id);
    bs.push_back(BlockMetaData(10, afterFI));
    bs.push_back(BlockMetaData(16, afterFI + 2 * (sizeof(Id) + sizeof(off_t))));
    FullRelationMetaData rmdF(1, 0, 6, 1, 1, false, true);
    BlockBasedRelationMetaData rmdB(afterLhs, afterRhs, bs);
    FullRelationMetaData rmdF2(rmdF);
    rmdF2._relId = 2;
    // The index MetaData does not have an explicit clear, so we
    // force destruction to close and reopen the mmap-File
    {
      IndexMetaDataMmap imd;
      // size of 3 would suffice, but we also want to simulate the sparseness
      imd.setup(5, FullRelationMetaData::empty, "_testtmp.imd.mmap");
      imd.add(rmdF, rmdB);
      imd.add(rmdF2, rmdB);

      ad_utility::File f("_testtmp.imd", "w");
      f << imd;
      f.close();
    }

    ad_utility::File in("_testtmp.imd", "r");
    IndexMetaDataMmap imd2;
    imd2.setup("_testtmp.imd.mmap", ad_utility::ReuseTag());
    imd2.readFromFile(&in);

    remove("_testtmp.rmd");

    ASSERT_EQ(rmdF._relId, imd2.getRmd(1)._rmdPairs._relId);
    ASSERT_EQ(rmdF._startFullIndex, imd2.getRmd(1)._rmdPairs._startFullIndex);
    ASSERT_EQ(rmdB._startRhs, imd2.getRmd(1)._rmdBlocks->_startRhs);
    ASSERT_EQ(rmdB._offsetAfter, imd2.getRmd(1)._rmdBlocks->_offsetAfter);
    ASSERT_EQ(rmdF.getNofElements(), imd2.getRmd(1)._rmdPairs.getNofElements());
    ASSERT_EQ(rmdB._blocks.size(), imd2.getRmd(1)._rmdBlocks->_blocks.size());
    ASSERT_EQ(rmdB._blocks[0]._firstLhs,
              imd2.getRmd(1)._rmdBlocks->_blocks[0]._firstLhs);
    ASSERT_EQ(rmdB._blocks[0]._startOffset,
              imd2.getRmd(1)._rmdBlocks->_blocks[0]._startOffset);
    ASSERT_EQ(rmdB._blocks[1]._firstLhs,
              imd2.getRmd(1)._rmdBlocks->_blocks[1]._firstLhs);
    ASSERT_EQ(rmdB._blocks[1]._startOffset,
              imd2.getRmd(1)._rmdBlocks->_blocks[1]._startOffset);

    ASSERT_EQ(rmdF2._relId, imd2.getRmd(2)._rmdPairs._relId);
    ASSERT_EQ(rmdF2._startFullIndex, imd2.getRmd(2)._rmdPairs._startFullIndex);
    ASSERT_EQ(rmdB._startRhs, imd2.getRmd(2)._rmdBlocks->_startRhs);
    ASSERT_EQ(rmdB._offsetAfter, imd2.getRmd(2)._rmdBlocks->_offsetAfter);
    ASSERT_EQ(rmdF2.getNofElements(),
              imd2.getRmd(2)._rmdPairs.getNofElements());
    ASSERT_EQ(rmdB._blocks.size(), imd2.getRmd(2)._rmdBlocks->_blocks.size());
    ASSERT_EQ(rmdB._blocks[0]._firstLhs,
              imd2.getRmd(2)._rmdBlocks->_blocks[0]._firstLhs);
    ASSERT_EQ(rmdB._blocks[0]._startOffset,
              imd2.getRmd(2)._rmdBlocks->_blocks[0]._startOffset);
    ASSERT_EQ(rmdB._blocks[1]._firstLhs,
              imd2.getRmd(2)._rmdBlocks->_blocks[1]._firstLhs);
    ASSERT_EQ(rmdB._blocks[1]._startOffset,
              imd2.getRmd(2)._rmdBlocks->_blocks[1]._startOffset);
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
