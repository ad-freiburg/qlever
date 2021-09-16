// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <fstream>

#include "../src/index/IndexMetaData.h"
#include "../src/util/File.h"
#include "../src/util/Serializer/FileSerializer.h"

TEST(RelationMetaDataTest, writeReadTest) {
  try {
    CompressedBlockMetaData rmdB{12, 34, 5, 0, 2, 13, 24};
    CompressedRelationMetaData rmdF{1, 3, 2.0, 42.0, 16};

    ad_utility::serialization::FileWriteSerializer f("_testtmp.rmd");
    f << rmdF;
    f << rmdB;
    f.close();

    ad_utility::serialization::FileReadSerializer in("_testtmp.rmd");
    CompressedRelationMetaData rmdF2;
    CompressedBlockMetaData rmdB2;
    in >> rmdF2;
    in >> rmdB2;

    remove("_testtmp.rmd");
    ASSERT_EQ(rmdF, rmdF2);
    ASSERT_EQ(rmdB, rmdB2);

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
    vector<CompressedBlockMetaData> bs;
    bs.push_back(CompressedBlockMetaData{12, 34, 5, 0, 2, 13, 24});
    bs.push_back(CompressedBlockMetaData{12, 34, 5, 0, 2, 13, 24});
    CompressedRelationMetaData rmdF{1, 3, 2.0, 42.0, 16};
    CompressedRelationMetaData rmdF2{2, 5, 3.0, 43.0, 10};
    IndexMetaDataHmap imd;
    imd.add(rmdF);
    imd.add(rmdF2);
    imd.blockData() = bs;

    const string filename = "_testtmp.imd";
    imd.writeToFile(filename);

    ad_utility::File in("_testtmp.imd", "r");
    IndexMetaDataHmap imd2;
    imd2.readFromFile(&in);
    remove("_testtmp.rmd");

    auto rmdFn = imd2.getMetaData(1);
    auto rmdFn2 = imd2.getMetaData(2);

    ASSERT_EQ(rmdF, rmdFn);
    ASSERT_EQ(rmdF2, rmdFn2);

    ASSERT_EQ(imd2.blockData(), bs);
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
    vector<CompressedBlockMetaData> bs;
    bs.push_back(CompressedBlockMetaData{12, 34, 5, 0, 2, 13, 24});
    bs.push_back(CompressedBlockMetaData{12, 34, 5, 0, 2, 13, 24});
    CompressedRelationMetaData rmdF{1, 3, 2.0, 42.0, 16};
    CompressedRelationMetaData rmdF2{2, 5, 3.0, 43.0, 10};
    // The index MetaData does not have an explicit clear, so we
    // force destruction to close and reopen the mmap-File
    {
      IndexMetaDataMmap imd;
      // size of 3 would suffice, but we also want to simulate the sparseness
      imd.setup(5, CompressedRelationMetaData::emptyMetaData(),
                "_testtmp.imd.mmap");
      imd.add(rmdF);
      imd.add(rmdF2);
      imd.blockData() = bs;

      const string filename = "_testtmp.imd";
      imd.writeToFile(filename);
    }

    ad_utility::File in("_testtmp.imd", "r");
    IndexMetaDataMmap imd2;
    imd2.setup("_testtmp.imd.mmap", ad_utility::ReuseTag());
    imd2.readFromFile(&in);

    remove("_testtmp.rmd");
    auto rmdFn = imd2.getMetaData(1);
    auto rmdFn2 = imd2.getMetaData(2);

    ASSERT_EQ(rmdF, rmdFn);
    ASSERT_EQ(rmdF2, rmdFn2);

    ASSERT_EQ(imd2.blockData(), bs);

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
