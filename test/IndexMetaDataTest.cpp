// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <fstream>

#include "../src/index/IndexMetaData.h"
#include "../src/util/File.h"
#include "../src/util/Serializer/FileSerializer.h"

auto I = [](const auto& id) { return Id::make(id); };

TEST(RelationMetaDataTest, writeReadTest) {
  try {
    CompressedBlockMetaData rmdB{12, 34, 5, I(0), I(2), I(13), I(24)};
    CompressedRelationMetaData rmdF{I(1), 3, 2.0, 42.0, 16};

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
    bs.push_back(CompressedBlockMetaData{12, 34, 5, I(0), I(2), I(13), I(24)});
    bs.push_back(CompressedBlockMetaData{12, 34, 5, I(0), I(2), I(13), I(24)});
    CompressedRelationMetaData rmdF{I(1), 3, 2.0, 42.0, 16};
    CompressedRelationMetaData rmdF2{I(2), 5, 3.0, 43.0, 10};
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

    auto rmdFn = imd2.getMetaData(I(1));
    auto rmdFn2 = imd2.getMetaData(I(2));

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
  std::string imdFilename = "_testtmp.imd";
  std::string mmapFilename = imdFilename + ".mmap";
  try {
    vector<CompressedBlockMetaData> bs;
    bs.push_back(CompressedBlockMetaData{12, 34, 5, I(0), I(2), I(13), I(24)});
    bs.push_back(CompressedBlockMetaData{12, 34, 5, I(0), I(2), I(13), I(24)});
    CompressedRelationMetaData rmdF{I(1), 3, 2.0, 42.0, 16};
    CompressedRelationMetaData rmdF2{I(2), 5, 3.0, 43.0, 10};
    // The index MetaData does not have an explicit clear, so we
    // force destruction to close and reopen the mmap-File
    {
      IndexMetaDataMmap imd;
      imd.setup(mmapFilename, ad_utility::CreateTag{});
      imd.add(rmdF);
      imd.add(rmdF2);
      imd.blockData() = bs;

      imd.writeToFile(imdFilename);
    }

    {
      ad_utility::File in(imdFilename, "r");
      IndexMetaDataMmap imd2;
      imd2.setup(mmapFilename, ad_utility::ReuseTag());
      imd2.readFromFile(&in);

      auto rmdFn = imd2.getMetaData(I(1));
      auto rmdFn2 = imd2.getMetaData(I(2));

      ASSERT_EQ(rmdF, rmdFn);
      ASSERT_EQ(rmdF2, rmdFn2);

      ASSERT_EQ(imd2.blockData(), bs);
    }
    ad_utility::deleteFile(imdFilename);
    ad_utility::deleteFile(mmapFilename);

  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}
