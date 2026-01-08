// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <fstream>

#include "./util/IdTestHelpers.h"
#include "index/IndexMetaData.h"
#include "util/File.h"
#include "util/Serializer/FileSerializer.h"

namespace {
auto V = ad_utility::testing::VocabId;
// A default/dummy graph used for several tests.
Id g = V(123405);
}  // namespace

TEST(RelationMetaDataTest, writeReadTest) {
  CompressedBlockMetadata rmdB{{{{{12, 34}, {46, 11}}},
                                5,
                                {V(0), V(2), V(13), g},
                                {V(3), V(24), V(62), g},
                                std::vector{V(85)},
                                true},
                               1039};
  CompressedRelationMetadata rmdF{V(1), 3, 2.0, 42.0, 16};

  ad_utility::serialization::FileWriteSerializer f("_testtmp.rmd");
  f << rmdF;
  f << rmdB;
  f.close();

  ad_utility::serialization::FileReadSerializer in("_testtmp.rmd");
  CompressedRelationMetadata rmdF2;
  CompressedBlockMetadata rmdB2;
  in >> rmdF2;
  in >> rmdB2;

  remove("_testtmp.rmd");
  ASSERT_EQ(rmdF, rmdF2);
  ASSERT_EQ(rmdB, rmdB2);
}

TEST(IndexMetaDataTest, writeReadTest2Mmap) {
  std::string imdFilename = "_testtmp.imd";
  std::string mmapFilename = imdFilename + ".mmap";
  std::vector<CompressedBlockMetadata> bs;
  // A value for the Graph Id.
  bs.push_back(CompressedBlockMetadata{{{{{12, 34}, {42, 17}}},
                                        5,
                                        {V(0), V(2), V(13), g},
                                        {V(2), V(24), V(62), g},
                                        std::vector{V(512)},
                                        true},
                                       17});
  bs.push_back(CompressedBlockMetadata{{{{{12, 34}, {16, 12}}},
                                        5,
                                        {V(0), V(2), V(13), g},
                                        {V(3), V(24), V(62), g},
                                        {},
                                        false},
                                       18});
  CompressedRelationMetadata rmdF{V(1), 3, 2.0, 42.0, 16};
  CompressedRelationMetadata rmdF2{V(2), 5, 3.0, 43.0, 10};
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

    auto rmdFn = imd2.getMetaData(V(1));
    auto rmdFn2 = imd2.getMetaData(V(2));

    ASSERT_EQ(rmdF, rmdFn);
    ASSERT_EQ(rmdF2, rmdFn2);

    ASSERT_EQ(imd2.blockData(), bs);
  }
  ad_utility::deleteFile(imdFilename);
  ad_utility::deleteFile(mmapFilename);
}
