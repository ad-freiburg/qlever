//  Copyright 2015 - 2026 The QLever Authors, in particular:
//
//  2015 Björn Buchhold <buchhold@cs.uni-freiburg.de>, UFR
//  2018 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <fstream>

#include "./util/IdTestHelpers.h"
#include "global/Constants.h"
#include "index/IndexMetaData.h"
#include "index/MetaDataHandler.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/MmapVectorLegacyFormat.h"
#include "util/Serializer/ByteBufferSerializer.h"
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

  std::string filename = gtestCurrentTestName();
  ad_utility::serialization::FileWriteSerializer f(filename);
  f << rmdF;
  f << rmdB;
  f.close();

  ad_utility::serialization::FileReadSerializer in(filename);
  CompressedRelationMetadata rmdF2;
  CompressedBlockMetadata rmdB2;
  in >> rmdF2;
  in >> rmdB2;

  ad_utility::deleteFile(filename);
  ASSERT_EQ(rmdF, rmdF2);
  ASSERT_EQ(rmdB, rmdB2);
}

TEST(IndexMetaDataTest, writeReadTest2) {
  std::string imdFilename = gtestCurrentTestName();
  std::string metaFilename = imdFilename + META_FILE_SUFFIX;
  absl::Cleanup cleanup{[&imdFilename, &metaFilename]() {
    ad_utility::deleteFile(imdFilename);
    ad_utility::deleteFile(metaFilename);
  }};
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
  {
    IndexMetaData imd;
    imd.add(rmdF);
    imd.add(rmdF2);
    imd.blockData() = bs;

    imd.writeToFile(imdFilename);
  }

  {
    IndexMetaData imd2;
    imd2.readFromFile(imdFilename);

    auto rmdFn = imd2.getMetaDataIfPresent(V(1)).value();
    auto rmdFn2 = imd2.getMetaDataIfPresent(V(2)).value();

    ASSERT_EQ(rmdF, rmdFn);
    ASSERT_EQ(rmdF2, rmdFn2);

    ASSERT_EQ(imd2.blockData(), bs);
  }
}

// Check that the RAM-based `MetaDataWrapperDense` is on-disk compatible with
// the legacy `MmapVector`-based format in both directions: it can read files
// written by older QLever versions (which used `MmapVector`), and files it
// writes can still be read the way an `MmapVectorView` would (as older versions
// would).
TEST(IndexMetaDataTest, mmapFormatBackwardsCompatibility) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup{[&filename]() { ad_utility::deleteFile(filename); }};
  CompressedRelationMetadata rmd1{V(1), 3, 2.0, 42.0, 16};
  CompressedRelationMetadata rmd2{V(2), 5, 3.0, 43.0, 10};

  // Reading a legacy file: write it in the legacy `MmapVector` on-disk layout
  // (the page-aligned array with the metadata trailer that older versions
  // produced) and read it back with the new wrapper.
  ad_utility::testing::writeLegacyMmapVectorFile<CompressedRelationMetadata>(
      filename, {rmd1, rmd2});
  MetaDataWrapperDense wrapper;
  {
    ad_utility::File file{filename, "r"};
    wrapper.readFromFile(file);
  }
  ASSERT_EQ(wrapper.size(), 2u);
  ASSERT_EQ(wrapper.getIfPresent(V(1)).value(), rmd1);
  ASSERT_EQ(wrapper.getIfPresent(V(2)).value(), rmd2);

  // Writing a file that older versions can still read: write it with the new
  // wrapper and read it back the way an old `MmapVectorView` would have.
  {
    ad_utility::File file{filename, "w"};
    wrapper.writeToFile(file);
  }
  auto elements =
      ad_utility::testing::readLegacyMmapVectorFile<CompressedRelationMetadata>(
          filename);
  ASSERT_EQ(elements.size(), 2u);
  ASSERT_EQ(elements[0], rmd1);
  ASSERT_EQ(elements[1], rmd2);
}

// _____________________________________________________________________________
TEST(IndexMetaDataTest, exchangeMultiplicities) {
  CompressedRelationMetadata crm1a{V(1), 3, 2.0, 2.0, 16};
  CompressedRelationMetadata crm1b{V(1), 3, 3.0, 3.0, 16};
  CompressedRelationMetadata crm2a{V(2), 5, 4.0, 4.0, 10};
  CompressedRelationMetadata crm2b{V(2), 5, 5.0, 5.0, 10};

  IndexMetaData imda;
  imda.add(crm1a);
  imda.add(crm2a);

  IndexMetaData imdb;
  imdb.add(crm1b);
  imdb.add(crm2b);

  imda.exchangeMultiplicities(imdb);

  EXPECT_FLOAT_EQ(imda.getMetaDataIfPresent(V(1)).value().multiplicityCol1_,
                  2.0);
  EXPECT_FLOAT_EQ(imda.getMetaDataIfPresent(V(1)).value().multiplicityCol2_,
                  3.0);
  EXPECT_FLOAT_EQ(imda.getMetaDataIfPresent(V(2)).value().multiplicityCol1_,
                  4.0);
  EXPECT_FLOAT_EQ(imda.getMetaDataIfPresent(V(2)).value().multiplicityCol2_,
                  5.0);

  EXPECT_FLOAT_EQ(imdb.getMetaDataIfPresent(V(1)).value().multiplicityCol1_,
                  3.0);
  EXPECT_FLOAT_EQ(imdb.getMetaDataIfPresent(V(1)).value().multiplicityCol2_,
                  2.0);
  EXPECT_FLOAT_EQ(imdb.getMetaDataIfPresent(V(2)).value().multiplicityCol1_,
                  5.0);
  EXPECT_FLOAT_EQ(imdb.getMetaDataIfPresent(V(2)).value().multiplicityCol2_,
                  4.0);
}

// _____________________________________________________________________________
TEST(IndexMetaDataTest, exchangeMultiplicitiesFailsWhenIncompatible) {
  CompressedRelationMetadata crm1{V(1), 3, 2.0, 2.0, 16};
  CompressedRelationMetadata crm2{V(1), 3, 3.0, 3.0, 16};
  CompressedRelationMetadata crm3{V(2), 5, 4.0, 4.0, 10};

  IndexMetaData imda;
  imda.add(crm1);

  IndexMetaData imdb;
  imdb.add(crm3);

  IndexMetaData imdc;
  imdc.add(crm2);
  imdc.add(crm3);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(imda.exchangeMultiplicities(imdb),
                                        ::testing::HasSubstr("same ids"),
                                        ad_utility::Exception);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(imda.exchangeMultiplicities(imdc),
                                        ::testing::HasSubstr("length"),
                                        ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(IndexMetaDataTest, addRejectsNonAscendingCol0Id) {
  MetaDataWrapperDense wrapper;
  CompressedRelationMetadata rmd1{V(2), 3, 2.0, 42.0, 16};
  CompressedRelationMetadata rmd2{V(1), 5, 3.0, 43.0, 10};
  wrapper.add(rmd1);
  // `rmd2` has a smaller `col0Id_` than the previously added `rmd1`.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(wrapper.add(rmd2),
                                        ::testing::HasSubstr("col0Id_"),
                                        ad_utility::Exception);
  // Adding with an equal `col0Id_` (not strictly ascending) is also rejected.
  CompressedRelationMetadata rmd3{V(2), 5, 3.0, 43.0, 10};
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(wrapper.add(rmd3),
                                        ::testing::HasSubstr("col0Id_"),
                                        ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(IndexMetaDataTest, deserializationRejectsWrongFormat) {
  using ad_utility::serialization::ByteBufferReadSerializer;
  using ad_utility::serialization::ByteBufferWriteSerializer;

  // A wrong magic number is rejected before anything else is read.
  {
    ByteBufferWriteSerializer writer;
    uint64_t wrongMagicNumber = MAGIC_NUMBER_FOR_SERIALIZATION - 1;
    writer | wrongMagicNumber;
    ByteBufferReadSerializer reader{std::move(writer).data()};
    IndexMetaData imd;
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        reader >> imd, ::testing::HasSubstr("no longer supported"),
        WrongFormatException);
  }

  // The magic number matches, but the version does not.
  {
    ByteBufferWriteSerializer writer;
    uint64_t magicNumber = MAGIC_NUMBER_FOR_SERIALIZATION;
    uint64_t wrongVersion = V_CURRENT - 1;
    writer | magicNumber;
    writer | wrongVersion;
    ByteBufferReadSerializer reader{std::move(writer).data()};
    IndexMetaData imd;
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        reader >> imd, ::testing::HasSubstr("no longer supported"),
        WrongFormatException);
  }
}

// _____________________________________________________________________________
TEST(IndexMetaDataTest, appendToFileRejectsClosedFile) {
  IndexMetaData imd;
  imd.add(CompressedRelationMetadata{V(1), 3, 2.0, 42.0, 16});

  // A default-constructed `File` is not open.
  ad_utility::File permutationFile;
  ad_utility::File metaFile;
  ASSERT_FALSE(permutationFile.isOpen());
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      imd.appendToFile(permutationFile, metaFile),
      ::testing::HasSubstr("isOpen"), ad_utility::Exception);
}
