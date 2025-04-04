//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Johannes Kalmbach <kalmbach@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "./util/IdTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/Serializer/TripleSerializer.h"

namespace {
auto I = ad_utility::testing::IntId;
auto V = ad_utility::testing::VocabId;
auto BN = ad_utility::testing::BlankNodeId;
TEST(TripleSerializer, simpleExample) {
  LocalVocab localVocab;
  std::vector<std::vector<Id>> ids;

  ids.emplace_back(std::vector{I(3), I(4), I(7)});
  ids.emplace_back(std::vector{I(1), V(2), V(3)});
  std::string filename = "tripleSerializerTestSimpleExample.dat";
  ad_utility::serializeIds(filename, localVocab, ids);

  ad_utility::BlankNodeManager bm;
  auto [localVocabOut, idsOut] = ad_utility::deserializeIds(filename, &bm);
  EXPECT_EQ(idsOut, ids);
  EXPECT_EQ(localVocabOut.size(), localVocab.size());
}

// _____________________________________________________________________________
TEST(TripleSerializer, localVocabIsRemapped) {
  ad_utility::testing::getQec();
  LocalVocab localVocab;
  auto LV = [&localVocab](std::string value) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes(
            std::move(value))));
  };
  std::vector<std::vector<Id>> ids;

  ids.emplace_back(std::vector{LV("Abc"), LV("def"), LV("ghi")});
  std::string filename = "tripleSerializerTestLocalVocabIsRemapped.dat";
  ad_utility::serializeIds(filename, localVocab, ids);

  ad_utility::BlankNodeManager bm;
  auto [localVocabOut, idsOut] = ad_utility::deserializeIds(filename, &bm);
  EXPECT_EQ(idsOut, ids);
  EXPECT_EQ(localVocabOut.size(), localVocab.size());
  EXPECT_THAT(localVocab.getAllWordsForTesting(),
              ::testing::UnorderedElementsAreArray(
                  localVocabOut.getAllWordsForTesting()));
  for (const LocalVocabEntry& entry : localVocab.getAllWordsForTesting()) {
    EXPECT_NE(localVocab.getIndexOrNullopt(entry),
              localVocabOut.getIndexOrNullopt(entry));
  }
}

// _____________________________________________________________________________
TEST(TripleSerializer, blankNodesAreRemapped) {
  ad_utility::testing::getQec();
  LocalVocab localVocab;
  std::vector<std::vector<Id>> ids;

  ids.emplace_back(std::vector{BN(1337), BN(1338), BN(1337)});
  std::string filename = "tripleSerializerTestBlankNodesAreRemapped.dat";
  ad_utility::serializeIds(filename, localVocab, ids);

  ad_utility::BlankNodeManager bm;
  auto [localVocabOut, idsOut] = ad_utility::deserializeIds(filename, &bm);
  ASSERT_THAT(
      idsOut,
      ::testing::ElementsAre(::testing::Each(AD_PROPERTY(
          ValueId, getDatatype, ::testing::Eq(Datatype::BlankNodeIndex)))));
  EXPECT_EQ(ids.at(0).size(), idsOut.at(0).size());
  EXPECT_NE(ids, idsOut);
  EXPECT_EQ(idsOut.at(0).at(0), idsOut.at(0).at(2));
  EXPECT_NE(idsOut.at(0).at(0), idsOut.at(0).at(1));
  EXPECT_NE(idsOut.at(0).at(1), idsOut.at(0).at(2));
}

// _____________________________________________________________________________
TEST(TripleSerializer, headerFormatIsCorrect) {
  ad_utility::serialization::ByteBufferWriteSerializer serializer;
  ad_utility::detail::writeHeader(serializer);

  EXPECT_THAT(serializer.data(),
              ::testing::ElementsAre('Q', 'L', 'E', 'V', 'E', 'R', '.', 'U',
                                     'P', 'D', 'A', 'T', 'E', 0, 0));
}

// _____________________________________________________________________________
TEST(TripleSerializer, errorOnWrongHeaderFormat) {
  // Wrong magic bytes
  {
    ad_utility::serialization::ByteBufferReadSerializer serializer{
        {'q', 'L', 'E', 'V', 'E', 'R', '.', 'U', 'P', 'D', 'A', 'T', 'E', 0,
         0}};
    EXPECT_THROW(ad_utility::detail::readHeader(serializer),
                 ad_utility::Exception);
  }
  {
    ad_utility::serialization::ByteBufferReadSerializer serializer{
        {'Q', 'L', 'E', 'V', 'E', 'R', '.', 'U', 'P', 'D', 'A', 'T', 'e', 0,
         0}};
    EXPECT_THROW(ad_utility::detail::readHeader(serializer),
                 ad_utility::Exception);
  }
  // Too short magic bytes
  {
    ad_utility::serialization::ByteBufferReadSerializer serializer{
        {'Q', 'L', 'E', 'V', 'E', 'R', 'U', 'P', 'D', 'A', 'T', 'E', 0, 0}};
    EXPECT_THROW(ad_utility::detail::readHeader(serializer),
                 ad_utility::Exception);
  }
  // Non zero version
  {
    ad_utility::serialization::ByteBufferReadSerializer serializer{
        {'Q', 'L', 'E', 'V', 'E', 'R', '.', 'U', 'P', 'D', 'A', 'T', 'E', 1,
         0}};
    EXPECT_THROW(ad_utility::detail::readHeader(serializer),
                 ad_utility::Exception);
  }
  {
    ad_utility::serialization::ByteBufferReadSerializer serializer{
        {'Q', 'L', 'E', 'V', 'E', 'R', '.', 'U', 'P', 'D', 'A', 'T', 'E', 0,
         1}};
    EXPECT_THROW(ad_utility::detail::readHeader(serializer),
                 ad_utility::Exception);
  }
}

// _____________________________________________________________________________
TEST(TripleSerializer, onlySingleWordSetSupportedForLocalVocab) {
  ad_utility::serialization::ByteBufferWriteSerializer serializer;
  LocalVocab localVocab;
  localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{
      ad_utility::triple_component::Literal::literalWithoutQuotes("abc")});
  // Move entry to otherWordSet
  localVocab = localVocab.clone();

  EXPECT_THROW(ad_utility::detail::serializeLocalVocab(serializer, localVocab),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(TripleSerializer, rethrowsOnInvalidFileAccess) {
  using namespace ::testing;
  auto tmpFile = std::filesystem::temp_directory_path() / "fileNoPermissions";
  // Create empty file
  std::ofstream{tmpFile}.close();
  absl::Cleanup cleanup{[&tmpFile]() { std::filesystem::remove(tmpFile); }};
  // Remove all permissions to make read fail
  std::filesystem::permissions(tmpFile, std::filesystem::perms::none);

  if (FILE* handle = fopen(tmpFile.c_str(), "r")) {
    fclose(handle);
    // This can happen in docker environments.
    GTEST_SKIP_("File permissions are not set to none");
  }

  ad_utility::BlankNodeManager bm;
  LocalVocab localVocab;

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      ad_utility::deserializeIds(tmpFile, &bm),
      AllOf(HasSubstr(tmpFile.generic_string()),
            HasSubstr("cannot be opened for reading"),
            HasSubstr("(Permission denied)")),
      std::runtime_error);
}
}  // namespace
