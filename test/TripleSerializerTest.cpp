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

TEST(TripleSerializer, blankNodesRemapper) {
  ad_utility::testing::getQec();
  ad_utility::BlankNodeManager bm;
  LocalVocab localVocab;
  std::vector<std::vector<Id>> ids;

  auto bn = [&]() {
    return Id::makeFromBlankNodeIndex(localVocab.getBlankNodeIndex(&bm));
  };

  ids.emplace_back(std::vector{bn(), bn(), bn()});
  std::string filename = "tripleSerializerTestBlankNodesAreRemapped.dat";
  ad_utility::serializeIds(filename, localVocab, ids);

  ad_utility::BlankNodeManager bm2;
  auto [localVocabOut, idsOut] = ad_utility::deserializeIds(filename, &bm2);
  // Blank nodes are now preserved (not remapped).
  EXPECT_EQ(ids, idsOut);

  // The deserialized blank node blocks are equal to the original blank node
  // blocks (same block indices + same uuid), but with an empty block (for new
  // blank node indices) prepended, such that we can safely add new indices,
  // without interfering with other local vocabs that share the same blocks.
  auto blankNodeBlocksOriginal = localVocab.getOwnedLocalBlankNodeBlocks();
  auto blankNodeBlocksDeserialized =
      localVocabOut.getOwnedLocalBlankNodeBlocks();

  ASSERT_EQ(blankNodeBlocksDeserialized.size(),
            blankNodeBlocksOriginal.size() + 1);
  EXPECT_TRUE(blankNodeBlocksDeserialized.at(0).blockIndices_.empty());
  for (size_t i = 0; i < blankNodeBlocksOriginal.size(); ++i) {
    EXPECT_EQ(blankNodeBlocksOriginal[i].uuid_,
              blankNodeBlocksDeserialized[i + 1].uuid_)
        << i;
    EXPECT_EQ(blankNodeBlocksOriginal[i].blockIndices_,
              blankNodeBlocksDeserialized[i + 1].blockIndices_)
        << i;
  }
}

// _____________________________________________________________________________
TEST(TripleSerializer, headerFormatIsCorrect) {
  ad_utility::serialization::ByteBufferWriteSerializer serializer;
  ad_utility::detail::writeHeader(serializer);

  EXPECT_THAT(serializer.data(),
              ::testing::ElementsAre('Q', 'L', 'E', 'V', 'E', 'R', '.', 'U',
                                     'P', 'D', 'A', 'T', 'E', 1, 0));
}

// _____________________________________________________________________________
TEST(TripleSerializer, errorOnWrongHeaderFormat) {
  // Wrong magic bytes
  {
    ad_utility::serialization::ByteBufferReadSerializer serializer{
        {'q', 'L', 'E', 'V', 'E', 'R', '.', 'U', 'P', 'D', 'A', 'T', 'E', 1,
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
  // Wrong version
  {
    ad_utility::serialization::ByteBufferReadSerializer serializer{
        {'Q', 'L', 'E', 'V', 'E', 'R', '.', 'U', 'P', 'D', 'A', 'T', 'E', 0,
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
TEST(TripleSerializer, multipleWordSetsInASerializedLocalVocab) {
  ad_utility::testing::getQec();
  LocalVocab localVocab;
  auto LV = [&localVocab](std::string value) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes(
            std::move(value))));
  };
  std::vector<std::vector<Id>> ids;

  ids.emplace_back(std::vector{LV("abc"), LV("def"), LV("ghi")});
  // Move entries to otherWordSet.
  localVocab = localVocab.clone();
  ids.emplace_back(std::vector{LV("xyz"), LV("123"), LV("456")});

  ad_utility::serialization::ByteBufferWriteSerializer writer;
  ad_utility::detail::serializeLocalVocab(writer, localVocab);

  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::move(writer).data()};

  ad_utility::BlankNodeManager bm;
  auto [localVocabOut, mapping] =
      ad_utility::detail::deserializeLocalVocab(reader, &bm);
  auto fromMapping = [&]() {
    return ::ranges::to<std::vector>(
        mapping | ql::views::values |
        ql::views::transform([](Id id) { return *id.getLocalVocabIndex(); }));
  };
  auto fromMappingOrigin = [&]() {
    return ::ranges::to<std::vector>(
        mapping | ql::views::keys | ql::views::transform([](Id::T id) {
          return *Id::fromBits(id).getLocalVocabIndex();
        }));
  };
  EXPECT_EQ(localVocabOut.size(), localVocab.size());
  auto allWords = localVocab.getAllWordsForTesting();
  EXPECT_THAT(allWords, ::testing::UnorderedElementsAreArray(
                            localVocabOut.getAllWordsForTesting()));
  EXPECT_THAT(allWords, ::testing::UnorderedElementsAreArray(fromMapping()));
  EXPECT_THAT(allWords,
              ::testing::UnorderedElementsAreArray(fromMappingOrigin()));
  // Clear the original local vocab, then ensure that the target side of the
  // mapping is still valid, which means that it's being kept alive by the
  // new local vocab.
  localVocab = LocalVocab{};
  EXPECT_THAT(allWords, ::testing::UnorderedElementsAreArray(fromMapping()));
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
