//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Johannes Kalmbach <kalmbach@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include <utility>

#include "./util/AllocatorTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "backports/filesystem.h"
#include "engine/idTable/IdTable.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/LocalVocabEntryTestHelpers.h"
#include "util/serializer/ByteBufferSerializer.h"
#include "util/serializer/TripleSerializer.h"

namespace {
auto I = ad_utility::testing::IntId;
auto V = ad_utility::testing::VocabId;
TEST(TripleSerializer, simpleExample) {
  auto* qec = ad_utility::testing::getQec();
  LocalVocab localVocab;
  std::vector<std::vector<Id>> ids;

  ids.emplace_back(std::vector{I(3), I(4), I(7)});
  ids.emplace_back(std::vector{I(1), V(2), V(3)});
  std::string filename = "tripleSerializerTestSimpleExample.dat";
  ad_utility::serializeIds(filename, localVocab, ids);

  auto [localVocabOut, idsOut] =
      ad_utility::deserializeIds(filename, qec->getLocalVocabContext());
  EXPECT_EQ(idsOut, ids);
  EXPECT_EQ(localVocabOut.size(), localVocab.size());
}

// _____________________________________________________________________________
TEST(TripleSerializer, localVocabIsRemapped) {
  auto* qec = ad_utility::testing::getQec();
  LocalVocab localVocab;
  auto LV = [&localVocab, qec](std::string_view value) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::literalWithoutQuotes(value,
                                              qec->getLocalVocabContext())));
  };
  std::vector<std::vector<Id>> ids;

  ids.emplace_back(std::vector{LV("Abc"), LV("def"), LV("ghi")});
  std::string filename = "tripleSerializerTestLocalVocabIsRemapped.dat";
  ad_utility::serializeIds(filename, localVocab, ids);

  auto [localVocabOut, idsOut] =
      ad_utility::deserializeIds(filename, qec->getLocalVocabContext());
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
  auto* qec = ad_utility::testing::getQec();
  LocalVocab localVocab;
  std::vector<std::vector<Id>> ids;

  auto bn = [&]() {
    return Id::makeFromBlankNodeIndex(
        localVocab.getBlankNodeIndex(qec->getIndex().getBlankNodeManager()));
  };

  ids.emplace_back(std::vector{bn(), bn(), bn()});
  std::string filename = "tripleSerializerTestBlankNodesAreRemapped.dat";
  ad_utility::serializeIds(filename, localVocab, ids);

  auto [localVocabOut, idsOut] =
      ad_utility::deserializeIds(filename, qec->getLocalVocabContext());
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
  auto* qec = ad_utility::testing::getQec();
  LocalVocab localVocab;
  auto LV = [&localVocab, qec](std::string_view value) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::literalWithoutQuotes(value,
                                              qec->getLocalVocabContext())));
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

  auto [localVocabOut, mapping] = ad_utility::detail::deserializeLocalVocab(
      reader, qec->getLocalVocabContext());
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
TEST(TripleSerializer, serializeOnlyBlankNodeBlocksFromLocalVocab) {
  auto* qec = ad_utility::testing::getQec();
  LocalVocab localVocab;
  auto LV = [&localVocab, qec](std::string_view value) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::literalWithoutQuotes(value,
                                              qec->getLocalVocabContext())));
  };
  auto bn = [&]() {
    return Id::makeFromBlankNodeIndex(
        localVocab.getBlankNodeIndex(qec->getIndex().getBlankNodeManager()));
  };
  // Add words (in two different word sets) as well as blank nodes.
  std::vector<Id> ids{LV("abc"), LV("def"), bn()};
  localVocab = localVocab.clone();
  ids.push_back(LV("ghi"));
  ids.push_back(bn());
  ASSERT_EQ(localVocab.size(), 3);

  ad_utility::serialization::ByteBufferWriteSerializer writer;
  ad_utility::detail::serializeOnlyBlankNodeBlocksFromLocalVocab(writer,
                                                                 localVocab);
  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::move(writer).data()};
  auto [localVocabOut, mapping] = ad_utility::detail::deserializeLocalVocab(
      reader, qec->getLocalVocabContext());

  // None of the words was written, so the deserialized local vocab is empty
  // and the mapping (from written to deserialized `Id`s) is empty as well.
  EXPECT_EQ(localVocabOut.size(), 0);
  EXPECT_THAT(localVocabOut.getAllWordsForTesting(), ::testing::IsEmpty());
  EXPECT_THAT(mapping, ::testing::IsEmpty());

  // The blank node blocks are written and read back unchanged (but with an
  // empty block prepended, see the `blankNodesRemapper` test above).
  auto blankNodeBlocksOriginal = localVocab.getOwnedLocalBlankNodeBlocks();
  auto blankNodeBlocksDeserialized =
      localVocabOut.getOwnedLocalBlankNodeBlocks();
  ASSERT_FALSE(blankNodeBlocksOriginal.empty());
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

// Test that an IRI of a serialized local vocab that the deserializing index
// encodes directly in an `Id` is mapped to that `Id`, not to a local vocab
// entry.
TEST(TripleSerializer, encodableIrisAreDeserializedAsEncodedIds) {
  // Serialize a local vocab from an index that does not encode any IRIs.
  auto* qecWithoutEncoding = ad_utility::testing::getQec();
  LocalVocab localVocab;
  auto LV = [&localVocab, qecWithoutEncoding](std::string_view iri) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        ad_utility::testing::iriLocalVocabEntry(
            iri, qecWithoutEncoding->getLocalVocabContext())));
  };
  Id encodable = LV("<http://example.org/42>");
  Id notEncodable = LV("<http://other.org/42>");

  ad_utility::serialization::ByteBufferWriteSerializer writer;
  ad_utility::detail::serializeLocalVocab(writer, localVocab);
  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::move(writer).data()};

  // Deserialize it for an index that encodes the first IRI directly in an
  // `Id`, so it must not end up in the deserialized local vocab.
  ad_utility::testing::TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets = {"http://example.org/"};
  auto* qecWithEncoding = ad_utility::testing::getQec(std::move(config));
  auto [localVocabOut, mapping] = ad_utility::detail::deserializeLocalVocab(
      reader, qecWithEncoding->getLocalVocabContext());

  EXPECT_EQ(localVocabOut.size(), 1);
  auto expectedId = qecWithEncoding->getIndex()
                        .encodedIriManager()
                        .encode("<http://example.org/42>")
                        .value();
  // Compare the bits, as `Id::operator==` also matches a `LocalVocabIndex`.
  EXPECT_EQ(mapping.at(encodable.getBits()).getBits(), expectedId.getBits());
  EXPECT_EQ(mapping.at(notEncodable.getBits()).getDatatype(),
            Datatype::LocalVocabIndex);
}

// _____________________________________________________________________________
TEST(TripleSerializer, rethrowsOnInvalidFileAccess) {
  using namespace ::testing;
  auto* qec = ad_utility::testing::getQec();
  auto tmpFile = ql::filesystem::temp_directory_path() / "fileNoPermissions";
  // Create empty file
  std::ofstream{tmpFile.string()}.close();
  absl::Cleanup cleanup{[&tmpFile]() { ql::filesystem::remove(tmpFile); }};
  // Remove all permissions to make read fail
  ql::filesystem::permissions(tmpFile, ql::filesystem_perms_none);

  // NOTE: `path::c_str()` is `const wchar_t*` on Windows; `string().c_str()`
  // is `const char*`.
  if (FILE* handle = fopen(tmpFile.string().c_str(), "r")) {
    fclose(handle);
    // This can happen in docker environments.
    GTEST_SKIP_("File permissions are not set to none");
  }

  LocalVocab localVocab;

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      ad_utility::deserializeIds(tmpFile, qec->getLocalVocabContext()),
      AllOf(HasSubstr(tmpFile.generic_string()),
            HasSubstr("cannot be opened for reading"),
            HasSubstr("(Permission denied)")),
      std::runtime_error);
}

// Return an `IdTable` with two columns and three rows, one of whose entries is
// an `Id` of a word that is added to the `localVocab`. This `Id` is also stored
// in `localVocabId`.
IdTable tableWithLocalVocabId(LocalVocab& localVocab, Id& localVocabId) {
  auto* qec = ad_utility::testing::getQec();
  localVocabId =
      Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
          LocalVocabEntry::literalWithoutQuotes("abc",
                                                qec->getLocalVocabContext())));
  IdTable table{2, ad_utility::testing::makeAllocator()};
  table.push_back({I(1), localVocabId});
  table.push_back({Id::makeFromBool(true), Id::makeUndefined()});
  table.push_back({V(5), I(-3)});
  return table;
}

// Test that `serializeIds` writes the columns of an `IdTable` in the format of
// the column itself, which the zero-copy path of the named result cache reads,
// and that `deserializeIds` reads them back into the columns of another
// `IdTable` and applies the `mapping` to the `Id`s of the local vocab. The
// latter is the path with a copy of the named result cache.
TEST(TripleSerializer, columnsOfAnIdTableRoundTrip) {
  LocalVocab localVocab;
  Id localVocabId;
  const IdTable table = tableWithLocalVocabId(localVocab, localVocabId);

  ad_utility::serialization::ByteBufferWriteSerializer writer;
  ad_utility::serialization::ByteBufferWriteSerializer direct;
  for (const auto& column : table.getColumns()) {
    ad_utility::detail::serializeIds(writer, column);
    direct << ConstIdColumnRef{column};
  }
  EXPECT_EQ(writer.data(), direct.data());

  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::move(writer).data()};
  const Id mappedId = I(99);
  const absl::flat_hash_map<Id::T, Id> mapping{
      {localVocabId.getBits(), mappedId}};
  IdTable result{table.numColumns(), ad_utility::testing::makeAllocator()};
  result.resize(table.numRows());
  for (auto&& column : result.getColumns()) {
    ad_utility::detail::deserializeIds(reader, mapping, column);
  }

  IdTable expected = table.clone();
  expected(0, 1) = mappedId;
  EXPECT_THAT(result, matchesIdTable(expected));
}

// Test that `remapLocalVocab` replaces exactly the `Id`s of the local vocab by
// the `Id` that the `mapping` gives for their bits, in place, for a
// `std::vector<Id>` and for a column of an `IdTable`. All other `Id`s stay as
// they are, even if the `mapping` has an entry for their bits, and a local
// vocab `Id` without an entry in the `mapping` is an error.
TEST(TripleSerializer, remapLocalVocab) {
  auto* qec = ad_utility::testing::getQec();
  LocalVocab localVocab;
  auto LV = [&localVocab, qec](std::string_view value) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::literalWithoutQuotes(value,
                                              qec->getLocalVocabContext())));
  };
  const Id abc = LV("abc");
  const Id def = LV("def");
  const Id notInMapping = LV("ghi");
  const absl::flat_hash_map<Id::T, Id> mapping{
      {abc.getBits(), I(100)}, {def.getBits(), V(200)}, {I(3).getBits(), I(4)}};

  const std::vector<Id> input{
      abc, I(3), def, V(7), Id::makeUndefined(), abc, Id::makeFromBool(true)};
  const std::vector<Id> expected{I(100),
                                 I(3),
                                 V(200),
                                 V(7),
                                 Id::makeUndefined(),
                                 I(100),
                                 Id::makeFromBool(true)};

  // A `std::vector<Id>`, including the empty one.
  std::vector<Id> vector = input;
  ad_utility::detail::remapLocalVocab(vector, mapping);
  EXPECT_EQ(vector, expected);
  std::vector<Id> empty;
  ad_utility::detail::remapLocalVocab(empty, mapping);
  EXPECT_TRUE(empty.empty());

  // A column of an `IdTable`. The other column stays as it is.
  IdTable table{2, ad_utility::testing::makeAllocator()};
  for (const Id id : input) {
    table.push_back({id, abc});
  }
  ad_utility::detail::remapLocalVocab(table.getColumn(0), mapping);
  for (size_t i = 0; i < input.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(table(i, 0)), expected.at(i)) << i;
    EXPECT_EQ(static_cast<Id>(table(i, 1)), abc) << i;
  }

  // A local vocab `Id` without an entry in the `mapping`.
  std::vector<Id> withUnmapped{notInMapping};
  EXPECT_ANY_THROW(ad_utility::detail::remapLocalVocab(withUnmapped, mapping));
}
}  // namespace
