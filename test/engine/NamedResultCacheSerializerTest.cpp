// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/NamedResultCache.h"
#include "engine/NamedResultCacheSerializer.h"
#include "index/LocalVocabEntry.h"
#include "util/Serializer/ByteBufferSerializer.h"

using namespace ad_utility::serialization;
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAreArray;

namespace {
auto V = ad_utility::testing::VocabId;

// Test fixture for NamedResultCacheSerializer tests.
class NamedResultCacheSerializerTest : public ::testing::Test {
 protected:
  // Blank node manager and allocator that can be used when we don't really
  // care about blank nodes and allocation details.
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
  ad_utility::AllocatorWithLimit<Id> alloc_{
      ad_utility::makeUnlimitedAllocator<Id>()};

  // Serialize and immediately deserialize and return the `value`.
  NamedResultCache::Value serializeAndDeserializeValue(
      const NamedResultCache::Value& value,
      ad_utility::AllocatorWithLimit<Id> allocator) const {
    ByteBufferWriteSerializer writeSerializer;
    writeSerializer << value;
    ByteBufferReadSerializer readSerializer{std::move(writeSerializer).data()};
    NamedResultCache::Value result;
    result.allocatorForSerialization_ = std::move(allocator);
    result.contextForSerialization_ = &qec_->getIndex().getLocalVocabContext();
    readSerializer >> result;
    return result;
  }

  // Overload that uses the default member variables.
  NamedResultCache::Value serializeAndDeserializeValue(
      const NamedResultCache::Value& value) {
    return serializeAndDeserializeValue(value, alloc_);
  }

  // Create a simple `Value` with a two-column table, one variable per column,
  // and the given `cacheKey`.
  static NamedResultCache::Value makeSimpleValue(std::string cacheKey) {
    VariableToColumnMap varColMap;
    varColMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
    varColMap[Variable{"?y"}] = makeAlwaysDefinedColumn(1);
    return NamedResultCache::Value{std::make_shared<const IdTable>(
                                       makeIdTableFromVector({{1, 2}, {3, 4}})),
                                   std::move(varColMap),
                                   {0},
                                   LocalVocab{},
                                   std::move(cacheKey),
                                   std::nullopt};
  }

  // Read a `Value` from the given serialized `data`.
  NamedResultCache::Value deserializeValue(std::vector<char> data) const {
    ByteBufferReadSerializer readSerializer{std::move(data)};
    NamedResultCache::Value value;
    value.allocatorForSerialization_ = ad_utility::makeUnlimitedAllocator<Id>();
    value.contextForSerialization_ = &qec_->getIndex().getLocalVocabContext();
    readSerializer >> value;
    return value;
  }
};

// Test serialization of a complete `NamedResultCache::Value`.
TEST_F(NamedResultCacheSerializerTest, ValueSerialization) {
  // Create a test Value
  LocalVocab localVocab;
  [[maybe_unused]] auto local =
      localVocab.getIndexAndAddIfNotContained(LocalVocabEntry::fromIriref(
          "<http://example.org/test>", qec_->getLocalVocabContext()));

  // Note: Currently the serialization throws if we pass a `LocalVocabIndex`
  // inside the `IdTable` As soon as we have improved the serialization of local
  // vocabs to work in all cases, we can again replace one of the entries in the
  // following table by `local` and adapt the remainder of the test accordingly.
  auto table = makeIdTableFromVector({{0, 7}, {9, 11}, {13, 17}});

  VariableToColumnMap varColMap;
  varColMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  varColMap[Variable{"?y"}] = makePossiblyUndefinedColumn(1);

  std::vector<ColumnIndex> sortedOn = {0, 1};

  std::string cacheKey = "test-cache-key";

  // Save original words for comparison
  auto origWords = localVocab.getAllWordsForTesting();

  NamedResultCache::Value value{
      std::make_shared<const IdTable>(table.clone()),
      varColMap,
      sortedOn,
      std::move(localVocab),
      cacheKey,
      std::nullopt  // No geo index for this test
  };

  auto deserializedValue = serializeAndDeserializeValue(value);

  // Check the result pointer is valid (the non-aligned serializer used here
  // always deserializes into the owning `shared_ptr<const IdTable>`
  // alternative, never a zero-copy view).
  ASSERT_THAT(deserializedValue.result_,
              ::testing::VariantWith<std::shared_ptr<const IdTable>>(
                  ::testing::Ne(nullptr)));

  // Check the local vocab.
  auto deserWords = deserializedValue.localVocab_.getAllWordsForTesting();
  EXPECT_EQ(origWords.size(), deserWords.size());
  for (size_t i = 0; i < origWords.size(); ++i) {
    EXPECT_EQ(origWords[i].toStringRepresentation(),
              deserWords[i].toStringRepresentation());
  }
  // Check the result
  EXPECT_THAT(ExplicitIdTableOperation::viewOf(deserializedValue.result_),
              matchesIdTable(table));
  EXPECT_THAT(deserializedValue.varToColMap_,
              UnorderedElementsAreArray(varColMap));
  EXPECT_THAT(deserializedValue.resultSortedOn_, ElementsAre(0, 1));
  EXPECT_EQ(deserializedValue.cacheKey_, cacheKey);
  EXPECT_FALSE(deserializedValue.cachedGeoIndex_.has_value());
}

// Test that deserializing from an `AlignedByteBufferReadSerializer` (which
// supports zero-copy deserialization) yields a non-owning `IdTableView<0>`
// that points directly into the serializer's buffer, instead of an owning
// `shared_ptr<const IdTable>`.
TEST_F(NamedResultCacheSerializerTest, ValueSerializationZeroCopy) {
  auto table = makeIdTableFromVector({{0, 7}, {9, 11}, {13, 17}});

  VariableToColumnMap varColMap;
  varColMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  varColMap[Variable{"?y"}] = makePossiblyUndefinedColumn(1);

  NamedResultCache::Value value{std::make_shared<const IdTable>(table.clone()),
                                varColMap,
                                {0, 1},
                                LocalVocab{},
                                "test-cache-key",
                                std::nullopt};

  AlignedByteBufferWriteSerializer writeSerializer;
  writeSerializer << value;
  AlignedByteBufferReadSerializer readSerializer{
      std::move(writeSerializer).data()};

  NamedResultCache::Value deserializedValue;
  deserializedValue.allocatorForSerialization_ = alloc_;
  deserializedValue.contextForSerialization_ =
      &qec_->getIndex().getLocalVocabContext();
  readSerializer >> deserializedValue;

  ASSERT_TRUE(
      std::holds_alternative<IdTableView<0>>(deserializedValue.result_));
  EXPECT_THAT(ExplicitIdTableOperation::viewOf(deserializedValue.result_),
              matchesIdTable(table));
  EXPECT_THAT(deserializedValue.varToColMap_,
              UnorderedElementsAreArray(varColMap));
  EXPECT_THAT(deserializedValue.resultSortedOn_, ElementsAre(0, 1));
  EXPECT_EQ(deserializedValue.cacheKey_, "test-cache-key");
}

// Test serialization of the entire NamedResultCache.
TEST_F(NamedResultCacheSerializerTest, CacheSerialization) {
  // Create a cache and add some entries
  NamedResultCache cache;

  auto table1 = makeIdTableFromVector({{1, 2}, {3, 4}});
  auto table2 = makeIdTableFromVector({{5, 6, 7}, {8, 9, 10}});

  VariableToColumnMap varColMap1;
  varColMap1[Variable{"?a"}] = makeAlwaysDefinedColumn(0);
  varColMap1[Variable{"?b"}] = makeAlwaysDefinedColumn(1);

  VariableToColumnMap varColMap2;
  varColMap2[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  varColMap2[Variable{"?y"}] = makeAlwaysDefinedColumn(1);
  varColMap2[Variable{"?z"}] = makeAlwaysDefinedColumn(2);

  const auto& localVocabContext = qec_->getLocalVocabContext();
  LocalVocab vocab1;
  vocab1.getIndexAndAddIfNotContained(
      LocalVocabEntry::fromIriref("<http://example.org/1>", localVocabContext));

  LocalVocab vocab2;
  vocab2.getIndexAndAddIfNotContained(
      LocalVocabEntry::fromIriref("<http://example.org/2>", localVocabContext));

  cache.store("query-1", NamedResultCache::Value{
                             std::make_shared<const IdTable>(table1.clone()),
                             varColMap1,
                             {0},
                             std::move(vocab1),
                             "key1",
                             std::nullopt});

  cache.store("query-2", NamedResultCache::Value{
                             std::make_shared<const IdTable>(table2.clone()),
                             varColMap2,
                             {1, 0},
                             std::move(vocab2),
                             "key2",
                             std::nullopt});

  EXPECT_EQ(cache.numEntries(), 2);

  auto cache2 = [this, &cache] {
    using namespace ad_utility::serialization;
    ByteBufferWriteSerializer writer;
    cache.writeToSerializer(writer);
    NamedResultCache cache2;
    ByteBufferReadSerializer reader{std::move(writer).data()};
    cache2.readFromSerializer(reader, ad_utility::makeUnlimitedAllocator<Id>(),
                              qec_->getLocalVocabContext());
    return cache2;
  }();

  // Check the deserialized cache
  EXPECT_EQ(cache2.numEntries(), 2);

  auto result1 = cache2.get("query-1");
  ASSERT_NE(result1, nullptr);
  EXPECT_THAT(ExplicitIdTableOperation::viewOf(result1->result_),
              matchesIdTable(table1));
  EXPECT_THAT(result1->varToColMap_, UnorderedElementsAreArray(varColMap1));
  EXPECT_THAT(result1->resultSortedOn_, ElementsAre(0));
  EXPECT_EQ(result1->cacheKey_, "key1");

  auto result2 = cache2.get("query-2");
  ASSERT_NE(result2, nullptr);
  EXPECT_THAT(ExplicitIdTableOperation::viewOf(result2->result_),
              matchesIdTable(table2));
  EXPECT_THAT(result2->varToColMap_, UnorderedElementsAreArray(varColMap2));
  EXPECT_THAT(result2->resultSortedOn_, ElementsAre(1, 0));
  EXPECT_EQ(result2->cacheKey_, "key2");
}

// Test empty cache serialization.
TEST_F(NamedResultCacheSerializerTest, EmptyCacheSerialization) {
  // Create an empty cache
  NamedResultCache cache;
  EXPECT_EQ(cache.numEntries(), 0);

  auto cache2 = [this, &cache] {
    using namespace ad_utility::serialization;
    ByteBufferWriteSerializer writer;
    cache.writeToSerializer(writer);
    NamedResultCache cache2;
    ByteBufferReadSerializer reader{std::move(writer).data()};
    cache2.readFromSerializer(reader, ad_utility::makeUnlimitedAllocator<Id>(),
                              qec_->getLocalVocabContext());
    return cache2;
  }();
  EXPECT_EQ(cache2.numEntries(), 0);
}

// Test that `readFromSerializer` throws a helpful error message when the
// magic byte or the format version of the input do not match, instead of
// silently misinterpreting unrelated or incompatible data.
TEST_F(NamedResultCacheSerializerTest, WrongMagicByteOrFormatVersionThrows) {
  NamedResultCache cache;
  ByteBufferWriteSerializer writer;
  cache.writeToSerializer(writer);
  auto data = std::move(writer).data();
  ASSERT_GE(data.size(), 3u);

  // Corrupt the magic byte, which is the very first byte of the serialized
  // data.
  auto dataWithWrongMagicByte = data;
  dataWithWrongMagicByte[0] = static_cast<char>(~dataWithWrongMagicByte[0]);
  ByteBufferReadSerializer readerWithWrongMagicByte{
      std::move(dataWithWrongMagicByte)};
  NamedResultCache cacheForWrongMagicByte;
  AD_EXPECT_THROW_WITH_MESSAGE(
      cacheForWrongMagicByte.readFromSerializer(
          readerWithWrongMagicByte, ad_utility::makeUnlimitedAllocator<Id>(),
          qec_->getLocalVocabContext()),
      ::testing::HasSubstr("magic byte"));

  // Corrupt the format version, which directly follows the magic byte.
  auto dataWithWrongVersion = data;
  dataWithWrongVersion[1] = static_cast<char>(dataWithWrongVersion[1] + 1);
  ByteBufferReadSerializer readerWithWrongVersion{
      std::move(dataWithWrongVersion)};
  NamedResultCache cacheForWrongVersion;
  AD_EXPECT_THROW_WITH_MESSAGE(
      cacheForWrongVersion.readFromSerializer(
          readerWithWrongVersion, ad_utility::makeUnlimitedAllocator<Id>(),
          qec_->getLocalVocabContext()),
      ::testing::HasSubstr("format version"));
}

// _____________________________________________________________________________
// Test that `getAllEntries` returns all entries of the cache, sorted by their
// key, no matter in which order they were stored.
TEST_F(NamedResultCacheSerializerTest, GetAllEntries) {
  NamedResultCache cache;
  EXPECT_THAT(cache.getAllEntries(), ::testing::IsEmpty());

  cache.store("zebra", makeSimpleValue("key-zebra"));
  cache.store("apple", makeSimpleValue("key-apple"));
  cache.store("mango", makeSimpleValue("key-mango"));

  auto entries = cache.getAllEntries();
  ASSERT_EQ(entries.size(), 3);
  EXPECT_THAT(entries | ql::views::keys,
              ElementsAre("apple", "mango", "zebra"));
  // The values are exactly the stored ones (the same objects that `get`
  // returns).
  for (const auto& [key, value] : entries) {
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value, cache.get(key));
    EXPECT_EQ(value->cacheKey_, absl::StrCat("key-", key));
  }
}

// _____________________________________________________________________________
// Test that the serialization is deterministic: identical contents yield
// identical bytes, also if the `varToColMap_` entries or the cache entries
// were inserted in a different order.
TEST_F(NamedResultCacheSerializerTest, SerializationIsDeterministic) {
  // Create a `Value` with four variables that are inserted in the given
  // order, but otherwise identical contents.
  auto makeValue = [](bool reverseInsertionOrder) {
    std::vector<std::string> names{"?d", "?c", "?b", "?a"};
    VariableToColumnMap varColMap;
    for (size_t i = 0; i < names.size(); ++i) {
      size_t index = reverseInsertionOrder ? names.size() - 1 - i : i;
      varColMap[Variable{names.at(index)}] = makeAlwaysDefinedColumn(index);
    }
    return NamedResultCache::Value{
        std::make_shared<const IdTable>(
            makeIdTableFromVector({{0, 1, 2, 3}, {4, 5, 6, 7}})),
        std::move(varColMap),
        {0, 1},
        LocalVocab{},
        "cache-key",
        std::nullopt};
  };
  auto serializeValue = [](const NamedResultCache::Value& value) {
    ByteBufferWriteSerializer writer;
    writer << value;
    return std::move(writer).data();
  };

  // Serializing the same value twice yields the same bytes.
  auto value = makeValue(false);
  EXPECT_EQ(serializeValue(value), serializeValue(value));

  // Two values that only differ in the insertion order of their
  // `varToColMap_` entries also yield the same bytes.
  EXPECT_EQ(serializeValue(makeValue(false)), serializeValue(makeValue(true)));

  // The same holds for a whole cache whose entries were stored in a different
  // order.
  auto serializeCache = [](const std::vector<std::string>& names) {
    NamedResultCache cache;
    for (const auto& name : names) {
      cache.store(name, makeSimpleValue(absl::StrCat("key-", name)));
    }
    ByteBufferWriteSerializer writer;
    cache.writeToSerializer(writer);
    return std::move(writer).data();
  };
  EXPECT_EQ(serializeCache({"apple", "mango", "zebra"}),
            serializeCache({"zebra", "apple", "mango"}));
}

// _____________________________________________________________________________
// Test `writeValue` with columns and a sort order that differ from those of
// the `Value`, and without the words of the local vocab, as a caller that has
// rewritten the `Id`s to a persistent vocabulary would use it.
TEST_F(NamedResultCacheSerializerTest, WriteValueWithReplacedColumns) {
  auto table = makeIdTableFromVector({{0, 7}, {9, 11}, {13, 17}});
  VariableToColumnMap varColMap;
  varColMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  varColMap[Variable{"?y"}] = makePossiblyUndefinedColumn(1);
  LocalVocab localVocab;
  localVocab.getIndexAndAddIfNotContained(LocalVocabEntry::fromIriref(
      "<http://example.org/test>", qec_->getLocalVocabContext()));
  ASSERT_EQ(localVocab.size(), 1);

  NamedResultCache::Value value{std::make_shared<const IdTable>(table.clone()),
                                std::move(varColMap),
                                {0, 1},
                                std::move(localVocab),
                                "test-cache-key",
                                std::nullopt};

  // The replaced columns have the same shape as the table of the `value`, but
  // different `Id`s. The sort order is shortened, because the replacement
  // invalidates the sortedness of the second column.
  std::vector<std::vector<Id>> replacedColumns{{V(1), V(2), V(3)},
                                               {V(4), V(5), V(6)}};
  std::vector<ColumnIndex> shortenedSortOrder{0};

  ByteBufferWriteSerializer writer;
  namedResultCacheSerializer::writeValue(writer, value, replacedColumns,
                                         shortenedSortOrder,
                                         /*writeLocalVocabWords=*/false);
  auto readValue = deserializeValue(std::move(writer).data());

  EXPECT_THAT(ExplicitIdTableOperation::viewOf(readValue.result_),
              matchesIdTableFromVector({{1, 4}, {2, 5}, {3, 6}}));
  EXPECT_THAT(readValue.resultSortedOn_, ElementsAre(0));
  EXPECT_EQ(readValue.cacheKey_, "test-cache-key");
  EXPECT_THAT(readValue.varToColMap_,
              UnorderedElementsAreArray(value.varToColMap_));
  // The words of the local vocab were not written, so the local vocab of the
  // deserialized value is empty.
  EXPECT_EQ(readValue.localVocab_.size(), 0);
  EXPECT_THAT(readValue.localVocab_.getAllWordsForTesting(),
              ::testing::IsEmpty());

  // The number of the columns and the number of rows in each column have to
  // agree with the table of the `value`.
  std::vector<std::vector<Id>> tooFewColumns{{V(1), V(2), V(3)}};
  AD_EXPECT_THROW_WITH_MESSAGE(
      namedResultCacheSerializer::writeValue(writer, value, tooFewColumns,
                                             shortenedSortOrder, false),
      ::testing::HasSubstr("resultView.numColumns()"));
  std::vector<std::vector<Id>> tooFewRows{{V(1), V(2)}, {V(4), V(5)}};
  AD_EXPECT_THROW_WITH_MESSAGE(
      namedResultCacheSerializer::writeValue(writer, value, tooFewRows,
                                             shortenedSortOrder, false),
      ::testing::HasSubstr("resultView.numRows()"));
}

}  // namespace
