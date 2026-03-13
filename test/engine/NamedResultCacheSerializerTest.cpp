// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/NamedResultCache.h"
#include "engine/NamedResultCacheSerializer.h"
#include "util/Serializer/ByteBufferSerializer.h"

using namespace ad_utility::serialization;
using ::testing::ElementsAre;
using ::testing::Pointee;
using ::testing::UnorderedElementsAreArray;

namespace {

// Test fixture for NamedResultCacheSerializer tests.
class NamedResultCacheSerializerTest : public ::testing::Test {
 protected:
  // Blank node manager and allocator that can be used when we don't really
  // care about blank nodes and allocation details.
  ad_utility::BlankNodeManager blankNodeManager_;
  ad_utility::AllocatorWithLimit<Id> alloc_{
      ad_utility::makeUnlimitedAllocator<Id>()};

  // Serialize and immediately deserialize and return the `value`.
  NamedResultCache::Value serializeAndDeserializeValue(
      const NamedResultCache::Value& value, ad_utility::BlankNodeManager& bm,
      ad_utility::AllocatorWithLimit<Id> allocator) const {
    ByteBufferWriteSerializer writeSerializer;
    writeSerializer << value;
    ByteBufferReadSerializer readSerializer{std::move(writeSerializer).data()};
    NamedResultCache::Value result;
    result.allocatorForSerialization_ = std::move(allocator);
    result.blankNodeManagerForSerialization_.emplace(bm);
    readSerializer >> result;
    return result;
  }

  // Overload that uses the default member variables, cannot be const, because
  // the calls might modify the `BlankNodeManager` or the `LocalVocab`.
  NamedResultCache::Value serializeAndDeserializeValue(
      const NamedResultCache::Value& value) {
    return serializeAndDeserializeValue(value, blankNodeManager_, alloc_);
  }
};

// Test serialization of a complete `NamedResultCache::Value`.
TEST_F(NamedResultCacheSerializerTest, ValueSerialization) {
  // we need to setup a dummy index somewhere, because otherwise the comparison
  // of `IdTable`s won't work;
  [[maybe_unused]] auto qec = ad_utility::testing::getQec();
  // Create a test Value
  LocalVocab localVocab;
  [[maybe_unused]] auto local = localVocab.getIndexAndAddIfNotContained(
      ad_utility::triple_component::LiteralOrIri::iriref(
          "<http://example.org/test>"));

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

  // Check the result pointer is valid.
  ASSERT_NE(deserializedValue.result_, nullptr);

  // Check the local vocab.
  auto deserWords = deserializedValue.localVocab_.getAllWordsForTesting();
  EXPECT_EQ(origWords.size(), deserWords.size());
  for (size_t i = 0; i < origWords.size(); ++i) {
    EXPECT_EQ(origWords[i].toStringRepresentation(),
              deserWords[i].toStringRepresentation());
  }
  // Check the result
  EXPECT_THAT(deserializedValue.result_, Pointee(matchesIdTable(table)));
  EXPECT_THAT(deserializedValue.varToColMap_,
              UnorderedElementsAreArray(varColMap));
  EXPECT_THAT(deserializedValue.resultSortedOn_, ElementsAre(0, 1));
  EXPECT_EQ(deserializedValue.cacheKey_, cacheKey);
  EXPECT_FALSE(deserializedValue.cachedGeoIndex_.has_value());
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

  LocalVocab vocab1;
  vocab1.getIndexAndAddIfNotContained(
      ad_utility::triple_component::LiteralOrIri::iriref(
          "<http://example.org/1>"));

  LocalVocab vocab2;
  vocab2.getIndexAndAddIfNotContained(
      ad_utility::triple_component::LiteralOrIri::iriref(
          "<http://example.org/2>"));

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

  auto qec = ad_utility::testing::getQec();
  auto cache2 = [&qec, &cache] {
    using namespace ad_utility::serialization;
    ByteBufferWriteSerializer writer;
    cache.writeToSerializer(writer);
    NamedResultCache cache2;
    ByteBufferReadSerializer reader{std::move(writer).data()};
    cache2.readFromSerializer(reader, ad_utility::makeUnlimitedAllocator<Id>(),
                              *qec->getIndex().getBlankNodeManager());
    return cache2;
  }();

  // Check the deserialized cache
  EXPECT_EQ(cache2.numEntries(), 2);

  auto result1 = cache2.get("query-1");
  ASSERT_NE(result1, nullptr);
  EXPECT_THAT(result1->result_, Pointee(matchesIdTable(table1)));
  EXPECT_THAT(result1->varToColMap_, UnorderedElementsAreArray(varColMap1));
  EXPECT_THAT(result1->resultSortedOn_, ElementsAre(0));
  EXPECT_EQ(result1->cacheKey_, "key1");

  auto result2 = cache2.get("query-2");
  ASSERT_NE(result2, nullptr);
  EXPECT_THAT(result2->result_, Pointee(matchesIdTable(table2)));
  EXPECT_THAT(result2->varToColMap_, UnorderedElementsAreArray(varColMap2));
  EXPECT_THAT(result2->resultSortedOn_, ElementsAre(1, 0));
  EXPECT_EQ(result2->cacheKey_, "key2");
}

// Test empty cache serialization.
TEST_F(NamedResultCacheSerializerTest, EmptyCacheSerialization) {
  // Create an empty cache
  NamedResultCache cache;
  EXPECT_EQ(cache.numEntries(), 0);

  auto qec = ad_utility::testing::getQec();
  auto cache2 = [&qec, &cache] {
    using namespace ad_utility::serialization;
    ByteBufferWriteSerializer writer;
    cache.writeToSerializer(writer);
    NamedResultCache cache2;
    ByteBufferReadSerializer reader{std::move(writer).data()};
    cache2.readFromSerializer(reader, ad_utility::makeUnlimitedAllocator<Id>(),
                              *qec->getIndex().getBlankNodeManager());
    return cache2;
  }();
  EXPECT_EQ(cache2.numEntries(), 0);
}

}  // namespace
