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
using ::testing::UnorderedElementsAreArray;

namespace {
// Serialize and immediately deserialize the value.
NamedResultCache::Value serializeAndDeserializeValue(
    const NamedResultCache::Value& value) {
  ByteBufferWriteSerializer writeSerializer;
  writeSerializer << value;
  ByteBufferReadSerializer readSerializer{std::move(writeSerializer).data()};
  NamedResultCache::Value result;
  readSerializer >> result;
  return result;
}

// Note: VariableToColumnMap serialization is tested as part of Value
// serialization since Variable is not default-constructible and thus
// can't be used with the generic HashMap serialization.

// Test serialization of IdTable with LocalVocab via NamedResultCache::Value
// (IdTable serialization is part of Value serialization)
TEST(NamedResultCacheSerializer, IdTableSerialization) {
  // Create an IdTable with some data
  auto table = makeIdTableFromVector({{3, 7}, {9, 11}});

  // Create a LocalVocab with some entries
  LocalVocab localVocab;
  auto localEntry = ad_utility::triple_component::LiteralOrIri::iriref(
      "<http://example.org/test>");
  auto localVocabIndex =
      localVocab.getIndexAndAddIfNotContained(std::move(localEntry));
  auto localId = Id::makeFromLocalVocabIndex(localVocabIndex);

  // Add the local vocab ID to the table
  table(0, 0) = localId;

  // Create a simple Value to test IdTable serialization
  NamedResultCache::Value value{std::make_shared<const IdTable>(table.clone()),
                                VariableToColumnMap{},
                                {},
                                std::move(localVocab),
                                "test-key",
                                std::nullopt};

  auto deserializedValue = serializeAndDeserializeValue(value);
  // Check the IdTable dimensions
  EXPECT_EQ(deserializedValue.result_->numRows(), 2);
  EXPECT_EQ(deserializedValue.result_->numColumns(), 2);

  // Check the local vocab entry
  EXPECT_EQ((*deserializedValue.result_)(0, 0).getDatatype(),
            Datatype::LocalVocabIndex);

  // Check other entries (without using matchesIdTable which needs index setup)
  EXPECT_EQ((*deserializedValue.result_)(0, 1), table(0, 1));
  EXPECT_EQ((*deserializedValue.result_)(1, 0), table(1, 0));
  EXPECT_EQ((*deserializedValue.result_)(1, 1), table(1, 1));
}

// Test serialization of a complete NamedResultCache::Value
TEST(NamedResultCacheSerializer, ValueSerialization) {
  // Create a test Value
  auto table = makeIdTableFromVector({{3, 7}, {9, 11}, {13, 17}});

  VariableToColumnMap varColMap;
  varColMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  varColMap[Variable{"?y"}] = makePossiblyUndefinedColumn(1);

  std::vector<ColumnIndex> sortedOn = {0, 1};

  LocalVocab localVocab;
  localVocab.getIndexAndAddIfNotContained(
      ad_utility::triple_component::LiteralOrIri::iriref(
          "<http://example.org/test>"));

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
  // Check the result
  EXPECT_THAT(*deserializedValue.result_, matchesIdTable(table));
  EXPECT_THAT(deserializedValue.varToColMap_,
              UnorderedElementsAreArray(varColMap));
  EXPECT_THAT(deserializedValue.resultSortedOn_, ElementsAre(0, 1));
  EXPECT_EQ(deserializedValue.cacheKey_, cacheKey);
  EXPECT_FALSE(deserializedValue.cachedGeoIndex_.has_value());

  // Check LocalVocab
  auto deserWords = deserializedValue.localVocab_.getAllWordsForTesting();
  EXPECT_EQ(origWords.size(), deserWords.size());
  for (size_t i = 0; i < origWords.size(); ++i) {
    EXPECT_EQ(origWords[i].toStringRepresentation(),
              deserWords[i].toStringRepresentation());
  }
}

// Test serialization of the entire NamedResultCache
TEST(NamedResultCacheSerializer, CacheSerialization) {
  std::string tempFile = "/tmp/test_named_result_cache.bin";

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

  // Serialize to disk
  cache.writeToDisk(tempFile);

  // Create a new cache and deserialize
  NamedResultCache cache2;
  auto qec = ad_utility::testing::getQec();
  cache2.readFromDisk(tempFile, ad_utility::makeUnlimitedAllocator<Id>(),
                      *qec->getIndex().getBlankNodeManager());

  // Check the deserialized cache
  EXPECT_EQ(cache2.numEntries(), 2);

  auto result1 = cache2.get("query-1");
  EXPECT_THAT(*result1->result_, matchesIdTable(table1));
  EXPECT_THAT(result1->varToColMap_, UnorderedElementsAreArray(varColMap1));
  EXPECT_THAT(result1->resultSortedOn_, ElementsAre(0));
  EXPECT_EQ(result1->cacheKey_, "key1");

  auto result2 = cache2.get("query-2");
  EXPECT_THAT(*result2->result_, matchesIdTable(table2));
  EXPECT_THAT(result2->varToColMap_, UnorderedElementsAreArray(varColMap2));
  EXPECT_THAT(result2->resultSortedOn_, ElementsAre(1, 0));
  EXPECT_EQ(result2->cacheKey_, "key2");

  // Clean up
  ad_utility::deleteFile(tempFile);
}

// Test empty cache serialization
TEST(NamedResultCacheSerializer, EmptyCacheSerialization) {
  std::string tempFile = "/tmp/test_empty_cache.bin";

  // Create an empty cache
  NamedResultCache cache;
  EXPECT_EQ(cache.numEntries(), 0);

  // Serialize to disk
  cache.writeToDisk(tempFile);

  // Deserialize
  NamedResultCache cache2;
  auto qec = ad_utility::testing::getQec();
  cache2.readFromDisk(tempFile, ad_utility::makeUnlimitedAllocator<Id>(),
                      *qec->getIndex().getBlankNodeManager());

  // Check
  EXPECT_EQ(cache2.numEntries(), 0);

  // Clean up
  std::filesystem::remove(tempFile);
}

}  // namespace
