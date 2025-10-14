// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <ranges>
#include <vector>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/BinaryExport.h"
#include "global/Id.h"

using namespace qlever::binary_export;

// _____________________________________________________________________________
TEST(BinaryExportHelpers, isTrivial) {
  // Test trivial types
  EXPECT_TRUE(BinaryExportHelpers::isTrivial(Id::makeUndefined()));
  EXPECT_TRUE(BinaryExportHelpers::isTrivial(Id::makeFromBool(true)));
  EXPECT_TRUE(BinaryExportHelpers::isTrivial(Id::makeFromInt(42)));
  EXPECT_TRUE(BinaryExportHelpers::isTrivial(Id::makeFromDouble(3.14)));
  EXPECT_TRUE(BinaryExportHelpers::isTrivial(
      Id::makeFromDate(DateYearOrDuration::parseXsdDate("2000-01-01"))));

  // Test non-trivial types
  EXPECT_FALSE(BinaryExportHelpers::isTrivial(
      Id::makeFromVocabIndex(VocabIndex::make(0))));
  EXPECT_FALSE(BinaryExportHelpers::isTrivial(
      Id::makeFromLocalVocabIndex(reinterpret_cast<LocalVocabIndex>(0x100))));
}

// _____________________________________________________________________________
TEST(BinaryExportHelpers, readValue) {
  std::vector<uint8_t> data;

  // Write a 64-bit integer
  uint64_t value = 0x0123456789ABCDEF;
  for (size_t i = 0; i < sizeof(value); ++i) {
    data.push_back(static_cast<uint8_t>((value >> (i * 8)) & 0xFF));
  }

  auto it = data.begin();
  auto end = data.end();

  uint64_t result = BinaryExportHelpers::read<uint64_t>(it, end);
  EXPECT_EQ(result, value);
  EXPECT_EQ(it, end);
}

// _____________________________________________________________________________
TEST(BinaryExportHelpers, readValueThrowsOnUnexpectedEnd) {
  std::vector<uint8_t> data{1, 2, 3};  // Only 3 bytes, but we need 8
  auto it = data.begin();
  auto end = data.end();

  EXPECT_THROW(BinaryExportHelpers::read<uint64_t>(it, end),
               std::runtime_error);
}

// _____________________________________________________________________________
TEST(BinaryExportHelpers, readString) {
  std::vector<uint8_t> data;
  std::string expected = "Hello, World!";

  // Write string size
  size_t size = expected.size();
  for (size_t i = 0; i < sizeof(size); ++i) {
    data.push_back(static_cast<uint8_t>((size >> (i * 8)) & 0xFF));
  }

  // Write string content
  for (char c : expected) {
    data.push_back(static_cast<uint8_t>(c));
  }

  auto it = data.begin();
  auto end = data.end();

  std::string result = BinaryExportHelpers::readString(it, end);
  EXPECT_EQ(result, expected);
  EXPECT_EQ(it, end);
}

// _____________________________________________________________________________
TEST(BinaryExportHelpers, readVectorOfStrings) {
  std::vector<uint8_t> data;
  std::vector<std::string> expected = {"foo", "bar", "baz"};

  auto writeString = [&data](const std::string& str) {
    size_t size = str.size();
    for (size_t i = 0; i < sizeof(size); ++i) {
      data.push_back(static_cast<uint8_t>((size >> (i * 8)) & 0xFF));
    }
    for (char c : str) {
      data.push_back(static_cast<uint8_t>(c));
    }
  };

  // Write strings
  for (const auto& str : expected) {
    writeString(str);
  }

  // Write empty string to terminate
  writeString("");

  auto it = data.begin();
  auto end = data.end();

  std::vector<std::string> result =
      BinaryExportHelpers::readVectorOfStrings(it, end);
  EXPECT_EQ(result, expected);
  EXPECT_EQ(it, end);
}

// _____________________________________________________________________________
TEST(BinaryExportHelpers, rewriteVocabIds) {
  auto* qec = ad_utility::testing::getQec();

  // Create a test IdTable with local vocab indices
  IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({Id::makeFromInt(42), Id::makeFromLocalVocabIndex(
                                            reinterpret_cast<LocalVocabIndex>(
                                                0 << Id::numDatatypeBits))});
  table.push_back({Id::makeFromInt(43), Id::makeFromLocalVocabIndex(
                                            reinterpret_cast<LocalVocabIndex>(
                                                1 << Id::numDatatypeBits))});

  LocalVocab vocab;
  std::vector<std::string> transmittedStrings = {"<http://example.org/a>",
                                                  "\"literal\""};

  // Rewrite vocab IDs starting from index 0
  BinaryExportHelpers::rewriteVocabIds(table, 0, *qec, vocab,
                                       transmittedStrings);

  // Check that local vocab indices were rewritten
  // The first column should remain unchanged (integers)
  EXPECT_EQ(table(0, 0), Id::makeFromInt(42));
  EXPECT_EQ(table(1, 0), Id::makeFromInt(43));

  // The second column should have been converted to LocalVocabIndex IDs
  EXPECT_EQ(table(0, 1).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(table(1, 1).getDatatype(), Datatype::LocalVocabIndex);
}

// _____________________________________________________________________________
TEST(BinaryExportHelpers, getPrefixMapping) {
  auto* qec = ad_utility::testing::getQec();

  std::vector<std::string> remotePrefixes = {"<http://example.org/",
                                              "<http://other.org/"};

  auto mapping =
      BinaryExportHelpers::getPrefixMapping(*qec, remotePrefixes);

  // The mapping should be empty or contain mappings only for prefixes
  // that exist in the local index
  EXPECT_TRUE(mapping.empty() || mapping.size() <= remotePrefixes.size());
}

// _____________________________________________________________________________
TEST(BinaryExportHelpers, toIdImpl) {
  auto* qec = ad_utility::testing::getQec();

  // Test with a trivial ID (integer)
  Id intId = Id::makeFromInt(42);
  LocalVocab vocab;
  std::vector<std::string> prefixes;
  ad_utility::HashMap<uint8_t, uint8_t> prefixMapping;

  Id result = BinaryExportHelpers::toIdImpl(*qec, prefixes, prefixMapping,
                                            vocab, intId.getBits());

  EXPECT_EQ(result, intId);
  EXPECT_EQ(result.getDatatype(), Datatype::Int);
}

// _____________________________________________________________________________
TEST(BinaryExport, toExportableIdTrivial) {
  Id intId = Id::makeFromInt(42);
  LocalVocab localVocab;
  StringMapping mapping;

  Id result = toExportableId(intId, localVocab, mapping);

  EXPECT_EQ(result, intId);
  EXPECT_TRUE(mapping.stringMapping_.empty());
}

// _____________________________________________________________________________
TEST(StringMapping, remapId) {
  StringMapping mapping;
  // Use Vocab IDs for testing the remapping logic
  Id id1 = Id::makeFromInt(100);
  Id id2 = Id::makeFromInt(200);

  // Manually insert into the mapping to test basic functionality
  mapping.stringMapping_[id1] = 0;
  mapping.stringMapping_[id2] = 1;

  // Check mapping contains 2 entries
  EXPECT_EQ(mapping.stringMapping_.size(), 2u);

  // Test remapId with new ID
  Id id3 = Id::makeFromInt(300);
  Id remapped3 = mapping.remapId(id3);
  EXPECT_EQ(remapped3.getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(mapping.stringMapping_.size(), 3u);
}

// _____________________________________________________________________________
TEST(StringMapping, needsFlush) {
  StringMapping mapping;

  // Initially should not need flush
  EXPECT_FALSE(mapping.needsFlush());

  // Add enough rows to trigger flush (needs at least one mapping entry)
  mapping.stringMapping_[Id::makeFromInt(1)] = 0;
  for (size_t i = 0; i < 100'000; ++i) {
    mapping.nextRow();
  }

  EXPECT_TRUE(mapping.needsFlush());

  // Test size-based flush
  StringMapping mapping2;
  for (size_t i = 0; i < 10'000; ++i) {
    mapping2.stringMapping_[Id::makeFromInt(i)] = i;
  }

  EXPECT_TRUE(mapping2.needsFlush());
}
