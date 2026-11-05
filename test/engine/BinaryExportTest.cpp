// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/LocalVocabEntryTestHelpers.h"
#include "ValuesForTesting.h"
#include "engine/BinaryExport.h"
#include "engine/QueryExecutionTree.h"
#include "global/Id.h"
#include "parser/SelectClause.h"

using namespace qlever::binary_export;

namespace {
// The `Id` with which the exporter refers to the `i`-th transmitted string of
// a batch, see `StringMapping`.
Id transmitted(size_t i) {
  return Id::makeFromLocalVocabIndex(
      reinterpret_cast<LocalVocabIndex>(i << Id::numDatatypeBits));
}

// Add `word` to the `localVocab` and return its `Id`.
Id localVocabId(LocalVocab& localVocab, std::string word,
                const QueryExecutionContext& qec) {
  return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
      ad_utility::testing::localVocabEntryFromStringRepresentation(
          std::move(word), qec.getLocalVocabContext())));
}

// The bytes of the trivially copyable `value`.
template <typename T>
std::string bytesOf(const T& value) {
  return std::string{reinterpret_cast<const char*>(&value), sizeof(value)};
}

// The bytes of the `ids`.
std::string bytesOf(const std::vector<Id>& ids) {
  std::string result;
  for (Id id : ids) {
    result.append(bytesOf(id.getBits()));
  }
  return result;
}

// A string as serialized by QLever's serialization framework.
std::string serialized(std::string_view string) {
  return absl::StrCat(bytesOf(string.size()), string);
}

// A vector of strings as serialized by QLever's serialization framework.
std::string serialized(const std::vector<std::string>& strings) {
  std::string result = bytesOf(strings.size());
  for (const auto& string : strings) {
    result.append(serialized(string));
  }
  return result;
}

// A batch consists of the IDs, followed by a marker (which has the size of an
// `Id`) and the strings.
void expectBatch(std::string_view batch, const std::vector<Id>& ids,
                 const std::vector<std::string>& strings,
                 ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  auto idBytes = bytesOf(ids);
  auto stringBytes = serialized(strings);
  ASSERT_EQ(batch.size(), idBytes.size() + sizeof(Id::T) + stringBytes.size());
  EXPECT_EQ(batch.substr(0, idBytes.size()), idBytes);
  EXPECT_EQ(batch.substr(idBytes.size() + sizeof(Id::T)), stringBytes);
}

// Export the `varNames` of the result of `qet` and return all the bytes.
std::string exportBytes(const QueryExecutionTree& qet,
                        const std::vector<std::string>& varNames,
                        LimitOffsetClause limitAndOffset = {}) {
  parsedQuery::SelectClause selectClause;
  selectClause.setSelected(ad_utility::transform(
      varNames, [](const std::string& name) { return Variable{name}; }));
  std::string result;
  for (std::string_view chunk : exportAsQLeverBinary(
           qet, selectClause, limitAndOffset,
           std::make_shared<ad_utility::CancellationHandle<>>())) {
    result.append(chunk);
  }
  return result;
}

// A `QueryExecutionTree` with the result `table` and the `variables`.
QueryExecutionTree makeQet(QueryExecutionContext* qec, IdTable table,
                           std::vector<std::optional<Variable>> variables) {
  return QueryExecutionTree{
      qec, std::make_shared<ValuesForTesting>(qec, std::move(table),
                                              std::move(variables))};
}
}  // namespace

// Test that `appendVectorOfStrings` appends the serialization of the strings,
// whose size `serializedSize` predicts.
TEST(BinaryExportHelpers, appendVectorOfStrings) {
  for (const std::vector<std::string>& strings :
       std::vector<std::vector<std::string>>{
           {}, {""}, {"a"}, {"abc", "", "defgh"}}) {
    std::string target = "prefix";
    BinaryExportHelpers::appendVectorOfStrings(target, strings);
    EXPECT_EQ(target, absl::StrCat("prefix", serialized(strings)));
    EXPECT_EQ(BinaryExportHelpers::serializedSize(strings),
              serialized(strings).size());
  }
}

// Test that an `ExportBatch` exports the selected columns, ends after the row
// that reaches one of its minimum sizes, and appends each string once.
TEST(ExportBatch, minimumSizesAndFormat) {
  auto* qec = ad_utility::testing::getQec();
  const auto& index = qec->getIndex();
  auto I = [](int64_t i) { return Id::makeFromInt(i); };

  IdTable table{3, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({I(1), I(2), I(3)});
  table.push_back({I(4), I(5), I(6)});
  table.push_back({I(7), I(8), I(9)});
  // Only two of the columns are exported, in a different order.
  std::vector<ColumnIndex> columns{2, 0};

  // The minimum size of the IDs (two IDs per row). The batch is finished after
  // the row that reaches the minimum, so it can be larger.
  {
    ExportBatch batch{index, {.minNumIdBytes_ = 3 * sizeof(Id::T)}};
    EXPECT_FALSE(batch.shouldFinish());
    batch.addRow(table.asStaticView<0>(), 0, columns);
    EXPECT_FALSE(batch.shouldFinish());
    batch.addRow(table.asStaticView<0>(), 1, columns);
    EXPECT_TRUE(batch.shouldFinish());
    EXPECT_EQ(batch.numRows(), 2);
    expectBatch(batch.finish(), {I(3), I(1), I(6), I(4)}, {});

    // After `finish`, a new batch starts.
    EXPECT_EQ(batch.numRows(), 0);
    EXPECT_FALSE(batch.shouldFinish());
    batch.addRow(table.asStaticView<0>(), 2, columns);
    EXPECT_FALSE(batch.shouldFinish());
    expectBatch(batch.finish(), {I(9), I(7)}, {});
  }

  // The minimum size of the strings, the strings are sent at the end of the
  // batch. A string is only counted once per batch.
  {
    LocalVocab localVocab;
    auto word = [&](std::string s) {
      return localVocabId(localVocab, std::move(s), *qec);
    };
    // A string that alone is larger than the minimum, like the WKT literal of
    // a large `MULTIPOLYGON`.
    std::string huge = absl::StrCat("\"", std::string(1000, 'x'), "\"");
    IdTable stringTable{1, ad_utility::makeUnlimitedAllocator<Id>()};
    stringTable.push_back({word("\"a\"")});
    stringTable.push_back({I(42)});
    stringTable.push_back({word("\"a\"")});
    stringTable.push_back({word("\"bc\"")});
    stringTable.push_back({word(huge)});
    stringTable.push_back({word(huge)});
    std::vector<ColumnIndex> stringColumns{0};
    auto view = stringTable.asStaticView<0>();

    // The strings `"a"` and `"bc"` together take up 7 bytes.
    ExportBatch batch{index, {.minNumStringBytes_ = 7}};
    for (size_t i = 0; i < 3; ++i) {
      batch.addRow(view, i, stringColumns);
      EXPECT_FALSE(batch.shouldFinish());
    }
    batch.addRow(view, 3, stringColumns);
    EXPECT_TRUE(batch.shouldFinish());
    // The strings are referred to by their index in the batch.
    auto string = transmitted;
    expectBatch(batch.finish(), {string(0), I(42), string(0), string(1)},
                {"\"a\"", "\"bc\""});

    // The huge string immediately finishes the batch.
    batch.addRow(view, 4, stringColumns);
    EXPECT_TRUE(batch.shouldFinish());
    expectBatch(batch.finish(), {string(0)}, {huge});

    // With a larger minimum, the repeated huge string is only sent once.
    ExportBatch largeBatch{index, {.minNumStringBytes_ = 10'000}};
    largeBatch.addRow(view, 4, stringColumns);
    largeBatch.addRow(view, 5, stringColumns);
    EXPECT_FALSE(largeBatch.shouldFinish());
    expectBatch(largeBatch.finish(), {string(0), string(0)}, {huge});
  }
}

// Test the complete format of `exportAsQLeverBinary`: the header, the IDs as
// they are sent, unbound variables, results without columns, and results that
// are sent in several batches.
TEST(BinaryExport, format) {
  ad_utility::testing::TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://example.org/"};
  auto* qec = ad_utility::testing::getQec(config);
  const auto& index = qec->getIndex();
  auto I = [](int64_t i) { return Id::makeFromInt(i); };
  Id vocab = ad_utility::testing::makeGetId(index)("<x>");
  Id blankNode = Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3));
  Id encoded =
      index.encodedIriManager().encode("<http://example.org/42>").value();
  auto qet = makeQet(
      qec,
      makeIdTableFromVector({{I(1), vocab, blankNode}, {I(2), encoded, vocab}}),
      {Variable{"?a"}, Variable{"?b"}, Variable{"?c"}});

  nlohmann::json patterns;
  detail::patternsToJson(patterns, index.encodedIriManager().patterns_,
                         EncodedIriManager::NumBitsEncoding);
  auto header = [&patterns](const std::vector<std::string>& variables,
                            const std::vector<std::string>& unbound) {
    return absl::StrCat(serialized("QLEVER.EXPORT"),
                        bytesOf(ad_utility::binaryQleverExportVersion),
                        bytesOf(GeoPoint::encoding()),
                        serialized(patterns.dump()), serialized(variables),
                        serialized(unbound));
  };
  // Return the bytes after the expected header.
  auto skipHeader = [&header](std::string_view bytes,
                              const std::vector<std::string>& variables,
                              const std::vector<std::string>& unbound) {
    auto expected = header(variables, unbound);
    EXPECT_EQ(bytes.substr(0, expected.size()), expected);
    return bytes.substr(expected.size());
  };

  // `?z` is selected, but not bound in the result.
  auto bytes = exportBytes(qet, {"?c", "?a", "?b", "?z"});
  // Blank nodes, encoded IRIs, and trivial IDs are sent as they are, the
  // string of the `VocabIndex` is sent once.
  expectBatch(skipHeader(bytes, {"?c", "?a", "?b"}, {"?z"}),
              {blankNode, I(1), transmitted(0), transmitted(0), I(2), encoded},
              {"<x>"});

  // For a result without columns, only the number of exported rows is sent.
  IdTable noColumns{0, ad_utility::makeUnlimitedAllocator<Id>()};
  noColumns.resize(5);
  bytes = exportBytes(makeQet(qec, std::move(noColumns), {}), {"?z"},
                      {._limit = 3, ._offset = 1, .exportLimit_ = 2});
  EXPECT_EQ(skipHeader(bytes, {}, {"?z"}), bytesOf(uint64_t{2}));

  // A large result is sent in several batches: The first one ends as soon as
  // it reaches the minimum size, the last one is always sent.
  std::vector<Id> ids;
  for (size_t i = 0; i <= defaultMinNumIdBytesPerBatch / sizeof(Id::T); ++i) {
    ids.push_back(I(static_cast<int64_t>(i)));
  }
  IdTable large{1, ad_utility::makeUnlimitedAllocator<Id>()};
  for (Id id : ids) {
    large.push_back({id});
  }
  bytes = exportBytes(makeQet(qec, std::move(large), {Variable{"?a"}}), {"?a"});
  auto batches = skipHeader(bytes, {"?a"}, {});
  // The IDs, the marker, and the size of the (empty) vector of strings.
  size_t firstBatchSize =
      defaultMinNumIdBytesPerBatch + sizeof(Id::T) + sizeof(size_t);
  Id lastId = ids.back();
  ids.pop_back();
  expectBatch(batches.substr(0, firstBatchSize), ids, {});
  expectBatch(batches.substr(firstBatchSize), {lastId}, {});
}
