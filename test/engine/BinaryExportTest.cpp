// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <random>
#include <ranges>
#include <vector>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "ValuesForTesting.h"
#include "engine/BinaryExport.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryExecutionTree.h"
#include "global/Id.h"
#include "parser/SelectClause.h"
#include "util/http/HttpClient.h"

using namespace qlever::binary_export;

// _____________________________________________________________________________
TEST(BinaryExportHelpers, isTrivial) {
  // Test trivial types
  EXPECT_TRUE(Id::makeUndefined().isTrivial());
  EXPECT_TRUE(Id::makeFromBool(true).isTrivial());
  EXPECT_TRUE(Id::makeFromInt(42).isTrivial());
  EXPECT_TRUE(Id::makeFromDouble(3.14).isTrivial());
  EXPECT_TRUE(Id::makeFromDate(DateYearOrDuration::parseXsdDate("2000-01-01"))
                  .isTrivial());

  // Test non-trivial types
  EXPECT_FALSE(Id::makeFromVocabIndex(VocabIndex::make(0)).isTrivial());
  EXPECT_FALSE(
      Id::makeFromLocalVocabIndex(reinterpret_cast<LocalVocabIndex>(0x100))
          .isTrivial());
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

// TODO<joka921> Check if this code is completely removed, or has to be tested
// using a different mechanism.
/*
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
*/

// _____________________________________________________________________________
TEST(BinaryExportHelpers, readVectorOfStrings) {
  std::vector<uint8_t> data;
  std::vector<std::string> expected = {"foo", "bar", "baz"};

  auto str = BinaryExportHelpers::writeVectorOfStrings(expected);
  ql::ranges::copy(str, std::back_inserter(data));

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

  auto mapping = BinaryExportHelpers::getPrefixMapping(*qec, remotePrefixes);

  // The mapping should be empty or contain mappings only for prefixes
  // that exist in the local index
  EXPECT_TRUE(mapping.size() <= remotePrefixes.size());
}

// _____________________________________________________________________________
TEST(BinaryExportHelpers, toIdImpl) {
  auto* qec = ad_utility::testing::getQec();

  // Test with a trivial ID (integer)
  Id intId = Id::makeFromInt(42);
  LocalVocab vocab;
  std::vector<std::string> prefixes;
  ad_utility::HashMap<uint8_t, uint8_t> prefixMapping;
  ad_utility::HashMap<Id::T, Id> blankNodeMapping;

  Id result = BinaryExportHelpers::toIdImpl(
      *qec, prefixes, prefixMapping, vocab, intId.getBits(), blankNodeMapping);

  EXPECT_EQ(result, intId);
}

// ============================================================================
// End-to-end round-trip tests for binary export/import.
// ============================================================================

// Test fixture with helpers for the binary export/import round-trip.
class BinaryExportRoundTrip : public ::testing::Test {
 protected:
  // Collect all bytes from the export generator into a single string.
  static std::string collectExportBytes(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause,
      LimitOffsetClause limitAndOffset = {}) {
    auto generator = exportAsQLeverBinary(
        qet, selectClause, limitAndOffset,
        std::make_shared<ad_utility::CancellationHandle<>>());
    std::string result;
    for (std::string_view chunk : generator) {
      result.append(chunk);
    }
    return result;
  }

  // Create an HttpOrHttpsResponse from raw bytes with random chunking.
  static HttpOrHttpsResponse makeResponse(std::string bytes) {
    auto body =
        [](std::string data) -> cppcoro::generator<ql::span<std::byte>> {
      std::mt19937 rng{std::random_device{}()};
      std::uniform_int_distribution<size_t> distribution{1,
                                                         data.size() / 2 + 1};
      for (size_t start = 0; start < data.size();) {
        size_t chunkSize = std::min(distribution(rng), data.size() - start);
        std::string chunk = data.substr(start, chunkSize);
        co_yield ql::as_writable_bytes(ql::span{chunk});
        start += chunkSize;
      }
    };
    return HttpOrHttpsResponse{
        .status_ = boost::beast::http::status::ok,
        .contentType_ = "application/qlever-export+octet-stream",
        .location_ = {},
        .body_ = body(std::move(bytes))};
  }

  // Create a QueryExecutionTree from an IdTable and variable names.
  static QueryExecutionTree makeQet(
      QueryExecutionContext* qec, IdTable table,
      std::vector<std::optional<Variable>> variables,
      LocalVocab localVocab = LocalVocab{}) {
    auto values = std::make_shared<ValuesForTesting>(
        qec, std::move(table), std::move(variables), false,
        std::vector<ColumnIndex>{}, std::move(localVocab));
    return QueryExecutionTree{qec, std::move(values)};
  }

  // Create a SelectClause from variable names.
  static parsedQuery::SelectClause makeSelectClause(
      const std::vector<std::string>& varNames) {
    parsedQuery::SelectClause clause;
    std::vector<Variable> vars;
    vars.reserve(varNames.size());
    for (const auto& name : varNames) {
      vars.emplace_back(name);
    }
    clause.setSelected(std::move(vars));
    return clause;
  }

  // Full round-trip: export from exportQec, import into importQec.
  static Result roundTrip(QueryExecutionContext* exportQec,
                          QueryExecutionContext* importQec, IdTable table,
                          std::vector<std::string> varNames,
                          LocalVocab localVocab = LocalVocab{},
                          bool requestLaziness = false) {
    // Build variable list for ValuesForTesting.
    std::vector<std::optional<Variable>> variables;
    for (const auto& name : varNames) {
      variables.push_back(Variable{name});
    }

    auto qet =
        makeQet(exportQec, std::move(table), variables, std::move(localVocab));
    auto selectClause = makeSelectClause(varNames);
    std::string bytes = collectExportBytes(qet, selectClause);
    auto response = makeResponse(std::move(bytes));
    return importBinaryHttpResponse(requestLaziness, std::move(response),
                                    *importQec, {});
  }

  // Resolve an Id to its string representation using a given index and
  // local vocab.
  static std::string idToString(const Index& index, Id id,
                                const LocalVocab& localVocab) {
    auto optLitOrIri = ExportQueryExecutionTrees::idToLiteralOrIri(
        index.getImpl(), id, localVocab);
    if (optLitOrIri.has_value()) {
      return optLitOrIri->toStringRepresentation();
    }
    return absl::StrCat("UnresolvableId:", id.getBits());
  }
};

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, trivialIdsRoundTrip) {
  auto* qec = ad_utility::testing::getQec();

  IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({Id::makeFromInt(42), Id::makeFromDouble(3.14)});
  table.push_back({Id::makeFromInt(-7), Id::makeFromBool(true)});
  table.push_back(
      {Id::makeFromDate(DateYearOrDuration::parseXsdDate("2000-01-01")),
       Id::makeFromInt(0)});

  auto result = roundTrip(qec, qec, table.clone(), {"?x", "?y"});
  ASSERT_TRUE(result.isFullyMaterialized());
  const auto& resultTable = result.idTable();
  ASSERT_EQ(resultTable.numRows(), 3);
  ASSERT_EQ(resultTable.numColumns(), 2);

  // Trivial IDs should match exactly.
  for (size_t row = 0; row < 3; ++row) {
    for (size_t col = 0; col < 2; ++col) {
      EXPECT_EQ(resultTable(row, col), table(row, col));
    }
  }
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, zeroColumns) {
  auto* qec = ad_utility::testing::getQec();

  // Create a table with 5 rows and 0 columns.
  IdTable table{0, ad_utility::makeUnlimitedAllocator<Id>()};
  table.resize(5);

  auto result = roundTrip(qec, qec, std::move(table), {});
  ASSERT_TRUE(result.isFullyMaterialized());
  EXPECT_EQ(result.idTable().numRows(), 5);
  EXPECT_EQ(result.idTable().numColumns(), 0);
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, emptyResult) {
  auto* qec = ad_utility::testing::getQec();

  IdTable table{3, ad_utility::makeUnlimitedAllocator<Id>()};
  // Zero rows.

  auto result = roundTrip(qec, qec, std::move(table), {"?a", "?b", "?c"});
  ASSERT_TRUE(result.isFullyMaterialized());
  EXPECT_EQ(result.idTable().numRows(), 0);
  EXPECT_EQ(result.idTable().numColumns(), 3);
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, nonTrivialIds) {
  auto* qec = ad_utility::testing::getQec();
  const auto& index = qec->getIndex();

  // Add some entries to a local vocab.
  LocalVocab localVocab;
  auto iri1 = ad_utility::triple_component::LiteralOrIri::iriref(
      "<http://example.org/testEntity1>");
  auto iri2 = ad_utility::triple_component::LiteralOrIri::iriref(
      "<http://example.org/testEntity2>");
  auto id1 = Id::makeFromLocalVocabIndex(
      localVocab.getIndexAndAddIfNotContained(iri1));
  auto id2 = Id::makeFromLocalVocabIndex(
      localVocab.getIndexAndAddIfNotContained(iri2));

  IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({Id::makeFromInt(42), id1});
  table.push_back({Id::makeFromInt(43), id2});

  auto result =
      roundTrip(qec, qec, table.clone(), {"?x", "?y"}, localVocab.clone());
  ASSERT_TRUE(result.isFullyMaterialized());
  const auto& resultTable = result.idTable();
  ASSERT_EQ(resultTable.numRows(), 2);

  // Trivial column should match exactly.
  EXPECT_EQ(resultTable(0, 0), Id::makeFromInt(42));
  EXPECT_EQ(resultTable(1, 0), Id::makeFromInt(43));

  // Non-trivial column: compare by resolved string.
  EXPECT_EQ(idToString(index, resultTable(0, 1), result.localVocab()),
            idToString(index, id1, localVocab));
  EXPECT_EQ(idToString(index, resultTable(1, 1), result.localVocab()),
            idToString(index, id2, localVocab));
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, differentPrefixMappingsPartialOverlap) {
  // Export QEC has prefix "http://example.org/", import QEC has both
  // "http://example.org/" and "http://other.org/".
  ad_utility::testing::TestIndexConfig exportConfig;
  exportConfig.encodedIriManager = EncodedIriManager({"http://example.org/"});

  ad_utility::testing::TestIndexConfig importConfig;
  importConfig.encodedIriManager =
      EncodedIriManager({"http://example.org/", "http://other.org/"});

  auto* exportQec = ad_utility::testing::getQec(exportConfig);
  auto* importQec = ad_utility::testing::getQec(importConfig);

  // Create an encoded IRI that matches the export prefix.
  auto encodedId = exportQec->getIndex().encodedIriManager().encode(
      "<http://example.org/42>");
  ASSERT_TRUE(encodedId.has_value());

  IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({encodedId.value(), Id::makeFromInt(100)});

  auto result = roundTrip(exportQec, importQec, table.clone(), {"?x", "?y"});
  ASSERT_TRUE(result.isFullyMaterialized());
  const auto& resultTable = result.idTable();
  ASSERT_EQ(resultTable.numRows(), 1);

  // The encoded IRI should be re-mapped to the import QEC's prefix encoding.
  EXPECT_EQ(resultTable(0, 0).getDatatype(), Datatype::EncodedVal);
  EXPECT_EQ(
      idToString(importQec->getIndex(), resultTable(0, 0), result.localVocab()),
      "<http://example.org/42>");
  EXPECT_EQ(resultTable(0, 1), Id::makeFromInt(100));
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, disjointPrefixMappings) {
  // Export QEC has prefix A, import QEC has prefix B (no overlap).
  ad_utility::testing::TestIndexConfig exportConfig;
  exportConfig.encodedIriManager =
      EncodedIriManager({"http://export-only.org/"});

  ad_utility::testing::TestIndexConfig importConfig;
  importConfig.encodedIriManager =
      EncodedIriManager({"http://import-only.org/"});

  auto* exportQec = ad_utility::testing::getQec(exportConfig);
  auto* importQec = ad_utility::testing::getQec(importConfig);

  auto encodedId = exportQec->getIndex().encodedIriManager().encode(
      "<http://export-only.org/123>");
  ASSERT_TRUE(encodedId.has_value());

  IdTable table{1, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({encodedId.value()});

  auto result = roundTrip(exportQec, importQec, table.clone(), {"?x"});
  ASSERT_TRUE(result.isFullyMaterialized());
  const auto& resultTable = result.idTable();
  ASSERT_EQ(resultTable.numRows(), 1);

  // No matching prefix on import side, so the IRI falls back to string-based
  // resolution (LocalVocab).
  EXPECT_EQ(
      idToString(importQec->getIndex(), resultTable(0, 0), result.localVocab()),
      "<http://export-only.org/123>");
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, lazyImport) {
  auto* qec = ad_utility::testing::getQec();

  IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({Id::makeFromInt(1), Id::makeFromInt(2)});
  table.push_back({Id::makeFromInt(3), Id::makeFromInt(4)});

  auto result =
      roundTrip(qec, qec, table.clone(), {"?x", "?y"}, LocalVocab{}, true);
  ASSERT_FALSE(result.isFullyMaterialized());

  // Consume the lazy result.
  IdTable aggregated{2, ad_utility::makeUnlimitedAllocator<Id>()};
  for (auto& [idTable, vocab] : result.idTables()) {
    aggregated.insertAtEnd(idTable);
  }
  ASSERT_EQ(aggregated.numRows(), 2);
  EXPECT_EQ(aggregated(0, 0), Id::makeFromInt(1));
  EXPECT_EQ(aggregated(0, 1), Id::makeFromInt(2));
  EXPECT_EQ(aggregated(1, 0), Id::makeFromInt(3));
  EXPECT_EQ(aggregated(1, 1), Id::makeFromInt(4));
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, trivialOnlyIdsNoVocabNeeded) {
  // All IDs are trivial, so no vocab is sent during export (but the trailing
  // vocab marker is still sent). Verify the importer handles this correctly.
  auto* qec = ad_utility::testing::getQec();

  IdTable table{1, ad_utility::makeUnlimitedAllocator<Id>()};
  for (int i = 0; i < 50; ++i) {
    table.push_back({Id::makeFromInt(i)});
  }

  auto result = roundTrip(qec, qec, table.clone(), {"?x"});
  ASSERT_TRUE(result.isFullyMaterialized());
  const auto& resultTable = result.idTable();
  ASSERT_EQ(resultTable.numRows(), 50);
  for (int i = 0; i < 50; ++i) {
    EXPECT_EQ(resultTable(i, 0), Id::makeFromInt(i));
  }
}
