// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <random>
#include <ranges>
#include <vector>

#include "../ValueGetterTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "ValuesForTesting.h"
#include "engine/BinaryExport.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryExecutionTree.h"
#include "global/Id.h"
#include "index/ExportIds.h"
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
      BinaryExportHelpers::read<std::vector<std::string>>(it, end);
  EXPECT_EQ(result, expected);
  EXPECT_EQ(it, end);
}

namespace {
// The `Id` with which the exporter refers to the `i`-th transmitted string of
// a batch, see `StringMapping`.
Id transmitted(size_t i) {
  return Id::makeFromLocalVocabIndex(
      reinterpret_cast<LocalVocabIndex>(i << Id::numDatatypeBits));
}

// A test index that encodes the IRIs with the prefix `http://example.org/`
// directly in the `Id`.
QueryExecutionContext* qecWithEncodedPrefix() {
  ad_utility::testing::TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://example.org/"};
  return ad_utility::testing::getQec(config);
}

// The plain prefix pattern for `prefix`, as used by `qecWithEncodedPrefix`.
encodedIri::Pattern prefixPattern(std::string prefix) {
  return encodedIri::plainPrefixPattern(std::move(prefix),
                                        EncodedIriManager::NumBitsEncoding);
}
}  // namespace

// _____________________________________________________________________________
TEST(IdRewriter, rewriteRows) {
  auto* qec = ad_utility::testing::getQec();

  // Create a test IdTable with references to the transmitted strings. The
  // first string is referenced twice.
  IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({Id::makeFromInt(41), transmitted(7)});
  table.push_back({Id::makeFromInt(42), transmitted(0)});
  table.push_back({Id::makeFromInt(43), transmitted(1)});
  table.push_back({Id::makeFromInt(44), transmitted(0)});
  table.push_back({Id::makeFromInt(45), transmitted(2)});

  // The first string is contained in the vocabulary of the index, the other
  // two are not.
  IdRewriter rewriter{*qec, {}, GeoPoint::encoding()};
  rewriter.startBatch({"<x>", "<http://example.org/a>", "\"literal\""});
  // The first row has already been rewritten (as part of a previous batch).
  rewriter.rewriteRows(table, 1);

  // The trivial IDs and the first row remain unchanged.
  EXPECT_EQ(table(0, 1).getBits(), transmitted(7).getBits());
  for (size_t i = 0; i < table.size(); ++i) {
    EXPECT_EQ(table(i, 0), Id::makeFromInt(41 + static_cast<int64_t>(i)));
  }

  // A string that is contained in the vocabulary of the index is converted to
  // its `VocabIndex` (bitwise, not only semantically), all occurrences in the
  // same way.
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  EXPECT_EQ(table(1, 1).getBits(), getId("<x>").getBits());
  EXPECT_EQ(table(3, 1).getBits(), getId("<x>").getBits());

  // The other strings are added to the local vocab.
  EXPECT_EQ(table(2, 1).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(table(4, 1).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(table(2, 1).getLocalVocabIndex()->toStringRepresentation(),
            "<http://example.org/a>");
  EXPECT_EQ(table(4, 1).getLocalVocabIndex()->toStringRepresentation(),
            "\"literal\"");
  EXPECT_EQ(rewriter.takeLocalVocab().size(), 2);
}

// _____________________________________________________________________________
TEST(IdRewriter, rewriteStringEncodesIris) {
  auto* qec = qecWithEncodedPrefix();
  IdRewriter rewriter{*qec, {}, GeoPoint::encoding()};
  rewriter.startBatch({"<http://example.org/42>"});

  // A transmitted string that can be encoded locally becomes an encoded IRI.
  EXPECT_EQ(
      rewriter.rewriteString(transmitted(0)),
      qec->getIndex().encodedIriManager().encode("<http://example.org/42>"));
  EXPECT_TRUE(rewriter.takeLocalVocab().empty());

  // A string that is not part of the batch.
  EXPECT_ANY_THROW(rewriter.rewriteString(transmitted(1)));
}

// _____________________________________________________________________________
TEST(IdRewriter, rewriteEncodedVal) {
  auto* qec = qecWithEncodedPrefix();
  const auto& manager = qec->getIndex().encodedIriManager();

  // The second remote pattern is the (only) local pattern, the first one
  // doesn't exist locally.
  IdRewriter rewriter{*qec,
                      {prefixPattern("<http://other.org/"),
                       prefixPattern("<http://example.org/")},
                      GeoPoint::encoding()};
  auto local = manager.encode("<http://example.org/42>").value();
  auto [localPrefixIdx, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(local);
  ASSERT_EQ(localPrefixIdx, 0);

  // A pattern that exists locally: only the index of the pattern changes.
  Id remote = EncodedIriManager::makeIdFromPrefixIdxAndPayload(1, payload);
  EXPECT_EQ(rewriter.rewriteEncodedVal(remote), local);
  EXPECT_EQ(rewriter(remote), local);

  // A pattern that doesn't exist locally: the IRI is decoded and stored in the
  // local vocab (consistently for repeated occurrences).
  Id remoteOther = EncodedIriManager::makeIdFromPrefixIdxAndPayload(0, payload);
  Id rewritten = rewriter.rewriteEncodedVal(remoteOther);
  ASSERT_EQ(rewritten.getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(rewritten.getLocalVocabIndex()->toStringRepresentation(),
            "<http://other.org/42>");
  EXPECT_EQ(rewriter.rewriteEncodedVal(remoteOther).getBits(),
            rewritten.getBits());
  EXPECT_EQ(rewriter.takeLocalVocab().size(), 1);
}

// _____________________________________________________________________________
TEST(IdRewriter, rewriteBlankNode) {
  auto* qec = ad_utility::testing::getQec();
  IdRewriter rewriter{*qec, {}, GeoPoint::encoding()};
  auto remote = [](uint64_t i) {
    return Id::makeFromBlankNodeIndex(BlankNodeIndex::make(i));
  };

  Id first = rewriter.rewriteBlankNode(remote(1));
  Id second = rewriter(remote(2));
  EXPECT_EQ(first.getDatatype(), Datatype::BlankNodeIndex);
  EXPECT_NE(first, second);
  EXPECT_EQ(rewriter.rewriteBlankNode(remote(1)), first);

  // The blank nodes are consistent also for the following batches.
  auto vocab = rewriter.takeLocalVocab();
  rewriter.startBatch({});
  EXPECT_EQ(rewriter.rewriteBlankNode(remote(2)), second);
}

// _____________________________________________________________________________
TEST(IdRewriter, rewriteGeoPoint) {
  using E = GeoPointEncodingEnum;
  auto* qec = ad_utility::testing::getQec();
  ASSERT_EQ(GeoPoint::encoding(), E::ZOrder);
  GeoPoint point{47.9, 7.8};
  Id local = Id::makeFromGeoPoint(point);

  // The same encoding: the `Id` is unchanged.
  EXPECT_EQ(IdRewriter(*qec, {}, E::ZOrder).rewriteGeoPoint(local), local);

  // A different encoding: the `Id` is converted.
  Id remote = [&point]() {
    absl::Cleanup resetEncoding{[]() { GeoPoint::setEncoding(E::ZOrder); }};
    GeoPoint::setEncoding(E::LatMajor);
    return Id::makeFromGeoPoint(point);
  }();
  ASSERT_NE(remote, local);
  IdRewriter rewriter{*qec, {}, E::LatMajor};
  EXPECT_EQ(rewriter.rewriteGeoPoint(remote), local);
  EXPECT_EQ(rewriter(remote), local);
}

// _____________________________________________________________________________
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

  // Return the bytes of the IDs in `ids`.
  auto bytesOf = [](std::vector<Id> ids) {
    std::string result;
    for (Id id : ids) {
      Id::T bits = id.getBits();
      result.append(reinterpret_cast<const char*>(&bits), sizeof(bits));
    }
    return result;
  };
  // A batch consists of the IDs, followed by a marker (which has the size of
  // an `Id`) and the strings.
  auto expectBatch = [&](const std::string& batch, std::vector<Id> ids,
                         const std::vector<std::string>& strings) {
    auto idBytes = bytesOf(std::move(ids));
    auto stringBytes = BinaryExportHelpers::writeVectorOfStrings(strings);
    ASSERT_EQ(batch.size(),
              idBytes.size() + sizeof(Id::T) + stringBytes.size());
    EXPECT_EQ(batch.substr(0, idBytes.size()), idBytes);
    EXPECT_EQ(batch.substr(idBytes.size() + sizeof(Id::T)), stringBytes);
  };

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
    auto word = [&](std::string_view s) {
      return Id::makeFromLocalVocabIndex(
          localVocab.getIndexAndAddIfNotContained(
              LocalVocabEntry::fromStringRepresentation(
                  std::string{s}, qec->getLocalVocabContext())));
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
    // The strings are referred to by their index in the batch (shifted by the
    // number of datatype bits, see `StringMapping`).
    auto string = [](size_t i) {
      return Id::makeFromLocalVocabIndex(
          reinterpret_cast<LocalVocabIndex>(i << Id::numDatatypeBits));
    };
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

// _____________________________________________________________________________
TEST(IdRewriter, getPrefixMapping) {
  auto* qec = qecWithEncodedPrefix();

  // Only the second remote pattern exists locally (as the first one).
  auto mapping = IdRewriter::getPrefixMapping(
      *qec, {prefixPattern("<http://other.org/"),
             prefixPattern("<http://example.org/")});
  EXPECT_THAT(mapping, ::testing::UnorderedElementsAre(::testing::Pair(1, 0)));

  EXPECT_TRUE(IdRewriter::getPrefixMapping(*qec, {}).empty());
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
    return HttpOrHttpsResponse{.status_ = boost::beast::http::status::ok,
                               .contentType_ = ad_utility::toString(
                                   ad_utility::MediaType::binaryQleverExport),
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
    return roundTrip(qet, importQec, std::move(varNames), requestLaziness);
  }

  // Export the result of `qet` (which has to contain all the `varNames`) and
  // import it into `importQec`. The imported result has the columns
  // `expectedVariables`, which default to `varNames`.
  static Result roundTrip(
      const QueryExecutionTree& qet, QueryExecutionContext* importQec,
      std::vector<std::string> varNames, bool requestLaziness = false,
      std::optional<std::vector<std::string>> expectedVariables =
          std::nullopt) {
    std::string bytes = collectExportBytes(qet, makeSelectClause(varNames));
    return importBinaryHttpResponse(requestLaziness,
                                    makeResponse(std::move(bytes)), *importQec,
                                    expectedVariables.value_or(varNames), {});
  }

  // Return the (possibly lazy) `result` as a single table, together with all
  // its local vocabs merged into one.
  static std::pair<IdTable, LocalVocab> materialize(const Result& result,
                                                    size_t numColumns) {
    if (result.isFullyMaterialized()) {
      return {result.idTableView().clone(), result.localVocab().clone()};
    }
    auto [table, vocabs] = aggregateTables(result.idTables(), numColumns);
    LocalVocab vocab;
    vocab.mergeWith(vocabs);
    return {std::move(table), std::move(vocab)};
  }

  // Resolve an Id to its string representation using a given index and
  // local vocab.
  static std::string idToString(const Index& index, Id id,
                                const LocalVocab& localVocab) {
    auto optLitOrIri =
        ql::exportIds::idToLiteralOrIri(index.getImpl(), id, localVocab);
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
  const auto& resultTable = result.idTableView();
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
  EXPECT_EQ(result.idTableView().numRows(), 5);
  EXPECT_EQ(result.idTableView().numColumns(), 0);
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, emptyResult) {
  auto* qec = ad_utility::testing::getQec();

  IdTable table{3, ad_utility::makeUnlimitedAllocator<Id>()};
  // Zero rows.

  auto result = roundTrip(qec, qec, std::move(table), {"?a", "?b", "?c"});
  ASSERT_TRUE(result.isFullyMaterialized());
  EXPECT_EQ(result.idTableView().numRows(), 0);
  EXPECT_EQ(result.idTableView().numColumns(), 3);
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, nonTrivialIds) {
  auto* qec = ad_utility::testing::getQec();
  const auto& index = qec->getIndex();

  // Add some entries to a local vocab.
  LocalVocab localVocab;
  auto iri1 = LocalVocabEntry::fromIriref("<http://example.org/testEntity1>",
                                          qec->getLocalVocabContext());
  auto iri2 = LocalVocabEntry::fromIriref("<http://example.org/testEntity2>",
                                          qec->getLocalVocabContext());
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
  const auto& resultTable = result.idTableView();
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
  exportConfig.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://example.org/"};

  ad_utility::testing::TestIndexConfig importConfig;
  importConfig.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://example.org/", "http://other.org/"};

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
  const auto& resultTable = result.idTableView();
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
  exportConfig.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://export-only.org/"};

  ad_utility::testing::TestIndexConfig importConfig;
  importConfig.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://import-only.org/"};

  auto* exportQec = ad_utility::testing::getQec(exportConfig);
  auto* importQec = ad_utility::testing::getQec(importConfig);

  auto encodedId = exportQec->getIndex().encodedIriManager().encode(
      "<http://export-only.org/123>");
  ASSERT_TRUE(encodedId.has_value());

  IdTable table{1, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({encodedId.value()});

  auto result = roundTrip(exportQec, importQec, table.clone(), {"?x"});
  ASSERT_TRUE(result.isFullyMaterialized());
  const auto& resultTable = result.idTableView();
  ASSERT_EQ(resultTable.numRows(), 1);

  // No matching prefix on import side, so the IRI falls back to string-based
  // resolution (LocalVocab).
  EXPECT_EQ(
      idToString(importQec->getIndex(), resultTable(0, 0), result.localVocab()),
      "<http://export-only.org/123>");
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, encodedIriIsReencodedWithDifferentPattern) {
  // The prefixes of the two instances are different, but overlap, so an IRI
  // that the exporting instance encodes with its prefix can also be encoded
  // with the (different) prefix of the importing instance.
  ad_utility::testing::TestIndexConfig exportConfig;
  exportConfig.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://overlap.org/"};

  ad_utility::testing::TestIndexConfig importConfig;
  importConfig.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://overlap.org/1"};

  auto* exportQec = ad_utility::testing::getQec(exportConfig);
  auto* importQec = ad_utility::testing::getQec(importConfig);

  static constexpr std::string_view iri = "<http://overlap.org/123>";
  auto encodedId = exportQec->getIndex().encodedIriManager().encode(iri);
  ASSERT_TRUE(encodedId.has_value());
  auto expectedId = importQec->getIndex().encodedIriManager().encode(iri);
  ASSERT_TRUE(expectedId.has_value());
  ASSERT_NE(encodedId.value(), expectedId.value());

  // The same IRI twice, which is decoded only once.
  IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({encodedId.value(), encodedId.value()});

  auto result = roundTrip(exportQec, importQec, table.clone(), {"?x", "?y"});
  ASSERT_TRUE(result.isFullyMaterialized());
  const auto& resultTable = result.idTableView();
  ASSERT_EQ(resultTable.numRows(), 1);
  EXPECT_EQ(resultTable(0, 0), expectedId.value());
  EXPECT_EQ(resultTable(0, 1), expectedId.value());
  EXPECT_TRUE(result.localVocab().empty());
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
  const auto& resultTable = result.idTableView();
  ASSERT_EQ(resultTable.numRows(), 50);
  for (int i = 0; i < 50; ++i) {
    EXPECT_EQ(resultTable(i, 0), Id::makeFromInt(i));
  }
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, allDatatypes) {
  using namespace valueGetterTestHelpers;
  AllDatatypesTestContext testContext;
  auto* qec = testContext.qec;
  const auto& index = qec->getIndex();

  // One `Id` per datatype that the export has to handle.
  Id blankNode = Id::makeFromBlankNodeIndex(BlankNodeIndex::make(42));
  std::vector<Id> ids{
      Id::makeUndefined(),
      Id::makeFromBool(true),
      Id::makeFromInt(-42),
      Id::makeFromDouble(1.5),
      testContext.getId(vocabTypedLiteral),
      testContext.localVocabId("\"onlyInLocalVocab\"@de"),
      testContext.secondaryVocabId(secondaryLangLiteral),
      Id::makeFromTextRecordIndex(TextRecordIndex::make(0)),
      Id::makeFromDate(DateYearOrDuration::parseXsdDate("2000-01-01")),
      Id::makeFromGeoPoint(GeoPoint{47.9, 7.8}),
      Id::makeFromWordVocabIndex(WordVocabIndex::make(0)),
      blankNode,
      testContext.encodedIriId()};
  // Make sure that every datatype is covered.
  ASSERT_EQ(ids.size(), static_cast<size_t>(Datatype::MaxValue) + 1);
  for (size_t i = 0; i < ids.size(); ++i) {
    ASSERT_EQ(static_cast<size_t>(ids[i].getDatatype()), i);
  }

  // Every datatype gets its own column, and the blank node appears twice.
  std::vector<std::string> varNames;
  for (size_t i = 0; i < ids.size(); ++i) {
    varNames.push_back(absl::StrCat("?col", i));
  }
  varNames.push_back("?secondBlankNode");
  IdTable table{varNames.size(), ad_utility::makeUnlimitedAllocator<Id>()};
  table.emplace_back();
  for (size_t i = 0; i < ids.size(); ++i) {
    table(0, i) = ids[i];
  }
  table(0, ids.size()) = blankNode;

  for (bool requestLaziness : {false, true}) {
    auto result = roundTrip(qec, qec, table.clone(), varNames,
                            testContext.localVocab.clone(), requestLaziness);
    auto [resultTable, resultVocab] = materialize(result, varNames.size());
    ASSERT_EQ(resultTable.numRows(), 1);

    for (size_t i = 0; i < ids.size(); ++i) {
      Id original = ids[i];
      Id imported = resultTable(0, i);
      switch (original.getDatatype()) {
        case Datatype::BlankNodeIndex:
          // A new local blank node, which is the same for both occurrences.
          EXPECT_EQ(imported.getDatatype(), Datatype::BlankNodeIndex);
          EXPECT_NE(imported, original);
          EXPECT_EQ(imported, resultTable(0, ids.size()));
          break;
        case Datatype::TextRecordIndex:
        case Datatype::WordVocabIndex:
        case Datatype::LocalVocabIndex:
          // These are transferred as strings, which are then looked up in the
          // vocabularies, or stored in the local vocab.
          EXPECT_EQ(idToString(index, imported, resultVocab),
                    idToString(index, original, testContext.localVocab));
          break;
        default:
          // The import uses the same index as the export, so the strings are
          // found in the vocabularies and the IDs are the same (bitwise, not
          // only semantically, which `operator==` would also accept for an
          // `Id` of the local vocab).
          EXPECT_EQ(imported.getBits(), original.getBits()) << i;
      }
    }
  }
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, variablesAreMatchedByName) {
  auto* qec = ad_utility::testing::getQec();
  IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({Id::makeFromInt(1), Id::makeFromInt(2)});
  auto qet = makeQet(qec, std::move(table), {Variable{"?x"}, Variable{"?y"}});

  // The columns are reordered, `?z` is not part of the result, so it is
  // undefined.
  auto result = roundTrip(qet, qec, {"?x", "?y"}, false,
                          std::vector<std::string>{"?y", "?z", "?x"});
  const auto& resultTable = result.idTableView();
  ASSERT_EQ(resultTable.numRows(), 1);
  ASSERT_EQ(resultTable.numColumns(), 3);
  EXPECT_EQ(resultTable(0, 0), Id::makeFromInt(2));
  EXPECT_EQ(resultTable(0, 1), Id::makeUndefined());
  EXPECT_EQ(resultTable(0, 2), Id::makeFromInt(1));

  // The same for a result without any columns.
  IdTable noColumns{0, ad_utility::makeUnlimitedAllocator<Id>()};
  noColumns.resize(2);
  auto qetNoColumns = makeQet(qec, std::move(noColumns), {});
  result =
      roundTrip(qetNoColumns, qec, {}, false, std::vector<std::string>{"?z"});
  ASSERT_EQ(result.idTableView().numRows(), 2);
  ASSERT_EQ(result.idTableView().numColumns(), 1);
  EXPECT_EQ(result.idTableView()(1, 0), Id::makeUndefined());
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, geoPointsAreConvertedToTheLocalEncoding) {
  auto* qec = ad_utility::testing::getQec();
  GeoPoint point{47.9, 7.8};
  ASSERT_EQ(GeoPoint::encoding(), GeoPointEncodingEnum::ZOrder);
  IdTable table{1, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({Id::makeFromGeoPoint(point)});
  auto bytes =
      collectExportBytes(makeQet(qec, std::move(table), {Variable{"?x"}}),
                         makeSelectClause({"?x"}));

  // Import with a different encoding, as if the importing QLever instance used
  // an index with a different encoding.
  GeoPoint::setEncoding(GeoPointEncodingEnum::LatMajor);
  absl::Cleanup resetEncoding{
      []() { GeoPoint::setEncoding(GeoPointEncodingEnum::ZOrder); }};
  auto result = importBinaryHttpResponse(false, makeResponse(std::move(bytes)),
                                         *qec, {"?x"}, {});
  Id imported = result.idTableView()(0, 0);
  EXPECT_EQ(imported, Id::makeFromGeoPoint(point));
  EXPECT_EQ(imported.getGeoPoint(), Id::makeFromGeoPoint(point).getGeoPoint());
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, invalidHeader) {
  auto* qec = ad_utility::testing::getQec();
  IdTable table{1, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({Id::makeFromInt(1)});
  auto bytes =
      collectExportBytes(makeQet(qec, std::move(table), {Variable{"?x"}}),
                         makeSelectClause({"?x"}));
  auto import = [qec](std::string bytes) {
    return importBinaryHttpResponse(false, makeResponse(std::move(bytes)), *qec,
                                    {"?x"}, {});
  };

  // The header starts with the magic bytes (serialized as their size followed
  // by the characters) and the version.
  std::string_view magic = "QLEVER.EXPORT";
  size_t versionOffset = sizeof(size_t) + magic.size();
  ASSERT_EQ(bytes.substr(sizeof(size_t), magic.size()), magic);
  uint16_t version;
  std::memcpy(&version, bytes.data() + versionOffset, sizeof(version));
  ASSERT_EQ(version, ad_utility::binaryQleverExportVersion);

  auto wrongVersion = bytes;
  ++wrongVersion[versionOffset];
  AD_EXPECT_THROW_WITH_MESSAGE(
      import(wrongVersion),
      ::testing::HasSubstr("only version 1 is supported"));

  auto wrongMagic = bytes;
  wrongMagic[sizeof(size_t)] = 'X';
  AD_EXPECT_THROW_WITH_MESSAGE(
      import(wrongMagic),
      ::testing::HasSubstr("not in QLever's binary export format"));

  // The result ends with the last row, the marker, and the (empty) trailing
  // batch of strings. Cut it off in the middle of that batch, directly after
  // the last row, and in the middle of the last row.
  size_t endOfLastRow = bytes.size() - sizeof(Id::T) - sizeof(size_t);
  EXPECT_ANY_THROW(import(bytes.substr(0, bytes.size() - 1)));
  AD_EXPECT_THROW_WITH_MESSAGE(import(bytes.substr(0, endOfLastRow)),
                               ::testing::HasSubstr("ended unexpectedly"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      import(bytes.substr(0, endOfLastRow - sizeof(Id::T) / 2)),
      ::testing::HasSubstr("Stream ended unexpectedly"));
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, lazyExportWithSeveralLocalVocabs) {
  auto* qec = ad_utility::testing::getQec();
  Id blankNode = Id::makeFromBlankNodeIndex(BlankNodeIndex::make(7));

  // Two tables, each with its own local vocab, and the same blank node.
  std::vector<Result::IdTableVocabPair> tables;
  std::vector<std::string> words{"\"first\"", "\"second\""};
  for (const auto& word : words) {
    LocalVocab localVocab;
    Id id = Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::fromStringRepresentation(
            word, qec->getLocalVocabContext())));
    IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
    table.push_back({id, blankNode});
    tables.emplace_back(std::move(table), std::move(localVocab));
  }

  for (bool requestLaziness : {false, true}) {
    std::vector<Result::IdTableVocabPair> tablesCopy;
    for (const auto& [table, vocab] : tables) {
      tablesCopy.emplace_back(table.clone(), vocab.clone());
    }
    QueryExecutionTree qet{qec, std::make_shared<ValuesForTesting>(
                                    qec, std::move(tablesCopy),
                                    std::vector<std::optional<Variable>>{
                                        Variable{"?x"}, Variable{"?y"}})};
    auto result = roundTrip(qet, qec, {"?x", "?y"}, requestLaziness);

    auto [resultTable, resultVocab] = materialize(result, 2);
    ASSERT_EQ(resultTable.numRows(), 2);
    for (size_t row = 0; row < 2; ++row) {
      EXPECT_EQ(idToString(qec->getIndex(), resultTable(row, 0), resultVocab),
                words[row]);
    }
    EXPECT_EQ(resultTable(0, 1).getDatatype(), Datatype::BlankNodeIndex);
    EXPECT_EQ(resultTable(0, 1), resultTable(1, 1));
  }
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, severalBatches) {
  auto* qec = ad_utility::testing::getQec();
  // Large distinct strings (of about 1 MB each), which take up more than the
  // minimum size of the strings of a batch, see `ExportBatch`.
  static constexpr size_t stringSize = 1 << 20;
  static constexpr size_t numRows = minNumStringBytesPerBatch / stringSize + 2;
  auto word = [](size_t i) {
    return absl::StrCat("\"word", i, std::string(stringSize, 'x'), "\"");
  };
  LocalVocab localVocab;
  IdTable table{1, ad_utility::makeUnlimitedAllocator<Id>()};
  for (size_t i = 0; i < numRows; ++i) {
    table.push_back(
        {Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
            LocalVocabEntry::fromStringRepresentation(
                word(i), qec->getLocalVocabContext())))});
  }
  for (bool requestLaziness : {false, true}) {
    auto result = roundTrip(qec, qec, table.clone(), {"?x"}, localVocab.clone(),
                            requestLaziness);
    auto [resultTable, resultVocab] = materialize(result, 1);
    ASSERT_EQ(resultTable.numRows(), numRows);
    for (size_t i = 0; i < numRows; ++i) {
      ASSERT_EQ(idToString(qec->getIndex(), resultTable(i, 0), resultVocab),
                word(i));
    }
  }
  {
    // The strings are sent in several batches.
    auto result =
        roundTrip(qec, qec, table.clone(), {"?x"}, localVocab.clone(), true);
    auto [resultTable, resultVocabs] = aggregateTables(result.idTables(), 1);
    EXPECT_GE(resultVocabs.size(), 2);
  }

  // More IDs than the minimum size of the IDs of a batch.
  static constexpr size_t numIntRows =
      minNumIdBytesPerBatch / sizeof(Id::T) + 1;
  IdTable intTable{1, ad_utility::makeUnlimitedAllocator<Id>()};
  for (size_t i = 0; i < numIntRows; ++i) {
    intTable.push_back({Id::makeFromInt(static_cast<int64_t>(i))});
  }
  auto result =
      roundTrip(qec, qec, intTable.clone(), {"?x"}, LocalVocab{}, true);
  auto [resultTable, resultVocabs] = aggregateTables(result.idTables(), 1);
  EXPECT_GE(resultVocabs.size(), 2);
  EXPECT_EQ(resultTable, intTable);
}
