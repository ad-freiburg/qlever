// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

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

// A test index that encodes the IRIs with one of the `prefixes` directly in
// the `Id`.
QueryExecutionContext* qecWithEncodedPrefixes(
    std::vector<std::string> prefixes = {"http://example.org/"}) {
  ad_utility::testing::TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets = std::move(prefixes);
  return ad_utility::testing::getQec(config);
}

// The plain prefix pattern for `prefix`, as used by `qecWithEncodedPrefixes`.
encodedIri::Pattern prefixPattern(std::string prefix) {
  return encodedIri::plainPrefixPattern(std::move(prefix),
                                        EncodedIriManager::NumBitsEncoding);
}

// Add `word` to the `localVocab` and return its `Id`.
Id localVocabId(LocalVocab& localVocab, std::string word,
                const QueryExecutionContext& qec) {
  return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
      LocalVocabEntry::fromStringRepresentation(std::move(word),
                                                qec.getLocalVocabContext())));
}

// Return the result of `f`, which is called while `GeoPoint`s use the
// `encoding` (the default encoding is restored afterwards).
template <typename F>
auto withGeoPointEncoding(GeoPointEncodingEnum encoding, F f) {
  absl::Cleanup resetEncoding{
      []() { GeoPoint::setEncoding(GeoPointEncodingEnum::ZOrder); }};
  GeoPoint::setEncoding(encoding);
  return f();
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
  auto* qec = qecWithEncodedPrefixes();
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
  auto* qec = qecWithEncodedPrefixes();
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
  Id remote = withGeoPointEncoding(
      E::LatMajor, [&point]() { return Id::makeFromGeoPoint(point); });
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

// _____________________________________________________________________________
TEST(IdRewriter, getPrefixMapping) {
  auto* qec = qecWithEncodedPrefixes();

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
  // The `Variable`s with the given names.
  static std::vector<Variable> toVariables(
      const std::vector<std::string>& varNames) {
    return ad_utility::transform(
        varNames, [](const std::string& name) { return Variable{name}; });
  }

  // Create a QueryExecutionTree from an IdTable and variable names.
  static QueryExecutionTree makeQet(QueryExecutionContext* qec, IdTable table,
                                    const std::vector<std::string>& varNames,
                                    LocalVocab localVocab = LocalVocab{}) {
    auto values = std::make_shared<ValuesForTesting>(
        qec, std::move(table),
        ad_utility::transform(
            toVariables(varNames),
            [](Variable v) -> std::optional<Variable> { return v; }),
        false, std::vector<ColumnIndex>{}, std::move(localVocab));
    return QueryExecutionTree{qec, std::move(values)};
  }

  // Export the columns `varNames` of the result of `qet` and collect all the
  // bytes into a single string.
  static std::string exportBytes(const QueryExecutionTree& qet,
                                 const std::vector<std::string>& varNames) {
    parsedQuery::SelectClause selectClause;
    selectClause.setSelected(toVariables(varNames));
    auto generator = exportAsQLeverBinary(
        qet, selectClause, {},
        std::make_shared<ad_utility::CancellationHandle<>>());
    std::string result;
    for (std::string_view chunk : generator) {
      result.append(chunk);
    }
    return result;
  }

  // Import the `bytes` into `qec`, as if they were the body of an HTTP
  // response (which arrives in randomly sized chunks).
  static Result importBytes(std::string bytes, QueryExecutionContext* qec,
                            std::vector<std::string> expectedVariables,
                            bool requestLaziness = false) {
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
    HttpOrHttpsResponse response{.status_ = boost::beast::http::status::ok,
                                 .contentType_ = ad_utility::toString(
                                     ad_utility::MediaType::binaryQleverExport),
                                 .location_ = {},
                                 .body_ = body(std::move(bytes))};
    return importBinaryHttpResponse(requestLaziness, std::move(response), *qec,
                                    std::move(expectedVariables), {});
  }

  // Export the result of `qet` (which has to contain all the `varNames`) and
  // import it into `importQec`. The imported result has the columns
  // `expectedVariables`, which default to `varNames`.
  static Result roundTrip(
      const QueryExecutionTree& qet, QueryExecutionContext* importQec,
      const std::vector<std::string>& varNames, bool requestLaziness = false,
      std::optional<std::vector<std::string>> expectedVariables =
          std::nullopt) {
    return importBytes(exportBytes(qet, varNames), importQec,
                       expectedVariables.value_or(varNames), requestLaziness);
  }

  // Full round-trip of the `table` with the columns `varNames`: export from
  // `exportQec`, import into `importQec`.
  static Result roundTrip(QueryExecutionContext* exportQec,
                          QueryExecutionContext* importQec, IdTable table,
                          const std::vector<std::string>& varNames,
                          LocalVocab localVocab = LocalVocab{},
                          bool requestLaziness = false) {
    auto qet =
        makeQet(exportQec, std::move(table), varNames, std::move(localVocab));
    return roundTrip(qet, importQec, varNames, requestLaziness);
  }

  // The (possibly lazy) result as a single table, together with all its local
  // vocabs merged into one, and the number of its blocks.
  struct Materialized {
    IdTable table_;
    LocalVocab localVocab_;
    size_t numBlocks_;
  };
  static Materialized materialize(const Result& result, size_t numColumns) {
    if (result.isFullyMaterialized()) {
      return {result.idTableView().clone(), result.localVocab().clone(), 1};
    }
    auto [table, vocabs] = aggregateTables(result.idTables(), numColumns);
    LocalVocab vocab;
    vocab.mergeWith(vocabs);
    return {std::move(table), std::move(vocab), vocabs.size()};
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
TEST_F(BinaryExportRoundTrip, noRowsOrNoColumns) {
  auto* qec = ad_utility::testing::getQec();

  IdTable noRows{3, ad_utility::makeUnlimitedAllocator<Id>()};
  auto result = roundTrip(qec, qec, std::move(noRows), {"?a", "?b", "?c"});
  ASSERT_TRUE(result.isFullyMaterialized());
  EXPECT_EQ(result.idTableView().numRows(), 0);
  EXPECT_EQ(result.idTableView().numColumns(), 3);

  IdTable noColumns{0, ad_utility::makeUnlimitedAllocator<Id>()};
  noColumns.resize(5);
  result = roundTrip(qec, qec, std::move(noColumns), {});
  ASSERT_TRUE(result.isFullyMaterialized());
  EXPECT_EQ(result.idTableView().numRows(), 5);
  EXPECT_EQ(result.idTableView().numColumns(), 0);
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, encodedIrisWithDifferentPrefixes) {
  // Export the `iri`, which the exporting instance encodes with one of its
  // `exportPrefixes`, to an instance with the `importPrefixes`. If one of the
  // latter matches (`expectEncoded`), the IRI is encoded with it, otherwise it
  // is stored in the local vocab.
  auto test = [](std::vector<std::string> exportPrefixes,
                 std::vector<std::string> importPrefixes, std::string_view iri,
                 bool expectEncoded) {
    auto* exportQec = qecWithEncodedPrefixes(std::move(exportPrefixes));
    auto* importQec = qecWithEncodedPrefixes(std::move(importPrefixes));
    Id exported = exportQec->getIndex().encodedIriManager().encode(iri).value();
    auto expected = importQec->getIndex().encodedIriManager().encode(iri);
    ASSERT_EQ(expected.has_value(), expectEncoded);

    // The same IRI twice, which is converted (and decoded) only once.
    auto result = roundTrip(
        exportQec, importQec,
        makeIdTableFromVector({{exported, exported, Id::makeFromInt(100)}}),
        {"?x", "?y", "?z"});
    ASSERT_TRUE(result.isFullyMaterialized());
    const auto& resultTable = result.idTableView();
    ASSERT_EQ(resultTable.numRows(), 1);
    EXPECT_EQ(resultTable(0, 1).getBits(), resultTable(0, 0).getBits());
    EXPECT_EQ(resultTable(0, 2), Id::makeFromInt(100));
    if (expectEncoded) {
      EXPECT_NE(expected.value(), exported);
      EXPECT_EQ(resultTable(0, 0), expected.value());
      EXPECT_TRUE(result.localVocab().empty());
    } else {
      EXPECT_EQ(resultTable(0, 0).getDatatype(), Datatype::LocalVocabIndex);
      EXPECT_EQ(idToString(importQec->getIndex(), resultTable(0, 0),
                           result.localVocab()),
                iri);
    }
  };
  // The prefix also exists on the importing side, but has a different index
  // there (the prefixes are sorted).
  test({"http://example.org/"}, {"http://a.org/", "http://example.org/"},
       "<http://example.org/42>", true);
  // The prefixes are different, but overlap.
  test({"http://overlap.org/"}, {"http://overlap.org/1"},
       "<http://overlap.org/123>", true);
  // The prefixes are disjoint.
  test({"http://export-only.org/"}, {"http://import-only.org/"},
       "<http://export-only.org/123>", false);
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
    EXPECT_EQ(result.isFullyMaterialized(), !requestLaziness);
    auto [resultTable, resultVocab, numBlocks] =
        materialize(result, varNames.size());
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
  auto qet = makeQet(qec, makeIdTableFromVector({{1, 2}}, &Id::makeFromInt),
                     {"?x", "?y"});

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
  result = roundTrip(makeQet(qec, std::move(noColumns), {}), qec, {}, false,
                     std::vector<std::string>{"?z"});
  ASSERT_EQ(result.idTableView().numRows(), 2);
  ASSERT_EQ(result.idTableView().numColumns(), 1);
  EXPECT_EQ(result.idTableView()(1, 0), Id::makeUndefined());
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, geoPointsAreConvertedToTheLocalEncoding) {
  auto* qec = ad_utility::testing::getQec();
  GeoPoint point{47.9, 7.8};
  ASSERT_EQ(GeoPoint::encoding(), GeoPointEncodingEnum::ZOrder);
  Id exported = Id::makeFromGeoPoint(point);
  auto bytes = exportBytes(
      makeQet(qec, makeIdTableFromVector({{exported}}), {"?x"}), {"?x"});

  // Import with a different encoding, as if the importing QLever instance used
  // an index with a different encoding.
  auto [imported, expected] =
      withGeoPointEncoding(GeoPointEncodingEnum::LatMajor, [&]() {
        return std::pair{
            importBytes(std::move(bytes), qec, {"?x"}).idTableView()(0, 0),
            Id::makeFromGeoPoint(point)};
      });
  ASSERT_NE(expected, exported);
  EXPECT_EQ(imported, expected);
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, invalidHeader) {
  auto* qec = ad_utility::testing::getQec();
  auto bytes = exportBytes(
      makeQet(qec, makeIdTableFromVector({{1}}, &Id::makeFromInt), {"?x"}),
      {"?x"});
  auto import = [qec](std::string bytes) {
    return importBytes(std::move(bytes), qec, {"?x"});
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
  std::vector<std::string> words{"\"first\"", "\"second\""};

  for (bool requestLaziness : {false, true}) {
    // Two tables, each with its own local vocab, and the same blank node.
    std::vector<Result::IdTableVocabPair> tables;
    for (const auto& word : words) {
      LocalVocab localVocab;
      Id id = localVocabId(localVocab, word, *qec);
      tables.emplace_back(makeIdTableFromVector({{id, blankNode}}),
                          std::move(localVocab));
    }
    QueryExecutionTree qet{qec, std::make_shared<ValuesForTesting>(
                                    qec, std::move(tables),
                                    std::vector<std::optional<Variable>>{
                                        Variable{"?x"}, Variable{"?y"}})};
    auto result = roundTrip(qet, qec, {"?x", "?y"}, requestLaziness);

    auto [resultTable, resultVocab, numBlocks] = materialize(result, 2);
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
    table.push_back({localVocabId(localVocab, word(i), *qec)});
  }
  for (bool requestLaziness : {false, true}) {
    auto result = roundTrip(qec, qec, table.clone(), {"?x"}, localVocab.clone(),
                            requestLaziness);
    auto [resultTable, resultVocab, numBlocks] = materialize(result, 1);
    ASSERT_EQ(resultTable.numRows(), numRows);
    for (size_t i = 0; i < numRows; ++i) {
      ASSERT_EQ(idToString(qec->getIndex(), resultTable(i, 0), resultVocab),
                word(i));
    }
    // Each batch becomes a block of the lazy result.
    EXPECT_GE(numBlocks, requestLaziness ? 2 : 1);
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
  auto [resultTable, resultVocab, numBlocks] = materialize(result, 1);
  EXPECT_GE(numBlocks, 2);
  EXPECT_EQ(resultTable, intTable);
}
