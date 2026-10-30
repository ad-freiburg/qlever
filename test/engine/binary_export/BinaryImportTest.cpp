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

#include <cstring>
#include <random>
#include <ranges>
#include <vector>

#include "../../ValueGetterTestHelpers.h"
#include "./BinaryExportTestHelpers.h"
#include "engine/binary_export/BinaryExport.h"
#include "engine/binary_export/BinaryImport.h"
#include "index/ExportIds.h"
#include "util/http/HttpClient.h"

using namespace qlever::binary_export;
using namespace binaryExportTestHelpers;

// Test fixture with helpers for the end-to-end round-trip tests of the binary
// export and import.
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

  // Export the `varNames` of the result of `qet` and collect all the bytes
  // into a single string.
  static std::string exportBytes(const QueryExecutionTree& qet,
                                 const std::vector<std::string>& varNames,
                                 LimitOffsetClause limitAndOffset = {}) {
    parsedQuery::SelectClause selectClause;
    selectClause.setSelected(toVariables(varNames));
    auto generator = exportAsQLeverBinary(
        qet, selectClause, limitAndOffset,
        std::make_shared<ad_utility::CancellationHandle<>>());
    std::string result;
    for (std::string_view chunk : generator) {
      result.append(chunk);
    }
    return result;
  }

  // Import the `bytes` into `qec`, as if they were the body of an HTTP
  // response (which arrives in randomly sized chunks, each of which is only
  // valid until the next one is requested).
  static Result importBytes(
      std::string bytes, QueryExecutionContext* qec,
      std::vector<std::string> expectedVariables, bool requestLaziness = false,
      ad_utility::MemorySize maxStringBatchSize =
          ad_utility::MemorySize::gigabytes(1),
      ad_utility::SharedCancellationHandle cancellationHandle =
          std::make_shared<ad_utility::CancellationHandle<>>()) {
    auto body =
        [](std::string data) -> cppcoro::generator<ql::span<std::byte>> {
      std::mt19937 rng{std::random_device{}()};
      std::uniform_int_distribution<size_t> distribution{1,
                                                         data.size() / 2 + 1};
      for (size_t start = 0; start < data.size();) {
        size_t chunkSize = std::min(distribution(rng), data.size() - start);
        std::string chunk = data.substr(start, chunkSize);
        co_yield ql::as_writable_bytes(ql::span{chunk});
        // Make sure that the chunk is not used after it has been "received".
        std::ranges::fill(chunk, '\0');
        start += chunkSize;
      }
    };
    HttpOrHttpsResponse response{.status_ = boost::beast::http::status::ok,
                                 .contentType_ = ad_utility::toString(
                                     ad_utility::MediaType::binaryQleverExport),
                                 .location_ = {},
                                 .body_ = body(std::move(bytes))};
    return importBinaryHttpResponse(requestLaziness, std::move(response), *qec,
                                    std::move(expectedVariables), {},
                                    maxStringBatchSize,
                                    std::move(cancellationHandle));
  }

  // Export the `varNames` of the result of `qet` and import them into
  // `importQec`.
  static Result roundTrip(const QueryExecutionTree& qet,
                          QueryExecutionContext* importQec,
                          const std::vector<std::string>& varNames,
                          bool requestLaziness = false) {
    return importBytes(exportBytes(qet, varNames), importQec, varNames,
                       requestLaziness);
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
  auto qet = makeQet(qec, std::move(noColumns), {});
  result = roundTrip(qet, qec, {});
  ASSERT_TRUE(result.isFullyMaterialized());
  EXPECT_EQ(result.idTableView().numRows(), 5);
  EXPECT_EQ(result.idTableView().numColumns(), 0);

  // Only the exported rows are counted.
  result = importBytes(
      exportBytes(qet, {}, {._limit = 3, ._offset = 1, .exportLimit_ = 2}), qec,
      {});
  EXPECT_EQ(result.idTableView().numRows(), 2);
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

  // The columns are reordered, `?z` is selected, but not bound in the result,
  // so it is undefined.
  auto bytes = exportBytes(qet, {"?x", "?y", "?z"});
  auto result = importBytes(bytes, qec, {"?y", "?z", "?x"});
  const auto& resultTable = result.idTableView();
  ASSERT_EQ(resultTable.numRows(), 1);
  ASSERT_EQ(resultTable.numColumns(), 3);
  EXPECT_EQ(resultTable(0, 0), Id::makeFromInt(2));
  EXPECT_EQ(resultTable(0, 1), Id::makeUndefined());
  EXPECT_EQ(resultTable(0, 2), Id::makeFromInt(1));

  // The variables of the result have to be exactly the expected ones.
  for (const std::vector<std::string>& wrongVariables :
       std::vector<std::vector<std::string>>{
           {"?x", "?y"}, {"?x", "?y", "?z", "?w"}, {"?x", "?y", "?w"}}) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        importBytes(bytes, qec, wrongVariables),
        ::testing::HasSubstr("contains the variables \"?x ?y ?z\", but"));
  }

  // The same for a result without any columns.
  IdTable noColumns{0, ad_utility::makeUnlimitedAllocator<Id>()};
  noColumns.resize(2);
  result = roundTrip(makeQet(qec, std::move(noColumns), {}), qec, {"?z"});
  ASSERT_EQ(result.idTableView().numRows(), 2);
  ASSERT_EQ(result.idTableView().numColumns(), 1);
  EXPECT_EQ(result.idTableView()(1, 0), Id::makeUndefined());
}

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, geoPointsAreConvertedToTheLocalEncoding) {
  auto* qec = ad_utility::testing::getQec();
  GeoPoint point{47.9, 7.8};
  ASSERT_EQ(GeoPoint::encoding(), GeoPointEncodingEnum::ZOrder);

  // Export with the `exportEncoding` and import with the `importEncoding`, as
  // if the two QLever instances used indices with different encodings.
  auto test = [qec, &point](GeoPointEncodingEnum exportEncoding,
                            GeoPointEncodingEnum importEncoding) {
    // The cache key of `ValuesForTesting` contains the decoded points, which
    // are the same for both encodings.
    qec->clearCacheUnpinnedOnly();
    Id exported = withGeoPointEncoding(
        exportEncoding, [&point]() { return Id::makeFromGeoPoint(point); });
    auto bytes = withGeoPointEncoding(exportEncoding, [&]() {
      return exportBytes(
          makeQet(qec, makeIdTableFromVector({{exported}}), {"?x"}), {"?x"});
    });
    auto [imported, expected] = withGeoPointEncoding(importEncoding, [&]() {
      return std::pair{
          importBytes(std::move(bytes), qec, {"?x"}).idTableView()(0, 0),
          Id::makeFromGeoPoint(point)};
    });
    EXPECT_NE(expected, exported);
    EXPECT_EQ(imported, expected);
  };
  test(GeoPointEncodingEnum::ZOrder, GeoPointEncodingEnum::LatMajor);
  test(GeoPointEncodingEnum::LatMajor, GeoPointEncodingEnum::ZOrder);
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

  auto notBinaryFormat =
      ::testing::HasSubstr("not in QLever's binary export format");
  auto wrongMagic = bytes;
  wrongMagic[sizeof(size_t)] = 'X';
  AD_EXPECT_THROW_WITH_MESSAGE(import(wrongMagic), notBinaryFormat);
  // A different format, the size of which is not used for an allocation.
  AD_EXPECT_THROW_WITH_MESSAGE(import(std::string(1000, 'x')), notBinaryFormat);
  // An empty response.
  AD_EXPECT_THROW_WITH_MESSAGE(import(""),
                               ::testing::HasSubstr("ended unexpectedly"));

  // The encoding of the geographic points follows the version.
  auto wrongEncoding = bytes;
  wrongEncoding[versionOffset + sizeof(version)] = 7;
  AD_EXPECT_THROW_WITH_MESSAGE(import(wrongEncoding),
                               ::testing::HasSubstr("invalid encoding"));

  // The result ends with the last row, the marker, and the (empty) trailing
  // batch of strings. Cut it off in the middle of that batch, directly after
  // the last row, and in the middle of the last row.
  size_t endOfLastRow = bytes.size() - sizeof(Id::T) - sizeof(size_t);
  EXPECT_ANY_THROW(import(bytes.substr(0, bytes.size() - 1)));
  AD_EXPECT_THROW_WITH_MESSAGE(import(bytes.substr(0, endOfLastRow)),
                               ::testing::HasSubstr("ended unexpectedly"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      import(bytes.substr(0, endOfLastRow - sizeof(Id::T) / 2)),
      ::testing::HasSubstr("range of chunks"));
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
  static constexpr size_t numRows =
      defaultMinNumStringBytesPerBatch / stringSize + 2;
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
      defaultMinNumIdBytesPerBatch / sizeof(Id::T) + 1;
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

// _____________________________________________________________________________
TEST_F(BinaryExportRoundTrip, limitsAndCancellation) {
  auto* qec = ad_utility::testing::getQec();
  LocalVocab localVocab;
  std::string word = absl::StrCat("\"", std::string(100, 'x'), "\"");
  Id id = localVocabId(localVocab, word, *qec);
  auto bytes = exportBytes(makeQet(qec, makeIdTableFromVector({{id}}), {"?x"},
                                   std::move(localVocab)),
                           {"?x"});
  using ad_utility::MemorySize;

  // The batch of strings consists of the single word.
  auto result = importBytes(bytes, qec, {"?x"}, false,
                            MemorySize::bytes(sizeof(std::string) + 102));
  EXPECT_EQ(
      result.idTableView()(0, 0).getLocalVocabIndex()->toStringRepresentation(),
      word);
  AD_EXPECT_THROW_WITH_MESSAGE(
      importBytes(bytes, qec, {"?x"}, false, MemorySize::bytes(100)),
      ::testing::HasSubstr("allocate more than the maximum of 100 bytes"));

  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  cancellationHandle->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(importBytes(bytes, qec, {"?x"}, false, MemorySize::gigabytes(1),
                           cancellationHandle),
               ad_utility::CancellationException);
}
