// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "../../util/IndexTestHelpers.h"
#include "../../util/LocalVocabEntryTestHelpers.h"
#include "../../util/LocalVocabTestHelpers.h"
#include "engine/binaryExport/BinaryExport.h"
#include "engine/binaryExport/BinaryFormat.h"
#include "engine/binaryExport/BlankNodeRewriter.h"
#include "engine/binaryExport/EncodedIriRewriter.h"
#include "engine/binaryExport/IdRewriter.h"
#include "engine/binaryExport/StringMapping.h"
#include "index/ExportIds.h"

using namespace qlever::binary_export;
using ad_utility::testing::makeLocalVocab;

namespace {
// The `Id` with which the exporter refers to the `i`-th transmitted string of
// a batch.
Id transmitted(size_t i) { return StringMapping::makeTransmittedId(i); }

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

// Rewrite the single `id` as a batch of its own, which refers to the
// `transmittedStrings`.
Id rewriteSingleId(IdRewriter& rewriter, Id id,
                   std::vector<std::string> transmittedStrings = {}) {
  IdTable table{1, ad_utility::makeUnlimitedAllocator<Id>()};
  table.push_back({id});
  rewriter.rewriteBatch(table, 0, std::move(transmittedStrings));
  return table(0, 0);
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

// A test index with the triples `turtle`, which encodes the IRIs with one of
// the `prefixes` directly in the `Id`.
QueryExecutionContext* qecWith(std::string turtle,
                               std::vector<std::string> prefixes) {
  ad_utility::testing::TestIndexConfig config{std::move(turtle)};
  config.encodedPrefixesWithoutAngleBrackets = std::move(prefixes);
  return ad_utility::testing::getQec(config);
}

// Read a trivially copyable value from `p` and advance `p`.
template <typename T>
T readFrom(const char*& p) {
  T value;
  std::memcpy(&value, p, sizeof(value));
  p += sizeof(value);
  return value;
}

// Split the bytes of an `ExportBatch` (the IDs, the `vocabMarker`, the
// serialized strings) into the IDs and the strings.
std::pair<std::vector<Id>, std::vector<std::string>> parseBatch(
    std::string_view bytes, size_t numIds) {
  const char* p = bytes.data();
  std::vector<Id> ids;
  for (size_t i = 0; i < numIds; ++i) {
    ids.push_back(Id::fromBits(readFrom<Id::T>(p)));
  }
  EXPECT_EQ(readFrom<Id::T>(p), vocabMarker);
  std::vector<std::string> strings;
  size_t numStrings = readFrom<size_t>(p);
  for (size_t i = 0; i < numStrings; ++i) {
    size_t size = readFrom<size_t>(p);
    strings.emplace_back(p, size);
    p += size;
  }
  EXPECT_EQ(p, bytes.data() + bytes.size());
  return {std::move(ids), std::move(strings)};
}

// The string representation and datatype of the `id` wrt the `index` and the
// `localVocab`.
std::string stringAndType(const Index& index, Id id,
                          const LocalVocab& localVocab) {
  auto result = ql::exportIds::idToStringAndType(index, id, localVocab);
  if (!result.has_value()) {
    return "UNDEF";
  }
  if (result->second == nullptr) {
    return result->first;
  }
  return absl::StrCat(result->first, "^^", result->second);
}
}  // namespace

// Test that `rewriteBatch` rewrites the rows from `firstRow` on: a transmitted
// string becomes the ID of its word in the index or in the local vocab, a
// trivial ID stays unchanged, and an index of a string that was not
// transmitted throws.
TEST(IdRewriter, rewriteBatch) {
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
  // two are not. The first row has already been rewritten (as part of a
  // previous batch).
  IdRewriter rewriter{*qec, {}, GeoPoint::encoding()};
  rewriter.rewriteBatch(table, 1,
                        {"<x>", "<http://example.org/a>", "\"literal\""});

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

  // A string that is not part of the batch.
  AD_EXPECT_THROW_WITH_MESSAGE(rewriteSingleId(rewriter, transmitted(0)),
                               ::testing::HasSubstr("not transmitted"));
}

// Test that several batches can share one local vocab, that a decoded IRI is
// cached only until `takeLocalVocab`, and that each local vocab contains all
// the blank nodes so far.
TEST(IdRewriter, severalBatchesPerLocalVocab) {
  auto* qec = qecWithEncodedPrefixes();
  IdRewriter rewriter{
      *qec, {prefixPattern("<http://other.org/")}, GeoPoint::encoding()};
  auto [prefixIdx, payload] = EncodedIriManager::splitIntoPrefixIdxAndPayload(
      qec->getIndex()
          .encodedIriManager()
          .encode("<http://example.org/42>")
          .value());
  Id remoteIri = EncodedIriManager::makeIdFromPrefixIdxAndPayload(0, payload);
  Id blankNode = rewriteSingleId(
      rewriter, Id::makeFromBlankNodeIndex(BlankNodeIndex::make(1)));

  // Two batches before the local vocab is taken: it contains the words of
  // both, the decoded IRI only once.
  Id first = rewriteSingleId(rewriter, transmitted(0), {"\"first\""});
  Id second = rewriteSingleId(rewriter, transmitted(0), {"\"second\""});
  Id decoded = rewriteSingleId(rewriter, remoteIri);
  EXPECT_EQ(rewriteSingleId(rewriter, remoteIri).getBits(), decoded.getBits());
  auto vocab = rewriter.takeLocalVocab();
  EXPECT_EQ(vocab.size(), 3);
  EXPECT_EQ(first.getLocalVocabIndex()->toStringRepresentation(), "\"first\"");
  EXPECT_EQ(second.getLocalVocabIndex()->toStringRepresentation(),
            "\"second\"");
  EXPECT_EQ(decoded.getLocalVocabIndex()->toStringRepresentation(),
            "<http://other.org/42>");

  // The next local vocab contains the decoded IRI again (as the IDs of the
  // previous one might no longer be alive), and the blank node, which might
  // occur again.
  Id decodedAgain = rewriteSingleId(rewriter, remoteIri);
  auto nextVocab = rewriter.takeLocalVocab();
  EXPECT_EQ(nextVocab.size(), 1);
  EXPECT_EQ(decodedAgain.getLocalVocabIndex()->toStringRepresentation(),
            "<http://other.org/42>");
  EXPECT_TRUE(
      nextVocab.isBlankNodeIndexContained(blankNode.getBlankNodeIndex()));
}

// Test that `rewriteBatch` dispatches on the datatype (trivial IDs,
// transmitted strings, encoded IRIs, blank nodes and `GeoPoint`s), and that an
// ID of another datatype throws.
TEST(IdRewriter, dispatch) {
  using E = GeoPointEncodingEnum;
  auto* qec = qecWithEncodedPrefixes();
  IdRewriter rewriter{
      *qec, {prefixPattern("<http://example.org/")}, E::LatMajor};

  // A trivial ID remains unchanged.
  EXPECT_EQ(rewriteSingleId(rewriter, Id::makeFromInt(42)),
            Id::makeFromInt(42));
  // A transmitted string.
  EXPECT_EQ(rewriteSingleId(rewriter, transmitted(0), {"<x>"}),
            ad_utility::testing::makeGetId(qec->getIndex())("<x>"));
  // An encoded IRI.
  Id iri = qec->getIndex()
               .encodedIriManager()
               .encode("<http://example.org/42>")
               .value();
  EXPECT_EQ(rewriteSingleId(rewriter, iri), iri);
  // A blank node.
  EXPECT_EQ(rewriteSingleId(rewriter,
                            Id::makeFromBlankNodeIndex(BlankNodeIndex::make(1)))
                .getDatatype(),
            Datatype::BlankNodeIndex);
  // A `GeoPoint`.
  GeoPoint point{47.9, 7.8};
  Id remotePoint = withGeoPointEncoding(
      E::LatMajor, [&point]() { return Id::makeFromGeoPoint(point); });
  EXPECT_EQ(rewriteSingleId(rewriter, remotePoint),
            Id::makeFromGeoPoint(point));

  // The exporter sends the strings instead of the `VocabIndex`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      rewriteSingleId(rewriter, Id::makeFromVocabIndex(VocabIndex::make(0))),
      ::testing::HasSubstr("unexpected datatype VocabIndex"));
}

// Test that `rewriteGeoPoint` converts a point between two encodings and
// leaves it unchanged for the local encoding.
TEST(IdRewriter, rewriteGeoPoint) {
  using E = GeoPointEncodingEnum;
  ASSERT_EQ(GeoPoint::encoding(), E::ZOrder);
  GeoPoint point{47.9, 7.8};
  Id local = Id::makeFromGeoPoint(point);

  // The same encoding: the `Id` is unchanged.
  EXPECT_EQ(IdRewriter::rewriteGeoPoint(local, E::ZOrder), local);

  // A different encoding: the `Id` is converted.
  Id remote = withGeoPointEncoding(
      E::LatMajor, [&point]() { return Id::makeFromGeoPoint(point); });
  ASSERT_NE(remote, local);
  EXPECT_EQ(IdRewriter::rewriteGeoPoint(remote, E::LatMajor), local);
}

// Test that an encoded IRI with a pattern that exists locally only changes
// its pattern index, that one with another pattern is decoded into the local
// vocab (cached until `invalidateCache`), and that an unknown pattern throws.
TEST(EncodedIriRewriter, rewrite) {
  const auto& index = qecWithEncodedPrefixes()->getIndex();
  const auto& manager = index.encodedIriManager();

  // The second remote pattern is the (only) local pattern, the first one
  // doesn't exist locally.
  EncodedIriRewriter rewriter{index,
                              {prefixPattern("<http://other.org/"),
                               prefixPattern("<http://example.org/")}};
  LocalVocab localVocab = makeLocalVocab();
  auto local = manager.encode("<http://example.org/42>").value();
  auto [localPrefixIdx, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(local);
  ASSERT_EQ(localPrefixIdx, 0);

  // A pattern that exists locally: only the index of the pattern changes.
  Id remote = EncodedIriManager::makeIdFromPrefixIdxAndPayload(1, payload);
  EXPECT_EQ(rewriter(remote, localVocab), local);
  EXPECT_TRUE(localVocab.empty());

  // A pattern that doesn't exist locally: the IRI is decoded and stored in the
  // local vocab (consistently for repeated occurrences).
  Id remoteOther = EncodedIriManager::makeIdFromPrefixIdxAndPayload(0, payload);
  Id rewritten = rewriter(remoteOther, localVocab);
  ASSERT_EQ(rewritten.getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(rewritten.getLocalVocabIndex()->toStringRepresentation(),
            "<http://other.org/42>");
  EXPECT_EQ(rewriter(remoteOther, localVocab).getBits(), rewritten.getBits());
  EXPECT_EQ(localVocab.size(), 1);

  // After `invalidateCache`, the IRI is decoded again (into a new local
  // vocab).
  rewriter.invalidateCache();
  LocalVocab nextLocalVocab = makeLocalVocab();
  Id rewrittenAgain = rewriter(remoteOther, nextLocalVocab);
  EXPECT_EQ(nextLocalVocab.size(), 1);
  EXPECT_EQ(rewrittenAgain.getLocalVocabIndex()->toStringRepresentation(),
            "<http://other.org/42>");

  // A pattern that the exporting instance didn't send.
  AD_EXPECT_THROW_WITH_MESSAGE(
      rewriter(EncodedIriManager::makeIdFromPrefixIdxAndPayload(2, payload),
               localVocab),
      ::testing::HasSubstr("unknown pattern"));
}

// Test that `getPrefixMapping` maps each remote pattern to the index of the
// equal local pattern, if there is one.
TEST(EncodedIriRewriter, getPrefixMapping) {
  const auto& manager =
      qecWithEncodedPrefixes()->getIndex().encodedIriManager();

  // Only the second remote pattern exists locally (as the first one).
  EXPECT_THAT(EncodedIriRewriter::getPrefixMapping(
                  manager, {prefixPattern("<http://other.org/"),
                            prefixPattern("<http://example.org/")}),
              ::testing::ElementsAre(std::nullopt, std::optional<uint8_t>{0}));

  EXPECT_TRUE(EncodedIriRewriter::getPrefixMapping(manager, {}).empty());
}

// Test that an encoded IRI is rewritten to the `Id` with which the local
// instance encodes it, also if it matches several local patterns.
TEST(EncodedIriRewriter, overlappingLocalPatterns) {
  // A local index with a plain prefix and two general patterns with the same
  // prefix. The remote instance only has the two general patterns.
  using encodedIri::Part;
  encodedIri::Pattern small{"http://example.org/", {Part{8, {}, ""}}};
  encodedIri::Pattern large{"http://example.org/", {Part{16, {}, ""}}};
  ad_utility::testing::TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets = {"http://example.org/"};
  config.encodedIriPatterns = {small, large};
  const auto& index = ad_utility::testing::getQec(config)->getIndex();
  const auto& manager = index.encodedIriManager();
  EncodedIriManager remoteManager{std::vector<std::string>{}, {small, large}};

  // Both general patterns exist locally, but none of them is the first local
  // pattern that might match their IRIs. The first remote pattern is the
  // hardcoded prefix for the graphs that QLever creates itself.
  auto mapping =
      EncodedIriRewriter::getPrefixMapping(manager, remoteManager.patterns_);
  ASSERT_EQ(mapping.size(), 3);
  EXPECT_TRUE(mapping[0].has_value());
  EXPECT_EQ(mapping[1], std::nullopt);
  EXPECT_EQ(mapping[2], std::nullopt);

  // The IRIs get the same `Id` as when the local instance encodes them (with
  // the plain prefix), no matter with which pattern the remote instance
  // encoded them.
  EncodedIriRewriter rewriter{index, remoteManager.patterns_};
  LocalVocab localVocab = makeLocalVocab();
  for (std::string iri :
       {"<http://example.org/5>", "<http://example.org/300>"}) {
    EXPECT_EQ(rewriter(remoteManager.encode(iri).value(), localVocab),
              manager.encode(iri).value());
  }
  EXPECT_TRUE(localVocab.empty());
}

// Test that the same remote blank node is always mapped to the same local
// one, and that `addBlankNodesTo` makes all the blank nodes so far (but no
// words) part of a local vocab.
TEST(BlankNodeRewriter, rewrite) {
  BlankNodeRewriter rewriter{
      ad_utility::testing::getQec()->getIndex().getBlankNodeManager(),
      ad_utility::testing::makeAllocator()};
  auto remote = [](uint64_t i) {
    return Id::makeFromBlankNodeIndex(BlankNodeIndex::make(i));
  };

  Id first = rewriter(remote(1));
  Id second = rewriter(remote(2));
  EXPECT_EQ(first.getDatatype(), Datatype::BlankNodeIndex);
  EXPECT_NE(first, second);
  EXPECT_EQ(rewriter(remote(1)), first);

  // Each local vocab contains all the blank nodes so far, but no words.
  LocalVocab vocab = makeLocalVocab();
  rewriter.addBlankNodesTo(vocab);
  EXPECT_TRUE(vocab.empty());
  EXPECT_TRUE(vocab.isBlankNodeIndexContained(first.getBlankNodeIndex()));
  EXPECT_TRUE(vocab.isBlankNodeIndexContained(second.getBlankNodeIndex()));

  Id third = rewriter(remote(3));
  EXPECT_EQ(rewriter(remote(2)), second);
  LocalVocab nextVocab = makeLocalVocab();
  rewriter.addBlankNodesTo(nextVocab);
  EXPECT_TRUE(nextVocab.isBlankNodeIndexContained(first.getBlankNodeIndex()));
  EXPECT_TRUE(nextVocab.isBlankNodeIndexContained(third.getBlankNodeIndex()));
}

// Test the round trip from the exporting index `A` to the importing index `B`
// (different vocabularies and encoded prefixes): the IDs of a result of `A`
// are exported by an `ExportBatch`, the bytes are parsed, and the `IdRewriter`
// of `B` converts the IDs. Every ID then denotes the same value on `B` as on
// `A`, and the IDs of `B` have the datatype that `B` uses for the value.
TEST(IdRewriter, roundTripBetweenIndices) {
  auto* qecA = qecWith(
      "<a1> <p> \"lit\" . <shared> <p> \"shared lit\" . "
      "<a1> <p> <http://both.org/7> . <a1> <p> <http://a.org/9> .",
      {"http://a.org/", "http://both.org/"});
  auto* qecB = qecWith(
      "<b1> <p> \"other\" . <shared> <p> \"shared lit\" . "
      "<b1> <p> <http://b.org/1> .",
      {"http://both.org/", "http://b.org/"});
  const Index& indexA = qecA->getIndex();
  const Index& indexB = qecB->getIndex();
  auto getIdA = ad_utility::testing::makeGetId(indexA);
  auto getIdB = ad_utility::testing::makeGetId(indexB);
  LocalVocab localVocabA = makeLocalVocab();
  auto localIdA = [&](std::string word) {
    return Id::makeFromLocalVocabIndex(localVocabA.getIndexAndAddIfNotContained(
        ad_utility::testing::localVocabEntryFromStringRepresentation(
            std::move(word), qecA->getLocalVocabContext())));
  };
  auto blankNodeA = [&]() {
    return Id::makeFromBlankNodeIndex(
        localVocabA.getBlankNodeIndex(indexA.getBlankNodeManager()));
  };
  Id blankA1 = blankNodeA();
  Id blankA2 = blankNodeA();

  // The result of `A`, one ID per row, with every datatype that the exporter
  // can send (the comments say how `B` represents the value).
  std::vector<Id> idsA{
      getIdA("<shared>"),            // `VocabIndex` of `B`
      getIdA("\"shared lit\""),      // `VocabIndex` of `B`
      getIdA("<a1>"),                // local vocab of `B`
      localIdA("\"fresh\""),         // local vocab of `B`
      localIdA("<http://b.org/5>"),  // `EncodedVal` (pattern only of `B`)
      indexA.encodedIriManager()
          .encode("<http://both.org/7>")
          .value(),  // `EncodedVal` (pattern of both)
      indexA.encodedIriManager()
          .encode("<http://a.org/9>")
          .value(),  // local vocab of `B` (pattern only of `A`)
      blankA1,
      blankA2,
      blankA1,
      Id::makeFromGeoPoint(GeoPoint{47.9, 7.8}),
      Id::makeFromInt(-42),
      Id::makeFromDouble(3.25),
      Id::makeFromBool(true),
      Id::makeFromDate(DateYearOrDuration{Date{2026, 11, 9}}),
      Id::makeUndefined(),
      getIdA("<shared>"),  // sent once, see `StringMapping`
  };
  IdTable tableA{1, ad_utility::makeUnlimitedAllocator<Id>()};
  for (Id id : idsA) {
    tableA.push_back({id});
  }

  // Export the table as one batch and parse the bytes.
  ExportBatch batch{indexA};
  std::vector<ColumnIndex> columns{0};
  for (size_t row = 0; row < tableA.size(); ++row) {
    batch.addRow(tableA.asStaticView<0>(), row, columns);
  }
  auto [transmittedIds, strings] = parseBatch(batch.finish(), idsA.size());
  ASSERT_EQ(transmittedIds.size(), idsA.size());
  EXPECT_THAT(strings, ::testing::UnorderedElementsAre(
                           "<shared>", "\"shared lit\"", "<a1>", "\"fresh\"",
                           "<http://b.org/5>"));

  // Import the batch on `B`.
  IdRewriter rewriter{*qecB, indexA.encodedIriManager().patterns_,
                      GeoPoint::encoding()};
  IdTable tableB{1, ad_utility::makeUnlimitedAllocator<Id>()};
  for (Id id : transmittedIds) {
    tableB.push_back({id});
  }
  rewriter.rewriteBatch(tableB, 0, std::move(strings));
  LocalVocab localVocabB = rewriter.takeLocalVocab();

  // Every value is the same on both sides, except for the blank nodes, which
  // become local ones of `B` (consistently).
  for (size_t row = 0; row < idsA.size(); ++row) {
    Id idB = tableB(row, 0);
    if (idsA[row].getDatatype() == Datatype::BlankNodeIndex) {
      EXPECT_EQ(idB.getDatatype(), Datatype::BlankNodeIndex) << row;
      EXPECT_TRUE(
          localVocabB.isBlankNodeIndexContained(idB.getBlankNodeIndex()))
          << row;
      continue;
    }
    EXPECT_EQ(stringAndType(indexA, idsA[row], localVocabA),
              stringAndType(indexB, idB, localVocabB))
        << row;
  }
  EXPECT_EQ(tableB(7, 0), tableB(9, 0));
  EXPECT_NE(tableB(7, 0), tableB(8, 0));

  // The datatypes on `B`, see the comments at `idsA`.
  EXPECT_EQ(tableB(0, 0).getBits(), getIdB("<shared>").getBits());
  EXPECT_EQ(tableB(1, 0).getBits(), getIdB("\"shared lit\"").getBits());
  EXPECT_EQ(tableB(16, 0).getBits(), getIdB("<shared>").getBits());
  EXPECT_EQ(tableB(2, 0).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(tableB(3, 0).getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(tableB(4, 0),
            indexB.encodedIriManager().encode("<http://b.org/5>").value());
  EXPECT_EQ(tableB(5, 0),
            indexB.encodedIriManager().encode("<http://both.org/7>").value());
  EXPECT_EQ(tableB(6, 0).getDatatype(), Datatype::LocalVocabIndex);
  for (size_t row : {10, 11, 12, 13, 14, 15}) {
    EXPECT_EQ(tableB(row, 0).getBits(), idsA[row].getBits()) << row;
  }
  EXPECT_EQ(localVocabB.size(), 3);
}
