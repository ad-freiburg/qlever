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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "../../util/IndexTestHelpers.h"
#include "engine/binaryExport/BlankNodeRewriter.h"
#include "engine/binaryExport/EncodedIriRewriter.h"
#include "engine/binaryExport/IdRewriter.h"
#include "engine/binaryExport/StringMapping.h"

using namespace qlever::binary_export;

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
}  // namespace

// _____________________________________________________________________________
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

// _____________________________________________________________________________
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

// _____________________________________________________________________________
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

// _____________________________________________________________________________
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

// _____________________________________________________________________________
TEST(EncodedIriRewriter, rewrite) {
  const auto& index = qecWithEncodedPrefixes()->getIndex();
  const auto& manager = index.encodedIriManager();

  // The second remote pattern is the (only) local pattern, the first one
  // doesn't exist locally.
  EncodedIriRewriter rewriter{index,
                              {prefixPattern("<http://other.org/"),
                               prefixPattern("<http://example.org/")}};
  LocalVocab localVocab;
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
  LocalVocab nextLocalVocab;
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

// _____________________________________________________________________________
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

// _____________________________________________________________________________
TEST(BlankNodeRewriter, rewrite) {
  BlankNodeRewriter rewriter{
      ad_utility::testing::getQec()->getIndex().getBlankNodeManager()};
  auto remote = [](uint64_t i) {
    return Id::makeFromBlankNodeIndex(BlankNodeIndex::make(i));
  };

  Id first = rewriter(remote(1));
  Id second = rewriter(remote(2));
  EXPECT_EQ(first.getDatatype(), Datatype::BlankNodeIndex);
  EXPECT_NE(first, second);
  EXPECT_EQ(rewriter(remote(1)), first);

  // Each local vocab contains all the blank nodes so far, but no words.
  LocalVocab vocab;
  rewriter.addBlankNodesTo(vocab);
  EXPECT_TRUE(vocab.empty());
  EXPECT_TRUE(vocab.isBlankNodeIndexContained(first.getBlankNodeIndex()));
  EXPECT_TRUE(vocab.isBlankNodeIndexContained(second.getBlankNodeIndex()));

  Id third = rewriter(remote(3));
  EXPECT_EQ(rewriter(remote(2)), second);
  LocalVocab nextVocab;
  rewriter.addBlankNodesTo(nextVocab);
  EXPECT_TRUE(nextVocab.isBlankNodeIndexContained(first.getBlankNodeIndex()));
  EXPECT_TRUE(nextVocab.isBlankNodeIndexContained(third.getBlankNodeIndex()));
}
