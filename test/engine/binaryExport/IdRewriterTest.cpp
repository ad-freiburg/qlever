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

#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "../../util/IndexTestHelpers.h"
#include "engine/binaryExport/IdRewriter.h"

using namespace qlever::binary_export;

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

  // A pattern that the exporting instance didn't send.
  AD_EXPECT_THROW_WITH_MESSAGE(
      rewriter(EncodedIriManager::makeIdFromPrefixIdxAndPayload(2, payload)),
      ::testing::HasSubstr("unknown pattern"));
}

// _____________________________________________________________________________
TEST(IdRewriter, unexpectedDatatype) {
  IdRewriter rewriter{*ad_utility::testing::getQec(), {}, GeoPoint::encoding()};
  EXPECT_EQ(rewriter(Id::makeFromInt(42)), Id::makeFromInt(42));
  // The exporter sends the strings instead of the `VocabIndex`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      rewriter(Id::makeFromVocabIndex(VocabIndex::make(0))),
      ::testing::HasSubstr("unexpected datatype VocabIndex"));
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

  // The blank nodes are consistent also for the following batches. The local
  // vocab of each batch contains all the blank nodes, but only the words of
  // its own batch.
  rewriter.startBatch({"\"word\""});
  Id word = rewriter.rewriteString(transmitted(0));
  auto vocab = rewriter.takeLocalVocab();
  EXPECT_EQ(vocab.size(), 1);
  EXPECT_TRUE(vocab.isBlankNodeIndexContained(first.getBlankNodeIndex()));
  rewriter.startBatch({});
  EXPECT_EQ(rewriter.rewriteBlankNode(remote(2)), second);
  auto nextVocab = rewriter.takeLocalVocab();
  EXPECT_EQ(nextVocab.size(), 0);
  EXPECT_TRUE(nextVocab.isBlankNodeIndexContained(first.getBlankNodeIndex()));
  EXPECT_TRUE(nextVocab.isBlankNodeIndexContained(second.getBlankNodeIndex()));
  EXPECT_EQ(word.getLocalVocabIndex()->toStringRepresentation(), "\"word\"");
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
TEST(IdRewriter, getPrefixMapping) {
  auto* qec = qecWithEncodedPrefixes();

  // Only the second remote pattern exists locally (as the first one).
  auto mapping = IdRewriter::getPrefixMapping(
      *qec, {prefixPattern("<http://other.org/"),
             prefixPattern("<http://example.org/")});
  EXPECT_THAT(mapping, ::testing::UnorderedElementsAre(::testing::Pair(1, 0)));

  EXPECT_TRUE(IdRewriter::getPrefixMapping(*qec, {}).empty());
}
