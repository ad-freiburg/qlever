// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "./BlobTestHelpers.h"
#include "libqlever/BlobLayout.h"

using namespace qlever;
using namespace testing;
using namespace blobTestHelpers;

namespace {
// Some triples with distinct subjects and objects.
std::string manyTriples(size_t n) {
  std::string result;
  for (size_t i = 0; i < n; ++i) {
    absl::StrAppend(&result, "<s", i, "> <p> <o", i, "> .\n<s", i,
                    "> <q> \"literal ", i, "\" .\n");
  }
  return result;
}

// Expect that `range` is a valid range inside `outer`.
void expectWithin(ByteRange range, ByteRange outer) {
  EXPECT_LE(outer.begin_, range.begin_);
  EXPECT_LE(range.begin_, range.end_);
  EXPECT_LE(range.end_, outer.end_);
}

// Expect that the ranges of the sections of `layout` are consistent.
void expectConsistent(const BlobLayout& layout) {
  ByteRange all{0, layout.totalSize_};
  expectWithin(layout.header_, all);
  EXPECT_LE(layout.header_.end_, layout.metadata_.begin_);
  EXPECT_LE(layout.metadata_.end_, layout.vocabulary_.begin_);
  EXPECT_LE(layout.vocabulary_.begin_, layout.vocabulary_.end_);
  uint64_t cursor = layout.vocabulary_.end_;
  if (layout.secondaryVocab_.present_) {
    EXPECT_LE(cursor, layout.secondaryVocab_.whole_.begin_);
    cursor = layout.secondaryVocab_.whole_.end_;
  }
  EXPECT_LE(cursor, layout.entriesHeader_.begin_);
  EXPECT_EQ(layout.entries_.end_, layout.totalSize_);
  cursor = layout.entriesHeader_.end_;
  for (const auto& entry : layout.entryLayouts_) {
    EXPECT_LE(cursor, entry.whole_.begin_);
    cursor = entry.whole_.end_;
    EXPECT_EQ(entry.columnPayloads_.size(), entry.numColumns_);
    uint64_t columnCursor = entry.whole_.begin_;
    for (const auto& column : entry.columnPayloads_) {
      EXPECT_EQ(column.size(), entry.numRows_ * 8);
      EXPECT_EQ(column.begin_ % 8, 0u);
      EXPECT_LE(columnCursor, column.begin_);
      columnCursor = column.end_;
      expectWithin(column, entry.whole_);
    }
    EXPECT_LE(columnCursor, entry.varToColMap_.begin_);
    EXPECT_LE(entry.varToColMap_.end_, entry.resultSortedOn_.begin_);
    EXPECT_EQ(entry.resultSortedOn_.size(), entry.sortedOnColumns_.size() * 8);
    EXPECT_LE(entry.resultSortedOn_.end_, entry.cacheKey_.begin_);
    expectWithin(entry.cacheKey_, entry.whole_);
    if (entry.hasGeoIndex_) {
      expectWithin(entry.geo_.whole_, entry.whole_);
      EXPECT_LE(entry.cacheKey_.end_, entry.geo_.whole_.begin_);
    }
  }
  EXPECT_EQ(cursor, layout.entries_.end_);
}
}  // namespace

// _____________________________________________________________________________
TEST(BlobLayout, blobWithoutSecondaryVocab) {
  auto config = buildTestIndex(manyTriples(20));
  Qlever source{EngineConfig{config}};
  source.queryAndPinResultWithName(
      "all", "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o");
  source.queryAndPinResultWithName(
      "subjects", "SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?s");
  auto blob = source.serializeVocabAndNamedCacheToCompressedBlob();
  ParsedBlob parsed{blob};
  const auto& layout = parsed.layout_;

  EXPECT_EQ(layout.blobVersion_, Manager::formatVersionWithoutSecondaryVocab);
  EXPECT_EQ(layout.entriesVersion_, 1u);
  EXPECT_EQ(layout.totalSize_, parsed.bytes_.size());
  EXPECT_EQ(layout.header_, (ByteRange{0, 10}));
  EXPECT_FALSE(layout.secondaryVocab_.present_);
  EXPECT_GT(layout.vocabulary_.size(), 0u);
  ASSERT_EQ(layout.entryLayouts_.size(), 2u);
  // The entries are sorted by key.
  EXPECT_EQ(layout.entryLayouts_[0].key_, "all");
  EXPECT_EQ(layout.entryLayouts_[0].numRows_, 40u);
  EXPECT_EQ(layout.entryLayouts_[0].numColumns_, 3u);
  EXPECT_EQ(layout.entryLayouts_[1].key_, "subjects");
  EXPECT_EQ(layout.entryLayouts_[1].numRows_, 20u);
  // The pinned entries also contain the columns that are not selected.
  EXPECT_EQ(layout.entryLayouts_[1].numColumns_, 3u);
  for (const auto& entry : layout.entryLayouts_) {
    EXPECT_LE(entry.sortedOnColumns_.size(), entry.numColumns_);
  }
  expectConsistent(layout);

  auto description = layout.describe();
  EXPECT_THAT(description, HasSubstr("\"all\""));
  EXPECT_THAT(description, HasSubstr("\"subjects\""));
  EXPECT_THAT(description, HasSubstr("main vocabulary"));
}

// _____________________________________________________________________________
TEST(BlobLayout, blobWithSecondaryVocab) {
  auto config = buildTestIndex(manyTriples(20));
  Qlever source{EngineConfig{config}};
  ad_utility::testing::applyUpdateToEngine(
      source, "INSERT DATA { <new1> <p> <o1> . <s1> <p> <new2> }");
  source.queryAndPinResultWithName(
      "all", "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o");
  auto blob = source.serializeVocabAndNamedCacheToCompressedBlob();
  ParsedBlob parsed{blob};
  const auto& layout = parsed.layout_;
  EXPECT_EQ(layout.blobVersion_, Manager::formatVersionWithSecondaryVocab);
  ASSERT_TRUE(layout.secondaryVocab_.present_);
  const auto& sv = layout.secondaryVocab_;
  ASSERT_EQ(sv.segments_.size(), 1u);
  expectWithin(sv.segments_[0], sv.whole_);
  EXPECT_LE(sv.segments_[0].end_, sv.segmentOffsets_.begin_);
  EXPECT_EQ(sv.segmentOffsets_.size(), 8u);
  EXPECT_LE(sv.segmentOffsets_.end_, sv.sortedIndices_.begin_);
  // Two new words.
  EXPECT_EQ(sv.sortedIndices_.size(), 16u);
  EXPECT_EQ(sv.sortedIndices_.end_, sv.whole_.end_);
  expectConsistent(layout);
  ASSERT_EQ(layout.entryLayouts_.size(), 1u);
  EXPECT_EQ(layout.entryLayouts_[0].numRows_, 42u);
  EXPECT_THAT(layout.describe(), HasSubstr("secondary vocabulary"));
}

// _____________________________________________________________________________
TEST(BlobLayout, blobWithGeoIndexVersion2) {
  auto config = buildTestIndex(
      "<s1> <asWKT> \"LINESTRING(7.8428469 47.9995367,7.8413293 "
      "47.9974942)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n");
  Qlever source{EngineConfig{config}};
  source.queryAndPinResultWithName(
      QueryExecutionContext::PinResultWithName{"geoPin", Variable{"?geo2"}},
      "SELECT * { ?s2 <asWKT> ?geo2 }");
  auto blob = source.serializeVocabAndNamedCacheToCompressedBlob();
  ParsedBlob parsed{blob};
  const auto& layout = parsed.layout_;
  // A blob with a geo index is always written with entries version 2, which
  // stores the segmented geo index (here a single segment).
  EXPECT_EQ(layout.blobVersion_, 2u);
  EXPECT_EQ(layout.entriesVersion_, 2u);
  ASSERT_EQ(layout.entryLayouts_.size(), 1u);
  const auto& entry = layout.entryLayouts_[0];
  ASSERT_TRUE(entry.hasGeoIndex_);
  EXPECT_GT(entry.geo_.whole_.size(), 0u);
  ASSERT_EQ(entry.geo_.segmentPayloads_.size(), 1u);
  EXPECT_GT(entry.geo_.segmentPayloads_[0].size(), 0u);
  EXPECT_EQ(entry.geo_.rowToShape_.size(), entry.numRows_ * sizeof(uint64_t));
  expectConsistent(layout);
}

// _____________________________________________________________________________
TEST(BlobLayout, handmadeBlobWithGeoIndexVersion2AndSecondaryVocab) {
  auto config = buildTestIndex(manyTriples(3));
  Qlever source{EngineConfig{config}};
  source.queryAndPinResultWithName("x", "SELECT ?s WHERE { ?s <p> ?o }");
  ParsedBlob real{source.serializeVocabAndNamedCacheToCompressedBlob()};
  auto prefix = prefixOf(real.span(), real.layout_.vocabulary_.end_);

  SecondaryVocabulary sv;
  sv.appendSegment([] {
    CompactVectorOfStrings<char> segment;
    segment.build(std::vector<std::string>{"aaa", "bbbb", "cc"});
    return segment;
  }());
  HandmadeEntry plain{"a", {{1, 2, 3}, {4, 5, 6}}, {0}, std::nullopt};
  HandmadeEntry geo{"b",
                    {{1, 2}},
                    {0},
                    HandmadeGeo{1.5, {"seg-one", "second segment"}, {7, 8}}};
  HandmadeEntry geoNoSimplification{
      "c", {{9}}, {}, HandmadeGeo{std::nullopt, {"only"}, {3}}};
  auto blob =
      makeHandmadeBlob(prefix, &sv, 2, {plain, geo, geoNoSimplification});
  auto layout = BlobLayout::parse(ql::span<const char>{blob});

  EXPECT_EQ(layout.blobVersion_, 2u);
  EXPECT_EQ(layout.entriesVersion_, 2u);
  EXPECT_EQ(layout.totalSize_, blob.size());
  ASSERT_TRUE(layout.secondaryVocab_.present_);
  EXPECT_EQ(layout.secondaryVocab_.segments_.size(), 1u);
  EXPECT_EQ(layout.secondaryVocab_.sortedIndices_.size(), 3u * 8);
  expectConsistent(layout);
  ASSERT_EQ(layout.entryLayouts_.size(), 3u);
  EXPECT_FALSE(layout.entryLayouts_[0].hasGeoIndex_);
  EXPECT_EQ(layout.entryLayouts_[0].numRows_, 3u);
  EXPECT_EQ(layout.entryLayouts_[0].numColumns_, 2u);

  const auto& geoLayout = layout.entryLayouts_[1].geo_;
  ASSERT_TRUE(layout.entryLayouts_[1].hasGeoIndex_);
  ASSERT_EQ(geoLayout.segmentPayloads_.size(), 2u);
  EXPECT_EQ(geoLayout.segmentPayloads_[0].size(), 7u);
  EXPECT_EQ(geoLayout.segmentPayloads_[1].size(), 14u);
  EXPECT_EQ(
      std::string_view(blob.data() + geoLayout.segmentPayloads_[1].begin_, 14),
      "second segment");
  EXPECT_EQ(geoLayout.rowToShape_.size(), 16u);
  uint64_t shape;
  std::memcpy(&shape, blob.data() + geoLayout.rowToShape_.begin_ + 8, 8);
  EXPECT_EQ(shape, 8u);
  EXPECT_EQ(geoLayout.rowToShape_.end_, geoLayout.whole_.end_);

  const auto& noSimplification = layout.entryLayouts_[2].geo_;
  ASSERT_EQ(noSimplification.segmentPayloads_.size(), 1u);
  EXPECT_EQ(noSimplification.rowToShape_.size(), 8u);
  EXPECT_TRUE(layout.entryLayouts_[2].sortedOnColumns_.empty());
}

// _____________________________________________________________________________
TEST(BlobLayout, malformedBlobsThrow) {
  auto config = buildTestIndex(manyTriples(3));
  Qlever source{EngineConfig{config}};
  source.queryAndPinResultWithName("x", "SELECT ?s WHERE { ?s <p> ?o }");
  ParsedBlob real{source.serializeVocabAndNamedCacheToCompressedBlob()};
  auto bytes = std::vector<char>(real.span().begin(), real.span().end());

  // Trailing bytes.
  auto withTrailing = bytes;
  withTrailing.resize(withTrailing.size() + 8);
  EXPECT_ANY_THROW(BlobLayout::parse(ql::span<const char>{withTrailing}));
  // Truncated.
  auto truncated = bytes;
  truncated.resize(truncated.size() - 4);
  EXPECT_ANY_THROW(BlobLayout::parse(ql::span<const char>{truncated}));
  // No blob at all.
  std::vector<char> garbage(64, 'x');
  EXPECT_ANY_THROW(BlobLayout::parse(ql::span<const char>{garbage}));
  EXPECT_ANY_THROW(BlobLayout::parse(ql::span<const char>{}));
  // Wrong magic byte of the entries.
  auto wrongMagic = bytes;
  wrongMagic[real.layout_.entriesHeader_.begin_] = 0;
  EXPECT_ANY_THROW(BlobLayout::parse(ql::span<const char>{wrongMagic}));
}
