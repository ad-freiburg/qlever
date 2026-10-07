// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "./BlobTestHelpers.h"
#include "backports/algorithm.h"
#include "libqlever/BlobDiff.h"
#include "libqlever/BlobLayout.h"
#include "util/BinaryDiffApplier.h"

using namespace qlever;
using namespace testing;
using namespace blobTestHelpers;
using ad_utility::BinaryDiffApplier;

namespace {
constexpr size_t NONE = std::numeric_limits<size_t>::max();

// A table of integers (given column by column) as `Id` columns.
struct TestTable {
  std::vector<std::vector<Id>> storage_;
  std::vector<ql::span<const Id>> spans_;

  explicit TestTable(const std::vector<std::vector<int64_t>>& columns) {
    for (const auto& column : columns) {
      storage_.emplace_back();
      for (int64_t value : column) {
        storage_.back().push_back(Id::makeFromInt(value));
      }
    }
    for (const auto& column : storage_) {
      spans_.emplace_back(column.data(), column.size());
    }
  }
  qlever::IdColumns columns() const { return spans_; }
};

// Return `alignRows` for the two tables, sorted on the columns `sortedOn`.
std::vector<size_t> merge(const std::vector<std::vector<int64_t>>& base,
                          const std::vector<std::vector<int64_t>>& target,
                          std::vector<uint64_t> sortedOn = {}) {
  TestTable b{base};
  TestTable t{target};
  EXPECT_TRUE(qlever::isInCanonicalOrder(b.columns(), sortedOn));
  EXPECT_TRUE(qlever::isInCanonicalOrder(t.columns(), sortedOn));
  return qlever::alignRows(b.columns(), t.columns(), sortedOn);
}

// Some triples with distinct subjects and objects.
std::string manyTriples(size_t n) {
  std::string result;
  for (size_t i = 0; i < n; ++i) {
    absl::StrAppend(&result, "<s", i, "> <p> <o", i, "> .\n<s", i,
                    "> <q> \"literal ", i, "\" .\n");
  }
  return result;
}

// Return the lines of `text` in sorted order.
std::vector<std::string> sortedLines(std::string_view text) {
  std::vector<std::string> lines = absl::StrSplit(text, '\n');
  ql::ranges::sort(lines);
  return lines;
}

// Apply `diff` to `base` and expect that the result is `target`.
void expectApplies(const BinaryDiffApplier& diff, ql::span<const char> base,
                   ql::span<const char> target) {
  auto result = diff.apply(base);
  EXPECT_EQ(result.size(), target.size());
  EXPECT_TRUE(
      std::equal(result.begin(), result.end(), target.begin(), target.end()));
}

// Compute the diff of two (decompressed) blobs, check that it reproduces the
// target, and return it.
BinaryDiffApplier diffAndCheck(ql::span<const char> base,
                               const BlobLayout& baseLayout,
                               ql::span<const char> target,
                               const BlobLayout& targetLayout) {
  auto diff = computeBlobDiff(base, baseLayout, target, targetLayout);
  expectApplies(diff, base, target);
  return diff;
}

// The pinned queries of the tests with real blobs.
constexpr std::string_view allQueryWithoutOrder =
    "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
constexpr std::string_view allQuery =
    "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o";
constexpr std::string_view subjectsQuery =
    "SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?s";

// Pin the result of `allQueryWithoutOrder` under `name`, sorted by all columns
// in the order of the columns of the pinned table. That order is a canonical
// order (see `computeBlobDiff`), and does in general not agree with the order
// of the variables in the query.
//
// NOTE: The blob writer does not yet sort the tables canonically. Until it
// does, only a table that is canonical before it is written (like the ones
// that are pinned here) leads to a small diff.
void pinSortedByColumnOrder(Qlever& qlever, const std::string& name) {
  qlever.queryAndPinResultWithName(name, std::string{allQueryWithoutOrder});
  std::vector<std::pair<size_t, std::string>> variables;
  for (const auto& [variable, info] :
       qlever.namedResultCache().get(name)->varToColMap_) {
    variables.emplace_back(info.columnIndex_, variable.name());
  }
  ql::ranges::sort(variables);
  std::string orderBy;
  for (const auto& [column, variable] : variables) {
    absl::StrAppend(&orderBy, " ", variable);
  }
  qlever.queryAndPinResultWithName(
      name, absl::StrCat(allQueryWithoutOrder, " ORDER BY", orderBy));
}

// Pin the two queries above under the names `all` and `subjects` and return
// the compressed blob.
std::vector<char> pinAndSerialize(Qlever& qlever) {
  pinSortedByColumnOrder(qlever, "all");
  qlever.queryAndPinResultWithName("subjects", std::string{subjectsQuery});
  return qlever.serializeVocabAndNamedCacheToCompressedBlob();
}

// Return true iff all the tables of the entries of the blob are in canonical
// order.
bool allTablesCanonical(const ParsedBlob& blob) {
  bool result = true;
  for (const auto& entry : blob.layout_.entryLayouts_) {
    std::vector<ql::span<const Id>> columns;
    for (const auto& range : entry.columnPayloads_) {
      columns.emplace_back(
          reinterpret_cast<const Id*>(blob.bytes_.data() + range.begin_),
          entry.numRows_);
    }
    result =
        result && qlever::isInCanonicalOrder(columns, entry.sortedOnColumns_);
  }
  return result;
}

// Two hand-made blobs that share a real prefix.
struct HandmadePrefix {
  std::vector<char> prefix_;
  HandmadePrefix() {
    auto config = buildTestIndex(manyTriples(3));
    Qlever source{EngineConfig{config}};
    source.queryAndPinResultWithName("x", "SELECT ?s WHERE { ?s <p> ?o }");
    ParsedBlob real{source.serializeVocabAndNamedCacheToCompressedBlob()};
    prefix_ = prefixOf(real.span(), real.layout_.vocabulary_.end_);
  }
};

// Return a `CompactVectorOfStrings` with the given (sorted) words.
CompactVectorOfStrings<char> segmentOf(const std::vector<std::string>& words) {
  CompactVectorOfStrings<char> segment;
  segment.build(words);
  return segment;
}
}  // namespace

// _____________________________________________________________________________
TEST(BlobDiffRowMerge, identicalTables) {
  EXPECT_THAT(merge({{1, 2, 3}, {4, 5, 6}}, {{1, 2, 3}, {4, 5, 6}}),
              ElementsAre(0u, 1u, 2u));
}

// _____________________________________________________________________________
TEST(BlobDiffRowMerge, insertionsAndDeletions) {
  // Row 2 of the base is deleted, and the rows (0, 0), (3, 9), and (9, 9) are
  // new.
  EXPECT_THAT(merge({{1, 2, 4, 5}, {1, 2, 4, 5}},
                    {{0, 1, 2, 3, 5, 9}, {0, 1, 2, 9, 5, 9}}),
              ElementsAre(NONE, 0u, 1u, NONE, 3u, NONE));
}

// _____________________________________________________________________________
TEST(BlobDiffRowMerge, duplicates) {
  // The i-th duplicate is matched with the i-th duplicate.
  EXPECT_THAT(merge({{1, 1, 2, 2, 2}}, {{1, 1, 1, 2, 2}}),
              ElementsAre(0u, 1u, NONE, 2u, 3u));
  EXPECT_THAT(merge({{1, 1, 1}}, {{1}}), ElementsAre(0u));
}

// _____________________________________________________________________________
TEST(BlobDiffRowMerge, allRowsNewAndEmptyTables) {
  EXPECT_THAT(merge({{5, 6}}, {{1, 2, 3}}), ElementsAre(NONE, NONE, NONE));
  EXPECT_THAT(merge({{1, 2}}, {{7, 8, 9}}), ElementsAre(NONE, NONE, NONE));
  EXPECT_THAT(merge({{}}, {{1, 2}}), ElementsAre(NONE, NONE));
  EXPECT_THAT(merge({{1, 2}}, {{}}), IsEmpty());
  EXPECT_THAT(merge({{}}, {{}}), IsEmpty());
}

// _____________________________________________________________________________
TEST(BlobDiffRowMerge, customSortOrder) {
  // Sorted by the second column first, then by the first column.
  std::vector<uint64_t> sortedOn{1};
  EXPECT_THAT(
      merge({{9, 8, 1}, {1, 2, 3}}, {{5, 9, 8, 1}, {1, 1, 2, 3}}, sortedOn),
      ElementsAre(NONE, 0u, 1u, 2u));
}

// _____________________________________________________________________________
TEST(BlobDiffRowMerge, isCanonicallySorted) {
  TestTable sorted{{{1, 1, 2}, {1, 2, 0}}};
  EXPECT_TRUE(
      qlever::isInCanonicalOrder(sorted.columns(), ql::span<const uint64_t>{}));
  // Sorted by the first column, but not by the second column on ties.
  TestTable unsortedOnTie{{{1, 1, 2}, {2, 1, 0}}};
  EXPECT_FALSE(qlever::isInCanonicalOrder(unsortedOnTie.columns(),
                                          ql::span<const uint64_t>{}));
  // The same table is not sorted by the second column first.
  EXPECT_FALSE(
      qlever::isInCanonicalOrder(sorted.columns(), std::vector<uint64_t>{1}));
  // Invalid sort columns.
  EXPECT_FALSE(
      qlever::isInCanonicalOrder(sorted.columns(), std::vector<uint64_t>{2}));
  // Tables without rows or columns.
  TestTable empty{{{}, {}}};
  EXPECT_TRUE(
      qlever::isInCanonicalOrder(empty.columns(), ql::span<const uint64_t>{}));
  EXPECT_TRUE(qlever::isInCanonicalOrder(qlever::IdColumns{},
                                         ql::span<const uint64_t>{}));
}

// _____________________________________________________________________________
TEST(BlobDiffRowMerge, greedyMatch) {
  EXPECT_THAT(
      qlever::detail::greedyMatch(std::vector<uint64_t>{1, 3, 5},
                                  std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6}),
      ElementsAre(NONE, 0u, NONE, 1u, NONE, 2u, NONE));
  EXPECT_THAT(qlever::detail::greedyMatch(ql::span<const uint64_t>{},
                                          std::vector<uint64_t>{1, 2}),
              ElementsAre(NONE, NONE));
  EXPECT_THAT(qlever::detail::greedyMatch(std::vector<uint64_t>{1, 2},
                                          ql::span<const uint64_t>{}),
              IsEmpty());
}

// _____________________________________________________________________________
TEST(BlobDiff, identicalBlobsAreOneCopy) {
  auto config = buildTestIndex(manyTriples(50));
  Qlever source{EngineConfig{config}};
  auto compressed = pinAndSerialize(source);
  ParsedBlob blob{compressed};
  auto diff =
      diffAndCheck(blob.span(), blob.layout_, blob.span(), blob.layout_);
  auto stats = diff.statistics();
  EXPECT_EQ(stats.numCopyInstructions_, 1u);
  EXPECT_EQ(stats.numInsertInstructions_, 0u);
  EXPECT_EQ(stats.numCopiedBytes_, blob.bytes_.size());
  EXPECT_EQ(stats.numInsertedBytes_, 0u);
  auto described = describeBlobDiff(diff, blob.layout_);
  EXPECT_THAT(described, HasSubstr("entry all: columns"));
  EXPECT_THAT(described, HasSubstr("main vocabulary"));
}

// _____________________________________________________________________________
TEST(BlobDiff, updateWithKnownWords) {
  auto config = buildTestIndex(manyTriples(2000));
  Qlever source{EngineConfig{config}};
  auto baseCompressed = pinAndSerialize(source);
  // Insert and delete a few triples that only use known words.
  ad_utility::testing::applyUpdateToEngine(
      source,
      "INSERT DATA { <s10> <q> <o20> . <s100> <p> <o1> . <s1999> <p> <o0> }");
  ad_utility::testing::applyUpdateToEngine(
      source, "DELETE DATA { <s5> <p> <o5> . <s1500> <q> \"literal 1500\" }");
  auto targetCompressed = pinAndSerialize(source);

  ParsedBlob base{baseCompressed};
  ParsedBlob target{targetCompressed};
  EXPECT_EQ(target.layout_.blobVersion_,
            Manager::formatVersionWithoutSecondaryVocab);
  // The diff is only small if the tables are in canonical order (which they
  // are, because the pinned queries are sorted by all their columns).
  EXPECT_TRUE(allTablesCanonical(base));
  EXPECT_TRUE(allTablesCanonical(target));
  EXPECT_EQ(target.layout_.entryLayouts_[0].numRows_, 4000u + 3 - 2);

  auto diff =
      diffAndCheck(base.span(), base.layout_, target.span(), target.layout_);
  auto stats = computeBlobDiffStatistics(diff, target.layout_);
  std::cout << "Diff for an update with known words (target has "
            << target.bytes_.size() << " bytes):\n"
            << stats.toString() << std::endl;
  EXPECT_LT(stats.instructions_.numInsertedBytes_, target.bytes_.size() / 20);
  EXPECT_EQ(stats.section("metadata").insertedBytes_, 0u);
  EXPECT_EQ(stats.section("main vocabulary").insertedBytes_, 0u);
  const auto& columns = stats.section("entry all: columns");
  EXPECT_EQ(columns.size_, 3u * 8 * target.layout_.entryLayouts_[0].numRows_);
  EXPECT_EQ(columns.insertedBytes_, 3u * 3 * 8);
  EXPECT_EQ(columns.copiedBytes_, columns.size_ - columns.insertedBytes_);
  // The diff is a lot smaller than the target, also after compression.
  EXPECT_LT(serializeBlobDiffToFile(diff).size(), targetCompressed.size() / 4);
}

// _____________________________________________________________________________
TEST(BlobDiff, updateWithNewWords) {
  auto config = buildTestIndex(manyTriples(2000));
  Qlever source{EngineConfig{config}};
  auto baseCompressed = pinAndSerialize(source);
  ad_utility::testing::applyUpdateToEngine(
      source,
      "INSERT DATA { <new1> <p> <o20> . <s100> <new2> <o1> . <s7> <p> "
      "\"new literal\" }");
  ad_utility::testing::applyUpdateToEngine(source,
                                           "DELETE DATA { <s5> <p> <o5> }");
  auto targetCompressed = pinAndSerialize(source);

  ParsedBlob base{baseCompressed};
  ParsedBlob target{targetCompressed};
  EXPECT_EQ(target.layout_.blobVersion_,
            Manager::formatVersionWithSecondaryVocab);
  auto diff =
      diffAndCheck(base.span(), base.layout_, target.span(), target.layout_);
  auto stats = computeBlobDiffStatistics(diff, target.layout_);
  std::cout << "Diff for an update with new words (target has "
            << target.bytes_.size() << " bytes):\n"
            << stats.toString() << std::endl;
  EXPECT_EQ(stats.section("metadata").insertedBytes_, 0u);
  EXPECT_EQ(stats.section("main vocabulary").insertedBytes_, 0u);
  // The writer always writes canonical tables, so the rows with the new words
  // (which have the `Id`s of the secondary vocabulary) are inserted into the
  // tables, and the rest of the tables is copied.
  EXPECT_TRUE(allTablesCanonical(target));
  EXPECT_LT(stats.instructions_.numInsertedBytes_, target.bytes_.size() / 20);
  const auto& columns = stats.section("entry all: columns");
  EXPECT_GT(columns.copiedBytes_, columns.size_ * 99 / 100);
}

// _____________________________________________________________________________
TEST(BlobDiff, diffFileRoundTripAndLoading) {
  auto config = buildTestIndex(manyTriples(300));
  Qlever source{EngineConfig{config}};
  auto baseCompressed = pinAndSerialize(source);
  ad_utility::testing::applyUpdateToEngine(
      source, "INSERT DATA { <new1> <p> <o20> . <s100> <p> <o1> }");
  ad_utility::testing::applyUpdateToEngine(source,
                                           "DELETE DATA { <s5> <p> <o5> }");
  auto targetCompressed = pinAndSerialize(source);

  ParsedBlob base{baseCompressed};
  ParsedBlob target{targetCompressed};
  auto diff =
      diffAndCheck(base.span(), base.layout_, target.span(), target.layout_);
  auto diffFile = serializeBlobDiffToFile(diff);
  auto readBack = readBlobDiffFromFile(diffFile);
  EXPECT_EQ(readBack.instructions(), diff.instructions());
  EXPECT_EQ(readBack.baseChecksum(), diff.baseChecksum());
  EXPECT_EQ(readBack.targetSize(), diff.targetSize());

  auto resultCompressed = applyBlobDiff(baseCompressed, diffFile);
  auto result = decompressOrFail(resultCompressed);
  EXPECT_TRUE(std::equal(result.begin(), result.end(), target.bytes_.begin(),
                         target.bytes_.end()));
  EXPECT_EQ(resultCompressed, targetCompressed);

  // The result is a standard blob that answers queries.
  Qlever loaded{EngineConfig{}, /*skipLoading=*/true};
  EXPECT_NO_THROW(
      loaded.deserializeVocabAndNamedCacheFromCompressedBlob(resultCompressed));
  auto expected =
      source.query(std::string{allQuery}, ad_utility::MediaType::tsv);
  // NOTE: The order of the rows with the new words differs (the `Id`s of the
  // secondary vocabulary are sorted after the ones of the main vocabulary), so
  // compare the sets of rows.
  auto loadedResult = loaded.query(
      "SELECT ?s ?p ?o WHERE { SERVICE ql:cached-result-with-name-all {} }",
      ad_utility::MediaType::tsv);
  EXPECT_THAT(sortedLines(loadedResult), Eq(sortedLines(expected)));
  EXPECT_THAT(expected, HasSubstr("<new1>"));
  EXPECT_THAT(expected, Not(HasSubstr("<s5>\t<p>\t<o5>")));
}

// _____________________________________________________________________________
TEST(BlobDiff, wrongBaseAndInvalidFilesThrow) {
  auto config = buildTestIndex(manyTriples(30));
  Qlever source{EngineConfig{config}};
  auto firstCompressed = pinAndSerialize(source);
  ad_utility::testing::applyUpdateToEngine(source,
                                           "INSERT DATA { <s1> <q> <o2> }");
  auto secondCompressed = pinAndSerialize(source);
  ParsedBlob first{firstCompressed};
  ParsedBlob second{secondCompressed};
  auto diff =
      diffAndCheck(first.span(), first.layout_, second.span(), second.layout_);
  auto diffFile = serializeBlobDiffToFile(diff);

  // The diff only fits its base.
  EXPECT_NO_THROW(applyBlobDiff(firstCompressed, diffFile));
  EXPECT_ANY_THROW(applyBlobDiff(secondCompressed, diffFile));
  EXPECT_ANY_THROW(diff.apply(second.span()));
  // The base is not a blob.
  std::vector<char> garbage(100, 'x');
  EXPECT_ANY_THROW(applyBlobDiff(garbage, diffFile));
  // The diff is not a diff file.
  EXPECT_ANY_THROW(applyBlobDiff(firstCompressed, garbage));
  EXPECT_ANY_THROW(readBlobDiffFromFile(firstCompressed));
  // A valid ZSTD frame with the wrong magic bytes.
  EXPECT_ANY_THROW(readBlobDiffFromFile(Manager::compressBlob(garbage)));
}

// _____________________________________________________________________________
TEST(BlobDiff, tableNotInCanonicalOrderFallsBackToInsert) {
  HandmadePrefix p;
  // The target is not sorted on the second column for the same first column.
  HandmadeEntry base{
      "e", {{1, 2, 3, 4}, {1, 1, 1, 1}, {10, 20, 30, 40}}, {0}, std::nullopt};
  HandmadeEntry target{
      "e", {{1, 2, 3, 4}, {1, 1, 1, 1}, {10, 30, 20, 40}}, {}, std::nullopt};
  auto baseBlob = makeHandmadeBlob(p.prefix_, nullptr, 1, {base});
  auto targetBlob = makeHandmadeBlob(p.prefix_, nullptr, 1, {target});
  auto baseLayout = BlobLayout::parse(ql::span<const char>{baseBlob});
  auto targetLayout = BlobLayout::parse(ql::span<const char>{targetBlob});
  // Here `resultSortedOn_` differs, so no merge is attempted.
  auto diff = diffAndCheck(baseBlob, baseLayout, targetBlob, targetLayout);
  auto stats = computeBlobDiffStatistics(diff, targetLayout);
  const auto& columns = stats.section("entry e: columns");
  // Only the columns that are identical are copied.
  EXPECT_EQ(columns.copiedBytes_, 2u * 4 * 8);
  EXPECT_EQ(columns.insertedBytes_, 4u * 8);

  // Same `resultSortedOn_`, but the target is not canonical (the first column
  // is not sorted, or the third column is not sorted for equal first and
  // second column), so the merge is not used.
  HandmadeEntry target2{
      "e", {{2, 1, 3, 4}, {1, 1, 1, 1}, {10, 30, 20, 40}}, {0}, std::nullopt};
  HandmadeEntry target3{
      "e", {{1, 1, 3, 4}, {1, 1, 1, 1}, {30, 20, 30, 40}}, {0}, std::nullopt};
  for (const auto& t : {target2, target3}) {
    auto blob = makeHandmadeBlob(p.prefix_, nullptr, 1, {t});
    auto layout = BlobLayout::parse(ql::span<const char>{blob});
    auto d = diffAndCheck(baseBlob, baseLayout, blob, layout);
    auto s = computeBlobDiffStatistics(d, layout);
    EXPECT_LT(s.section("entry e: columns").copiedBytes_, 3u * 4 * 8);
  }
}

// _____________________________________________________________________________
TEST(BlobDiff, rowMergeWithDuplicatesAndNewTables) {
  HandmadePrefix p;
  HandmadeEntry base{
      "e", {{1, 1, 2, 5, 5, 7}, {1, 1, 2, 5, 5, 7}}, {0, 1}, std::nullopt};
  // Delete (2, 2) and one of the duplicates (5, 5), add a third (1, 1), and
  // add (6, 6) and (9, 9).
  HandmadeEntry target{"e",
                       {{1, 1, 1, 5, 6, 7, 9}, {1, 1, 1, 5, 6, 7, 9}},
                       {0, 1},
                       std::nullopt};
  // A table with only new rows, and an entry that is new.
  HandmadeEntry allNew{"f", {{100, 101}, {1, 1}}, {0}, std::nullopt};
  HandmadeEntry baseF{"f", {{1, 2}, {1, 1}}, {0}, std::nullopt};
  auto baseBlob = makeHandmadeBlob(p.prefix_, nullptr, 1, {base, baseF});
  auto targetBlob = makeHandmadeBlob(
      p.prefix_, nullptr, 1,
      {target, allNew, HandmadeEntry{"g", {{4, 5}, {6, 7}}, {}, std::nullopt}});
  auto baseLayout = BlobLayout::parse(ql::span<const char>{baseBlob});
  auto targetLayout = BlobLayout::parse(ql::span<const char>{targetBlob});
  auto diff = diffAndCheck(baseBlob, baseLayout, targetBlob, targetLayout);
  auto stats = computeBlobDiffStatistics(diff, targetLayout);
  std::cout << stats.toString() << std::endl;
  // 7 of the 14 values of the entry "e" are new (a third (1, 1), (6, 6), and
  // (9, 9), in each of the two columns).
  EXPECT_EQ(stats.section("entry e: columns").insertedBytes_, 3u * 2 * 8);
  EXPECT_EQ(stats.section("entry e: columns").copiedBytes_, 4u * 2 * 8);
  // Everything in the entries "f" and "g" is new. Note that the second column
  // of "f" is identical, but is also found via the merge (no row of the first
  // column matches, so the whole column is inserted).
  EXPECT_EQ(stats.section("entry f: columns").copiedBytes_, 0u);
  EXPECT_EQ(stats.section("entry g: columns").copiedBytes_, 0u);
}

// _____________________________________________________________________________
TEST(BlobDiff, secondaryVocabSegmentsAndGeoIndexAreCopied) {
  HandmadePrefix p;
  SecondaryVocabulary baseVocab;
  baseVocab.appendSegment(segmentOf({"aaa", "bbbb", "cc"}));
  SecondaryVocabulary targetVocab;
  targetVocab.appendSegment(segmentOf({"aaa", "bbbb", "cc"}));
  targetVocab.appendSegment(segmentOf({"ab", "zz"}));

  HandmadeEntry base{"geo",
                     {{1, 2, 3, 4}},
                     {0},
                     HandmadeGeo{1.5, {std::string(100, 'a')}, {0, 1, 2, 3}}};
  // The row 1 is deleted, and a new row with a new shape in a new segment is
  // added.
  HandmadeEntry target{
      "geo",
      {{1, 3, 4, 5}},
      {0},
      HandmadeGeo{1.5,
                  {std::string(100, 'a'), std::string(50, 'b')},
                  {0, 2, 3, uint64_t{1} << 32}}};
  auto baseBlob = makeHandmadeBlob(p.prefix_, &baseVocab, 2, {base});
  auto targetBlob = makeHandmadeBlob(p.prefix_, &targetVocab, 2, {target});
  auto baseLayout = BlobLayout::parse(ql::span<const char>{baseBlob});
  auto targetLayout = BlobLayout::parse(ql::span<const char>{targetBlob});
  auto diff = diffAndCheck(baseBlob, baseLayout, targetBlob, targetLayout);
  auto stats = computeBlobDiffStatistics(diff, targetLayout);
  std::cout << stats.toString() << std::endl;
  // The first segment of the secondary vocabulary is copied.
  EXPECT_GE(stats.section("secondary vocabulary").copiedBytes_,
            baseLayout.secondaryVocab_.segments_[0].size());
  // The first S2 segment (100 bytes) and three of the shape ids are copied.
  EXPECT_GE(stats.section("entry geo: geo index").copiedBytes_, 100u + 3 * 8);
  EXPECT_GE(stats.section("entry geo: columns").copiedBytes_, 3u * 8);
  EXPECT_EQ(stats.section("entry geo: columns").insertedBytes_, 8u);

  // A base without a secondary vocabulary (and a geo index with the legacy
  // layout) also works, everything is inserted.
  auto baseNoVocab = makeHandmadeBlob(p.prefix_, nullptr, 1, {});
  auto baseNoVocabLayout = BlobLayout::parse(ql::span<const char>{baseNoVocab});
  diffAndCheck(baseNoVocab, baseNoVocabLayout, targetBlob, targetLayout);
  diffAndCheck(baseBlob, baseLayout, baseNoVocab, baseNoVocabLayout);
}
