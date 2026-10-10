// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>
#include <future>
#include <iterator>
#include <string>
#include <vector>

#include "../../util/FileTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "VocabularyTestHelpers.h"
#include "index/vocabulary/GeoVocabulary.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "rdfTypes/GeoCellGrid.h"

using ad_utility::GeoCellGrid;
using ad_utility::VocabularyType;

namespace {
// The words of the tests below: `numWords` words in a fixed order, of which
// every fourth is a WKT literal (so that a geo split vocabulary gets words in
// both of its sub-vocabularies), together with an external flag that is `true`
// for all but every seventh word (so that an internal-external vocabulary
// caches some of them because of the flag and some because of the
// milestones).
std::vector<std::pair<std::string, bool>> makeWords(size_t numWords) {
  std::vector<std::pair<std::string, bool>> words;
  words.reserve(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    std::string word =
        i % 4 == 3 ? absl::StrCat("\"POINT(", i % 360 - 180, " ", i % 170 - 85,
                                  ")\"^^<http://www.opengis.net/ont/geosparql#"
                                  "wktLiteral>")
                   : absl::StrCat("<http://example.org/word/", i, ">");
    words.emplace_back(std::move(word), i % 7 != 0);
  }
  return words;
}

// Read a whole file.
std::string readFile(const std::string& filename) {
  std::ifstream file{filename, std::ios::binary};
  EXPECT_TRUE(file.is_open()) << filename;
  return std::string{std::istreambuf_iterator<char>{file}, {}};
}

// Write the `words` with the word-by-word writer of a vocabulary of the given
// `type` to `filename` and return the index of every word.
std::vector<uint64_t> writeWordByWord(
    VocabularyType type, const std::string& filename,
    const std::vector<std::pair<std::string, bool>>& words) {
  auto writer = PolymorphicVocabulary::makeDiskWriterPtr(filename, type);
  std::vector<uint64_t> indices;
  for (const auto& [word, isExternal] : words) {
    indices.push_back((*writer)(word, isExternal));
  }
  writer->finish();
  return indices;
}

// Write the `words` with the parallel `writer`, as the vocabulary merger of a
// follow-up PR will do it: each word goes to the block of its sub-vocabulary,
// full blocks are prepared on threads of their own (several at a time) and
// appended in order. Return the index of every word, as `indexOf` reports it.
std::vector<uint64_t> writeInBlocks(
    ParallelWordWriterBase& writer,
    const std::vector<std::pair<std::string, bool>>& words) {
  const uint8_t numSubs = writer.numSubVocabularies();
  std::vector<WordBlock> openBlocks(numSubs);
  std::vector<uint64_t> numWordsWritten(numSubs, 0);
  std::vector<std::vector<std::future<AppendBlock>>> preparedBlocks(numSubs);
  std::vector<uint64_t> indices;

  // Prepare a full (or the last) block of the sub-vocabulary `sub`
  // asynchronously.
  auto flush = [&](uint8_t sub) {
    WordBlock block = std::exchange(openBlocks[sub], WordBlock{});
    openBlocks[sub].payloadSize_ = block.payloadSize_;
    if (block.empty()) {
      return;
    }
    block.firstPosition_ = numWordsWritten[sub];
    numWordsWritten[sub] += block.numWords();
    auto& blockWriter = writer.blockWriter(sub);
    preparedBlocks[sub].push_back(std::async(
        std::launch::async, [&blockWriter, block = std::move(block)]() mutable {
          return blockWriter.prepare(std::move(block));
        }));
  };

  std::vector<uint64_t> numWords(numSubs, 0);
  for (uint8_t sub = 0; sub < numSubs; ++sub) {
    openBlocks[sub].payloadSize_ =
        writer.blockWriter(sub).precomputedPayloadSize();
  }
  std::string payload;
  for (const auto& [word, isExternal] : words) {
    uint8_t sub = writer.subVocabularyOf(word);
    indices.push_back(writer.indexOf(sub, numWords[sub]++, word));
    payload.resize(openBlocks[sub].payloadSize_);
    if (!payload.empty()) {
      writer.blockWriter(sub).precomputePayload(word, payload);
    }
    openBlocks[sub].push(word, isExternal, payload);
    if (openBlocks[sub].numWords() == writer.blockWriter(sub).blockSize()) {
      flush(sub);
    }
  }
  for (uint8_t sub = 0; sub < numSubs; ++sub) {
    flush(sub);
    for (auto& future : preparedBlocks[sub]) {
      AppendBlock appendBlock = future.get();
      std::move(appendBlock)();
    }
  }
  writer.finish();
  return indices;
}

// Same as above, for the parallel writer of a vocabulary of the given `type`
// that writes to `filename`.
std::vector<uint64_t> writeInBlocks(
    VocabularyType type, const std::string& filename,
    const std::vector<std::pair<std::string, bool>>& words) {
  PolymorphicVocabulary vocabulary;
  vocabulary.resetToType(type);
  auto writer = vocabulary.makeParallelWriterPtr(filename);
  return writeInBlocks(*writer, words);
}

// The WKT literal with the given `content`.
std::string wkt(std::string_view content) {
  return absl::StrCat("\"", content,
                      "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
}

// The block of the `geoWriter` (a `GeoVocabulary::BlockWriter`) with the given
// `words` (all external), starting at `firstPosition`, with their precomputed
// payload.
WordBlock makeGeoBlock(const BlockWriterBase& geoWriter, uint64_t firstPosition,
                       const std::vector<std::string>& words) {
  WordBlock block;
  block.firstPosition_ = firstPosition;
  block.payloadSize_ = geoWriter.precomputedPayloadSize();
  std::string payload(block.payloadSize_, '\0');
  for (const auto& word : words) {
    geoWriter.precomputePayload(word, payload);
    block.push(word, true, payload);
  }
  return block;
}
}  // namespace

// Test that for every vocabulary type that an index can be built with, the
// parallel writer (blocks, prepared concurrently) produces exactly the files
// of the word-by-word writer, and reports the same indices. The number of
// words exceeds the block size of the compressed vocabularies, so the main
// vocabulary of every type has more than one block.
TEST(BlockWriter, sameFilesAsWordWriter) {
  const auto words = makeWords((1u << 20) + 123'456);
  for (auto type : VocabularyType::allForIndexBuilding_) {
    VocabularyType vocabType{type};
    std::string filenameA = absl::StrCat(gtestCurrentTestName(), ".",
                                         vocabType.toString(), ".wordByWord");
    std::string filenameB = absl::StrCat(gtestCurrentTestName(), ".",
                                         vocabType.toString(), ".blocks");
    auto cleanupA = vocabulary_test::makeVocabFileCleanup(
        filenameA, PolymorphicVocabulary::fileSuffixes(vocabType));
    auto cleanupB = vocabulary_test::makeVocabFileCleanup(
        filenameB, PolymorphicVocabulary::fileSuffixes(vocabType));

    auto indicesA = writeWordByWord(vocabType, filenameA, words);
    auto indicesB = writeInBlocks(vocabType, filenameB, words);
    EXPECT_EQ(indicesA, indicesB) << vocabType.toString();
    for (const auto& suffix : PolymorphicVocabulary::fileSuffixes(vocabType)) {
      EXPECT_EQ(readFile(absl::StrCat(filenameA, suffix)),
                readFile(absl::StrCat(filenameB, suffix)))
          << vocabType.toString() << suffix;
    }
  }
}

// Test the block writer that is derived from a word callback (the fallback for
// vocabularies without a block writer of their own): the words reach the
// callback in order, the callback's `finish` is called, and a callback that
// reports a wrong position is detected.
TEST(BlockWriter, fromCallback) {
  struct Callback {
    std::vector<std::pair<std::string, bool>> words_;
    bool finished_ = false;
    uint64_t offset_ = 0;
    uint64_t operator()(std::string_view word, bool isExternal) {
      words_.emplace_back(word, isExternal);
      return words_.size() - 1 + offset_;
    }
    void finish() { finished_ = true; }
  };
  BlockWriterFromCallback<Callback> writer{Callback{}};
  EXPECT_EQ(writer.blockSize(), DEFAULT_WORDS_PER_VOCABULARY_BLOCK);
  EXPECT_EQ(writer.indexOf(42, "x"), 42u);
  WordBlock block;
  block.push("a", true);
  block.push("bc", false);
  AppendBlock appendBlock = writer.prepare(std::move(block));
  std::move(appendBlock)();
  WordBlock block2;
  block2.firstPosition_ = 2;
  block2.push("d", true);
  writer.prepare(std::move(block2))();
  EXPECT_THAT(
      writer.callback().words_,
      ::testing::ElementsAre(std::pair{"a", true}, std::pair{"bc", false},
                             std::pair{"d", true}));
  EXPECT_FALSE(writer.callback().finished_);
  writer.finish();
  EXPECT_TRUE(writer.callback().finished_);

  // A callback whose positions are off.
  BlockWriterFromCallback<Callback> writerOff{Callback{{}, false, 1}};
  WordBlock block3;
  block3.push("a", true);
  EXPECT_ANY_THROW(writerOff.prepare(std::move(block3))());
  writerOff.finish();
}

// Test the accessors of a `WordBlock`.
TEST(BlockWriter, wordBlock) {
  // An empty block has no data and no offsets.
  WordBlock block;
  EXPECT_TRUE(block.empty());
  EXPECT_TRUE(block.data().empty());
  EXPECT_TRUE(block.offsets().empty());

  // A block with three words (one of them empty).
  block.push("ab", true);
  block.push("", false);
  block.push("cde", true);
  EXPECT_EQ(block.numWords(), 3u);
  EXPECT_EQ(block.word(2), "cde");
  EXPECT_THAT(block.words(), ::testing::ElementsAre("ab", "", "cde"));
  EXPECT_EQ(std::string_view(block.data().data(), block.data().size()),
            "abcde");
  EXPECT_THAT(block.offsets(), ::testing::ElementsAre(0, 2, 2, 5));
}

// Test that with a geo cell grid, the `BlockWriter` of a `GeoVocabulary`
// reports the same indices and writes the same files as its `WordWriter`, and
// that the opened vocabulary recomputes these indices from the stored geometry
// info.
TEST(GeoBlockWriter, sameFilesAndIndicesWithGrid) {
  using GV = GeoVocabulary<VocabularyInMemory>;
  const GeoCellGrid grid{2};
  const std::string filenameA =
      absl::StrCat(gtestCurrentTestName(), ".wordByWord");
  const std::string filenameB = absl::StrCat(gtestCurrentTestName(), ".blocks");
  auto cleanupA = vocabulary_test::makeVocabFileCleanup<GV>(filenameA);
  auto cleanupB = vocabulary_test::makeVocabFileCleanup<GV>(filenameB);

  // Points, small polygons, polygons that span several cells, and invalid
  // literals, ordered by their cell, more than two blocks of them. Many
  // corners lie on the boundaries of the cells (multiples of 90 degrees of
  // longitude and of 45 degrees of latitude).
  std::vector<std::pair<std::string, bool>> words;
  for (size_t i = 0; i < 2 * DEFAULT_WORDS_PER_VOCABULARY_BLOCK + 1; ++i) {
    long lng = static_cast<long>((i * 7) % 300) - 180;
    long lat = static_cast<long>((i * 11) % 130) - 90;
    auto polygon = [lng, lat](long size) {
      return wkt(absl::StrCat("POLYGON((", lng, " ", lat, ", ", lng + size, " ",
                              lat, ", ", lng + size, " ", lat + size, ", ", lng,
                              " ", lat, "))"));
    };
    switch (i % 5) {
      case 0:
      case 1:
        words.emplace_back(wkt(absl::StrCat("POINT(", lng, " ", lat, ")")),
                           true);
        break;
      case 2:
        words.emplace_back(polygon(1), true);
        break;
      case 3:
        words.emplace_back(polygon(50), true);
        break;
      default:
        words.emplace_back(wkt("NOTAGEOMETRY"), true);
    }
  }
  ql::ranges::stable_sort(words, {}, [&grid](const auto& word) {
    return grid.cellIndexFromWktLiteral(word.first);
  });
  EXPECT_NE(grid.cellIndexFromWktLiteral(words.front().first),
            grid.sentinelCell());
  EXPECT_EQ(grid.cellIndexFromWktLiteral(words.back().first),
            grid.sentinelCell());

  // The word-by-word writer.
  std::vector<uint64_t> indicesA;
  {
    GV vocabulary;
    vocabulary.setGeoCellGrid(grid);
    auto writer = vocabulary.makeDiskWriterPtr(filenameA);
    for (const auto& [word, isExternal] : words) {
      indicesA.push_back((*writer)(word, isExternal));
    }
    writer->finish();
  }

  // The block writer.
  std::vector<uint64_t> indicesB;
  {
    GV vocabulary;
    vocabulary.setGeoCellGrid(grid);
    SingleVocabularyParallelWriter writer{
        vocabulary.makeBlockWriterPtr(filenameB)};
    indicesB = writeInBlocks(writer, words);
  }
  EXPECT_EQ(indicesA, indicesB);
  for (const auto& suffix : GV::fileSuffixes()) {
    EXPECT_EQ(readFile(absl::StrCat(filenameA, suffix)),
              readFile(absl::StrCat(filenameB, suffix)))
        << suffix;
  }

  // The opened vocabulary recomputes the same indices.
  GV vocabulary;
  vocabulary.setGeoCellGrid(grid);
  vocabulary.open(filenameB);
  std::vector<uint64_t> recomputedIndices;
  for (size_t i = 0; i < words.size(); ++i) {
    recomputedIndices.push_back(
        vocabulary.indexFromPosition(i, words[i].first));
  }
  EXPECT_EQ(recomputedIndices, indicesA);
}

// Test that the `BlockWriter` of a `GeoVocabulary` with a geo cell grid
// rejects words that are not ordered by their cell, within a block and across
// blocks.
TEST(GeoBlockWriter, cellOrderIsChecked) {
  using GV = GeoVocabulary<VocabularyInMemory>;
  const GeoCellGrid grid{2};
  const std::string filename = gtestCurrentTestName();
  auto cleanup = vocabulary_test::makeVocabFileCleanup<GV>(filename);
  GV vocabulary;
  vocabulary.setGeoCellGrid(grid);
  const std::string cell3 = wkt("POINT(170 -80)");
  const std::string cell12 = wkt("POINT(-170 80)");
  ASSERT_LT(grid.cellIndexFromWktLiteral(cell3),
            grid.cellIndexFromWktLiteral(cell12));

  // Within a block, the order is checked in `prepare`.
  const auto wrongOrder =
      ::testing::HasSubstr("the order of their geo grid cells");
  {
    auto writer = vocabulary.makeBlockWriterPtr(filename);
    AD_EXPECT_THROW_WITH_MESSAGE(
        writer->prepare(makeGeoBlock(*writer, 0, {cell12, cell3})), wrongOrder);
    writer->finish();
  }

  // Across blocks, the order is checked in the append step.
  {
    auto writer = vocabulary.makeBlockWriterPtr(filename);
    writer->prepare(makeGeoBlock(*writer, 0, {cell3, cell12}))();
    auto appendBlock = writer->prepare(makeGeoBlock(*writer, 2, {cell3}));
    AD_EXPECT_THROW_WITH_MESSAGE(std::move(appendBlock)(), wrongOrder);
    writer->finish();
  }
}
