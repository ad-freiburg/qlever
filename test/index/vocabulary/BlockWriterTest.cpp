// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
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
#include "VocabularyTestHelpers.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/VocabularyTypes.h"

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

// Write the `words` with the parallel writer of a vocabulary of the given
// `type` to `filename`, as the vocabulary merger does it: each word goes to
// the block of its sub-vocabulary, full blocks are prepared on threads of
// their own (several at a time) and appended in order. Return the index of
// every word, as `indexOf` reports it.
std::vector<uint64_t> writeInBlocks(
    VocabularyType type, const std::string& filename,
    const std::vector<std::pair<std::string, bool>>& words) {
  PolymorphicVocabulary vocabulary;
  vocabulary.resetToType(type);
  auto writer = vocabulary.makeParallelWriterPtr(filename);
  const uint8_t numSubs = writer->numSubVocabularies();
  std::vector<WordBlock> openBlocks(numSubs);
  std::vector<uint64_t> numWordsWritten(numSubs, 0);
  std::vector<std::vector<std::future<std::unique_ptr<PreparedBlockBase>>>>
      preparedBlocks(numSubs);
  std::vector<uint64_t> indices;

  // Prepare a full (or the last) block of the sub-vocabulary `sub`
  // asynchronously.
  auto flush = [&](uint8_t sub) {
    WordBlock block = std::exchange(openBlocks[sub], WordBlock{});
    if (block.empty()) {
      return;
    }
    block.firstPosition_ = numWordsWritten[sub];
    numWordsWritten[sub] += block.numWords();
    auto& blockWriter = writer->blockWriter(sub);
    preparedBlocks[sub].push_back(std::async(
        std::launch::async, [&blockWriter, block = std::move(block)]() mutable {
          return blockWriter.prepare(std::move(block));
        }));
  };

  std::vector<uint64_t> numWords(numSubs, 0);
  for (const auto& [word, isExternal] : words) {
    uint8_t sub = writer->subVocabularyOf(word);
    indices.push_back(writer->indexOf(sub, numWords[sub]++, word));
    openBlocks[sub].push(word, isExternal);
    if (openBlocks[sub].numWords() == writer->blockWriter(sub).blockSize()) {
      flush(sub);
    }
  }
  for (uint8_t sub = 0; sub < numSubs; ++sub) {
    flush(sub);
    for (auto& future : preparedBlocks[sub]) {
      writer->blockWriter(sub).append(future.get());
    }
  }
  writer->finish();
  return indices;
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
    std::string filenameA =
        absl::StrCat("blockWriterTest.", vocabType.toString(), ".wordByWord");
    std::string filenameB =
        absl::StrCat("blockWriterTest.", vocabType.toString(), ".blocks");
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
// vocabularies without a block writer of their own, and the writer of the
// tests of the vocabulary merger): the words reach the callback in order, the
// callback's `finish` is called, and a callback that reports a wrong position
// is detected.
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
  auto prepared = writer.prepare(std::move(block));
  writer.append(std::move(prepared));
  WordBlock block2;
  block2.firstPosition_ = 2;
  block2.push("d", true);
  writer.append(writer.prepare(std::move(block2)));
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
  EXPECT_ANY_THROW(writerOff.append(writerOff.prepare(std::move(block3))));
  writerOff.finish();
}
