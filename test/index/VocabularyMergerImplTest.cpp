//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <string>
#include <tuple>
#include <vector>

#include "../util/GTestHelpers.h"
#include "backports/filesystem.h"
#include "index/VocabularyMergerImpl.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"

namespace {
using ad_utility::vocabulary_merger::writePartialVocabularyToFile;

// Helper to conveniently create an entry for `ItemVec`.
ItemVec::value_type makeEntry(std::string_view word, bool isExternalized,
                              uint64_t id) {
  return {word, ItemVec::value_type::second_type{id, isExternalized}};
}

// Read back a file written by `writePartialVocabularyToFile` and return its
// contents as a vector of (word, isExternalized, id) tuples.
std::vector<std::tuple<std::string, bool, uint64_t>> readBack(
    const std::string& fileName) {
  ad_utility::serialization::FileReadSerializer reader{fileName};
  std::vector<std::tuple<std::string, bool, uint64_t>> result;
  reader >> result;
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(IndexVocabularyMergerImpl, writePartialVocabularyToFile) {
  std::string fileName =
      absl::StrCat(::testing::TempDir(), "/writePartialVocabularyToFile.tmp");
  absl::Cleanup cleanup{[&fileName]() { ad_utility::deleteFile(fileName); }};

  // A word large enough to cross the 16 MB flush threshold inside the function,
  // exercising the mid-loop flush in addition to the final flush.
  std::string bigWord(17ULL * 1024 * 1024, 'x');

  ItemVec els{
      makeEntry("\"alpha\"", false, 7),
      makeEntry("\"beta\"", true, 42),
      makeEntry("_:blank", false, 0),
      makeEntry(bigWord, true, 99),
  };

  writePartialVocabularyToFile(els, fileName);

  auto roundtrip = readBack(fileName);
  ASSERT_EQ(roundtrip.size(), els.size());
  EXPECT_EQ(roundtrip.at(0),
            std::make_tuple(std::string{"\"alpha\""}, false, uint64_t{7}));
  EXPECT_EQ(roundtrip.at(1),
            std::make_tuple(std::string{"\"beta\""}, true, uint64_t{42}));
  EXPECT_EQ(roundtrip.at(2),
            std::make_tuple(std::string{"_:blank"}, false, uint64_t{0}));
  EXPECT_EQ(roundtrip.at(3), std::make_tuple(bigWord, true, uint64_t{99}));

  // Empty input: only the size header (zero) is written.
  ItemVec empty;
  writePartialVocabularyToFile(empty, fileName);
  EXPECT_TRUE(readBack(fileName).empty());
}

namespace {
// A single entry of the sparse splitter index at the end of a file written by
// `writePartialVocabularyToFile`.
struct SplitterEntry {
  uint64_t byteOffsetOfFirstWordOfBlock_;
  uint64_t numWordsBeforeThisBlock_;
  std::string firstWord_;
  bool firstIsExternal_;
  std::string lastWord_;
  bool lastIsExternal_;
};

// Read the splitter index at the end of the given file. Check that the magic
// footer is present, then return all the entries of the index.
std::vector<SplitterEntry> readSplitterIndex(const std::string& fileName) {
  std::vector<SplitterEntry> result;
  auto fileSize = std::filesystem::file_size(fileName);
  EXPECT_GE(fileSize, 2 * sizeof(uint64_t));
  ad_utility::serialization::FileReadSerializer reader{fileName};
  reader.setSerializationPosition(fileSize - 2 * sizeof(uint64_t));
  uint64_t indexStartOffset = 0;
  uint64_t magic = 0;
  reader >> indexStartOffset;
  reader >> magic;
  EXPECT_EQ(magic, PARTIAL_VOCAB_INDEX_MAGIC);

  reader.setSerializationPosition(indexStartOffset);
  uint64_t numIndexEntries = 0;
  reader >> numIndexEntries;
  for (uint64_t i = 0; i < numIndexEntries; ++i) {
    SplitterEntry entry;
    reader >> entry.byteOffsetOfFirstWordOfBlock_;
    reader >> entry.numWordsBeforeThisBlock_;
    reader >> entry.firstWord_;
    reader >> entry.firstIsExternal_;
    reader >> entry.lastWord_;
    reader >> entry.lastIsExternal_;
    result.push_back(std::move(entry));
  }
  return result;
}

// Read the single word record that starts at the given byte offset of the
// given file.
std::tuple<std::string, bool, uint64_t> readWordAt(const std::string& fileName,
                                                   uint64_t byteOffset) {
  ad_utility::serialization::FileReadSerializer reader{fileName};
  reader.setSerializationPosition(byteOffset);
  std::string word;
  bool isExternal = false;
  uint64_t id = 0;
  reader >> word;
  reader >> isExternal;
  reader >> id;
  return {std::move(word), isExternal, id};
}

// Create `numWords` distinct words. The words have different lengths, such
// that the byte offsets in the splitter index cannot accidentally be correct
// because of a uniform record size.
std::vector<std::string> makeWords(size_t numWords) {
  std::vector<std::string> words;
  for (size_t i = 0; i < numWords; ++i) {
    words.push_back(absl::StrCat("\"word", std::string(i % 5, 'a'), i, "\""));
  }
  return words;
}

// Convert the `words` to an `ItemVec` with consecutive ids and alternating
// `isExternal` flags. The `words` have to outlive the returned `ItemVec`.
ItemVec makeItemVec(const std::vector<std::string>& words) {
  ItemVec els;
  for (size_t i = 0; i < words.size(); ++i) {
    els.push_back(makeEntry(words.at(i), i % 3 == 0, 2 * i + 1));
  }
  return els;
}

// Check that the splitter index of the given file correctly describes the
// given `els` for the given `splitterInterval`.
void checkSplitterIndex(const std::string& fileName, const ItemVec& els,
                        size_t splitterInterval) {
  auto entries = readSplitterIndex(fileName);
  size_t numWords = els.size();
  ASSERT_EQ(entries.size(),
            (numWords + splitterInterval - 1) / splitterInterval);
  for (size_t j = 0; j < entries.size(); ++j) {
    const auto& entry = entries.at(j);
    size_t firstIdx = j * splitterInterval;
    size_t lastIdx = std::min(firstIdx + splitterInterval, numWords) - 1;
    EXPECT_EQ(entry.numWordsBeforeThisBlock_, firstIdx);
    EXPECT_EQ(entry.firstWord_, els.at(firstIdx).first);
    EXPECT_EQ(entry.firstIsExternal_, els.at(firstIdx).second.isExternal());
    EXPECT_EQ(entry.lastWord_, els.at(lastIdx).first);
    EXPECT_EQ(entry.lastIsExternal_, els.at(lastIdx).second.isExternal());
    // The recorded byte offset has to point exactly at the record of the first
    // word of the block.
    EXPECT_EQ(readWordAt(fileName, entry.byteOffsetOfFirstWordOfBlock_),
              std::make_tuple(std::string{els.at(firstIdx).first},
                              els.at(firstIdx).second.isExternal(),
                              els.at(firstIdx).second.id()));
  }
}
}  // namespace

// _____________________________________________________________________________
TEST(IndexVocabularyMergerImpl, splitterIndexRoundtrip) {
  std::string fileName = absl::StrCat(gtestCurrentTestName(), ".tmp");
  constexpr size_t splitterInterval = 3;
  auto words = makeWords(10);
  auto els = makeItemVec(words);

  writePartialVocabularyToFile(els, fileName, splitterInterval);
  absl::Cleanup cleanup{[&fileName]() { ad_utility::deleteFile(fileName); }};

  // The words themselves are still readable as before, the appended index is
  // simply ignored.
  EXPECT_EQ(readBack(fileName).size(), els.size());

  checkSplitterIndex(fileName, els, splitterInterval);
}

// _____________________________________________________________________________
TEST(IndexVocabularyMergerImpl, splitterIndexEmptyVocabulary) {
  std::string fileName = absl::StrCat(gtestCurrentTestName(), ".tmp");
  ItemVec els;

  writePartialVocabularyToFile(els, fileName, 3);
  absl::Cleanup cleanup{[&fileName]() { ad_utility::deleteFile(fileName); }};

  EXPECT_TRUE(readBack(fileName).empty());
  // The footer is valid, but the index has no entries.
  EXPECT_TRUE(readSplitterIndex(fileName).empty());
  checkSplitterIndex(fileName, els, 3);
}

// _____________________________________________________________________________
TEST(IndexVocabularyMergerImpl, splitterIndexExactMultiple) {
  std::string fileName = absl::StrCat(gtestCurrentTestName(), ".tmp");
  constexpr size_t splitterInterval = 4;
  // The number of words is an exact multiple of the interval, so there is no
  // partial last block.
  auto words = makeWords(12);
  auto els = makeItemVec(words);

  writePartialVocabularyToFile(els, fileName, splitterInterval);
  absl::Cleanup cleanup{[&fileName]() { ad_utility::deleteFile(fileName); }};

  EXPECT_EQ(readBack(fileName).size(), els.size());
  ASSERT_EQ(readSplitterIndex(fileName).size(), 3u);
  checkSplitterIndex(fileName, els, splitterInterval);
}

namespace {
using ad_utility::vocabulary_merger::PartialVocabRunsInput;

// Return the name of the partial vocabulary file with the given index for the
// given `basename`.
std::string partialVocabFileName(const std::string& basename,
                                 size_t fileIndex) {
  return absl::StrCat(basename, PARTIAL_VOCAB_WORDS_INFIX, fileIndex);
}

// Check that the run with the given index of the `input` describes exactly the
// given `els`, both via its metadata and via the words that `readBlock`
// returns.
void checkRun(const PartialVocabRunsInput& input, size_t run,
              const ItemVec& els, size_t splitterInterval,
              ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ASSERT_EQ(input.numBlocks(run),
            (els.size() + splitterInterval - 1) / splitterInterval);
  size_t numWordsSoFar = 0;
  for (size_t block = 0; block < input.numBlocks(run); ++block) {
    size_t numElements = input.numElementsInBlock(run, block);
    ASSERT_EQ(numElements,
              std::min(splitterInterval, els.size() - numWordsSoFar));
    const auto& first = els.at(numWordsSoFar);
    const auto& last = els.at(numWordsSoFar + numElements - 1);
    EXPECT_EQ(input.firstKey(run, block).iriOrLiteral(), first.first);
    EXPECT_EQ(input.firstKey(run, block).isExternal(),
              first.second.isExternal());
    EXPECT_EQ(input.lastKey(run, block).iriOrLiteral(), last.first);
    EXPECT_EQ(input.lastKey(run, block).isExternal(), last.second.isExternal());

    auto words = input.readBlock(run, block);
    ASSERT_EQ(words.size(), numElements);
    for (size_t i = 0; i < numElements; ++i) {
      const auto& expected = els.at(numWordsSoFar + i);
      EXPECT_EQ(words.at(i).iriOrLiteral(), expected.first);
      EXPECT_EQ(words.at(i).isExternal(), expected.second.isExternal());
      EXPECT_EQ(words.at(i).id(), expected.second.id());
      EXPECT_EQ(words.at(i).partialFileId_, run);
    }
    numWordsSoFar += numElements;
  }
  EXPECT_EQ(numWordsSoFar, els.size());
}

// Check that the two given inputs have exactly the same block metadata.
void checkSameMetadata(
    const PartialVocabRunsInput& a, const PartialVocabRunsInput& b,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ASSERT_EQ(a.numRuns(), b.numRuns());
  for (size_t run = 0; run < a.numRuns(); ++run) {
    const auto& blocksA = a.blockMetadata(run);
    const auto& blocksB = b.blockMetadata(run);
    ASSERT_EQ(blocksA.size(), blocksB.size());
    for (size_t i = 0; i < blocksA.size(); ++i) {
      EXPECT_EQ(blocksA.at(i).byteOffset_, blocksB.at(i).byteOffset_);
      EXPECT_EQ(blocksA.at(i).numWordsBefore_, blocksB.at(i).numWordsBefore_);
      EXPECT_EQ(blocksA.at(i).firstKey_.word_, blocksB.at(i).firstKey_.word_);
      EXPECT_EQ(blocksA.at(i).firstKey_.isExternal_,
                blocksB.at(i).firstKey_.isExternal_);
      EXPECT_EQ(blocksA.at(i).lastKey_.word_, blocksB.at(i).lastKey_.word_);
      EXPECT_EQ(blocksA.at(i).lastKey_.isExternal_,
                blocksB.at(i).lastKey_.isExternal_);
    }
  }
}

// Remove the magic number at the end of the given file, such that a reader has
// to fall back to a sequential scan.
void destroyMagicFooter(const std::string& fileName) {
  ql::filesystem::resize_file(fileName,
                              ql::filesystem::file_size(fileName) - 8);
}
}  // namespace

// _____________________________________________________________________________
TEST(IndexVocabularyMergerImpl, partialVocabRunsInput) {
  std::string basename = gtestCurrentTestName();
  constexpr size_t splitterInterval = 3;
  // The two runs have different sizes, and only the second one has a number of
  // words that is an exact multiple of the splitter interval.
  auto words0 = makeWords(10);
  auto words1 = makeWords(6);
  auto els0 = makeItemVec(words0);
  auto els1 = makeItemVec(words1);
  std::array<std::string, 2> fileNames{partialVocabFileName(basename, 0),
                                       partialVocabFileName(basename, 1)};
  writePartialVocabularyToFile(els0, fileNames.at(0), splitterInterval);
  writePartialVocabularyToFile(els1, fileNames.at(1), splitterInterval);
  absl::Cleanup cleanup{[&fileNames]() {
    for (const auto& fileName : fileNames) {
      ad_utility::deleteFile(fileName);
    }
  }};

  // The metadata that was read from the sparse splitter index describes the
  // words correctly.
  PartialVocabRunsInput input{basename, 2, splitterInterval};
  ASSERT_EQ(input.numRuns(), 2u);
  checkRun(input, 0, els0, splitterInterval);
  checkRun(input, 1, els1, splitterInterval);

  // The largest block consists of `splitterInterval` words, so its memory is
  // bounded by that many words plus their fixed overhead.
  EXPECT_GT(input.maxBlockMemory().getBytes(), 0u);

  // Now destroy the footer of both files, such that the metadata has to be
  // built by the sequential fallback scan. The result has to be exactly the
  // same, including the byte offsets.
  for (const auto& fileName : fileNames) {
    destroyMagicFooter(fileName);
  }
  PartialVocabRunsInput fallbackInput{basename, 2, splitterInterval};
  ASSERT_EQ(fallbackInput.numRuns(), 2u);
  checkRun(fallbackInput, 0, els0, splitterInterval);
  checkRun(fallbackInput, 1, els1, splitterInterval);
  checkSameMetadata(input, fallbackInput);
}

// _____________________________________________________________________________
TEST(IndexVocabularyMergerImpl, partialVocabRunsInputEmptyVocabulary) {
  std::string basename = gtestCurrentTestName();
  std::string fileName = partialVocabFileName(basename, 0);
  ItemVec els;
  writePartialVocabularyToFile(els, fileName, 3);
  absl::Cleanup cleanup{[&fileName]() { ad_utility::deleteFile(fileName); }};

  PartialVocabRunsInput input{basename, 1, 3};
  ASSERT_EQ(input.numRuns(), 1u);
  EXPECT_EQ(input.numBlocks(0), 0u);
  EXPECT_EQ(input.maxBlockMemory().getBytes(), 0u);

  // The fallback scan yields the same (empty) result.
  destroyMagicFooter(fileName);
  PartialVocabRunsInput fallbackInput{basename, 1, 3};
  EXPECT_EQ(fallbackInput.numBlocks(0), 0u);
  checkSameMetadata(input, fallbackInput);
}
