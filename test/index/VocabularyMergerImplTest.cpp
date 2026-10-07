//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "../util/GTestHelpers.h"
#include "index/VocabularyMergerImpl.h"
#include "index/vocabulary_merger/PartialVocabularySkipPointers.h"
#include "util/Serializer/BufferedPreadReadSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"

namespace {
using ad_utility::vocabulary_merger::PartialVocabularySkipPointers;
using ad_utility::vocabulary_merger::readPartialVocabularySkipPointers;
using ad_utility::vocabulary_merger::writePartialVocabularyToFile;
using namespace ad_utility::memory_literals;

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

// Return `numWords` sorted words of varying lengths (including the empty
// word), and alternating `isExternal` flags.
std::vector<std::string> makeWords(size_t numWords) {
  std::vector<std::string> words;
  for (size_t i = 0; i < numWords; ++i) {
    words.push_back(absl::StrCat(i / 10, std::string(i % 10, 'x')));
  }
  if (!words.empty()) {
    words.front().clear();
  }
  return words;
}

// Return the `ItemVec` for the given `words`, where the `i`-th word has the
// local ID `2 * i + 1` and is external iff `i` is odd.
ItemVec makeItemVec(const std::vector<std::string>& words) {
  ItemVec els;
  for (size_t i = 0; i < words.size(); ++i) {
    els.push_back(makeEntry(words.at(i), i % 2 == 1, 2 * i + 1));
  }
  return els;
}

// Check that `actual` is the `i`-th entry of `els`.
void expectWord(const TripleComponentWithIndex& actual, const ItemVec& els,
                size_t i,
                ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  const auto& [word, idAndExternal] = els.at(i);
  EXPECT_EQ(actual.iriOrLiteral_, word);
  EXPECT_EQ(actual.isExternal_, idAndExternal.isExternal());
  EXPECT_EQ(actual.index_, idAndExternal.id());
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

// _____________________________________________________________________________
// Write partial vocabularies of different sizes with different skip pointer
// intervals, read back the skip pointers, and read each block separately via a
// `BufferedPreadReadSerializer` that starts at the block's byte offset.
TEST(IndexVocabularyMergerImpl, skipPointersRoundTrip) {
  std::string fileName = gtestCurrentTestName();
  absl::Cleanup cleanup{[&fileName]() { ad_utility::deleteFile(fileName); }};

  // The sizes include an empty vocabulary, sizes that are an exact multiple of
  // some of the intervals, and sizes that are smaller than some intervals.
  for (size_t numWords : {0, 1, 2, 6, 7, 21, 22, 100}) {
    auto words = makeWords(numWords);
    ItemVec els = makeItemVec(words);
    for (size_t interval : {1, 2, 3, 7, 50, 1000}) {
      writePartialVocabularyToFile(els, fileName, interval);
      auto trace = generateLocationTrace(
          AD_CURRENT_SOURCE_LOC(),
          absl::StrCat("numWords: ", numWords, ", interval: ", interval));

      // The old sequential reader, which ignores the skip pointers, still
      // works.
      auto roundtrip = readBack(fileName);
      ASSERT_EQ(roundtrip.size(), numWords);
      for (size_t i = 0; i < numWords; ++i) {
        EXPECT_EQ(std::get<0>(roundtrip.at(i)), words.at(i));
      }

      auto skipPointers = readPartialVocabularySkipPointers(fileName);
      EXPECT_EQ(skipPointers.numWords_, numWords);
      size_t numBlocks = (numWords + interval - 1) / interval;
      ASSERT_EQ(skipPointers.numBlocks(), numBlocks);

      auto file = std::make_shared<ad_utility::File>(fileName, "r");
      for (size_t block = 0; block < numBlocks; ++block) {
        const auto& pointer = skipPointers.skipPointers_.at(block);
        size_t begin = block * interval;
        size_t end = std::min(begin + interval, numWords);
        EXPECT_EQ(pointer.numWordsBefore_, begin);
        EXPECT_EQ(skipPointers.numWordsInBlock(block), end - begin);
        expectWord(pointer.firstWord_, els, begin);
        expectWord(pointer.lastWord_, els, end - 1);

        // Reading exactly the words of the block from its byte offset yields
        // the expected words and ends exactly at the end of the block.
        ad_utility::serialization::BufferedPreadReadSerializer reader{
            file, pointer.byteOffset_, 5_B};
        for (size_t i = begin; i < end; ++i) {
          TripleComponentWithIndex word;
          reader >> word;
          expectWord(word, els, i);
        }
        EXPECT_EQ(reader.getSerializationPosition(),
                  skipPointers.byteEndOfBlock(block));
      }
    }
  }
}

// _____________________________________________________________________________
// The default interval is `PARTIAL_VOCAB_SKIP_POINTER_INTERVAL`.
TEST(IndexVocabularyMergerImpl, skipPointersDefaultInterval) {
  std::string fileName = gtestCurrentTestName();
  absl::Cleanup cleanup{[&fileName]() { ad_utility::deleteFile(fileName); }};
  auto words = makeWords(2 * PARTIAL_VOCAB_SKIP_POINTER_INTERVAL + 1);
  writePartialVocabularyToFile(makeItemVec(words), fileName);
  auto skipPointers = readPartialVocabularySkipPointers(fileName);
  ASSERT_EQ(skipPointers.numBlocks(), 3u);
  EXPECT_EQ(skipPointers.skipPointers_.at(1).numWordsBefore_,
            PARTIAL_VOCAB_SKIP_POINTER_INTERVAL);
  EXPECT_EQ(skipPointers.numWordsInBlock(2), 1u);
  EXPECT_ANY_THROW(writePartialVocabularyToFile({}, fileName, 0));
}

// _____________________________________________________________________________
// A file without skip pointers (written in the format that was used before
// they were introduced) or with corrupted skip pointers is rejected.
TEST(IndexVocabularyMergerImpl, skipPointersMissingOrCorrupted) {
  std::string fileName = gtestCurrentTestName();
  absl::Cleanup cleanup{[&fileName]() { ad_utility::deleteFile(fileName); }};
  using ::testing::HasSubstr;
  namespace ser = ad_utility::serialization;

  // Write the file in the format without skip pointers.
  auto writeWithoutSkipPointers = [&fileName](size_t numWords) {
    ser::FileWriteSerializer writer{fileName};
    writer << uint64_t{numWords};
    for (size_t i = 0; i < numWords; ++i) {
      writer << TripleComponentWithIndex{absl::StrCat("word", i), false, i};
    }
  };
  for (size_t numWords : {0, 1, 5}) {
    writeWithoutSkipPointers(numWords);
    AD_EXPECT_THROW_WITH_MESSAGE(readPartialVocabularySkipPointers(fileName),
                                 HasSubstr("has no skip pointers"));
  }
  // An empty file.
  { ser::FileWriteSerializer writer{fileName}; }
  AD_EXPECT_THROW_WITH_MESSAGE(readPartialVocabularySkipPointers(fileName),
                               HasSubstr("has no skip pointers"));

  // Overwrite the 8 bytes at `offset` (counted from the end of the file if
  // `fromEnd` is true) with `value`.
  auto overwrite = [&fileName](uint64_t offset, bool fromEnd, uint64_t value) {
    ad_utility::File file{fileName, "r+"};
    if (fromEnd) {
      offset = file.sizeOfFile() - offset;
    }
    file.write(&value, sizeof(value), static_cast<off_t>(offset));
  };
  auto words = makeWords(10);
  auto els = makeItemVec(words);
  auto expectCorrupted = [&fileName](std::string_view what) {
    AD_EXPECT_THROW_WITH_MESSAGE(readPartialVocabularySkipPointers(fileName),
                                 HasSubstr(what));
  };

  // The start of the skip pointers (stored in front of the magic number) is
  // out of range.
  writePartialVocabularyToFile(els, fileName, 3);
  overwrite(16, true, 1'000'000);
  expectCorrupted("their start offset is out of range");

  // The number of words in the header doesn't match the skip pointers.
  writePartialVocabularyToFile(els, fileName, 3);
  overwrite(0, false, 9);
  expectCorrupted("a block starts behind the last word");
  writePartialVocabularyToFile(els, fileName, 3);
  overwrite(0, false, 0);
  expectCorrupted("their number doesn't match the number of words");

  // The start of the skip pointers points into the middle of the words, so
  // their number is garbage.
  writePartialVocabularyToFile(els, fileName, 3);
  overwrite(16, true, 8);
  expectCorrupted("corrupted");
}
