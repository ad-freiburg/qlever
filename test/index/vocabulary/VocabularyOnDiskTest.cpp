// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022-2026 Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de), UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <optional>
#include <random>

#include "../../util/GTestHelpers.h"
#include "../../util/MmapVectorLegacyFormat.h"
#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "global/Constants.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/MmapVector.h"
#include "util/jthread.h"

namespace {
using namespace vocabulary_test;

// Store a `VocabularyOnDisk` and read it back from file. For each instance of
// `VocabularyCreator` that exists at the same time, a different filename has to
// be chosen.
class VocabularyCreator {
 private:
  std::string vocabFilename_;

 public:
  explicit VocabularyCreator(std::string filename)
      : vocabFilename_{std::move(filename)} {
    deleteVocabularyFiles<VocabularyOnDisk>(vocabFilename_);
  }
  // Move-only: a moved-from creator has an empty filename and deletes nothing.
  VocabularyCreator(VocabularyCreator&& other) noexcept
      : vocabFilename_{std::exchange(other.vocabFilename_, {})} {}
  VocabularyCreator& operator=(VocabularyCreator&&) =
      delete;  // not needed (TODO: why?)
  VocabularyCreator(const VocabularyCreator&) = delete;
  VocabularyCreator& operator=(const VocabularyCreator&) = delete;

  ~VocabularyCreator() {
    if (!vocabFilename_.empty()) {
      deleteVocabularyFiles<VocabularyOnDisk>(vocabFilename_);
    }
  }

  // Create and return a `VocabularyOnDisk` from words.
  void createVocabularyImpl(const std::vector<std::string>& words) {
    auto writer = VocabularyOnDisk::WordWriter(vocabFilename_);
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      EXPECT_EQ(writer(word, false), static_cast<uint64_t>(i));
    }
    writer.readableName() = "blubb";
    EXPECT_EQ(writer.readableName(), "blubb");
    static std::atomic<unsigned> doFinish = 0;
    // In some tests, call `finish` explicitly, in others let the destructor
    // handle this.
    if (doFinish.fetch_add(1) % 2 == 0) {
      writer.finish();
    }
  }

  // Create and return a `VocabularyOnDisk` from words. The ids will be [0, ..
  // words.size()).
  auto createVocabulary(const std::vector<std::string>& words) {
    createVocabularyImpl(words);
    VocabularyOnDisk vocabulary;
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }
};

// Owns a `VocabularyOnDisk` together with the `VocabularyCreator` that manages
// its backing file, so the file lives as long as the vocabulary reading from
// it.
class VocabularyOnDiskHandle {
 public:
  VocabularyOnDiskHandle(std::string filename,
                         const std::vector<std::string>& words)
      : creator_{std::move(filename)},
        vocabulary_{creator_.createVocabulary(words)} {}

  // Non-copyable/movable: a copy would give two `creator_`s the same file, so
  // both destructors would unlink it (double free).
  VocabularyOnDiskHandle(const VocabularyOnDiskHandle&) = delete;
  VocabularyOnDiskHandle& operator=(const VocabularyOnDiskHandle&) = delete;
  VocabularyOnDiskHandle(VocabularyOnDiskHandle&&) = delete;
  VocabularyOnDiskHandle& operator=(VocabularyOnDiskHandle&&) = delete;

 private:
  // `vocabulary_` is declared after `creator_`, because the `vocabulary_`
  // should be destroyed before the `creator_`: the `vocabulary_` must be torn
  // down before the `creator_` unlinks the file.
  VocabularyCreator creator_;
  VocabularyOnDisk vocabulary_;

 public:
  // Access the underlying vocabulary transparently, so call sites can treat
  // the handle like the `VocabularyOnDisk` it wraps.
  VocabularyOnDisk& operator*() { return vocabulary_; }
  VocabularyOnDisk* operator->() { return &vocabulary_; }
};

VocabularyOnDiskHandle createVocabularyFromWords(
    const std::vector<std::string>& words) {
  return VocabularyOnDiskHandle{absl::StrCat(gtestCurrentTestName(), ".dat"),
                                words};
}

// Words of different lengths that are distinct for each `prefix`.
std::vector<std::string> makeWords(size_t numWords,
                                   std::string_view prefix = "word") {
  std::vector<std::string> words;
  for (size_t i = 0; i < numWords; ++i) {
    words.push_back(absl::StrCat(prefix, i, std::string(i % 13, 'x')));
  }
  return words;
}

// Look up the `indices` one by one via `operator[]` and expect the
// corresponding `words`.
void expectLookupsYieldWords(
    const VocabularyOnDisk& vocabulary, const std::vector<std::string>& words,
    const std::vector<size_t>& indices,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  for (size_t idx : indices) {
    ASSERT_EQ(vocabulary[idx], words.at(idx)) << "at index " << idx;
  }
}

// The indices `first, first + stride, ...` that are smaller than `end`.
std::vector<size_t> stridedIndices(size_t first, size_t end, size_t stride) {
  std::vector<size_t> indices;
  for (size_t idx = first; idx < end; idx += stride) {
    indices.push_back(idx);
  }
  return indices;
}

auto createVocabulary() {
  return [c = VocabularyCreator{absl::StrCat(gtestCurrentTestName(), ".dat")}](
             auto&&... args) mutable {
    return c.createVocabulary(AD_FWD(args)...);
  };
}

VocabularyOnDiskHandle createExampleVocabulary() {
  return createVocabularyFromWords({"alpha", "delta", "beta", "42", "gamma"});
}

// Create a `VocabularyOnDisk` from `words` and assert that `scanAll` yields
// exactly those words in order: both as bare words and as `IndexAndWord`s with
// contiguous indices `0, 1, 2, ...` (also across batch boundaries).
void expectScanAllYields(const std::vector<std::string>& words) {
  VocabularyCreator creator{gtestCurrentTestName()};
  auto vocabulary = creator.createVocabulary(words);

  EXPECT_THAT(scanAllToVector(vocabulary.scanAll()),
              ::testing::ElementsAreArray(words));

  auto indexAndWords = scanAllToIndexAndWordVector(vocabulary.scanAll());
  ASSERT_EQ(indexAndWords.size(), words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(indexAndWords[i].first, i) << "at index " << i;
    EXPECT_EQ(indexAndWords[i].second, words[i]) << "at index " << i;
  }
}

}  // namespace

TEST(VocabularyOnDisk, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(createVocabulary());
}

TEST(VocabularyOnDisk, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(createVocabulary());
}

TEST(VocabularyOnDisk, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary());
}

TEST(VocabularyOnDisk, AccessOperatorWithNonContiguousIds) {
  std::vector<std::string> words{"game",  "4",      "nobody", "33",
                                 "alpha", "\n\1\t", "222",    "1111"};
  std::vector<uint64_t> ids{2, 4, 8, 16, 17, 19, 42, 42 * 42 + 7};
  testAccessOperatorForUnorderedVocabulary(createVocabulary());
}

TEST(VocabularyOnDisk, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary());
}

// Older versions of QLever stored the offsets file as an
// `ad_utility::MmapVector<uint64_t>`. Such a file rounds its capacity up to a
// multiple of the page size, so the offsets array is followed by a (large)
// region of unused capacity before the metadata trailer at the very end. This
// test writes the offsets file in exactly that legacy format and makes sure
// that the current `VocabularyOnDisk` still reads it back correctly.
TEST(VocabularyOnDisk, ReadLegacyMmapVectorOffsetsFormat) {
  std::string vocabFilename = "vocabularyOnDisk.legacyMmapFormat";
  std::string offsetsFilename = vocabFilename + ".offsets";
  auto cleanup = makeVocabFileCleanup<VocabularyOnDisk>(vocabFilename);

  const std::array<std::string_view, 7> words{
      "alpha",
      "bravo",
      "charlie",
      "",
      "a longer word with spaces and \1\n\t control chars",
      "delta",
      "z"};

  // Write the words file (the plain concatenation of all words) and collect the
  // offsets: one offset per word plus a final offset marking the end of the
  // last word.
  std::vector<uint64_t> offsets;
  {
    ad_utility::File wordsFile{vocabFilename, "w"};
    uint64_t currentOffset = 0;
    for (std::string_view word : words) {
      offsets.push_back(currentOffset);
      currentOffset += wordsFile.write(word.data(), word.size());
    }
    offsets.push_back(currentOffset);
  }
  // Write the offsets file in the legacy `MmapVector<uint64_t>` on-disk layout,
  // which rounds its capacity up to a page boundary and appends the metadata
  // trailer, reproducing the legacy format with a region of unused capacity.
  ad_utility::testing::writeLegacyMmapVectorFile(offsetsFilename, offsets);

  // Sanity check that we actually exercise the "unused capacity" path: the
  // offsets file is considerably larger than the offsets plus the trailer alone
  // would require.
  ad_utility::File offsetsFile{offsetsFilename, "r"};
  EXPECT_GT(offsetsFile.sizeOfFile(),
            static_cast<off_t>((words.size() + 1) * sizeof(uint64_t)) +
                ad_utility::MmapVectorMetaData::numBytes);
  offsetsFile.close();

  VocabularyOnDisk vocabulary;
  vocabulary.open(vocabFilename);
  ASSERT_EQ(vocabulary.size(), words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(vocabulary[i], words[i]) << "at index " << i;
  }
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAll) {
  // A basic scan over many small words (fits into a single batch).
  std::vector<std::string> words;
  for (size_t i = 0; i < 3000; ++i) {
    words.push_back(absl::StrCat("word", i, std::string(i % 7, 'x')));
  }
  expectScanAllYields(words);
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAllEmptyVocabulary) {
  VocabularyCreator creator{gtestCurrentTestName()};
  auto vocabulary = creator.createVocabulary({});
  auto range = vocabulary.scanAll();
  EXPECT_FALSE(range.get().has_value());
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAllByteLimitForcesMultipleBatches) {
  // `scanAll` caps a batch's word data at
  // `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH` (10 MB). Four words of 3 MB each
  // (12 MB total) therefore don't fit into a single batch: the byte limit (not
  // the word-count limit) forces a batch boundary after three words.
  constexpr size_t wordSize = 3'000'000;
  expectScanAllYields({std::string(wordSize, 'a'), std::string(wordSize, 'b'),
                       std::string(wordSize, 'c'), std::string(wordSize, 'd')});
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAllSingleWordExceedsLimit) {
  // A single word larger than `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH` (10 MB)
  // must still be scanned; it is returned in a batch of its own even though it
  // exceeds the limit, and the surrounding small words are unaffected.
  expectScanAllYields({"before", std::string(11'000'000, 'x'), "after"});
}

// A `lookupBatch` result must equal the individual `vocab[]` lookups for the
// same indices, including for reordered and duplicated indices.
TEST(VocabularyOnDisk, LookupBatchMatchesIndividualLookups) {
  auto vocab = createExampleVocabulary();
  std::array<size_t, 8> indices{2, 0, 3, 1, 1, 4, 0, 3};
  auto result = vocab->lookupBatch(indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(*vocab, result,
                                                                indices);
}

// An empty batch is an invalid request and must throw.
TEST(VocabularyOnDisk, LookupBatchEmptyThrows) {
  auto vocab = createExampleVocabulary();
  EXPECT_ANY_THROW(vocab->lookupBatch(ql::span<const size_t>{}));
}

// An out-of-range index in a batch must throw.
TEST(VocabularyOnDisk, LookupBatchOutOfRangeIndexThrows) {
  auto vocab = createExampleVocabulary();
  std::array<size_t, 2> indices{0, 99};
  EXPECT_ANY_THROW(vocab->lookupBatch(indices));
}

// Each batch yielded by `lookupBatchesStreamed` must equal the individual
// `vocab[]` lookups for that batch's indices, and the batches must be yielded
// in input order.
TEST(VocabularyOnDisk, LookupBatchesStreamedMatchesIndividualLookups) {
  auto vocab = createExampleVocabulary();

  std::vector<std::vector<size_t>> batches{{2, 0, 3}, {1}, {4, 0, 1}};
  // `VocabLookupInput` takes ownership of the batches, so keep a copy to
  // compare against.
  const auto expectedBatches = batches;
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  vocabulary_test::assertStreamedLookupMatchesVocabularyAtIndices(
      *vocab, streamed, expectedBatches);
}

// An empty input stream (no batches) is valid and must produce no results.
TEST(VocabularyOnDisk, LookupBatchesStreamedEmptyStreamYieldsNothing) {
  auto vocab = createExampleVocabulary();
  std::vector<std::vector<size_t>> noBatches;
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(noBatches)});
  EXPECT_EQ(ql::ranges::distance(streamed), 0);
}

// An out-of-range index within a streamed batch must throw when that batch is
// pulled.
TEST(VocabularyOnDisk, LookupBatchesStreamedOutOfRangeIndexThrows) {
  auto vocab = createExampleVocabulary();
  std::vector<std::vector<size_t>> batches{{0, 99}};
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  EXPECT_ANY_THROW({
    for ([[maybe_unused]] auto& r : streamed) {
    }
  });
}

// An empty batch within the stream is an invalid request and must throw when
// the batch is pulled (an empty input stream with no batches is still valid,
// see above).
TEST(VocabularyOnDisk, LookupBatchesStreamedEmptyBatchThrows) {
  auto vocab = createExampleVocabulary();
  std::vector<std::vector<size_t>> batches{{2, 0}, {}, {1}};
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  EXPECT_ANY_THROW({
    for ([[maybe_unused]] auto& r : streamed) {
    }
  });
}

// The read-ahead of `operator[]` must not change the results, no matter in
// which order the words are looked up.
TEST(VocabularyOnDisk, AccessOperatorReadAhead) {
  // More words than the maximal window, so that the window is limited.
  auto words = makeWords(3 * VOCABULARY_READ_AHEAD_MAX_WORDS + 17);
  auto vocab = createVocabularyFromWords(words);
  size_t numWords = words.size();
  // Sequential, also with gaps that are within, at, and beyond the maximal gap
  // for a read-ahead.
  for (size_t stride :
       {size_t{1}, size_t{2}, size_t{10}, VOCABULARY_READ_AHEAD_MAX_GAP,
        VOCABULARY_READ_AHEAD_MAX_GAP + 1, size_t{1000}}) {
    expectLookupsYieldWords(*vocab, words, stridedIndices(0, numWords, stride));
  }
  // The same word repeatedly, also after a window has been read.
  expectLookupsYieldWords(*vocab, words, {5, 5, 6, 7, 7, 7, 4, 4});
  // Backwards.
  auto backwards = stridedIndices(0, numWords, 1);
  ql::ranges::reverse(backwards);
  expectLookupsYieldWords(*vocab, words, backwards);
  // Random order.
  auto shuffled = stridedIndices(0, numWords, 1);
  std::shuffle(shuffled.begin(), shuffled.end(), std::mt19937{42});
  expectLookupsYieldWords(*vocab, words, shuffled);
  // Sequential up to the last word (the window is limited by the end of the
  // vocabulary), then jumping back.
  expectLookupsYieldWords(*vocab, words,
                          stridedIndices(numWords - 100, numWords, 1));
  expectLookupsYieldWords(*vocab, words, stridedIndices(0, 100, 1));
  EXPECT_ANY_THROW((*vocab)[numWords]);
}

// The read-ahead of a thread is kept for one vocabulary at a time, so
// alternating between two vocabularies must not mix up their words.
TEST(VocabularyOnDisk, AccessOperatorReadAheadAlternatingVocabularies) {
  auto wordsA = makeWords(1000, "a");
  auto wordsB = makeWords(1000, "b");
  VocabularyOnDiskHandle vocabA{absl::StrCat(gtestCurrentTestName(), "A"),
                                wordsA};
  VocabularyOnDiskHandle vocabB{absl::StrCat(gtestCurrentTestName(), "B"),
                                wordsB};
  // Alternate after single lookups, and after runs of sequential lookups.
  for (size_t runLength : {1, 50}) {
    for (size_t first = 0; first + runLength <= 1000; first += runLength) {
      auto indices = stridedIndices(first, first + runLength, 1);
      expectLookupsYieldWords(*vocabA, wordsA, indices);
      expectLookupsYieldWords(*vocabB, wordsB, indices);
    }
  }
}

// A vocabulary that is opened at the address of a previous vocabulary must
// not get the words of the previous vocabulary from the read-ahead.
TEST(VocabularyOnDisk, AccessOperatorReadAheadReusedAddress) {
  auto wordsA = makeWords(1000, "a");
  auto wordsB = makeWords(1000, "b");
  VocabularyCreator creatorA{absl::StrCat(gtestCurrentTestName(), "A")};
  VocabularyCreator creatorB{absl::StrCat(gtestCurrentTestName(), "B")};
  std::optional<VocabularyOnDisk> vocab;
  vocab.emplace(creatorA.createVocabulary(wordsA));
  const VocabularyOnDisk* address = &vocab.value();
  expectLookupsYieldWords(*vocab, wordsA, stridedIndices(0, 500, 1));
  vocab.reset();
  vocab.emplace(creatorB.createVocabulary(wordsB));
  ASSERT_EQ(&vocab.value(), address);
  expectLookupsYieldWords(*vocab, wordsB, stridedIndices(400, 1000, 1));
}

// Words that are too large for the read-ahead (see
// `VOCABULARY_READ_AHEAD_MAX_WORD_DATA`) are read individually, also when they
// are next to small words.
TEST(VocabularyOnDisk, AccessOperatorReadAheadLargeWords) {
  size_t maxBytes = VOCABULARY_READ_AHEAD_MAX_WORD_DATA.getBytes();
  std::vector<std::string> words;
  for (size_t i = 0; i < 40; ++i) {
    // Every fifth word is larger than the limit, the following one fills half
    // of the limit, the others are small.
    size_t numBytes = i % 5 == 0 ? maxBytes + 1 : i % 5 == 1 ? maxBytes / 2 : 3;
    words.push_back(std::string(numBytes, static_cast<char>('a' + i % 26)));
  }
  auto vocab = createVocabularyFromWords(words);
  expectLookupsYieldWords(*vocab, words, stridedIndices(0, words.size(), 1));
  expectLookupsYieldWords(*vocab, words, stridedIndices(1, words.size(), 3));
}

// The read-ahead is kept per thread, so concurrent sequential lookups on the
// same vocabulary must not interfere with each other.
TEST(VocabularyOnDisk, AccessOperatorReadAheadConcurrentThreads) {
  auto words = makeWords(20'000);
  auto vocab = createVocabularyFromWords(words);
  const VocabularyOnDisk& vocabulary = *vocab;
  std::vector<ad_utility::JThread> threads;
  for (size_t i = 0; i < 4; ++i) {
    threads.emplace_back([&vocabulary, &words, i]() {
      expectLookupsYieldWords(vocabulary, words,
                              stridedIndices(i, words.size(), i + 1));
    });
  }
}
