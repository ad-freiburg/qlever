// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/Generator.h"
#include "util/MmapVector.h"

namespace {
using namespace vocabulary_test;

// Store a VocabularyOnDisk and read it back from file. For each instance of
// `VocabularyCreator` that exists at the same time, a different filename has to
// be chosen.
class VocabularyCreator {
 private:
  std::string vocabFilename_;

 public:
  explicit VocabularyCreator(std::string filename)
      : vocabFilename_{std::move(filename)} {
    ad_utility::deleteFile(vocabFilename_, false);
  }
  ~VocabularyCreator() { ad_utility::deleteFile(vocabFilename_); }

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

auto createVocabulary(std::string filename) {
  return [c = VocabularyCreator{std::move(filename)}](auto&&... args) mutable {
    return c.createVocabulary(AD_FWD(args)...);
  };
}
}  // namespace

TEST(VocabularyOnDisk, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(createVocabulary("lowerUpperBoundStdLess"));
}

TEST(VocabularyOnDisk, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      createVocabulary("lowerUpperBoundNumeric"));
}

TEST(VocabularyOnDisk, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary("AccessOperator"));
}

TEST(VocabularyOnDisk, AccessOperatorWithNonContiguousIds) {
  std::vector<std::string> words{"game",  "4",      "nobody", "33",
                                 "alpha", "\n\1\t", "222",    "1111"};
  std::vector<uint64_t> ids{2, 4, 8, 16, 17, 19, 42, 42 * 42 + 7};
  testAccessOperatorForUnorderedVocabulary(
      createVocabulary("AccessOperatorWithNonContiguousIds"));
}

TEST(VocabularyOnDisk, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary("EmptyVocabulary"));
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
  absl::Cleanup cleanup{[&]() {
    ad_utility::deleteFile(vocabFilename);
    ad_utility::deleteFile(offsetsFilename);
  }};

  const std::array<std::string_view, 7> words{
      "alpha",
      "bravo",
      "charlie",
      "",
      "a longer word with spaces and \1\n\t control chars",
      "delta",
      "z"};

  // Write the words file (the plain concatenation of all words) and the offsets
  // file. The offsets are written via a real `MmapVector<uint64_t>`, which on
  // destruction rounds its capacity up to a page boundary and appends the
  // metadata trailer, reproducing the legacy on-disk format with unused
  // capacity. The offsets file holds one offset per word plus a final offset
  // marking the end of the last word.
  {
    ad_utility::File wordsFile{vocabFilename, "w"};
    ad_utility::MmapVector<uint64_t> offsets{offsetsFilename,
                                             ad_utility::CreateTag{}};
    uint64_t currentOffset = 0;
    for (std::string_view word : words) {
      offsets.push_back(currentOffset);
      currentOffset += wordsFile.write(word.data(), word.size());
    }
    offsets.push_back(currentOffset);
  }

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

// A `lookupBatch` result must equal the individual `vocab[]` lookups for the
// same indices, including for reordered and duplicated indices.
TEST(VocabularyOnDisk, LookupBatchMatchesIndividualLookups) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42", "gamma"};
  VocabularyCreator creator{"LookupBatch"};
  auto vocab = creator.createVocabulary(words);

  std::array<size_t, 8> indices{2, 0, 3, 1, 1, 4, 0, 3};
  auto result = vocab.lookupBatch(indices);
  ASSERT_EQ(result->size(), indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    EXPECT_EQ((*result)[i], vocab[indices[i]]) << "at position " << i;
  }
}

// An empty batch is an invalid request and must throw.
TEST(VocabularyOnDisk, LookupBatchEmptyThrows) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42"};
  VocabularyCreator creator{"LookupBatchEmpty"};
  auto vocab = creator.createVocabulary(words);
  EXPECT_ANY_THROW(vocab.lookupBatch(ql::span<const size_t>{}));
}

// An out-of-range index in a batch must throw.
TEST(VocabularyOnDisk, LookupBatchOutOfRangeIndexThrows) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42"};
  VocabularyCreator creator{"LookupBatchOutOfRange"};
  auto vocab = creator.createVocabulary(words);
  std::array<size_t, 2> indices{0, 99};
  EXPECT_ANY_THROW(vocab.lookupBatch(indices));
}

// Each batch yielded by `lookupBatchesStreamed` must equal the individual
// `vocab[]` lookups for that batch's indices, and the batches must be yielded
// in input order.
TEST(VocabularyOnDisk, LookupBatchesStreamedMatchesIndividualLookups) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42", "gamma"};
  VocabularyCreator creator{"LookupBatchesStreamed"};
  auto vocab = creator.createVocabulary(words);

  std::vector<std::vector<size_t>> batches{{2, 0, 3}, {1}, {4, 0, 1}};
  // `VocabLookupInput` takes ownership of the batches, so keep a copy to
  // compare against.
  const auto expectedBatches = batches;
  auto streamed =
      vocab.lookupBatchesStreamed(VocabLookupInput{std::move(batches)});

  size_t b = 0;
  for (auto& result : streamed) {
    ASSERT_LT(b, expectedBatches.size());
    const auto& batchIndices = expectedBatches[b];
    ASSERT_EQ(result->size(), batchIndices.size()) << "at batch " << b;
    for (size_t i = 0; i < batchIndices.size(); ++i) {
      EXPECT_EQ((*result)[i], vocab[batchIndices[i]])
          << "at batch " << b << " position " << i;
    }
    ++b;
  }
  EXPECT_EQ(b, expectedBatches.size());
}

// A stream over many batches must still return every batch correctly and in
// order.
TEST(VocabularyOnDisk, LookupBatchesStreamedManyBatches) {
  constexpr size_t kNumWords = 1000;
  std::vector<std::string> words;
  words.reserve(kNumWords);
  for (size_t i = 0; i < kNumWords; ++i) {
    words.push_back(absl::StrCat("word", i));
  }
  VocabularyCreator creator{"LookupBatchesStreamedMany"};
  auto vocab = creator.createVocabulary(words);

  // One single-index batch per word, in order.
  std::vector<std::vector<size_t>> batches;
  batches.reserve(kNumWords);
  for (size_t i = 0; i < kNumWords; ++i) {
    batches.push_back({i});
  }
  auto streamed =
      vocab.lookupBatchesStreamed(VocabLookupInput{std::move(batches)});

  size_t i = 0;
  for (auto& r : streamed) {
    ASSERT_EQ(r->size(), 1) << "at batch " << i;
    EXPECT_EQ((*r)[0], vocab[i]) << "at batch " << i;
    ++i;
  }
  EXPECT_EQ(i, kNumWords);
}

// An empty input stream (no batches) is valid and must produce no results.
TEST(VocabularyOnDisk, LookupBatchesStreamedEmptyStreamYieldsNothing) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42"};
  VocabularyCreator creator{"LookupBatchesStreamedEmptyStream"};
  auto vocab = creator.createVocabulary(words);
  std::vector<std::vector<size_t>> noBatches;
  auto streamed =
      vocab.lookupBatchesStreamed(VocabLookupInput{std::move(noBatches)});
  size_t count = 0;
  for ([[maybe_unused]] auto& r : streamed) {
    ++count;
  }
  EXPECT_EQ(count, 0);
}

// An out-of-range index within a streamed batch must throw when that batch is
// pulled.
TEST(VocabularyOnDisk, LookupBatchesStreamedOutOfRangeIndexThrows) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42"};
  VocabularyCreator creator{"LookupBatchesStreamedOutOfRange"};
  auto vocab = creator.createVocabulary(words);
  std::vector<std::vector<size_t>> batches{{0, 99}};
  auto streamed =
      vocab.lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  EXPECT_ANY_THROW({
    for ([[maybe_unused]] auto& r : streamed) {
    }
  });
}

// An empty batch within the stream is an invalid request and must throw when
// the batch is pulled (an empty input *stream* with no batches is still valid,
// see above).
TEST(VocabularyOnDisk, LookupBatchesStreamedEmptyBatchThrows) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42"};
  VocabularyCreator creator{"LookupBatchesStreamedEmptyBatch"};
  auto vocab = creator.createVocabulary(words);
  std::vector<std::vector<size_t>> batches{{2, 0}, {}, {1}};
  auto streamed =
      vocab.lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  EXPECT_ANY_THROW({
    for ([[maybe_unused]] auto& r : streamed) {
    }
  });
}
