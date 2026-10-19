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

#include <string>

#include "index/ConstantsIndexBuilding.h"
#include "index/VocabularyMergerImpl.h"
#include "util/GlobalExecutor.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"

namespace {
using ad_utility::vocabulary_merger::writePartialVocabularyToFile;

// Helper to conveniently create an entry for `ItemVec`.
ItemVec::value_type makeEntry(std::string_view word, bool isExternalized,
                              uint64_t index, uint64_t geoSortKey = 0) {
  return {word, PartialVocabIndexWithExternalFlag{index, isExternalized},
          geoSortKey};
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
      makeEntry(bigWord, true, 99, 12345),
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

// Test the options of the parallel merge of the partial vocabularies: the
// output block size, the number of chunks and the number of chunks in flight.
TEST(IndexVocabularyMergerImpl, vocabularyMergeOptions) {
  using ad_utility::MemorySize;
  using ad_utility::vocabulary_merger::vocabularyMergeOptions;
  const size_t parallelism = ad_utility::globalExecutorNumThreads();
  const size_t inputPerChunk = VOCAB_MERGER_INPUT_PER_CHUNK.getBytes();

  // The output blocks are the batches of the pipeline behind the merge.
  auto options = vocabularyMergeOptions(3, 0, MemorySize::gigabytes(4));
  EXPECT_EQ(options.outputBlockSize.maxNumElements(),
            VOCAB_MERGER_WORD_BATCH_SIZE);
  EXPECT_EQ(options.outputBlockSize.maxMemory(),
            VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE);
  EXPECT_EQ(options.parallelism(), parallelism);

  // No input still gives one chunk per thread, `10.5` chunks worth of input
  // gives `11` chunks, distributed over the threads.
  EXPECT_EQ(options.targetChunksPerThread, 1u);
  options = vocabularyMergeOptions(3, 10 * inputPerChunk + inputPerChunk / 2,
                                   MemorySize::gigabytes(4));
  EXPECT_EQ(options.targetChunksPerThread,
            (11 + parallelism - 1) / parallelism);

  // The chunks in flight are bounded by the memory from below (at least one)
  // and by the threads from above.
  options = vocabularyMergeOptions(3, 0, MemorySize::bytes(1));
  EXPECT_EQ(options.maxNumChunksInFlight, 1u);
  options = vocabularyMergeOptions(3, 0, MemorySize::terabytes(100));
  EXPECT_EQ(options.maxNumChunksInFlight,
            VOCAB_MERGER_CHUNKS_IN_FLIGHT_PER_THREAD * parallelism);
  // In between, the memory decides: one chunk takes twice its input plus one
  // block per partial vocabulary.
  const size_t memoryPerChunk =
      2 * (3 * PARTIAL_VOCAB_BLOCK_SIZE.getBytes() + inputPerChunk);
  options = vocabularyMergeOptions(3, 0, MemorySize::bytes(3 * memoryPerChunk));
  EXPECT_EQ(options.maxNumChunksInFlight,
            std::min<size_t>(
                3, VOCAB_MERGER_CHUNKS_IN_FLIGHT_PER_THREAD * parallelism));
}
