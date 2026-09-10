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

#include "VocabularyMergerTestHelpers.h"
#include "index/vocabulary_merger/IdMapBatch.h"

using namespace ad_utility::vocabulary_merger;
using namespace vocabularyMergerTestHelpers;
using ad_utility::vocabulary_merger::detail::IdMapBatch;
using ad_utility::vocabulary_merger::detail::IdMapBatchWriter;
using ad_utility::vocabulary_merger::detail::LocalIdxToBatchMapping;
using ad_utility::vocabulary_merger::detail::LocalIdxToBatchMappings;

namespace {
// The basename of the partial vocabularies that the tests below create. It
// needs no test-specific part, because each test runs in its own working
// directory (see `makePartialVocabularyFilenamesInFreshDirectory`).
const std::string partialVocabBasename = "vocab-";

// Create an `IdMapBatch` from the given `mappings` and `globalIds`. In
// contrast to the `WordBatchBuilder` (which allocates the mappings in
// advance), the `numMappings_` here is simply the size of the `mappings`.
IdMapBatch makeBatch(const std::vector<LocalIdxToBatchMapping>& mappings,
                     const std::vector<Id>& globalIds) {
  LocalIdxToBatchMappings localIdxMappings;
  localIdxMappings.mappings_.insert(localIdxMappings.mappings_.end(),
                                    mappings.begin(), mappings.end());
  localIdxMappings.numMappings_ = mappings.size();
  return IdMapBatch{std::move(localIdxMappings), globalIds};
}
}  // namespace

// _____________________________________________________________________________
// The `IdMapBatchWriter` distributes the mappings of its batches over one ID
// map per partial vocabulary, resolves the `indexOfWordInBatch_` of each
// mapping via the `globalIds_` of its batch, and keeps the order in which the
// mappings were pushed.
TEST(IdMapBatchWriter, writeSeveralBatches) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 3);

  {
    IdMapBatchWriter writer{partialVocabBasename,
                            filenames.numPartialVocabularies_};
    // The first batch has two distinct words with the global IDs `10` and
    // `11`. The first word occurs in the partial vocabularies `0` and `2`, the
    // second one only in `0`.
    writer.writeBatch(makeBatch(
        {LocalIdxToBatchMapping{0, 0, L(7)}, LocalIdxToBatchMapping{2, 0, L(8)},
         LocalIdxToBatchMapping{0, 1, L(9)}},
        {V(10), V(11)}));
    // The second batch has a single word with the global ID `12`, which occurs
    // in all three partial vocabularies.
    writer.writeBatch(makeBatch({LocalIdxToBatchMapping{0, 0, L(100)},
                                 LocalIdxToBatchMapping{1, 0, L(101)},
                                 LocalIdxToBatchMapping{2, 0, L(102)}},
                                {V(12)}));
    writer.finish();
  }

  EXPECT_THAT(
      getIdMapFromFile(filenames.idMapFiles_[0]),
      ::testing::ElementsAre(IdMapEntry{L(7), V(10)}, IdMapEntry{L(9), V(11)},
                             IdMapEntry{L(100), V(12)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[1]),
              ::testing::ElementsAre(IdMapEntry{L(101), V(12)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[2]),
              ::testing::ElementsAre(IdMapEntry{L(8), V(10)},
                                     IdMapEntry{L(102), V(12)}));
}

// _____________________________________________________________________________
// An `IdMapBatchWriter` to which no batch was written creates one empty ID map
// per partial vocabulary. Its destructor closes those maps, so an explicit
// call to `finish()` is not required.
TEST(IdMapBatchWriter, noBatches) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
  {
    IdMapBatchWriter writer{partialVocabBasename,
                            filenames.numPartialVocabularies_};
  }
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[0]), ::testing::IsEmpty());
}

// _____________________________________________________________________________
// Only the first `numMappings_` of the `mappings_` of a batch are valid; the
// remaining (uninitialized) ones must not be written.
TEST(IdMapBatchWriter, onlyValidMappingsAreWritten) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);

  auto batch = makeBatch({LocalIdxToBatchMapping{0, 0, L(42)}}, {V(43)});
  // Allocate (but do not initialize) space for many more mappings, exactly as
  // the `WordBatchBuilder` does.
  batch.localIdxMappings_.mappings_.resize(1000);
  {
    IdMapBatchWriter writer{partialVocabBasename,
                            filenames.numPartialVocabularies_};
    writer.writeBatch(batch);
  }
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[0]),
              ::testing::ElementsAre(IdMapEntry{L(42), V(43)}));
}
