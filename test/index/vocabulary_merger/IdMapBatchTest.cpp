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

#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IdTestHelpers.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/IdMapBatch.h"
#include "util/File.h"

using namespace ad_utility::vocabulary_merger;
using ad_utility::vocabulary_merger::detail::IdMapBatch;
using ad_utility::vocabulary_merger::detail::IdMapBatchWriter;
using ad_utility::vocabulary_merger::detail::QueuedIdMapBatch;
using ad_utility::vocabulary_merger::detail::QueuedIdMapEntry;

namespace {
auto V = ad_utility::testing::VocabId;

// Create an `IdMapBatch` from the given `entries` and `globalIds`. In contrast
// to the `WordBatchBuilder` (which allocates the entries in advance), the
// `numEntries_` here is simply the size of the `entries`.
IdMapBatch makeBatch(const std::vector<QueuedIdMapEntry>& entries,
                     const std::vector<Id>& globalIds) {
  QueuedIdMapBatch queuedEntries;
  queuedEntries.entries_.insert(queuedEntries.entries_.end(), entries.begin(),
                                entries.end());
  queuedEntries.numEntries_ = entries.size();
  return IdMapBatch{std::move(queuedEntries), globalIds};
}
}  // namespace

// _____________________________________________________________________________
// The `IdMapBatchWriter` distributes the entries of its batches over one ID map
// per partial vocabulary, resolves the `indexOfWordInBatch_` of each entry via
// the `globalIds_` of its batch, and keeps the order in which the entries were
// pushed.
TEST(IdMapBatchWriter, writeSeveralBatches) {
  static constexpr size_t numFiles = 3;
  std::string basename = absl::StrCat(gtestCurrentTestName(), "-");
  std::vector<std::string> filenames;
  for (size_t i = 0; i < numFiles; ++i) {
    filenames.push_back(absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i));
  }
  absl::Cleanup cleanup = [&filenames] {
    for (const auto& filename : filenames) {
      ad_utility::deleteFile(filename, false);
    }
  };

  {
    IdMapBatchWriter writer{basename, numFiles};
    // The first batch has two distinct words with the global IDs `10` and
    // `11`. The first word occurs in the partial vocabularies `0` and `2`, the
    // second one only in `0`.
    writer.writeBatch(
        makeBatch({QueuedIdMapEntry{0, 0, 7}, QueuedIdMapEntry{2, 0, 8},
                   QueuedIdMapEntry{0, 1, 9}},
                  {V(10), V(11)}));
    // The second batch has a single word with the global ID `12`, which occurs
    // in all three partial vocabularies.
    writer.writeBatch(
        makeBatch({QueuedIdMapEntry{0, 0, 100}, QueuedIdMapEntry{1, 0, 101},
                   QueuedIdMapEntry{2, 0, 102}},
                  {V(12)}));
    writer.finish();
  }

  EXPECT_THAT(getIdMapFromFile(filenames[0]),
              ::testing::ElementsAre(IdMapEntry{7, V(10)}, IdMapEntry{9, V(11)},
                                     IdMapEntry{100, V(12)}));
  EXPECT_THAT(getIdMapFromFile(filenames[1]),
              ::testing::ElementsAre(IdMapEntry{101, V(12)}));
  EXPECT_THAT(
      getIdMapFromFile(filenames[2]),
      ::testing::ElementsAre(IdMapEntry{8, V(10)}, IdMapEntry{102, V(12)}));
}

// _____________________________________________________________________________
// An `IdMapBatchWriter` to which no batch was written creates one empty ID map
// per partial vocabulary. Its destructor closes those maps, so an explicit
// call to `finish()` is not required.
TEST(IdMapBatchWriter, noBatches) {
  std::string basename = absl::StrCat(gtestCurrentTestName(), "-");
  std::string filename = absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, 0);
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };
  { IdMapBatchWriter writer{basename, 1}; }
  EXPECT_THAT(getIdMapFromFile(filename), ::testing::IsEmpty());
}

// _____________________________________________________________________________
// Only the first `numEntries_` of the `entries_` of a batch are valid; the
// remaining (uninitialized) ones must not be written.
TEST(IdMapBatchWriter, onlyValidEntriesAreWritten) {
  std::string basename = absl::StrCat(gtestCurrentTestName(), "-");
  std::string filename = absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, 0);
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };

  auto batch = makeBatch({QueuedIdMapEntry{0, 0, 42}}, {V(43)});
  // Allocate (but do not initialize) space for many more entries, exactly as
  // the `WordBatchBuilder` does.
  batch.queuedEntries_.entries_.resize(1000);
  {
    IdMapBatchWriter writer{basename, 1};
    writer.writeBatch(batch);
  }
  EXPECT_THAT(getIdMapFromFile(filename),
              ::testing::ElementsAre(IdMapEntry{42, V(43)}));
}
