// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPBATCH_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPBATCH_H

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/IdMap.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/UninitializedAllocator.h"
#include "util/Views.h"

// The intermediate stages of the vocabulary merger (see
// `index/VocabularyMerger.h`) that deal with the ID map entries while their
// global IDs are not yet known. They also never look at the words themselves,
// but only at the indices of the words within their batch.
namespace ad_utility::vocabulary_merger::detail {

// A single entry of one of the partial ID maps, as it is created by the
// merging thread. NOTE: At that point, the global ID of the corresponding
// word is not yet known (it is only determined when the word is written to
// the vocabulary), so the entry instead stores the index of the word within
// its batch (see `IdMapBatch::globalIds_`).
struct QueuedIdMapEntry {
  uint32_t partialFileId_;
  uint32_t indexOfWordInBatch_;
  uint64_t localIndex_;
};

// All the ID map entries of a single batch, before the global IDs that they
// refer to are known. This is what the merging thread produces (see
// `WordBatchBuilder`) and what the vocabulary writer consumes.
struct QueuedIdMapBatch {
  // The entries. NOTE: The vector is allocated (but not initialized) in
  // advance, and only the first `numEntries_` of its elements are valid, see
  // `WordBatchBuilder::startNewBatch`.
  ad_utility::UninitializedVector<QueuedIdMapEntry> entries_;
  size_t numEntries_ = 0;
};

// A complete batch of ID map entries: the queued entries, together with the
// global IDs that they refer to. This is what the vocabulary writer produces
// (it is the stage that determines the global IDs, see `VocabularyWriter`) and
// what the `IdMapBatchWriter` below consumes.
struct IdMapBatch {
  QueuedIdMapBatch queuedEntries_;
  // The global IDs of the distinct words of the batch. The
  // `indexOfWordInBatch_` of each of the `queuedEntries_` is an index into
  // this vector.
  std::vector<Id> globalIds_;
};

// The third stage of the merging pipeline: write the entries of a complete
// `IdMapBatch` to the partial ID maps, one of which is created per partial
// vocabulary.
//
// NOTE: This class is used exclusively by the thread of the
// `idMapWriterQueue_` of the `VocabularyMergePipeline`.
class IdMapBatchWriter {
 private:
  // The ID maps, one per partial vocabulary.
  std::vector<IdMapWriter> idMaps_;

 public:
  // Create the ID map for each of the `numFiles` partial vocabularies. The
  // filenames are `basename + PARTIAL_VOCAB_IDMAP_INFIX + i`.
  IdMapBatchWriter(const std::string& basename, size_t numFiles) {
    // The index of the partial vocabulary is stored in a `uint32_t` for each of
    // the (very many) ID map entries, see `QueuedIdMapEntry`.
    AD_CORRECTNESS_CHECK(numFiles <= std::numeric_limits<uint32_t>::max());
    idMaps_.reserve(numFiles);
    for (size_t i : ad_utility::integerRange(numFiles)) {
      idMaps_.emplace_back(
          absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i));
    }
  }

  // Write all the entries of the `batch` to their respective ID maps.
  void writeBatch(const IdMapBatch& batch) {
    AD_LOG_TRACE << "Start writing a batch of ID map entries\n";
    const auto& globalIds = batch.globalIds_;
    const auto& queuedEntries = batch.queuedEntries_;
    for (size_t i = 0; i < queuedEntries.numEntries_; ++i) {
      const auto& entry = queuedEntries.entries_[i];
      idMaps_[entry.partialFileId_].push_back(
          IdMapEntry{entry.localIndex_, globalIds[entry.indexOfWordInBatch_]});
    }
  }

  // Flush and close all the ID maps. After this, no more batches may be
  // written.
  void finish() {
    for (auto& idMap : idMaps_) {
      idMap.finish();
    }
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPBATCH_H
