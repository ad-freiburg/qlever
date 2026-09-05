// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEPIPELINE_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEPIPELINE_H

#include <re2/re2.h>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/IdMapBatch.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "index/vocabulary_merger/VocabularyWriter.h"
#include "index/vocabulary_merger/WordBatch.h"
#include "util/TaskQueue.h"

// The asynchronous part of the merging pipeline of the vocabulary merger (see
// the comment above `mergeVocabulary` in `index/VocabularyMerger.h`), which is
// not part of the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// The stages of the merging pipeline that run asynchronously to the merging
// thread (stages 2 to 4 in the comment above `mergeVocabulary`).
//
// NOTE: Each of the queues has exactly one worker thread, so the batches are
// processed in exactly the order in which the merging thread creates them, and
// the state of the individual stages requires no further synchronization.
class VocabularyMergePipeline {
 private:
  // NOTE: The order of the following declarations is important, because the
  // members are destroyed in the reverse order of their declaration, and the
  // destructor of a queue blocks until all its pending tasks have been run. The
  // `wordWriterQueue_` pushes to the two other queues, and the
  // `idMapWriterQueue_` writes to the `idMapBatchWriter_`, so this is the only
  // order in which no task can be pushed to (or run on) an already destroyed
  // object.
  IdMapBatchWriter idMapBatchWriter_;
  VocabularyWriter vocabularyWriter_;
  ad_utility::TaskQueue<false> idMapWriterQueue_{queueSize, 1,
                                                 "Writing the ID maps"};
  ad_utility::TaskQueue<false> mergedWordsDestructionQueue_{
      queueSize, 1, "Destroying the merged words"};
  ad_utility::TaskQueue<false> wordWriterQueue_{
      queueSize, 1, "Writing the merged vocabulary"};

 public:
  // Create the pipeline. The `basename` and `numFiles` determine the files of
  // the partial ID maps (see `IdMapBatchWriter`).
  VocabularyMergePipeline(const std::string& basename, size_t numFiles)
      : idMapBatchWriter_{basename, numFiles} {}

  // Asynchronously process a single `batch` of merged words: write its
  // distinct words to the vocabulary (via the `wordCallback` and the
  // `blankNodeIriRegexes`), then write its index mappings and destroy the
  // merged words that it was created from. Block if the pipeline is busy.
  //
  // NOTE: The `wordCallback` and the `blankNodeIriRegexes` are captured *by
  // reference* into the asynchronous task, so both of them have to stay alive
  // (and must not be modified from the outside) until `finish()` has
  // returned.
  CPP_template(typename C)(requires WordCallback<C>) void push(
      WordBatch batch, C& wordCallback,
      const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes);

  // Wait until all the batches that were pushed have been processed
  // completely, close the ID maps, and return the metadata of the merged
  // vocabulary. After this, no more batches may be pushed.
  VocabularyMetaData finish();
};

// _____________________________________________________________________________
CPP_template_def(typename C)(
    requires WordCallback<C>) void VocabularyMergePipeline::
    push(WordBatch batch, C& wordCallback,
         const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes) {
  wordWriterQueue_.push([this, batch = std::move(batch), &wordCallback,
                         &blankNodeIriRegexes]() mutable {
    auto idMapBatch = vocabularyWriter_.writeWordsToVocabulary(
        batch.uniqueWords_, std::move(batch.localIdxMappings_), wordCallback,
        blankNodeIriRegexes);

    // The merged words are no longer needed. Their destruction (which involves
    // freeing one string per word) is expensive enough to be done by yet
    // another thread. NOTE: The `clear()` is the actual work of this task; it
    // happens on the queue's thread, as does the destruction of the (then
    // empty) buffers.
    mergedWordsDestructionQueue_.push(
        [buffers = std::move(batch.mergedWordBuffers_)]() mutable {
          buffers.clear();
        });

    idMapWriterQueue_.push([this, idMapBatch = std::move(idMapBatch)]() {
      idMapBatchWriter_.writeBatch(idMapBatch);
    });
  });
}

// _____________________________________________________________________________
inline VocabularyMetaData VocabularyMergePipeline::finish() {
  // NOTE: The order is important, see the declaration of the members.
  wordWriterQueue_.finish();
  mergedWordsDestructionQueue_.finish();
  idMapWriterQueue_.finish();
  idMapBatchWriter_.finish();
  vocabularyWriter_.logFinalProgress();
  return std::move(vocabularyWriter_.metaData());
}
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEPIPELINE_H
