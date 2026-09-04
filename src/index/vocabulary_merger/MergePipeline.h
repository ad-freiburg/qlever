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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "global/Id.h"
#include "index/IndexBuilderTypes.h"
#include "index/vocabulary_merger/IdMapBatch.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "index/vocabulary_merger/WordBatch.h"
#include "index/vocabulary_merger/WordCallbacks.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/ProgressBar.h"
#include "util/TaskQueue.h"

// The individual stages of the merging pipeline of the vocabulary merger, see
// the comment above `mergeVocabulary` in `index/VocabularyMerger.h`. None of
// them is part of the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// The first stage of the merging pipeline: eliminate the duplicates from the
// merged words and collect the distinct words as well as the entries for the
// partial ID maps in batches.
//
// The last distinct word that was merged is deliberately *held back* and only
// added to a batch once a different word arrives (or once the merging is
// finished). The reason is that a word may occur in many of the partial
// vocabularies, possibly with different `isExternal` flags, of which the
// merged word has to get the logical OR. Only a word that is not added to a
// batch yet can still be changed that way; as soon as it is, the next stage
// may already have written it to the vocabulary.
//
// NOTE: This class is used exclusively by the merging thread; the complete
// `WordBatch`es are the only thing that it hands on to the other stages.
class WordBatchBuilder {
 private:
  // The distinct word that was merged last, which is held back (see the class
  // comment above). It typically is a view into one of the
  // `mergedWordBuffers_` of the `currentBatch_`; only if the batch that it was
  // merged from has already been handed on, it is a view into the
  // `carriedOverWord_` of the `currentBatch_`.
  std::string_view pendingWord_;
  bool hasPendingWord_ = false;
  // Whether any of the occurrences of the `pendingWord_` that have been seen
  // so far was marked as external.
  bool pendingWordIsExternal_ = false;
  // The ID map entries for the occurrences of the `pendingWord_` that have
  // been seen so far. Their `indexOfWordInBatch_` is only filled in by
  // `commitPendingWord`. NOTE: A word occurs at most once per partial
  // vocabulary, so this vector stays small.
  std::vector<QueuedIdMapEntry> pendingEntries_;
  // The batch that is currently being filled.
  WordBatch currentBatch_;

 public:
  WordBatchBuilder() { startNewBatch(); }

  // Eliminate the duplicates from a `buffer` of merged words and add the
  // resulting distinct words as well as one ID map entry per merged word to
  // the current batch. The last distinct word and its entries are held back
  // (see the class comment above). Whenever a batch is full, it is handed to
  // the `batchCallback`. The `QueueWord`s must be passed in alphabetical order
  // wrt the `comparator` (also across multiple calls). NOTE: This order is only
  // checked if the expensive checks are enabled (see `AD_EXPENSIVE_CHECK`),
  // because the additional comparison per word is rather costly.
  CPP_template(typename W, typename F)(
      requires WordComparator<W> CPP_and WordBatchCallback<
          F>) void addMergedWords(std::vector<QueueWord> buffer,
                                  const W& comparator, const F& batchCallback);

  // Signal that no more words will be added, and hand the remaining words
  // (including the word that is still held back, see the class comment) to the
  // `batchCallback`. After a call to `finish()`, no more words may be added.
  CPP_template(typename F)(requires WordBatchCallback<F>) void finish(
      const F& batchCallback);

 private:
  // Hand the current (typically only partially filled) batch to the
  // `batchCallback` and start a new batch. Do nothing if the current batch is
  // empty.
  CPP_template(typename F)(requires WordBatchCallback<F>) void flush(
      const F& batchCallback);

  // Add the `pendingWord_` and its `pendingEntries_` to the `currentBatch_`.
  // This must only be called once it is known that no further occurrence of
  // that word can arrive. Do nothing if there is no pending word.
  void commitPendingWord();

  // Reset the `currentBatch_` and allocate its buffers.
  void startNewBatch();
};

// The second stage of the merging pipeline: write the distinct words of a
// batch to the vocabulary (via the word callback) and thereby determine their
// global IDs.
//
// NOTE: This class is used exclusively by the thread of the
// `wordWriterQueue_` of the `VocabularyMergePipeline` below.
class VocabularyWriter {
 private:
  // The metadata of the merged vocabulary, which is built up incrementally as
  // the words are written.
  VocabularyMetaData metaData_;
  ad_utility::ProgressBar progressBar_{metaData_.numWordsTotal(),
                                       "Words merged: "};

 public:
  // Write the `uniqueWords` to the vocabulary and return the ID map batch that
  // consists of the `queuedEntries` together with the global IDs of those
  // words (which is then complete and can be handed on to the third stage).
  CPP_template(typename C)(requires WordCallback<C>) IdMapBatch
      writeWordsToVocabulary(
          const std::vector<UniqueWord>& uniqueWords,
          QueuedIdMapBatch queuedEntries, C& wordCallback,
          const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes);

  // The metadata, which is complete as soon as all the batches have been
  // written.
  VocabularyMetaData& metaData() { return metaData_; }

  // Log the final state of the progress bar. This has to be called exactly
  // once, after the last batch has been written.
  void logFinalProgress();
};

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
  // `blankNodeIriRegexes`), then write its ID map entries and destroy the
  // merged words that it was created from. Block if the pipeline is busy.
  CPP_template(typename C)(requires WordCallback<C>) void push(
      WordBatch batch, C& wordCallback,
      const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes);

  // Wait until all the batches that were pushed have been processed
  // completely, close the ID maps, and return the metadata of the merged
  // vocabulary. After this, no more batches may be pushed.
  VocabularyMetaData finish();
};

// _____________________________________________________________________________
CPP_template_def(typename W,
                 typename F)(requires WordComparator<W> CPP_and_def
                                 WordBatchCallback<F>) void WordBatchBuilder::
    addMergedWords(std::vector<QueueWord> buffer,
                   [[maybe_unused]] const W& comparator,
                   const F& batchCallback) {
  // NOTE: The buffer is deliberately not consumed, but kept alive as part of
  // the batch, such that the merged words neither have to be moved nor
  // destroyed by the merging thread.
  currentBatch_.mergedWordBuffers_.push_back(std::move(buffer));
  const auto& words = currentBatch_.mergedWordBuffers_.back();

  // Iterate (avoid duplicates).
  for (const auto& top : words) {
    if (!hasPendingWord_ || top.iriOrLiteral() != pendingWord_) {
      AD_EXPENSIVE_CHECK(
          !hasPendingWord_ || comparator(pendingWord_, pendingWordIsExternal_,
                                         top.iriOrLiteral(), top.isExternal()),
          "Total vocabulary order violated for ", pendingWord_, " and ",
          top.iriOrLiteral());
      // A word that differs from the `pendingWord_` was merged, so no further
      // occurrence of the latter can arrive and it can be committed.
      commitPendingWord();
      pendingWord_ = top.iriOrLiteral();
      pendingWordIsExternal_ = top.isExternal();
      hasPendingWord_ = true;
    } else {
      // If a word appears with different values for `isExternal`, then we
      // externalize it. NOTE: This is only correct because the word is still
      // held back, and hence has not been written to the vocabulary yet.
      pendingWordIsExternal_ = pendingWordIsExternal_ || top.isExternal();
    }
    // Remember the local index of this occurrence of the `pendingWord_`. The
    // index of the word within its batch is only filled in by
    // `commitPendingWord`, and the actual entry of the ID map is only created
    // (and written) once the global ID of the word is known.
    pendingEntries_.push_back(QueuedIdMapEntry{
        static_cast<uint32_t>(top.partialFileId_), 0, top.id()});
  }

  if (currentBatch_.queuedIdMapBatch_.numEntries_ >= idMapEntryBatchSize) {
    flush(batchCallback);
  }
}

// _____________________________________________________________________________
CPP_template_def(typename F)(
    requires WordBatchCallback<
        F>) void WordBatchBuilder::finish(const F& batchCallback) {
  // No further words can arrive, so the word that is held back can now be
  // committed.
  commitPendingWord();
  flush(batchCallback);
}

// _____________________________________________________________________________
CPP_template_def(typename F)(
    requires WordBatchCallback<
        F>) void WordBatchBuilder::flush(const F& batchCallback) {
  if (currentBatch_.queuedIdMapBatch_.numEntries_ == 0) {
    return;
  }
  // The `pendingWord_` is a view into one of the buffers of the current batch,
  // which the pipeline destroys as soon as the batch has been handed on, so we
  // have to create the copy that the next batch owns *before* handing the
  // current batch on.
  std::unique_ptr<std::string> carriedOverWord;
  if (hasPendingWord_) {
    carriedOverWord = std::make_unique<std::string>(pendingWord_);
  }
  batchCallback(std::move(currentBatch_));
  startNewBatch();
  if (carriedOverWord) {
    pendingWord_ = *carriedOverWord;
    currentBatch_.carriedOverWord_ = std::move(carriedOverWord);
  }
}

// _____________________________________________________________________________
inline void WordBatchBuilder::commitPendingWord() {
  if (!hasPendingWord_) {
    AD_CORRECTNESS_CHECK(pendingEntries_.empty());
    return;
  }
  auto& entries = currentBatch_.queuedIdMapBatch_.entries_;
  size_t& numEntries = currentBatch_.queuedIdMapBatch_.numEntries_;
  // The entries are allocated in advance (see `startNewBatch`), so the
  // following `resize` (which would have to copy the entries that are already
  // in the buffer) typically is a no-op, and the writes below are simple
  // unchecked stores.
  if (entries.size() < numEntries + pendingEntries_.size()) {
    entries.resize(numEntries + pendingEntries_.size());
  }
  auto& uniqueWords = currentBatch_.uniqueWords_;
  uniqueWords.push_back(UniqueWord{pendingWord_, pendingWordIsExternal_});
  auto indexOfWordInBatch = static_cast<uint32_t>(uniqueWords.size() - 1);
  for (auto entry : pendingEntries_) {
    entry.indexOfWordInBatch_ = indexOfWordInBatch;
    entries[numEntries] = entry;
    ++numEntries;
  }
  pendingEntries_.clear();
  hasPendingWord_ = false;
}

// _____________________________________________________________________________
inline void WordBatchBuilder::startNewBatch() {
  // NOTE: A moved-from vector is in a valid but unspecified state, so we have
  // to explicitly reset the batch.
  currentBatch_ = WordBatch{};
  // The entries are stored in a vector with a `default_init_allocator`, so this
  // `resize` is a plain allocation that doesn't touch the memory.
  currentBatch_.queuedIdMapBatch_.entries_.resize(idMapEntryBatchSize);
  // A word typically occurs in several of the partial vocabularies, so most of
  // the merged words are duplicates. This is only a rough estimate; the vector
  // grows if it doesn't suffice.
  currentBatch_.uniqueWords_.reserve(idMapEntryBatchSize / 4);
}

// _____________________________________________________________________________
CPP_template_def(typename C)(requires WordCallback<C>)
    IdMapBatch VocabularyWriter::writeWordsToVocabulary(
        const std::vector<UniqueWord>& uniqueWords,
        QueuedIdMapBatch queuedEntries, C& wordCallback,
        const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes) {
  AD_LOG_TIMING << "Start writing a batch of merged words\n";

  // TODO<optimization> If we aim to further speed this up, we could
  // order all the write requests to _outfile _externalOutfile and all the
  // idVecs to have a more useful external access pattern.
  std::vector<Id> globalIds;
  globalIds.reserve(uniqueWords.size());
  for (const auto& uniqueWord : uniqueWords) {
    const auto& word = uniqueWord.word_;
    if (isBlankNode(word, blankNodeIriRegexes)) {
      globalIds.push_back(Id::makeFromBlankNodeIndex(
          BlankNodeIndex::make(metaData_.getNextBlankNodeIndex())));
    } else {
      auto wordIndex = wordCallback(word, uniqueWord.isExternal_);
      metaData_.addWord(word, wordIndex);
      globalIds.push_back(Id::makeFromVocabIndex(VocabIndex::make(wordIndex)));
    }
    if (progressBar_.update()) {
      AD_LOG_INFO << progressBar_.getProgressString() << std::flush;
    }
  }
  return IdMapBatch{std::move(queuedEntries), std::move(globalIds)};
}

// _____________________________________________________________________________
inline void VocabularyWriter::logFinalProgress() {
  AD_LOG_INFO << progressBar_.getFinalProgressString() << std::flush;
}

// _____________________________________________________________________________
CPP_template_def(typename C)(
    requires WordCallback<C>) void VocabularyMergePipeline::
    push(WordBatch batch, C& wordCallback,
         const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes) {
  wordWriterQueue_.push([this, batch = std::move(batch), &wordCallback,
                         &blankNodeIriRegexes]() mutable {
    auto idMapBatch = vocabularyWriter_.writeWordsToVocabulary(
        batch.uniqueWords_, std::move(batch.queuedIdMapBatch_), wordCallback,
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
