// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDBATCHBUILDER_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDBATCHBUILDER_H

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/QueueWord.h"
#include "index/vocabulary_merger/WordBatch.h"
#include "util/Exception.h"

// The first stage of the vocabulary merger (see the comment above
// `mergeVocabulary` in `index/VocabularyMerger.h`), which is not part of the
// public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// The first stage of the merging: eliminate the duplicates from the merged
// words and collect the distinct words as well as the index mappings for the
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
// `WordBatch`es are the only thing that it hands on to the writing thread.
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
  // The occurrences of the `pendingWord_` live in two places, both of which
  // `commitPendingWord` writes to the batch directly. The merged word from
  // which the `pendingWord_` was taken (if it still lives in one of the
  // `mergedWordBuffers_` of the `currentBatch_`, which is not the case for a
  // word that was carried over from the previous batch) holds its own
  // occurrences, see `QueueWord::moreOccurrences_`; the `pendingMappings_`
  // hold the occurrences of further merged words that were equal to the
  // `pendingWord_` (the same word at the boundary of two buffers), and those
  // of a carried-over word. Their `indexOfWordInBatch_` is only filled in by
  // `commitPendingWord`. NOTE: A word occurs at most once per partial
  // vocabulary, so this vector stays small.
  const QueueWord* pendingQueueWord_ = nullptr;
  std::vector<LocalIdxToBatchMapping> pendingMappings_;
  // The batch that is currently being filled.
  WordBatch currentBatch_{VOCAB_MERGER_WORD_BATCH_SIZE};
  // The total size of the words that were merged into the `currentBatch_`
  // (including the duplicates, as the batch keeps all of them alive). NOTE:
  // This is deliberately a plain number of bytes and not an
  // `ad_utility::MemorySize`, because it is updated once per merged word.
  size_t currentBatchWordSizeInBytes_ = 0;

 public:
  // Eliminate the duplicates from a `buffer` of merged words and add the
  // resulting distinct words as well as one index mapping per merged word to
  // the current batch. The last distinct word and its mappings are held back
  // (see the class comment above). Whenever a batch is full (see
  // `VOCAB_MERGER_WORD_BATCH_SIZE` and `VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE`),
  // it is handed to the `batchCallback`. The `QueueWord`s must be passed in
  // alphabetical order wrt the `comparator` (also across multiple calls), and
  // the words of a single `buffer` must be distinct (the merge folds the
  // occurrences of a word within a buffer into a single merged word, see
  // `QueueWord::moreOccurrences_`), so that only the first word of a buffer can
  // be equal to the last word of the previous one. NOTE: The order is only
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

  // Add the `pendingWord_` and its occurrences to the `currentBatch_`. This
  // must only be called once it is known that no further occurrence of that
  // word can arrive. Do nothing if there is no pending word.
  void commitPendingWord();

  // Append the occurrences of the merged `word` (its own and the ones folded
  // into it, see `QueueWord::moreOccurrences_`) to the `pendingMappings_`.
  void appendOccurrencesToPendingMappings(const QueueWord& word);

  // Reset the `currentBatch_` and allocate its buffers.
  void startNewBatch();
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

  // Iterate (avoid duplicates). NOTE: The merge has already folded the
  // occurrences of a word within a buffer into a single merged word (see
  // `QueueWord::moreOccurrences_`), so the words of a buffer are distinct, and
  // only its first word can be equal to the `pendingWord_` (the same word at
  // the boundary of two buffers). The comparison is therefore only done for
  // that first word: reading the bytes of every word (which are spread over the
  // heap) was the most expensive part of this loop. The occurrences of a word
  // are written to the batch straight from the merged word when the word is
  // committed, see `commitPendingWord`.
  bool isFirstWordOfBuffer = true;
  for (const auto& top : words) {
    if (isFirstWordOfBuffer && hasPendingWord_ &&
        top.iriOrLiteral() == pendingWord_) {
      // If a word appears with different values for `isExternal`, then we
      // externalize it. NOTE: This is only correct because the word is still
      // held back, and hence has not been written to the vocabulary yet.
      pendingWordIsExternal_ = pendingWordIsExternal_ || top.isExternal();
      appendOccurrencesToPendingMappings(top);
    } else {
      AD_EXPENSIVE_CHECK(
          !hasPendingWord_ || comparator(pendingWord_, top.iriOrLiteral()),
          "Total vocabulary order violated for ", pendingWord_, " and ",
          top.iriOrLiteral());
      // A word that differs from the `pendingWord_` was merged, so no further
      // occurrence of the latter can arrive and it can be committed.
      commitPendingWord();
      pendingWord_ = top.iriOrLiteral();
      pendingWordIsExternal_ = top.isExternal();
      pendingQueueWord_ = &top;
      hasPendingWord_ = true;
    }
    isFirstWordOfBuffer = false;
    currentBatchWordSizeInBytes_ += top.iriOrLiteral().size();
  }

  // A batch is complete as soon as one of the two limits is reached.
  if (currentBatch_.localIdxMappings_.numMappings_ >=
          VOCAB_MERGER_WORD_BATCH_SIZE ||
      currentBatchWordSizeInBytes_ >=
          VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE.getBytes()) {
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
  if (currentBatch_.empty()) {
    return;
  }
  // The `pendingWord_` is a view into one of the `mergedWordBuffers_` of the
  // current batch, which the fourth stage of the pipeline destroys
  // asynchronously as soon as the batch has been handed on (see the
  // `mergedWordsDestructionQueue_` in
  // `index/vocabulary_merger/MergePipeline.h`), so we have to create the copy
  // that the next batch owns *before* handing the current batch on.
  std::unique_ptr<std::string> carriedOverWord;
  if (hasPendingWord_) {
    carriedOverWord = std::make_unique<std::string>(pendingWord_);
    // The merged word itself goes with the batch, so its occurrences have to
    // be copied as well.
    if (pendingQueueWord_ != nullptr) {
      appendOccurrencesToPendingMappings(*pendingQueueWord_);
      pendingQueueWord_ = nullptr;
    }
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
    AD_CORRECTNESS_CHECK(pendingMappings_.empty() &&
                         pendingQueueWord_ == nullptr);
    return;
  }
  auto& mappings = currentBatch_.localIdxMappings_.mappings_;
  size_t& numMappings = currentBatch_.localIdxMappings_.numMappings_;
  const size_t numNewMappings =
      (pendingQueueWord_ == nullptr
           ? 0
           : 1 + pendingQueueWord_->moreOccurrences_.size()) +
      pendingMappings_.size();
  // The mappings are allocated in advance (see `startNewBatch`), so the
  // following `resize` (which would have to copy the mappings that are already
  // in the buffer) typically is a no-op, and the writes below are simple
  // unchecked stores.
  if (mappings.size() < numMappings + numNewMappings) {
    mappings.resize(numMappings + numNewMappings);
  }
  auto& uniqueWords = currentBatch_.uniqueWords_;
  uniqueWords.push_back(UniqueWord{pendingWord_, pendingWordIsExternal_});
  // The index of a word within its batch is stored in 32 bits, and has to stay
  // below the `indexOfWordInBatchDummy` (see there). NOTE: This check is
  // performed once per distinct word, so it is not on the hot path of the
  // merging (which is one iteration per merged word, see `addMergedWords`).
  AD_CORRECTNESS_CHECK(uniqueWords.size() <= maxNumUniqueWordsPerBatch);
  auto indexOfWordInBatch = static_cast<uint32_t>(uniqueWords.size() - 1);
  // The occurrences of the merged word itself (see the `pendingQueueWord_`),
  // then the ones that were collected from equal merged words.
  if (pendingQueueWord_ != nullptr) {
    const auto& word = *pendingQueueWord_;
    mappings[numMappings] =
        LocalIdxToBatchMapping{static_cast<uint32_t>(word.partialFileId_),
                               indexOfWordInBatch, VocabIndex::make(word.id())};
    ++numMappings;
    for (const auto& [partialFileId, localIndex] : word.moreOccurrences_) {
      mappings[numMappings] = LocalIdxToBatchMapping{
          partialFileId, indexOfWordInBatch, VocabIndex::make(localIndex)};
      ++numMappings;
    }
    pendingQueueWord_ = nullptr;
  }
  for (auto mapping : pendingMappings_) {
    mapping.indexOfWordInBatch_ = indexOfWordInBatch;
    mappings[numMappings] = mapping;
    ++numMappings;
  }
  pendingMappings_.clear();
  hasPendingWord_ = false;
}

// _____________________________________________________________________________
inline void WordBatchBuilder::appendOccurrencesToPendingMappings(
    const QueueWord& word) {
  // The index of the word within its batch is only filled in by
  // `commitPendingWord`, so the dummy `indexOfWordInBatchDummy` is written for
  // now.
  pendingMappings_.push_back(LocalIdxToBatchMapping{
      static_cast<uint32_t>(word.partialFileId_), indexOfWordInBatchDummy,
      VocabIndex::make(word.id())});
  for (const auto& [partialFileId, localIndex] : word.moreOccurrences_) {
    pendingMappings_.push_back(LocalIdxToBatchMapping{
        partialFileId, indexOfWordInBatchDummy, VocabIndex::make(localIndex)});
  }
}

// _____________________________________________________________________________
inline void WordBatchBuilder::startNewBatch() {
  // NOTE: A moved-from vector is in a valid but unspecified state, so we have
  // to explicitly reset the batch. Its constructor allocates the buffers.
  currentBatch_ = WordBatch{VOCAB_MERGER_WORD_BATCH_SIZE};
  currentBatchWordSizeInBytes_ = 0;
}
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDBATCHBUILDER_H
