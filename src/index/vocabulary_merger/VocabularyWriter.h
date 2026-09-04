// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_VOCABULARYWRITER_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_VOCABULARYWRITER_H

#include <re2/re2.h>

#include <memory>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "global/Id.h"
#include "index/IndexBuilderTypes.h"
#include "index/vocabulary_merger/IdMapBatch.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "index/vocabulary_merger/WordBatch.h"
#include "index/vocabulary_merger/WordCallbacks.h"
#include "util/Log.h"
#include "util/ProgressBar.h"

// The second stage of the merging pipeline of the vocabulary merger (see the
// comment above `mergeVocabulary` in `index/VocabularyMerger.h`), which is not
// part of the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// The second stage of the merging pipeline: write the distinct words of a
// batch to the vocabulary (via the word callback) and thereby determine their
// global IDs.
//
// NOTE: This class is used exclusively by the thread of the
// `wordWriterQueue_` of the `VocabularyMergePipeline` (see
// `index/vocabulary_merger/MergePipeline.h`).
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

// _____________________________________________________________________________
CPP_template_def(typename C)(requires WordCallback<C>)
    IdMapBatch VocabularyWriter::writeWordsToVocabulary(
        const std::vector<UniqueWord>& uniqueWords,
        QueuedIdMapBatch queuedEntries, C& wordCallback,
        const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes) {
  AD_LOG_TRACE << "Start writing a batch of merged words\n";

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
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_VOCABULARYWRITER_H
