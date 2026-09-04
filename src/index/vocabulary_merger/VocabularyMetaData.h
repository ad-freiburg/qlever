// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_VOCABULARYMETADATA_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_VOCABULARYMETADATA_H

#include <string>
#include <string_view>
#include <utility>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/SpecialIds.h"
#include "util/HashMap.h"

// NOTE: This class is the return type of `mergeVocabulary` and hence part of
// the public interface of the vocabulary merger. It only lives in its own
// header (instead of in `index/VocabularyMerger.h`, which is the user-facing
// header and which includes this one) because the internal stages of the
// merging pipeline need it, and those in turn are included by
// `index/VocabularyMerger.h`.
namespace ad_utility::vocabulary_merger {

// The result of a call to `mergeVocabulary` (see `index/VocabularyMerger.h`).
struct VocabularyMetaData {
  // This struct is used to incrementally construct the range of IDs that
  // correspond to a given prefix. To use it, all the words from the
  // vocabulary must be passed to the member function `addIfWordMatches` in
  // sorted order. After that, the range `[begin(), end())` is the range of
  // all the words that start with the prefix.
  struct IdRangeForPrefix {
    explicit IdRangeForPrefix(std::string prefix)
        : prefix_{std::move(prefix)} {}
    // Check if `word` starts with the `prefix_`. If so, `wordIndex`
    // will become part of the range that this struct represents. The function
    // returns `true` in this case, else `false`. For this to work, all the
    // words that start with the `prefix_` have to be passed in consecutively
    // and their indices have to be consecutive and ascending.
    bool addIfWordMatches(std::string_view word, size_t wordIndex) {
      if (!ql::starts_with(word, prefix_)) {
        return false;
      }
      if (!beginWasSeen_) {
        begin_ = Id::makeFromVocabIndex(VocabIndex::make(wordIndex));
        beginWasSeen_ = true;
      }
      end_ = Id::makeFromVocabIndex(VocabIndex::make(wordIndex + 1));
      return true;
    }

    Id begin() const { return begin_; }
    Id end() const { return end_; }

    // Return true if the `id` belongs to this range.
    bool contains(Id id) const { return begin_ <= id && id < end_; }

   private:
    Id begin_ = Id::makeUndefined();
    Id end_ = Id::makeUndefined();
    std::string prefix_;
    bool beginWasSeen_ = false;
  };

 public:
  // This function has to be called for every *DISTINCT* word (IRI or literal,
  // not blank nodes) and the index, which is assigned to this word by the merge
  // procedure. It automatically updates the various prefix ranges, the total
  // number of words, and the mapping of the special IDs.
  void addWord(std::string_view word, size_t wordIndex) {
    ++numWordsTotal_;
    if (langTaggedPredicates_.addIfWordMatches(word, wordIndex)) {
      return;
    }
    if (internalEntities_.addIfWordMatches(word, wordIndex)) {
      if (globalSpecialIds_->contains(word)) {
        specialIdMapping_[std::string{word}] =
            Id::makeFromVocabIndex(VocabIndex::make(wordIndex));
      }
    }
  }

  // Return the index of the next blank node and increment the internal counter
  // of blank nodes. This has to be called for every distinct blank node that
  // is encountered.
  size_t getNextBlankNodeIndex() {
    auto res = numBlankNodesTotal_;
    ++numBlankNodesTotal_;
    return res;
  }

  // The mapping from the `qlever::specialIds` to their actual IDs.
  // This is created on the fly by the calls to `addWord`.
  const auto& specialIdMapping() const { return specialIdMapping_; }
  // The prefix range for the `@en@<predicate` style predicates.
  const auto& langTaggedPredicates() const { return langTaggedPredicates_; }
  // The prefix range for the internal IRIs in the `ql:` namespace.
  const auto& internalEntities() const { return internalEntities_; }
  // The number of words for which `addWord()` has been called. Needs to return
  // a reference to be used in combination with a `ProgressBar`.
  const size_t& numWordsTotal() const { return numWordsTotal_; }

  // Return true iff the `id` belongs to one of the two ranges that contain
  // the internal IDs that were added by QLever and were not part of the
  // input.
  bool isQleverInternalId(Id id) const {
    return internalEntities_.contains(id) || langTaggedPredicates_.contains(id);
  }

 private:
  // The number of distinct words (size of the created vocabulary).
  size_t numWordsTotal_ = 0;
  // The number of distinct blank nodes that were found and immediately
  // converted to an ID without becoming part of the vocabulary.
  size_t numBlankNodesTotal_ = 0;
  IdRangeForPrefix langTaggedPredicates_{
      std::string{ad_utility::languageTaggedPredicatePrefix}};
  IdRangeForPrefix internalEntities_{
      std::string{QLEVER_INTERNAL_PREFIX_IRI_WITHOUT_CLOSING_BRACKET}};

  ad_utility::HashMap<std::string, Id> specialIdMapping_;
  const ad_utility::HashMap<std::string, Id>* globalSpecialIds_ =
      &qlever::specialIds();
};
}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_VOCABULARYMETADATA_H
