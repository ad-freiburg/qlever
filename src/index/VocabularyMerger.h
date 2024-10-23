// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>
#pragma once

#include <string>
#include <utility>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexBuilderTypes.h"
#include "index/Vocabulary.h"
#include "util/HashMap.h"
#include "util/MmapVector.h"
#include "util/ProgressBar.h"

using IdPairMMapVec = ad_utility::MmapVector<std::pair<Id, Id>>;
using IdPairMMapVecView = ad_utility::MmapVectorView<std::pair<Id, Id>>;

using TripleVec =
    ad_utility::CompressedExternalIdTable<NumColumnsIndexBuilding>;

namespace ad_utility::vocabulary_merger {
// Concept for a callback that can be called with a `string_view` and a `bool`.
// If the `bool` is true, then the word is to be stored in the external
// vocabulary else in the internal vocabulary.
template <typename T>
concept WordCallback = std::invocable<T, std::string_view, bool>;
// Concept for a callable that compares to `string_view`s.
template <typename T>
concept WordComparator = std::predicate<T, std::string_view, std::string_view>;

// The result of a call to `mergeVocabulary` (see below).
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
      if (!word.starts_with(prefix_)) {
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
    Id begin_ = ID_NO_VALUE;
    Id end_ = ID_NO_VALUE;
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
  IdRangeForPrefix internalEntities_{std::string{INTERNAL_ENTITIES_URI_PREFIX}};

  ad_utility::HashMap<std::string, Id> specialIdMapping_;
  const ad_utility::HashMap<std::string, Id>* globalSpecialIds_ =
      &qlever::specialIds();
};
// _______________________________________________________________
// Merge the partial vocabularies in the  binary files
// `basename + PARTIAL_VOCAB_FILE_NAME + to_string(i)`
// where `0 <= i < numFiles`.
// Return the number of total Words merged and the lower and upper bound of
// language tagged predicates. Argument `comparator` gives the way to order
// strings (case-sensitive or not). Argument `wordCallback`
// is called for each merged word in the vocabulary in the order of their
// appearance.
VocabularyMetaData mergeVocabulary(const std::string& basename, size_t numFiles,
                                   WordComparator auto comparator,
                                   WordCallback auto& wordCallback,
                                   ad_utility::MemorySize memoryToUse);

// A helper class that implements the `mergeVocabulary` function (see
// above). Everything in this class is private and only the
// `mergeVocabulary` function is a friend.
class VocabularyMerger {
 private:
  // private data members

  // The result (mostly metadata) which we'll return.
  VocabularyMetaData metaData_;
  std::optional<TripleComponentWithIndex> lastTripleComponent_ = std::nullopt;
  // we will store pairs of <partialId, globalId>
  std::vector<IdPairMMapVec> idVecs_;

  const size_t bufferSize_ = BATCH_SIZE_VOCABULARY_MERGE;

  // Friend declaration for the publicly available function.
  friend VocabularyMetaData mergeVocabulary(const std::string& basename,
                                            size_t numFiles,
                                            WordComparator auto comparator,
                                            WordCallback auto& wordCallback,
                                            ad_utility::MemorySize memoryToUse);
  VocabularyMerger() = default;

  // _______________________________________________________________
  // The function that performs the actual merge. See the static global
  // `mergeVocabulary` function for details.
  VocabularyMetaData mergeVocabulary(const std::string& basename,
                                     size_t numFiles,
                                     WordComparator auto comparator,
                                     WordCallback auto& wordCallback,
                                     ad_utility::MemorySize memoryToUse);

  // Helper `struct` for a word from a partial vocabulary.
  struct QueueWord {
    QueueWord() = default;
    QueueWord(TripleComponentWithIndex&& v, size_t file)
        : entry_(std::move(v)), partialFileId_(file) {}
    TripleComponentWithIndex entry_;  // the word, its local ID and the
                                      // information if it will be externalized
    size_t partialFileId_;  // from which partial vocabulary did this word come

    [[nodiscard]] const bool& isExternal() const { return entry_.isExternal(); }
    [[nodiscard]] bool& isExternal() { return entry_.isExternal(); }

    [[nodiscard]] const std::string& iriOrLiteral() const {
      return entry_.iriOrLiteral();
    }

    [[nodiscard]] const auto& id() const { return entry_.index_; }
  };

  constexpr static auto sizeOfQueueWord = [](const QueueWord& q) {
    return ad_utility::MemorySize::bytes(sizeof(QueueWord) +
                                         q.entry_.iriOrLiteral().size());
  };

  // Write the queue words in the buffer to their corresponding `idPairVecs`.
  // The `QueueWord`s must be passed in alphabetical order wrt `lessThan` (also
  // across multiple calls).
  void writeQueueWordsToIdVec(
      const std::vector<QueueWord>& buffer, WordCallback auto& wordCallback,
      std::predicate<TripleComponentWithIndex,
                     TripleComponentWithIndex> auto const& lessThan,
      ad_utility::ProgressBar& progressBar);

  // Close all associated files and MmapVectors and reset all internal
  // variables.
  void clear() {
    metaData_ = VocabularyMetaData{};
    lastTripleComponent_ = std::nullopt;
    idVecs_.clear();
  }

  // Inner helper function for the parallel pipeline, which performs the actual
  // write to the IdPairVecs. Format of argument is `<vecToWriteTo<internalId,
  // globalId>>`.
  void doActualWrite(
      const std::vector<std::pair<size_t, std::pair<size_t, Id>>>& buffer);
};

// ____________________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const string& mmapFilename);

/**
 * @brief Create a hashMap that maps the Id of the pair<string, Id> to the
 * position of the string in the vector. The resulting ids will be ascending and
 * duplicates strings that appear adjacent to each other will be given the same
 * ID. If Input is sorted this will mean if result[x] == result[y] then the
 * strings that were connected to x and y in the input were identical. Also
 * modifies the input Ids to their mapped values.
 *
 * @param els  Must be sorted(at least duplicates must be adjacent) according to
 * the strings and the Ids must be unique to work correctly.
 */
ad_utility::HashMap<uint64_t, uint64_t> createInternalMapping(ItemVec* els);

/**
 * @brief for each of the IdTriples in <input>: map the three Ids using the
 * <map> and write the resulting Id triple to <*writePtr>
 */
void writeMappedIdsToExtVec(const auto& input,
                            const ad_utility::HashMap<Id, Id>& map,
                            std::unique_ptr<TripleVec>* writePtr);

/**
 * @brief Serialize a std::vector<std::pair<string, Id>> to a binary file
 *
 * For each string first writes the size of the string (64 bits). Then the
 * actual string content (no trailing zero) and then the Id (sizeof(Id)
 *
 * @param els The input
 * @param fileName will write to this file. If it exists it will be overwritten
 */
void writePartialVocabularyToFile(const ItemVec& els, const string& fileName);

/**
 * @brief Take an Array of HashMaps of strings to Ids and insert all the
 * elements from all the hashMaps into a single vector No reordering or
 * deduplication is done, so result.size() == summed size of all the hash maps
 */
ItemVec vocabMapsToVector(ItemMapArray& map);

// _____________________________________________________________________________________________________________
/**
 * @brief Sort the input in-place according to the strings as compared by the
 * StringComparator
 * @tparam A binary Function object to compare strings (e.g.
 * std::less<std::string>())
 * @param doParallelSort if true and USE_PARALLEL_SORT is true, use the gnu
 * parallel extension for sorting.
 */
template <class StringSortComparator>
void sortVocabVector(ItemVec* vecPtr, StringSortComparator comp,
                     bool doParallelSort);
}  // namespace ad_utility::vocabulary_merger

#include "VocabularyMergerImpl.h"
