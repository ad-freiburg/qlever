// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGER_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGER_H

#include <memory>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexBuilderTypes.h"
#include "index/vocabulary/Vocabulary.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/QueueWord.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "index/vocabulary_merger/WordBatch.h"
#include "index/vocabulary_merger/WordBatchBuilder.h"
#include "util/HashMap.h"
#include "util/ProgressBar.h"
#include "util/Serializer/FileSerializer.h"
#include "util/TaskQueue.h"
#include "util/TypeTraits.h"

using TripleVec =
    ad_utility::CompressedExternalIdTable<NumColumnsIndexBuilding>;

// This header is the public interface of the vocabulary merger. The parts of
// it that are understandable (and testable) on their own live in
// `src/index/vocabulary_merger/`, and all of them are made available by this
// header: the `VocabularyMetaData` (the return type of `mergeVocabulary`), the
// concepts for its callbacks, the `IdMap` types, the `detail::QueueWord`, and
// the first stage of the merging (the `detail::WordBatchBuilder`).
namespace ad_utility::vocabulary_merger {

// _______________________________________________________________
// Merge the partial vocabularies in the  binary files
// `basename + PARTIAL_VOCAB_WORDS_INFIX + suffix` for each `suffix` in
// `partialVocabularySuffixes`. The mapping from the partial to the global IDs
// is written to `basename + PARTIAL_VOCAB_IDMAP_INFIX + suffix`.
// Return the number of total Words merged and the lower and upper bound of
// language tagged predicates. Argument `comparator` gives the way to order
// strings (case-sensitive or not). Argument `wordCallback`
// is called for each merged word in the vocabulary in the order of their
// appearance. Argument `blankNodeIriRegexes` is a (possibly empty) set of
// compiled regexes; IRIs that are fully matched by any of them are treated as
// blank nodes (see `TripleComponentWithIndex::isBlankNode`). The regexes are
// compiled by the caller (see `IndexImpl::setBlankNodeIriRegexes`).
//
// The merging is split into two stages, which run on two threads that work
// concurrently:
//
// 1. The thread that calls `mergeVocabulary` obtains the merged words in
//    sorted order and eliminates the duplicates (a word typically occurs in
//    many of the partial vocabularies). It collects the distinct words as well
//    as the index mappings for the partial ID maps in batches (see
//    `detail::WordBatchBuilder`) and hands each complete batch to the second
//    thread.
// 2. The thread of the `wordBatchQueue_` writes the distinct words of a batch
//    to the vocabulary (via the `wordCallback`), which determines their global
//    IDs, and then writes the index mappings of that batch to the partial ID
//    maps (see `VocabularyMerger::writeWordBatch`).
template <typename W, typename C>
auto mergeVocabulary(const std::string& basename,
                     const std::vector<std::string>& partialVocabularySuffixes,
                     W comparator, C& wordCallback,
                     ad_utility::MemorySize memoryToUse,
                     const ad_utility::RegexSet& blankNodeIriRegexes = {})
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>);

// A helper class that implements the `mergeVocabulary` function (see
// above). Everything in this class is private and only the
// `mergeVocabulary` function is a friend.
class VocabularyMerger {
 private:
  // private data members

  // The result (mostly metadata) which we'll return.
  VocabularyMetaData metaData_;
  ad_utility::ProgressBar progressBar_{metaData_.numWordsTotal(),
                                       "Words merged: "};
  // The writers for the partial ID maps, one per partial vocabulary. Each of
  // them writes the mapping from the local indices of its partial vocabulary
  // to the global IDs.
  std::vector<IdMapWriter> idMapWriters_;
  // The first stage of the merging, which runs on the thread that calls
  // `mergeVocabulary`.
  detail::WordBatchBuilder batchBuilder_;
  // The second stage of the merging. NOTE: The queue has exactly one worker
  // thread, so the batches are written in exactly the order in which the
  // `batchBuilder_` creates them, and the state that `writeWordBatch` touches
  // requires no further synchronization. The queue is deliberately declared
  // last, because its destructor blocks until all its pending tasks have been
  // run, and those tasks access all the members above.
  ad_utility::TaskQueue<false> wordBatchQueue_{
      VOCAB_MERGER_WORD_BATCH_QUEUE_SIZE, 1, "Writing the merged vocabulary"};

  // Friend declaration for the publicly available function.
  template <typename W, typename C>
  friend auto mergeVocabulary(
      const std::string& basename,
      const std::vector<std::string>& partialVocabularySuffixes, W comparator,
      C& wordCallback, ad_utility::MemorySize memoryToUse,
      const ad_utility::RegexSet& blankNodeIriRegexes)
      -> CPP_ret(VocabularyMetaData)(
          requires WordComparator<W>&& WordCallback<C>);
  VocabularyMerger() = default;

  // _______________________________________________________________
  // The function that performs the actual merge. See the static global
  // `mergeVocabulary` function for details.
  template <typename W, typename C>
  auto mergeVocabulary(
      const std::string& basename,
      const std::vector<std::string>& partialVocabularySuffixes, W comparator,
      C& wordCallback, ad_utility::MemorySize memoryToUse,
      const ad_utility::RegexSet& blankNodeIriRegexes)
      -> CPP_ret(VocabularyMetaData)(
          requires WordComparator<W>&& WordCallback<C>);

  using QueueWord = detail::QueueWord;

  // Write a single complete `batch`: its distinct words to the vocabulary (via
  // the `wordCallback`), which determines their global IDs, and then its index
  // mappings to the corresponding `idMapWriters_`.
  //
  // NOTE: This is called exclusively by the thread of the `wordBatchQueue_`.
  CPP_template(typename C)(requires WordCallback<C>) void writeWordBatch(
      const detail::WordBatch& batch, C& wordCallback,
      const ad_utility::RegexSet& blankNodeIriRegexes);

  // Close all associated files and file-based vectors and reset all internal
  // variables.
  void clear() {
    metaData_ = VocabularyMetaData{};
    // NOTE: The destructor of an `IdMapWriter` also finishes it, but only
    // an explicit `finish()` can propagate errors as exceptions.
    for (auto& idMapWriter : idMapWriters_) {
      idMapWriter.finish();
    }
    idMapWriters_.clear();
  }
};

// Read the partial ID map from the given file (see `IdMapWriter`) into a hash
// map. NOTE: The keys are plain `VocabIndex`es, because inside a partial
// vocabulary a word is always a `VocabIndex`. The values are full `Id`s,
// because a merged word may also become a blank node (see `isBlankNode`).
ad_utility::HashMap<VocabIndex, Id> IdMapFromPartialIdMapFile(
    const std::string& filename);

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
ad_utility::HashMap<uint64_t, uint64_t> createInternalMapping(ItemVec& els);

/**
 * @brief for each of the IdTriples in <input>: map the three Ids using the
 * <map> and write the resulting Id triple to <*writePtr>
 */
void writeMappedIdsToExtVec(
    const std::vector<std::array<Id, NumColumnsIndexBuilding>>& input,
    const HashMap<Id, Id>& map, TripleVec& vec);

/**
 * @brief Serialize a std::vector<std::pair<string, Id>> to a binary file
 *
 * For each string first writes the size of the string (64 bits). Then the
 * actual string content (no trailing zero) and then the Id (sizeof(Id)
 *
 * @param els The input
 * @param fileName will write to this file. If it exists it will be overwritten
 */
void writePartialVocabularyToFile(const ItemVec& els,
                                  const std::string& fileName);

/**
 * @brief Take a HashMap of strings to Ids and insert all its elements into a
 * single vector. No reordering or deduplication is done, so result.size() ==
 * size of the hash map
 */
ItemVec vocabMapsToVector(const ItemMapAndBuffer& map);

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

#include "index/VocabularyMergerImpl.h"

#endif  // QLEVER_SRC_INDEX_VOCABULARYMERGER_H
