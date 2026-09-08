// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGER_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGER_H

#include <memory>
#include <optional>
#include <string>
#include <utility>
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
#include "util/HashMap.h"
#include "util/ProgressBar.h"
#include "util/Serializer/FileSerializer.h"
#include "util/TypeTraits.h"

using TripleVec =
    ad_utility::CompressedExternalIdTable<NumColumnsIndexBuilding>;

// This header is the public interface of the vocabulary merger. The parts of
// it that are understandable (and testable) on their own live in
// `src/index/vocabulary_merger/`, and all of them are made available by this
// header: the `VocabularyMetaData` (the return type of `mergeVocabulary`), the
// concepts for its callbacks, the `IdMap` types, and the `detail::QueueWord`.
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
  std::optional<TripleComponentWithIndex> lastTripleComponent_ = std::nullopt;
  // Whether `lastTripleComponent_` is a blank node. Cached here so that
  // `isBlankNode` (which may run a set of regexes) is evaluated only once per
  // distinct word.
  bool lastTripleComponentIsBlankNode_ = false;
  // The writers for the partial ID maps, one per partial vocabulary. Each of
  // them writes the mapping from the local indices of its partial vocabulary
  // to the global IDs.
  std::vector<IdMapWriter> idMapWriters_;

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

  // Write the queue words in the buffer to their corresponding
  // `idMapWriters_`.
  // The `QueueWord`s must be passed in alphabetical order wrt `lessThan` (also
  // across multiple calls).
  // clang-format off
    CPP_template(typename C, typename L)(
      requires WordCallback<C> CPP_and ranges::predicate<
          L, TripleComponentWithIndex, TripleComponentWithIndex>)
      // clang-format on
      void writeQueueWordsToIdMap(
          std::vector<QueueWord>& buffer, C& wordCallback, const L& lessThan,
          const ad_utility::RegexSet& blankNodeIriRegexes,
          ad_utility::ProgressBar& progressBar);

  // Close all associated files and file-based vectors and reset all internal
  // variables.
  void clear() {
    metaData_ = VocabularyMetaData{};
    lastTripleComponent_ = std::nullopt;
    lastTripleComponentIsBlankNode_ = false;
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
