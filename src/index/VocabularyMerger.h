// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGER_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGER_H

#include <memory>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexBuilderTypes.h"
#include "index/vocabulary/Vocabulary.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/MergePipeline.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "index/vocabulary_merger/WordBatchBuilder.h"
#include "util/HashMap.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/SerializePair.h"
#include "util/Serializer/SerializeVector.h"

using TripleVec =
    ad_utility::CompressedExternalIdTable<NumColumnsIndexBuilding>;

// The vocabulary merger. It merges the partial vocabularies that the index
// builder has written to disk into the final (sorted and duplicate-free)
// vocabulary, and writes one partial ID map per partial vocabulary, which maps
// the local index of each word to the global ID that the merged vocabulary
// assigns to it.
//
// This header is the public interface of the vocabulary merger. The individual
// stages of the merging are implemented in `src/index/vocabulary_merger/`; of
// those, only `VocabularyMetaData` (the return type of `mergeVocabulary`), the
// concepts for its callbacks, and the `IdMap` types are part of the public
// interface, and all of them are made available by this header.
namespace ad_utility::vocabulary_merger {

// _______________________________________________________________
// Merge the partial vocabularies in the  binary files
// `basename + PARTIAL_VOCAB_WORDS_INFIX + to_string(i)`
// where `0 <= i < numFiles`.
// Return the number of total Words merged and the lower and upper bound of
// language tagged predicates. Argument `comparator` gives the way to order
// strings (case-sensitive or not). Argument `wordCallback`
// is called for each merged word in the vocabulary in the order of their
// appearance. Argument `blankNodeIriRegexes` is a (possibly empty) list of
// compiled regexes; IRIs that are fully matched by any of them are treated as
// blank nodes (see `TripleComponentWithIndex::isBlankNode`). The regexes are
// compiled by the caller (see `IndexImpl::setBlankNodeIriRegexes`).
//
// The merging is organized as a pipeline of four threads, which communicate
// via task queues, such that all of them can work concurrently:
//
// 1. The thread that calls `mergeVocabulary` obtains the merged words in
//    sorted order and eliminates the duplicates (a word typically occurs in
//    many of the partial vocabularies). It collects the distinct words as well
//    as the index mappings for the partial ID maps in batches (see
//    `detail::WordBatchBuilder`) and hands each batch to the second thread.
// 2. The `wordWriterQueue_`'s thread writes the distinct words of a batch to
//    the vocabulary (via the `wordCallback`) and thereby determines their
//    global IDs (see `detail::VocabularyWriter`).
// 3. The `idMapWriterQueue_`'s thread writes the entries of the partial ID
//    maps (which only now know their global IDs) to those maps (see
//    `detail::IdMapBatchWriter`).
// 4. The `mergedWordsDestructionQueue_`'s thread destroys the merged words of
//    a batch (which involves freeing one string per word) once they have been
//    written to the vocabulary.
//
// The last three of those stages are owned by the
// `detail::VocabularyMergePipeline`.
template <typename W, typename C>
auto mergeVocabulary(
    const std::string& basename, size_t numFiles, W comparator, C& wordCallback,
    ad_utility::MemorySize memoryToUse,
    const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes = {})
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>);

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
    const HashMap<Id, Id>& map, std::unique_ptr<TripleVec>* writePtr);

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
 * @brief Take an Array of HashMaps of strings to Ids and insert all the
 * elements from all the hashMaps into a single vector No reordering or
 * deduplication is done, so result.size() == summed size of all the hash maps
 */
ItemVec vocabMapsToVector(const ItemMapArray& map);

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
