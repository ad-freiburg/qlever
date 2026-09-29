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
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexBuilderTypes.h"
#include "index/PartialVocabularyFilenames.h"
#include "index/vocabulary/Vocabulary.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/QueueWord.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "util/HashMap.h"
#include "util/MemorySize/MemorySize.h"
#include "util/TypeTraits.h"

// This header is the public interface of the vocabulary merger. The parts of
// it that are understandable (and testable) on their own live in
// `src/index/vocabulary_merger/`, and all of them are made available by this
// header: the `VocabularyMetaData` (the return type of `mergeVocabulary`), the
// concepts for its callbacks, the `IdMap` types, the `detail::QueueWord`, and
// the individual stages of the merging pipeline (see below).
namespace ad_utility::vocabulary_merger {

// _______________________________________________________________
// Merge the partial vocabularies in the binary files
// `partialVocabularyWordsFilename(basename, idx)` for each `idx` in
// `[0, numPartialVocabularies)`. The mapping from the partial to the global
// IDs is written to `partialVocabularyIdMapFilename(basename, idx)`.
// Return the number of total Words merged and the lower and upper bound of
// language tagged predicates. Argument `comparator` gives the way to order
// strings (case-sensitive or not). The merged words are written to the
// `writer` (see `ParallelWordWriterBase` in `index/vocabulary/
// VocabularyTypes.h`), which is not finished by this function. Argument
// `blankNodeIriRegexes` is a (possibly empty) set of compiled regexes; IRIs
// that are fully matched by any of them are treated as blank nodes (see
// `TripleComponentWithIndex::isBlankNode`). The regexes are compiled by the
// caller (see `IndexImpl::setBlankNodeIriRegexes`).
//
// The partial vocabularies are merged by the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`) on the global thread pool:
// the words are split into ranges by the block index of the partial
// vocabulary files (see `index/vocabulary_merger/PartialVocabularyFile.h`),
// each range is merged by a chunk of its own, and the merged blocks arrive in
// the order of the vocabulary on the thread that calls this function. That
// thread does no per-word work at all:
//
// 1. It cuts the merged blocks into segments (see
//    `index/vocabulary_merger/Segment.h`), each of which is a task on the
//    thread pool: eliminate the duplicates, detect the blank nodes, assign the
//    words to the sub-vocabularies of the `writer` and give them their
//    segment-local IDs, collect the ID map entries, and destroy the merged
//    words.
// 2. It commits the finished segments in order (see
//    `index/vocabulary_merger/SegmentCommitter.h`): the global IDs are the
//    segment-local IDs plus the number of words in the previous segments, the
//    ID map entries go to the threads that write the partial ID maps (see
//    `index/vocabulary_merger/IdMapWriters.h`), and the words go to the blocks
//    of their sub-vocabulary, which are prepared (compressed) on the thread
//    pool and appended to the files by one thread per sub-vocabulary.
template <typename W>
auto mergeVocabulary(const std::string& basename, size_t numPartialVocabularies,
                     W comparator, ParallelWordWriterBase& writer,
                     ad_utility::MemorySize memoryToUse,
                     const ad_utility::RegexSet& blankNodeIriRegexes = {})
    -> CPP_ret(VocabularyMetaData)(requires WordComparator<W>);

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

// The triples that were mapped using a single partial vocabulary are stored in
// a file of their own (see `unsortedTriplesFilename`). The following two
// functions are the only writer and the only reader of that file format, so
// they always have to be changed together.

// For each of the IdTriples in `input`: map its Ids using the `map` and
// serialize the resulting batch of Id triples to the file `filename`, which is
// created and closed by this function. `input` is only passed by reference so
// that the caller can reuse its memory; its contents are unspecified
// afterwards. Counterpart of `readMappedIdsFromFile`.
void writeMappedIdsToFile(
    std::vector<std::array<Id, NumColumnsIndexBuilding>>& input,
    const HashMap<uint64_t, uint64_t>& map, const std::string& filename);

// Read back the Id triples that `writeMappedIdsToFile` has written to the file
// `filename`.
IdTableStatic<NumColumnsIndexBuilding> readMappedIdsFromFile(
    const std::string& filename);

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
