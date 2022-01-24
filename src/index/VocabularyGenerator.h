// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>
#pragma once

#include <string>
#include <stxxl/vector>
#include <utility>

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/HashMap.h"
#include "../util/MmapVector.h"
#include "./ConstantsIndexBuilding.h"
#include "./IndexBuilderTypes.h"
#include "Vocabulary.h"

using IdPairMMapVec = ad_utility::MmapVector<std::pair<Id, Id>>;
using IdPairMMapVecView = ad_utility::MmapVectorView<std::pair<Id, Id>>;
using std::string;

using TripleVec = stxxl::vector<array<Id, 3>>;

/**
 * Class for merging the partial vocabularies. The main function is still in the
 * `mergeVocabulary` function, but the parallel pipeline is easier when this is
 * encapsulated within a class.
 */
class VocabularyMerger {
 public:
  // If this is set, then we will only output the internal vocabulary.
  // This is useful for the prefix compression, where we don't need the
  // external part of the vocabulary and the mapping from local to global IDs.
  bool _noIdMapsAndIgnoreExternalVocab = false;
  // result of a call to mergeVocabulary
  struct VocMergeRes {
    size_t _numWordsTotal;   // that many distinct words were found (size of the
                             // vocabulary)
    Id _langPredLowerBound;  // inclusive lower bound (as Id within the
                             // vocabulary) for the added @en@rdfs:label style
                             // predicates
    Id _langPredUpperBound;  // exclusive upper bound ...
  };

  VocabularyMerger() = default;

  // _______________________________________________________________
  // merge the partial vocabularies in the  binary files
  // basename + PARTIAL_VOCAB_FILE_NAME + to_string(i)
  // where 0 <= i < numFiles
  // Directly Writes .vocabulary file at basename (no more need to pass
  // through Vocabulary class
  // Writes file "externalTextFile" which can be used to directly write external
  // Literals
  // Returns the number of total Words merged and via the parameters
  // the lower and upper bound of language tagged predicates
  // Argument comparator gives the way to order strings (case-sensitive or not)
  // This automatically resets the inner members after finishing, to leave the
  // external interface stateless
  template <typename Comp, typename InternalVocabularyAction>
  VocMergeRes mergeVocabulary(const std::string& basename, size_t numFiles,
                              Comp comparator,
                              InternalVocabularyAction& action);

 private:
  // helper struct used in the priority queue for merging.
  // represents tokens/words in a certain partial vocabulary
  struct QueueWord {
    QueueWord() = default;
    QueueWord(string&& v, size_t file, Id word)
        : _value(std::move(v)), _partialFileId(file), _partialWordId(word) {}
    string _value;          // the word
    size_t _partialFileId;  // from which partial vocabulary did this word come
    Id _partialWordId;  // which partial id did the word have in this partial
    // vocabulary
  };

  // write the queu words in the buffer to their corresponding idPairVecs.
  // Requires that all the QueueWords that are ever passed are ordered
  // alphabetically (Also across multiple calls)
  template <typename InternalVocabularyAction>
  void writeQueueWordsToIdVec(
      const std::vector<QueueWord>& buffer,
      InternalVocabularyAction& internalVocabularyAction);

  // close all associated files and MmapVectors and reset all internal variables
  void clear() {
    _totalWritten = 0;
    _lastWritten = "";
    _outfileExternal = std::ofstream();
    _idVecs.clear();
    _firstLangPredSeen = false;
    _langPredLowerBound = 0;
    _langPredUpperBound = 0;
  }

  // private data members

  // the number of words we have written. This also is the global Id of the next
  // word we see, unless it is is equal to the previous word
  size_t _totalWritten = 0;
  // keep track of the last seen word to correctly handle duplicates
  std::string _lastWritten;
  std::ofstream _outfileExternal;
  // we will store pairs of <partialId, globalId>
  std::vector<IdPairMMapVec> _idVecs;
  bool _firstLangPredSeen = false;
  Id _langPredLowerBound = 0;
  Id _langPredUpperBound = 0;

  const size_t _bufferSize = 10000000;

  // inner helper function for the parallel pipeline. perform the actual write
  // to the IdPairVecs. Format of argument is <vecToWriteTo<internalId,
  // globalId>>
  void doActualWrite(
      const std::vector<std::pair<size_t, std::pair<size_t, size_t>>>& buffer);
};

// ______________
// TODO<joka921> is this even used
// anymore?___________________________________________________________________________
template <class Comp>
void writePartialIdMapToBinaryFileForMerging(
    std::shared_ptr<const ItemMapArray> map, const string& fileName, Comp comp,
    bool doParallelSort);

// _________________________________________________________________________________________
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
ad_utility::HashMap<Id, Id> createInternalMapping(ItemVec* els);

/**
 * @brief for each of the IdTriples in <input>: map the three Ids using the
 * <map> and write the resulting Id triple to <*writePtr>
 */
void writeMappedIdsToExtVec(const TripleVec& input,
                            const ad_utility::HashMap<Id, Id>& map,
                            TripleVec::bufwriter_type* writePtr);

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
 * @brief Take an Array of HashMaps of Strings to Ids and insert all the
 * elements from all the hashMaps into a single vector No reordering or
 * deduplication is done, so result.size() == summed size of all the hash maps
 */
ItemVec vocabMapsToVector(std::unique_ptr<ItemMapArray> map);

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

#include "VocabularyGeneratorImpl.h"
