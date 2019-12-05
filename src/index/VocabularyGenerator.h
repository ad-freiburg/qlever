// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>
#pragma once

#include <string>
#include <utility>

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/HashMap.h"
#include "../util/MmapVector.h"
#include "Vocabulary.h"

using IdPairMMapVec = ad_utility::MmapVector<std::pair<Id, Id>>;
using IdPairMMapVecView = ad_utility::MmapVectorView<std::pair<Id, Id>>;
using std::string;

/**
 * class for merging the partial vocabularies. The main function is still in the
 * mergeVocabulary function but the parallel pipeline is easier when this is
 * encapsulated within a class.
 */
class VocabularyMerger {
 public:
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
  // Argument comp gives the way to order strings (case-sensitive or not)
  // This automatically resets the inner members after finishing, to leave the
  // external interface stateless
  VocMergeRes mergeVocabulary(const std::string& basename, size_t numFiles,
                              StringSortComparator comp);

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
  void writeQueueWordsToIdVec(const std::vector<QueueWord>& buffer);

  // close all associated files and MmapVectors and reset all internal variables
  void clear() {
    _totalWritten = 0;
    _lastWritten = "";
    _outfile = std::ofstream();
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
  std::ofstream _outfile;
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

// _________________________________________________________________________________________
void writePartialIdMapToBinaryFileForMerging(
    std::shared_ptr<const ad_utility::HashMap<string, std::pair<Id, std::string>>> map,
    const string& fileName, StringSortComparator comp, std::locale loc, bool doParallelSort);

// _________________________________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const string& mmapFilename);
