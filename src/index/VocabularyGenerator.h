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

class VocabularyMerger {
 public:
  struct VocMergeRes {
    size_t _numWordsTotal;
    Id _langPredLowerBound;
    Id _langPredUpperBound;
  };
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
  VocMergeRes mergeVocabulary(const std::string& basename, size_t numFiles,
                              StringSortComparator comp);

 private:
  // helper struct used in the priority queue for merging.
  // represents tokens/words in a certain partial vocabulary
  struct QueueWord {
    QueueWord() = default;
    QueueWord(const string& v, size_t file, Id word)
        : _value(v), _partialFileId(file), _partialWordId(word) {}
    string _value;          // the word
    size_t _partialFileId;  // from which partial vocabulary did this word come
    Id _partialWordId;  // which partial id did the word have in this partial
    // vocabulary
  };

  void writeQueueWordsToIdVec(const std::vector<QueueWord>& buffer);
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

  void doActualWrite(
      const std::vector<std::pair<size_t, std::pair<size_t, size_t>>>& buffer);
};

// _________________________________________________________________________________________
void writePartialIdMapToBinaryFileForMerging(
    std::shared_ptr<const ad_utility::HashMap<string, Id>> map,
    const string& fileName, StringSortComparator comp, bool doParallelSort);

// _________________________________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const string& mmapFilename);
