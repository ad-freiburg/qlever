// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#pragma once

#include <fstream>
#include <future>
#include <iostream>
#include <queue>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../parser/RdfEscaping.h"
#include "../util/Conversions.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./ConstantsIndexBuilding.h"
#include "./Vocabulary.h"
#include "./VocabularyGenerator.h"
#include "../util/Serializer/FileSerializer.h"

// ___________________________________________________________________
template <class Comp>
VocabularyMerger::VocMergeRes VocabularyMerger::mergeVocabulary(
    const std::string& basename, size_t numFiles, Comp comp) {
  // we sort alphabetically by the token according to the comparator that was
  // given to us

  auto queueCompare = [&comp](const QueueWord& p1, const QueueWord& p2) {
    // if p1 is smaller (alphabetically)
    // _comp will return false if called like this
    // and the priority queue will thus emit p1 first
    if (p1._value == p2._value) {
      return p1._isExternal && !p2._isExternal;
    }
    return comp(p2._value, p1._value);
  };

  std::vector<ad_utility::serialization::FileReadSerializer> infiles;
  std::vector<uint64_t> numWordsLeftInPartialVocabulary;


  _outfile.open(basename + ".vocabulary");
  AD_CHECK(_outfile.is_open());
  if (!_noIdMapsAndIgnoreExternalVocab) {
    _outfileExternal.open(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
    AD_CHECK(_outfileExternal.is_open());
  }

  // Priority queue for the k-way merge
  std::priority_queue<QueueWord, std::vector<QueueWord>, decltype(queueCompare)>
      queue(queueCompare);


  auto pushWordFromPartialVocabularyToQueue = [&](size_t i) {
    if (numWordsLeftInPartialVocabulary[i] > 0) {
      std::string word;
      Id id;
      bool isExternal;
      infiles[i] >> word;
      infiles[i] >> id;
      infiles[i] >> isExternal;
      queue.push(QueueWord(std::move(word), i, id, isExternal));
      numWordsLeftInPartialVocabulary[i]--;
    }

  };
  // open and prepare all infiles and mmap output vectors
  for (size_t i = 0; i < numFiles; i++) {
    infiles.emplace_back(basename + PARTIAL_VOCAB_FILE_NAME +
                         std::to_string(i));
    numWordsLeftInPartialVocabulary.emplace_back();
    // read the number of words in the partial vocabulary;
    infiles.back() >> numWordsLeftInPartialVocabulary.back();
    if (!_noIdMapsAndIgnoreExternalVocab) {
      _idVecs.emplace_back(0, basename + PARTIAL_MMAP_IDS + std::to_string(i));
    }

    pushWordFromPartialVocabularyToQueue(i);
  }

  std::vector<QueueWord> sortedBuffer;
  sortedBuffer.reserve(_bufferSize);

  std::future<void> writeFuture;

  // start k-way merge
  while (!queue.empty()) {
    // for the prefix compression vocabulary, we don't need the external
    // vocabulary
    if (_noIdMapsAndIgnoreExternalVocab &&
        queue.top()._value >= EXTERNALIZED_LITERALS_PREFIX) {
      break;
    }

    // accumulated the globally ordered queue words in a buffer.
    sortedBuffer.push_back(std::move(queue.top()));
    queue.pop();
    auto i = sortedBuffer.back()._partialFileId;

    if (sortedBuffer.size() >= _bufferSize) {
      // asynchronously write the next batch of sorted
      // queue words
      auto writeTask = [this, buf = std::move(sortedBuffer)]() {
        this->writeQueueWordsToIdVec(buf);
      };
      sortedBuffer.clear();
      sortedBuffer.reserve(_bufferSize);
      // wait for the last batch

      LOG(TIMING) << "A new batch of words is ready" << std::endl;
      if (writeFuture.valid()) {
        writeFuture.get();
      }
      writeFuture = std::async(writeTask);
      // we have moved away our buffer, start over
    }

    // add next word from the same infile to the priority queue
    pushWordFromPartialVocabularyToQueue(i);
  }

  // wait for the active write tasks to finish
  if (writeFuture.valid()) {
    writeFuture.get();
  }

  // Handle remaining words in the buffer
  if (!sortedBuffer.empty()) {
    writeQueueWordsToIdVec(sortedBuffer);
  }
  VocMergeRes result;
  result._numWordsTotal = _totalWritten;
  result._langPredLowerBound = _langPredLowerBound;
  result._langPredUpperBound = _langPredUpperBound;

  // completely reset all the inner state
  clear();
  return result;
}

// ________________________________________________________________________________
void VocabularyMerger::writeQueueWordsToIdVec(
    const std::vector<QueueWord>& buffer) {
  LOG(TIMING) << "Start writing a batch of merged words\n";

  // smaller grained buffer for the actual inner write
  auto bufSize = _bufferSize / 5;
  std::future<void> writeFut;
  std::vector<std::pair<size_t, std::pair<size_t, size_t>>> writeBuf;
  writeBuf.reserve(bufSize);
  // avoid duplicates
  for (auto& top : buffer) {
    if (top._value != _lastWritten._lastWrittenWord) {
      _lastWritten._lastWrittenWord = top._value;
      _lastWritten._wasExternalized = top._isExternal;

      // TODO<optimization> If we aim to further speed this up, we could
      // order all the write requests to _outfile _externalOutfile and all the
      // idVecs to have a more useful external access pattern.

      // write the new word to the vocabulary
      if (!_lastWritten._wasExternalized) {
        _outfile << RdfEscaping::escapeNewlinesAndBackslashes(_lastWritten._lastWrittenWord)
                 << '\n';
      } else {
        _outfileExternal << RdfEscaping::escapeNewlinesAndBackslashes(
                                _lastWritten._lastWrittenWord)
                         << '\n';
      }

      // TODO<joka921> Get the correct Ids once the other PR is merged.
      // write id to corresponding vec
      writeBuf.emplace_back(top._partialFileId,
                            std::make_pair(top._partialWordId, _totalWritten));

      if (top._value.size() > 0 && top._value[0] == '@') {
        if (!_firstLangPredSeen) {
          // inclusive
          _langPredLowerBound = _totalWritten;
          _firstLangPredSeen = true;
        }
        // exclusive
        _langPredUpperBound = _totalWritten + 1;
      }
      _totalWritten++;
      if (_totalWritten % _bufferSize == 0) {
        LOG(INFO) << "Merged " << _totalWritten << "Words" << std::endl;
      }
    } else {
      // this is a duplicate which already occured in another partial vocabulary
      // in the last step.
      // we already have increased total written, so for the duplicate
      // we have to subtract one again
      size_t minusOne = _totalWritten - 1;
      writeBuf.emplace_back(top._partialFileId,
                            std::make_pair(top._partialWordId, minusOne));
    }

    if (writeBuf.size() >= bufSize) {
      auto task = [this, buf = std::move(writeBuf)]() {
        this->doActualWrite(buf);
      };
      if (writeFut.valid()) {
        writeFut.get();
      }
      writeFut = std::async(task);
      writeBuf.clear();
      writeBuf.reserve(bufSize);
    }
  }

  if (writeFut.valid()) {
    writeFut.get();
  }

  if (!writeBuf.empty()) {
    doActualWrite(writeBuf);
  }

  LOG(INFO) << "Finished writing batch of merged words\n";
}

// ____________________________________________________________________________________________________________
void VocabularyMerger::doActualWrite(
    const std::vector<std::pair<size_t, std::pair<size_t, size_t>>>& buffer) {
  if (_noIdMapsAndIgnoreExternalVocab) {
    return;
  }
  for (const auto& [id, value] : buffer) {
    _idVecs[id].push_back(value);
  }
}

// ____________________________________________________________________________________________________________
ad_utility::HashMap<Id, Id> createInternalMapping(ItemVec* elsPtr) {
  auto& els = *elsPtr;
  ad_utility::HashMap<Id, Id> res;
  bool first = true;
  std::string lastWord;
  size_t nextWordId = 0;
  for (auto& el : els) {
    if (!first && lastWord != el.first) {
      nextWordId++;
      lastWord = el.first;
    }
    AD_CHECK(!res.count(el.second.m_id));
    res[el.second.m_id] = nextWordId;
    el.second.m_id = nextWordId;
    first = false;
  }
  return res;
}

// ________________________________________________________________________________________________________
void writeMappedIdsToExtVec(const TripleVec& input,
                            const ad_utility::HashMap<Id, Id>& map,
                            TripleVec::bufwriter_type* writePtr) {
  auto& writer = *writePtr;
  for (const auto& curTriple : input) {
    // for all triple elements find their mapping from partial to global ids
    ad_utility::HashMap<Id, Id>::const_iterator iterators[3];
    for (size_t k = 0; k < 3; ++k) {
      iterators[k] = map.find(curTriple[k]);
      if (iterators[k] == map.end()) {
        LOG(INFO) << "not found in partial local Vocab: " << curTriple[k]
                  << '\n';
        AD_CHECK(false);
      }
    }

    // update the Element
    writer << array<Id, 3>{
        {iterators[0]->second, iterators[1]->second, iterators[2]->second}};
  }
}

// _________________________________________________________________________________________________________
void writePartialVocabularyToFile(const ItemVec& els, const string& fileName) {
  LOG(INFO) << "Writing vocabulary to binary file " << fileName << "\n";
  ad_utility::serialization::FileWriteSerializer serializer{fileName};
  uint64_t size = els.size();  // really make sure that this has 64bits;
  serializer << size;
  for (const auto& [word, idAndSplitVal] : els) {
    // When merging the vocabulary, we need the actual word, the (internal) id
    // we have given this word, and the information, whether this word belongs
    // into the internal or external vocabulary.
    serializer << word;
    serializer << idAndSplitVal.m_id;
    serializer << idAndSplitVal.m_splitVal.isExternal;
  }
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// ______________________________________________________________________________________________
template <class Pred>
void writePartialIdMapToBinaryFileForMerging(
    std::shared_ptr<const ItemMapArray> map, const string& fileName, Pred comp,
    const bool doParallelSort) {
  LOG(INFO) << "Creating partial vocabulary from set ...\n";
  ItemVec els;
  size_t totalEls = std::accumulate(
      map->begin(), map->end(), 0,
      [](const auto& x, const auto& y) { return x + y.size(); });
  els.reserve(totalEls);
  for (const auto& singleMap : *map) {
    els.insert(end(els), begin(singleMap), end(singleMap));
  }
  LOG(INFO) << "... sorting ...\n";

  sortVocabVector(&els, comp, doParallelSort);

  LOG(INFO) << "Done creating vocabulary.\n";

  writePartialVocabularyToFile(els, fileName);
}

// __________________________________________________________________________________________________
ItemVec vocabMapsToVector(std::unique_ptr<ItemMapArray> map) {
  ItemVec els;
  size_t totalEls = std::accumulate(
      map->begin(), map->end(), 0,
      [](const auto& x, const auto& y) { return x + y.size(); });
  els.reserve(totalEls);
  for (auto& singleMap : *map) {
    els.insert(end(els), std::make_move_iterator(begin(singleMap)),
               std::make_move_iterator(end(singleMap)));
  }
  return els;
}

// _______________________________________________________________________________________________________________________
template <class StringSortComparator>
void sortVocabVector(ItemVec* vecPtr, StringSortComparator comp,
                     const bool doParallelSort) {
  auto& els = *vecPtr;
  if constexpr (USE_PARALLEL_SORT) {
    if (doParallelSort) {
      ad_utility::parallel_sort(begin(els), end(els), comp,
                                ad_utility::parallel_tag(NUM_SORT_THREADS));
    } else {
      std::sort(begin(els), end(els), comp);
    }
  } else {
    std::sort(begin(els), end(els), comp);
    (void)doParallelSort;  // avoid compiler warning for unused value.
  }
}

// _____________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const string& mmapFilename) {
  ad_utility::HashMap<Id, Id> res;
  IdPairMMapVecView vec(mmapFilename);
  for (const auto& [partialId, globalId] : vec) {
    res[partialId] = globalId;
  }
  return res;
}
