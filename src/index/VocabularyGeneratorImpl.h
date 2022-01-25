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

// ___________________________________________________________________
template <typename Comparator, typename InternalVocabularyAction>
VocabularyMerger::VocMergeRes VocabularyMerger::mergeVocabulary(
    const std::string& basename, size_t numFiles, Comparator comparator,
    InternalVocabularyAction& internalVocabularyAction) {
  // we sort alphabetically by the token according to the comparator that was
  // given to us

  auto queueCompare = [&comparator](const QueueWord& p1, const QueueWord& p2) {
    // if p1 is smaller (alphabetically)
    // _comp will return false if called like this
    // and the priority queue will thus emit p1 first
    return comparator(p2._value, p1._value);
  };

  std::vector<std::ifstream> infiles;

  if (!_noIdMapsAndIgnoreExternalVocab) {
    _outfileExternal.open(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
    AD_CHECK(_outfileExternal.is_open());
  }
  std::vector<bool> endOfFile(numFiles, false);

  // Priority queue for the k-way merge
  std::priority_queue<QueueWord, std::vector<QueueWord>, decltype(queueCompare)>
      queue(queueCompare);

  // open and prepare all infiles and mmap output vectors
  for (size_t i = 0; i < numFiles; i++) {
    infiles.emplace_back(basename + PARTIAL_VOCAB_FILE_NAME +
                         std::to_string(i));
    if (!_noIdMapsAndIgnoreExternalVocab) {
      _idVecs.emplace_back(0, basename + PARTIAL_MMAP_IDS + std::to_string(i));
    }
    AD_CHECK(infiles.back().is_open());

    // read the first entry of the vocabulary and add it to the queue
    endOfFile[i] = true;

    size_t len;
    if (infiles[i].read((char*)&len, sizeof(len))) {
      std::string word(len, '\0');
      infiles[i].read(&(word[0]), len);
      Id id;
      infiles[i].read((char*)&id, sizeof(id));
      queue.push(QueueWord(std::move(word), i, id));
      endOfFile[i] = false;
    }
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
      auto writeTask = [this, buf = std::move(sortedBuffer),
                        &internalVocabularyAction]() {
        this->writeQueueWordsToIdVec(buf, internalVocabularyAction);
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
    if (endOfFile[i]) {
      continue;
    }  // file is exhausted, nothing to add

    endOfFile[i] = true;
    size_t len;
    if (infiles[i].read((char*)&len, sizeof(len))) {
      std::string word(len, '\0');
      infiles[i].read(&(word[0]), len);
      Id id;
      infiles[i].read((char*)&id, sizeof(id));
      queue.push(QueueWord(std::move(word), i, id));
      endOfFile[i] = false;
    }
  }

  // wait for the active write tasks to finish
  if (writeFuture.valid()) {
    writeFuture.get();
  }

  // Handle remaining words in the buffer
  if (!sortedBuffer.empty()) {
    writeQueueWordsToIdVec(sortedBuffer, internalVocabularyAction);
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
template <typename InternalVocabularyAction>
void VocabularyMerger::writeQueueWordsToIdVec(
    const std::vector<QueueWord>& buffer,
    InternalVocabularyAction& internalVocabularyAction) {
  LOG(TIMING) << "Start writing a batch of merged words\n";

  // smaller grained buffer for the actual inner write
  auto bufSize = _bufferSize / 5;
  std::future<void> writeFut;
  std::vector<std::pair<size_t, std::pair<size_t, size_t>>> writeBuf;
  writeBuf.reserve(bufSize);
  // avoid duplicates
  for (auto& top : buffer) {
    if (top._value != _lastWritten) {
      _lastWritten = top._value;

      // TODO<optimization> If we aim to further speed this up, we could
      // order all the write requests to _outfile _externalOutfile and all the
      // idVecs to have a more useful external access pattern.

      // write the new word to the vocabulary
      if (_lastWritten < EXTERNALIZED_LITERALS_PREFIX) {
        internalVocabularyAction(_lastWritten);
      } else {
        // we have to strip the externalization character again
        auto& c = _lastWritten[0];
        // Keep a copy of the first character to later restore it.
        auto originalFirstChar = c;
        switch (c) {
          case EXTERNALIZED_LITERALS_PREFIX_CHAR:
            c = '"';
            break;
          case EXTERNALIZED_ENTITIES_PREFIX_CHAR:
            c = '<';
            break;
          default:
            LOG(ERROR) << "Illegal Externalization character met in vocabulary "
                          "merging. This "
                          "should never happen\n";
            AD_CHECK(false)
        }
        _outfileExternal << RdfEscaping::escapeNewlinesAndBackslashes(
                                _lastWritten)
                         << '\n';
        // restore the original value, so that the check _lastWritten ==
        // top.value above works again.
        c = originalFirstChar;
      }

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
      if (_totalWritten % 100'000'000 == 0) {
        LOG(INFO) << "Words merged: " << _totalWritten << std::endl;
      }
    } else {
      // this is a duplicate which already occured in another partial vocabulary
      // in the last step.
      // We have already incremented _totalWritten for the next round, hence the
      // -1 here.
      writeBuf.emplace_back(top._partialFileId,
                            std::pair{top._partialWordId, _totalWritten - 1});
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

  LOG(DEBUG) << "Finished writing batch of merged words" << std::endl;
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
  LOG(DEBUG) << "Writing partial vocabulary to: " << fileName << "\n";
  std::ofstream out(fileName.c_str(),
                    std::ios_base::out | std::ios_base::binary);
  AD_CHECK(out.is_open());
  for (const auto& el : els) {
    std::string_view word = el.first;
    size_t len = word.size();
    out.write((char*)&len, sizeof(len));
    out.write(word.data(), len);
    Id id = el.second.m_id;
    out.write((char*)&id, sizeof(id));
  }
  out.close();
  LOG(DEBUG) << "Done writing partial vocabulary\n";
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
