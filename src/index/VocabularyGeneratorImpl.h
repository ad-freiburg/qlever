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
#include "../util/Serializer/FileSerializer.h"
#include "../util/Serializer/SerializeString.h"
#include "./ConstantsIndexBuilding.h"
#include "./Vocabulary.h"
#include "./VocabularyGenerator.h"

// ___________________________________________________________________
template <typename Comparator, typename InternalVocabularyAction>
VocabularyMerger::VocMergeRes VocabularyMerger::mergeVocabulary(
    const std::string& basename, size_t numFiles, Comparator comparator,
    InternalVocabularyAction& internalVocabularyAction) {
  // Return true iff p1 >= p2 according to the lexicographic order of the IRI
  // or literal. All internal IRIs or literals come before all external ones.
  // TODO<joka921> Change this as soon as we have Interleaved Ids via the
  // MilestoneIdManager
  // TODO<joka921> Split up the actual comparison of QueueWords and the
  // "inversion" because of `std::priority_queue`
  auto queueCompare = [&comparator](const QueueWord& p1, const QueueWord& p2) {
    if (p1.isExternal() != p2.isExternal()) {
      return p1.isExternal();
    }
    return comparator(p2.iriOrLiteral(), p1.iriOrLiteral());
  };

  std::vector<ad_utility::serialization::FileReadSerializer> infiles;
  std::vector<uint64_t> numWordsLeftInPartialVocabulary;

  if (!_noIdMapsAndIgnoreExternalVocab) {
    _outfileExternal.open(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
    AD_CHECK(_outfileExternal.is_open());
  }
  std::vector<bool> endOfFile(numFiles, false);

  // Priority queue for the k-way merge
  std::priority_queue<QueueWord, std::vector<QueueWord>, decltype(queueCompare)>
      queue(queueCompare);

  auto pushWordFromPartialVocabularyToQueue = [&](size_t i) {
    if (numWordsLeftInPartialVocabulary[i] > 0) {
      TripleComponentWithId tripleComponent;
      infiles[i] >> tripleComponent;
      queue.push(QueueWord(std::move(tripleComponent), i));
      numWordsLeftInPartialVocabulary[i]--;
    }
  };

  // Open and prepare all infiles and mmap output vectors.
  infiles.reserve(numFiles);
  for (size_t i = 0; i < numFiles; i++) {
    infiles.emplace_back(basename + PARTIAL_VOCAB_FILE_NAME +
                         std::to_string(i));
    numWordsLeftInPartialVocabulary.emplace_back();
    // Read the number of words in the partial vocabulary.
    infiles.back() >> numWordsLeftInPartialVocabulary.back();
    if (!_noIdMapsAndIgnoreExternalVocab) {
      _idVecs.emplace_back(0, basename + PARTIAL_MMAP_IDS + std::to_string(i));
    }
    // Read the first entry of the vocabulary and add it to the queue.
    pushWordFromPartialVocabularyToQueue(i);
  }

  std::vector<QueueWord> sortedBuffer;
  sortedBuffer.reserve(_bufferSize);

  std::future<void> writeFuture;

  // start k-way merge
  while (!queue.empty()) {
    // for the prefix compression vocabulary, we don't need the external
    // vocabulary
    // TODO<joka921> Don't include external literals at all in this vocabulary.
    if (_noIdMapsAndIgnoreExternalVocab && queue.top().isExternal()) {
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
    pushWordFromPartialVocabularyToQueue(i);
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
    if (!_lastTripleComponent.has_value() ||
        top.iriOrLiteral() != _lastTripleComponent.value().iriOrLiteral()) {
      // TODO<joka921> Once we have interleaved IDs using the MilestoneIdManager
      // we have to compute the correct Ids here.
      _lastTripleComponent = TripleComponentWithId{
          top.iriOrLiteral(), top.isExternal(), _totalWritten};

      // TODO<optimization> If we aim to further speed this up, we could
      // order all the write requests to _outfile _externalOutfile and all the
      // idVecs to have a more useful external access pattern.

      // write the new word to the vocabulary
      if (!_lastTripleComponent.value().isExternal()) {
        internalVocabularyAction(_lastTripleComponent.value().iriOrLiteral());
      } else {
        _outfileExternal << RdfEscaping::escapeNewlinesAndBackslashes(
                                _lastTripleComponent.value().iriOrLiteral())
                         << '\n';
      }

      if (top.iriOrLiteral().starts_with('@')) {
        if (!_firstLangPredSeen) {
          // inclusive
          _langPredLowerBound = _lastTripleComponent.value()._id;
          _firstLangPredSeen = true;
        }
        // exclusive
        _langPredUpperBound = _lastTripleComponent.value()._id + 1;
      }
      _totalWritten++;
      if (_totalWritten % 100'000'000 == 0) {
        LOG(INFO) << "Words merged: " << _totalWritten << std::endl;
      }
    }
    // Write pair of local and global ID to buffer.
    writeBuf.emplace_back(
        top._partialFileId,
        std::pair{top.id(), _lastTripleComponent.value()._id});

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
void writeMappedIdsToExtVec(const auto& input,
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
    writer << std::array<Id, 3>{
        {iterators[0]->second, iterators[1]->second, iterators[2]->second}};
  }
}

// _________________________________________________________________________________________________________
void writePartialVocabularyToFile(const ItemVec& els, const string& fileName) {
  LOG(DEBUG) << "Writing partial vocabulary to: " << fileName << "\n";
  ad_utility::serialization::FileWriteSerializer serializer{fileName};
  uint64_t size = els.size();  // really make sure that this has 64bits;
  serializer << size;
  for (const auto& [word, idAndSplitVal] : els) {
    // When merging the vocabulary, we need the actual word, the (internal) id
    // we have assigned to this word, and the information, whether this word
    // belongs to the internal or external vocabulary.
    const auto& [id, splitVal] = idAndSplitVal;
    TripleComponentWithId entry{word, splitVal.isExternalized, id};
    serializer << entry;
  }
  serializer.close();
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
