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
#include "./ConstantsIndexCreation.h"
#include "./Vocabulary.h"
#include "./VocabularyGenerator.h"
#include "../util/Serializer/FileSerializer.h"
#include "../util/Serializer/SerializePair.h"
#include "../util/Serializer/SerializeString.h"

// ___________________________________________________________________
template <size_t I, class Comp>
void VocabularyMerger::mergeVocabularySingleDatatype(VocMergeRes& result, const std::string& basename, size_t numFiles, Comp comp){

  // we sort alphabetically by the token according to the comparator that was
  // given to us
  using ValueType = std::decay_t<decltype(std::get<I>(ItemVec{})[0].first)>;
  static constexpr bool IsString = std::is_same_v<ValueType, std::string>;

  using QWord = QueueWord<ValueType>;

  auto queueCompare = [&comp](const QWord & p1, const QWord& p2) {
    // if p1 is smaller (alphabetically)
    // _comp will return false if called like this
    // and the priority queue will thus emit p1 first
    return comp(p2._value, p1._value);
  };

  std::vector<ad_utility::serialization::FileReadSerializer> infiles;

  _outfile = ad_utility::serialization::FileWriteSerializer{basename + ".vocabulary." + std::to_string(I)};

  if constexpr (IsString) {
    _outfileExternal.open(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
    AD_CHECK(_outfileExternal.is_open());
  }
  std::vector<bool> endOfFile(numFiles, false);

  // Priority queue for the k-way merge
  std::priority_queue<QWord, std::vector<QWord>, decltype(queueCompare)> queue(
      queueCompare);

  // open and prepare all infiles and mmap output vectors
  for (size_t i = 0; i < numFiles; i++) {
    infiles.emplace_back(basename + PARTIAL_VOCAB_FILE_NAME + std::to_string(i) + "." + std::to_string(I));

    // read the first entry of the vocabulary and add it to the queue
    endOfFile[i] = true;

    if (!infiles[i].isExhausted()) {
      try {
      std::pair<ValueType, Id> wordAndId;
      infiles[i] & wordAndId;
      queue.push(QueueWord(std::move(wordAndId.first), i, wordAndId.second));
      endOfFile[i] = false;
    } catch (const ad_utility::serialization::SerializationException&) {

    }
  }
  }

  std::vector<QWord> sortedBuffer;
  sortedBuffer.reserve(_bufferSize);

  std::future<void> writeFuture;

  // start k-way merge
  while (!queue.empty()) {
    // accumulated the globally ordered queue words in a buffer.
    sortedBuffer.push_back(std::move(queue.top()));
    queue.pop();
    auto i = sortedBuffer.back()._partialFileId;
    auto lastWritten = sortedBuffer.back()._value;

    if (sortedBuffer.size() >= _bufferSize) {
      // asynchronously write the next batch of sorted
      // queue words
      auto writeTask = [this, buf = std::move(sortedBuffer), comp]() {
        this->writeQueueWordsToIdVec<I>(buf, comp);
      };
      sortedBuffer.clear();
      sortedBuffer.reserve(_bufferSize);
      // wait for the last batch

      if (writeFuture.valid()) {
        LOG(TRACE) << "Waiting for the asynchronous write to finish\n";
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
    if (!infiles[i].isExhausted()) {
      try {
      std::pair<ValueType, Id> wordAndId;
      infiles[i] & wordAndId;
      if (comp(wordAndId.first, lastWritten)) {
          LOG(ERROR) << "Wrong order!\n";
      }
      queue.push(QueueWord(std::move(wordAndId.first), i, wordAndId.second));
      endOfFile[i] = false;
      } catch (const ad_utility::serialization::SerializationException&) {

      }
    }
  }

  // wait for the active write tasks to finish
  if (writeFuture.valid()) {
    writeFuture.get();
  }

  // Handle remaining words in the buffer
  if (!sortedBuffer.empty()) {
    writeQueueWordsToIdVec<I>(sortedBuffer, comp);
  }
  AD_CHECK(_actualCallsToWrite == _totalWritten);
  result._numWordsTotal = _totalWritten;
  result._langPredLowerBound = _langPredLowerBound;
  result._langPredUpperBound = _langPredUpperBound;

  _outfile->close();
}

template<size_t I>
void VocabularyMerger::mergeVocabularyHelper(VocMergeRes& result, const std::string& basename,
                                                                size_t numFiles) {
  if constexpr (I < std::tuple_size_v<AllVocabTypesTuple>) {
    mergeVocabularySingleDatatype<I>(result, basename, numFiles, std::less<>{});
    mergeVocabularyHelper<I+1>(result, basename, numFiles);
  }
}

// ___________________________________________________________________
template <class Comp>
VocabularyMerger::VocMergeRes VocabularyMerger::mergeVocabulary(const std::string& basename,
                                                                size_t numFiles, Comp comp) {
  for (size_t i = 0; i < numFiles; i++) {
    _idVecs.emplace_back(0, basename + PARTIAL_MMAP_IDS + std::to_string(i));
  }

    // read the first entry of the vocabula
  VocMergeRes result;
  mergeVocabularySingleDatatype<0>(result, basename, numFiles, comp);
  mergeVocabularyHelper<1>(result, basename, numFiles);

  // completely reset all the inner state
  clear();
  return result;
}

// ________________________________________________________________________________
template<size_t I, typename T, typename Comp>
void VocabularyMerger::writeQueueWordsToIdVec(const std::vector<QueueWord<T>>& buffer, Comp comp) {
  LOG(TRACE) << "Start writing a batch of merged words\n";

  // smaller grained buffer for the actual inner write
  auto bufSize = _bufferSize / 5;
  std::future<void> writeFut;
  std::vector<std::pair<size_t, std::pair<size_t, size_t>>> writeBuf;
  writeBuf.reserve(bufSize);
  auto& lastWritten = std::get<I>(_lastWritten);
  // avoid duplicates
  for (auto& top : buffer) {
    if (!_isFirstWritten[I] || top._value != lastWritten) {
      if (comp(top._value, lastWritten)) {
        LOG(ERROR) << "Wrong merge order?!";
      }
      lastWritten = top._value;
      _isFirstWritten[I] = true;

      // TODO<optimization> If we aim to further speed this up, we could
      // order all the write requests to _outfile _externalOutfile and all the
      // idVecs to have a more useful external access pattern.

      // write the new word to the vocabulary
      if constexpr (std::is_same_v<T, std::string>) {
        if (lastWritten < EXTERNALIZED_LITERALS_PREFIX) {
          if (lastWritten.starts_with("<Zuleikha")) {
            LOG(INFO) << "Zuleikha encountered\n";
          }
          (*_outfile) & lastWritten;
          _actualCallsToWrite++;
} else {
          // we have to strip the externalization character again
          auto& c = lastWritten[0];
          switch (c) {
            case EXTERNALIZED_LITERALS_PREFIX_CHAR:
              c = '"';
              break;
            case EXTERNALIZED_ENTITIES_PREFIX_CHAR:
              c = '<';
              break;
            default:
              LOG(ERROR)
                  << "Illegal Externalization character met in vocabulary "
                     "merging. This "
                     "should never happen\n";
              AD_CHECK(false)
          }
          _outfileExternal << RdfEscaping::escapeNewlinesAndBackslashes(
                                  lastWritten)
                           << '\n';
        }
      } else {
       (*_outfile)& lastWritten;
       _actualCallsToWrite++;
  }

      // write id to corresponding vec
      writeBuf.emplace_back(top._partialFileId, std::make_pair(top._partialWordId, _totalWritten));

      if constexpr (std::is_same_v<std::string, T>) {
        if (top._value.size() > 0 && top._value[0] == '@') {
          if (!_firstLangPredSeen) {
            // inclusive
            _langPredLowerBound = _totalWritten;
            _firstLangPredSeen = true;
          }
          // exclusive
          _langPredUpperBound = _totalWritten + 1;
        }
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
      writeBuf.emplace_back(top._partialFileId, std::make_pair(top._partialWordId, minusOne));
    }

    if (writeBuf.size() >= bufSize) {
      auto task = [this, buf = std::move(writeBuf)]() { this->doActualWrite(buf); };
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
  for (const auto& [id, value] : buffer) {
    _idVecs[id].push_back(value);
  }
}

template<size_t I>
void createInternalMappingImpl(ad_utility::HashMap<Id, Id>* map, Id nextWordId, ItemVec* vectors) {
  if constexpr (I < std::tuple_size_v<ItemVec>) {
    auto& els = std::get<I>(*vectors);
    auto& res = *map;
    bool first = true;
    if (!els.empty()) {
      auto lastWord = els[0].first;
      for (auto& el : els) {
        if (!first && lastWord != el.first) {
          nextWordId++;
          lastWord = el.first;
        }
        AD_CHECK(!res.count(el.second));
        res[el.second] = nextWordId;
        el.second = nextWordId;
        first = false;
      }
    }
    createInternalMappingImpl<I + 1>(map, nextWordId + 1, vectors);
  }
}

// ____________________________________________________________________________________________________________
ad_utility::HashMap<Id, Id> createInternalMapping(ItemVec* elsPtr) {
  auto& els = std::get<0>(*elsPtr);
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
  createInternalMappingImpl<1>(&res, nextWordId + 1, elsPtr);
  return res;
}

// ________________________________________________________________________________________________________
void writeMappedIdsToExtVec(const TripleVec& input, const ad_utility::HashMap<Id, Id>& map,
                            TripleVec::bufwriter_type* writePtr) {
  auto& writer = *writePtr;
  for (const auto& curTriple : input) {
    // for all triple elements find their mapping from partial to global ids
    ad_utility::HashMap<Id, Id>::const_iterator iterators[3];
    for (size_t k = 0; k < 3; ++k) {
      iterators[k] = map.find(curTriple[k]);
      if (iterators[k] == map.end()) {
        LOG(INFO) << "not found in partial local Vocab: " << curTriple[k] << '\n';
        AD_CHECK(false);
      }
    }

    // update the Element
    writer << array<Id, 3>{{iterators[0]->second, iterators[1]->second, iterators[2]->second}};
  }
}

// _________________________________________________________________________________________________________
template<size_t I>
void writePartialVocabularyToFileImpl(const ItemVec& vectors, const string& baseFileName) {
  if constexpr (I < std::tuple_size_v<ItemVec>) {
    auto fileName = baseFileName + "." + std::to_string(I);
    LOG(INFO) << "Writing vocabulary to binary file " << fileName << "\n";
    ad_utility::serialization::FileWriteSerializer serializer{fileName};
    auto& els = std::get<I>(vectors);
    auto getPairWithOnlyId = []<typename F, typename S>(std::pair<F, S> pair) {
      if constexpr (std::is_same_v<S, IdAndSplitVal>) {
        return std::pair{std::move(pair.first), pair.second.m_id};
      } else {
        return pair;
      }
    };
    for (const auto& el : els) {
      serializer & getPairWithOnlyId(el);
    }
    LOG(INFO) << "Done writing vocabulary to file.\n";
    writePartialVocabularyToFileImpl<I+1>(vectors, baseFileName);
  }
}

// _________________________________________________________________________________________________________
void writePartialVocabularyToFile(const ItemVec& els, const string& fileName) {
  LOG(INFO) << "Writing vocabulary to binary file " << fileName << "\n";
  writePartialVocabularyToFileImpl<0>(els, fileName);
}

// ______________________________________________________________________________________________
template <class Pred>
void writePartialIdMapToBinaryFileForMerging(std::shared_ptr<const ItemMapArray> map,
                                             const string& fileName, Pred comp,
                                             const bool doParallelSort) {
  LOG(INFO) << "Creating partial vocabulary from set ...\n";
  ItemVec els = vocabMapsToVector(map);
  LOG(INFO) << "... sorting ...\n";

  sortVocabVector(&els, comp, doParallelSort);

  LOG(INFO) << "Done creating vocabulary.\n";

  writePartialVocabularyToFile(els, fileName);
}

template <size_t I>
void vocabMapsToVectorImpl(const std::shared_ptr<const ItemMapArray>& maps, ItemVec* resultPtr) {
  if constexpr (I < std::tuple_size_v<ItemVec>) {
    auto& els = std::get<I>(*resultPtr);
    size_t totalEls = std::accumulate(maps->begin(), maps->end(), 0,
                                      [](const auto& x, const auto& y) { return x + std::get<I>(y).size(); });
    els.reserve(totalEls);
    for (const auto& singleMap : *maps) {
      els.insert(end(els), begin(std::get<I>(singleMap)), end(std::get<I>(singleMap)));
    }
    vocabMapsToVectorImpl<I+1>(maps, resultPtr);
  }
}

// __________________________________________________________________________________________________
ItemVec vocabMapsToVector(std::shared_ptr<const ItemMapArray> map) {
  ItemVec els;
  vocabMapsToVectorImpl<0>(map, &els);
  return els;
}


// _______________________________________________________________________________________________________________________
template <size_t I>
void sortVocabVectorImpl(ItemVec* vecPtr, const bool doParallelSort) {
  if constexpr (I < std::tuple_size_v<ItemVec>) {
    auto& els = std::get<I>(*vecPtr);
    if constexpr (USE_PARALLEL_SORT) {
      if (doParallelSort) {
        ad_utility::parallel_sort(begin(els), end(els), std::less<>{},
                                  ad_utility::parallel_tag(NUM_SORT_THREADS));
      } else {
        std::sort(begin(els), end(els));
      }
    } else {
      std::sort(begin(els), end(els));
      (void)doParallelSort;  // avoid compiler warning for unused value.
    }
    sortVocabVectorImpl<I + 1>(vecPtr, doParallelSort);
  }
}

// _______________________________________________________________________________________________________________________
template <class StringSortComparator>
void sortVocabVector(ItemVec* vecPtr, StringSortComparator comp, const bool doParallelSort) {
  auto& els = std::get<0>(*vecPtr);
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
  sortVocabVectorImpl<1>(vecPtr, doParallelSort);
}

// _____________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(const string& mmapFilename) {
  ad_utility::HashMap<Id, Id> res;
  IdPairMMapVecView vec(mmapFilename);
  for (const auto& [partialId, globalId] : vec) {
    res[partialId] = globalId;
  }
  return res;
}
