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
VocabularyMerger::VocabularyMetaData VocabularyMerger::mergeVocabulary(
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
    outfileExternal_ =
        ad_utility::makeOfstream(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
  }
  std::vector<bool> endOfFile(numFiles, false);

  // Priority queue for the k-way merge
  std::priority_queue<QueueWord, std::vector<QueueWord>, decltype(queueCompare)>
      queue(queueCompare);

  auto pushWordFromPartialVocabularyToQueue = [&](size_t i) {
    if (numWordsLeftInPartialVocabulary[i] > 0) {
      TripleComponentWithIndex tripleComponent;
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
      idVecs_.emplace_back(0, basename + PARTIAL_MMAP_IDS + std::to_string(i));
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

  auto metaData = std::move(metaData_);
  // completely reset all the inner state
  clear();
  return metaData;
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
    if (!lastTripleComponent_.has_value() ||
        top.iriOrLiteral() != lastTripleComponent_.value().iriOrLiteral()) {
      // TODO<joka921> Once we have interleaved IDs using the MilestoneIdManager
      // we have to compute the correct Ids here.
      lastTripleComponent_ = TripleComponentWithIndex{
          top.iriOrLiteral(), top.isExternal(), metaData_.numWordsTotal_};

      // TODO<optimization> If we aim to further speed this up, we could
      // order all the write requests to _outfile _externalOutfile and all the
      // idVecs to have a more useful external access pattern.

      // write the new word to the vocabulary
      if (!lastTripleComponent_.value().isExternal()) {
        internalVocabularyAction(lastTripleComponent_.value().iriOrLiteral());
      } else {
        outfileExternal_ << RdfEscaping::escapeNewlinesAndBackslashes(
                                lastTripleComponent_.value().iriOrLiteral())
                         << '\n';
      }

      metaData_.internalEntities_.addIfWordMatches(
          top.iriOrLiteral(), lastTripleComponent_.value()._index);
      metaData_.langTaggedPredicates_.addIfWordMatches(
          top.iriOrLiteral(), lastTripleComponent_.value()._index);
      metaData_.numWordsTotal_++;
      if (metaData_.numWordsTotal_ % 100'000'000 == 0) {
        LOG(INFO) << "Words merged: " << metaData_.numWordsTotal_ << std::endl;
      }
    }
    // Write pair of local and global ID to buffer.
    writeBuf.emplace_back(
        top._partialFileId,
        std::pair{top.id(), lastTripleComponent_.value()._index});

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
inline void VocabularyMerger::doActualWrite(
    const std::vector<std::pair<size_t, std::pair<size_t, size_t>>>& buffer) {
  if (_noIdMapsAndIgnoreExternalVocab) {
    return;
  }
  for (const auto& [id, value] : buffer) {
    idVecs_[id].push_back(
        {Id::makeFromVocabIndex(VocabIndex::make(value.first)),
         Id::makeFromVocabIndex(VocabIndex::make(value.second))});
  }
}

// ____________________________________________________________________________________________________________
inline ad_utility::HashMap<uint64_t, uint64_t> createInternalMapping(
    ItemVec* elsPtr) {
  auto& els = *elsPtr;
  ad_utility::HashMap<uint64_t, uint64_t> res;
  bool first = true;
  std::string lastWord;
  size_t nextWordId = 0;
  for (auto& el : els) {
    if (!first && lastWord != el.first) {
      nextWordId++;
      lastWord = el.first;
    }
    AD_CONTRACT_CHECK(!res.count(el.second.m_id));
    res[el.second.m_id] = nextWordId;
    el.second.m_id = nextWordId;
    first = false;
  }
  return res;
}

// ________________________________________________________________________________________________________
inline void writeMappedIdsToExtVec(
    const auto& input, const ad_utility::HashMap<uint64_t, uint64_t>& map,
    TripleVec::bufwriter_type* writePtr) {
  auto& writer = *writePtr;
  for (const auto& curTriple : input) {
    std::array<Id, 3> mappedTriple;
    // for all triple elements find their mapping from partial to global ids
    for (size_t k = 0; k < 3; ++k) {
      if (curTriple[k].getDatatype() != Datatype::VocabIndex) {
        mappedTriple[k] = curTriple[k];
        continue;
      }
      auto iterator = map.find(curTriple[k].getVocabIndex().get());
      if (iterator == map.end()) {
        LOG(ERROR) << "not found in partial local vocabulary: " << curTriple[k]
                   << std::endl;
        AD_FAIL();
      }
      mappedTriple[k] =
          Id::makeFromVocabIndex(VocabIndex::make(iterator->second));
    }
    writer << mappedTriple;
  }
}

// _________________________________________________________________________________________________________
inline void writePartialVocabularyToFile(const ItemVec& els,
                                         const string& fileName) {
  LOG(DEBUG) << "Writing partial vocabulary to: " << fileName << "\n";
  ad_utility::serialization::FileWriteSerializer serializer{fileName};
  uint64_t size = els.size();  // really make sure that this has 64bits;
  serializer << size;
  for (const auto& [word, idAndSplitVal] : els) {
    // When merging the vocabulary, we need the actual word, the (internal) id
    // we have assigned to this word, and the information, whether this word
    // belongs to the internal or external vocabulary.
    const auto& [id, splitVal] = idAndSplitVal;
    TripleComponentWithIndex entry{word, splitVal.isExternalized, id};
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
  LOG(INFO) << "Creating partial vocabulary from set ..." << std::endl;
  ItemVec els;
  size_t totalEls = std::accumulate(
      map->begin(), map->end(), 0,
      [](const auto& x, const auto& y) { return x + y.size(); });
  els.reserve(totalEls);
  for (const auto& singleMap : *map) {
    els.insert(end(els), begin(singleMap), end(singleMap));
  }
  LOG(TRACE) << "Sorting ..." << std::endl;

  sortVocabVector(&els, comp, doParallelSort);

  LOG(INFO) << "Done creating vocabulary" << std::endl;

  writePartialVocabularyToFile(els, fileName);
}

// __________________________________________________________________________________________________
inline ItemVec vocabMapsToVector(std::unique_ptr<ItemMapArray> map) {
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
inline ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const string& mmapFilename) {
  ad_utility::HashMap<Id, Id> res;
  IdPairMMapVecView vec(mmapFilename);
  for (const auto& [partialId, globalId] : vec) {
    res[partialId] = globalId;
  }
  return res;
}
