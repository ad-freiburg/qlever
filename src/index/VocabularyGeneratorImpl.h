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

#include "index/ConstantsIndexBuilding.h"
#include "index/Vocabulary.h"
#include "index/VocabularyGenerator.h"
#include "parser/RdfEscaping.h"
#include "util/Conversions.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Log.h"
#include "util/ParallelMultiwayMerge.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Timer.h"

// ___________________________________________________________________
template <typename Comparator, typename InternalVocabularyAction>
VocabularyMerger::VocabularyMetaData VocabularyMerger::mergeVocabulary(
    const std::string& basename, size_t numFiles, Comparator comparator,
    InternalVocabularyAction& internalVocabularyAction,
    ad_utility::MemorySize memoryToUse) {
  // Return true iff p1 >= p2 according to the lexicographic order of the IRI
  // or literal. All internal IRIs or literals come before all external ones.
  // TODO<joka921> Change this as soon as we have Interleaved Ids via the
  // MilestoneIdManager
  auto lessThan = [&comparator](const TripleComponentWithIndex& t1,
                                const TripleComponentWithIndex& t2) {
    if (t1.isExternal() != t2.isExternal()) {
      return t2.isExternal();
    }
    return comparator(t1.iriOrLiteral_, t2.iriOrLiteral_);
  };
  auto lessThanForQueue = [&lessThan](const QueueWord& p1,
                                      const QueueWord& p2) {
    return lessThan(p1._entry, p2._entry);
  };

  std::vector<cppcoro::generator<QueueWord>> generators;

  auto makeGenerator = [&](size_t fileIdx) -> cppcoro::generator<QueueWord> {
    ad_utility::serialization::FileReadSerializer infile{
        absl::StrCat(basename, PARTIAL_VOCAB_FILE_NAME, fileIdx)};
    uint64_t numWords;
    infile >> numWords;
    TripleComponentWithIndex val;
    for ([[maybe_unused]] auto idx : ad_utility::integerRange(numWords)) {
      infile >> val;
      QueueWord word{std::move(val), fileIdx};
      co_yield word;
    }
  };
  if (!_noIdMapsAndIgnoreExternalVocab) {
    outfileExternal_ =
        ad_utility::makeOfstream(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
  }

  // Open and prepare all infiles and mmap output vectors.
  generators.reserve(numFiles);
  for (size_t i = 0; i < numFiles; i++) {
    generators.push_back(makeGenerator(i));
    if (!_noIdMapsAndIgnoreExternalVocab) {
      idVecs_.emplace_back(0, basename + PARTIAL_MMAP_IDS + std::to_string(i));
    }
  }

  std::vector<QueueWord> sortedBuffer;
  sortedBuffer.reserve(_bufferSize);

  std::future<void> writeFuture;

  // Some memory (that is hard to measure exactly) is used for the writing of a
  // batch of merged words, so we only give 80% of the total memory to the
  // merging. This is very approximate and should be investigated in more
  // detail.
  auto mergedWords =
      ad_utility::parallelMultiwayMerge<QueueWord, true,
                                        decltype(sizeOfQueueWord)>(
          0.8 * memoryToUse, generators, lessThanForQueue);
  for (QueueWord& currentWord : std::views::join(mergedWords)) {
    // for the prefix compression vocabulary, we don't need the external
    // vocabulary
    // TODO<joka921> Don't include external literals at all in this
    // vocabulary.
    if (_noIdMapsAndIgnoreExternalVocab && currentWord.isExternal()) {
      break;
    }

    // accumulated the globally ordered queue words in a buffer.
    sortedBuffer.push_back(std::move(currentWord));

    if (sortedBuffer.size() >= _bufferSize) {
      // asynchronously write the next batch of sorted
      // queue words
      auto writeTask = [this, buf = std::move(sortedBuffer),
                        &internalVocabularyAction, &lessThan]() {
        this->writeQueueWordsToIdVec(buf, internalVocabularyAction, lessThan);
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
  }

  // wait for the active write tasks to finish
  if (writeFuture.valid()) {
    writeFuture.get();
  }

  // Handle remaining words in the buffer
  if (!sortedBuffer.empty()) {
    writeQueueWordsToIdVec(sortedBuffer, internalVocabularyAction, lessThan);
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
    InternalVocabularyAction& internalVocabularyAction, const auto& lessThan) {
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
      if (lastTripleComponent_.has_value() &&
          !lessThan(lastTripleComponent_.value(), top._entry)) {
        LOG(WARN) << "Total vocabulary order violated for "
                  << lastTripleComponent_->iriOrLiteral() << " and "
                  << top.iriOrLiteral() << std::endl;
      }
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
          top.iriOrLiteral(), lastTripleComponent_.value().index_);
      metaData_.langTaggedPredicates_.addIfWordMatches(
          top.iriOrLiteral(), lastTripleComponent_.value().index_);
      metaData_.numWordsTotal_++;
      if (metaData_.numWordsTotal_ % 100'000'000 == 0) {
        LOG(INFO) << "Words merged: " << metaData_.numWordsTotal_ << std::endl;
      }
    }
    // Write pair of local and global ID to buffer.
    writeBuf.emplace_back(
        top._partialFileId,
        std::pair{top.id(), lastTripleComponent_.value().index_});

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
  res.reserve(2 * els.size());
  bool first = true;
  std::string_view lastWord;
  size_t nextWordId = 0;
  for (auto& el : els) {
    if (!first && lastWord != el.first) {
      nextWordId++;
      lastWord = el.first;
    }
    AD_CONTRACT_CHECK(!res.count(el.second.id_));
    res[el.second.id_] = nextWordId;
    el.second.id_ = nextWordId;
    first = false;
  }
  return res;
}

// ________________________________________________________________________________________________________
inline void writeMappedIdsToExtVec(
    const auto& input, const ad_utility::HashMap<uint64_t, uint64_t>& map,
    std::unique_ptr<TripleVec>* writePtr) {
  auto& vec = *(*writePtr);
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
    vec.push(mappedTriple);
  }
}

// _________________________________________________________________________________________________________
inline void writePartialVocabularyToFile(const ItemVec& els,
                                         const string& fileName) {
  LOG(DEBUG) << "Writing partial vocabulary to: " << fileName << "\n";
  ad_utility::serialization::ByteBufferWriteSerializer byteBuffer;
  byteBuffer.reserve(1'000'000'000);
  ad_utility::serialization::FileWriteSerializer serializer{fileName};
  uint64_t size = els.size();  // really make sure that this has 64bits;
  serializer << size;
  for (const auto& [word, idAndSplitVal] : els) {
    // When merging the vocabulary, we need the actual word, the (internal) id
    // we have assigned to this word, and the information, whether this word
    // belongs to the internal or external vocabulary.
    const auto& [id, splitVal] = idAndSplitVal;
    byteBuffer << word;
    byteBuffer << splitVal.isExternalized_;
    byteBuffer << id;
  }
  {
    ad_utility::TimeBlockAndLog t{"performing the actual write"};
    serializer.serializeBytes(byteBuffer.data().data(),
                              byteBuffer.data().size());
    serializer.close();
  }
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
      [](const auto& x, const auto& y) { return x + y.map_.size(); });
  els.reserve(totalEls);
  for (const auto& singleMap : *map) {
    els.insert(end(els), begin(singleMap.map_), end(singleMap.map_));
  }
  LOG(TRACE) << "Sorting ..." << std::endl;

  sortVocabVector(&els, comp, doParallelSort);

  LOG(INFO) << "Done creating vocabulary" << std::endl;

  writePartialVocabularyToFile(els, fileName);
}

// __________________________________________________________________________________________________
inline ItemVec vocabMapsToVector(ItemMapArray& map) {
  ItemVec els;
  std::vector<size_t> offsets;
  size_t totalEls =
      std::accumulate(map.begin(), map.end(), 0,
                      [&offsets](const auto& x, const auto& y) mutable {
                        offsets.push_back(x);
                        return x + y.map_.size();
                      });
  els.resize(totalEls);
  std::vector<std::future<void>> futures;
  size_t i = 0;
  for (auto& singleMap : map) {
    futures.push_back(
        std::async(std::launch::async, [&singleMap, &els, &offsets, i] {
          using T = ItemVec::value_type;
          std::ranges::transform(singleMap.map_, els.begin() + offsets[i],
                                 [](auto& el) -> T {
                                   return {el.first, std::move(el.second)};
                                 });
        }));
    ++i;
  }
  for (auto& fut : futures) {
    fut.get();
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
                                ad_utility::parallel_tag(10));
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
