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
#include "index/VocabularyMerger.h"
#include "parser/RdfEscaping.h"
#include "util/Conversions.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Log.h"
#include "util/ParallelMultiwayMerge.h"
#include "util/ProgressBar.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Timer.h"

namespace ad_utility::vocabulary_merger {
// _________________________________________________________________
VocabularyMetaData mergeVocabulary(const std::string& basename, size_t numFiles,
                                   WordComparator auto comparator,
                                   WordCallback auto& internalWordCallback,
                                   ad_utility::MemorySize memoryToUse) {
  VocabularyMerger merger;
  return merger.mergeVocabulary(basename, numFiles, std::move(comparator),
                                internalWordCallback, memoryToUse);
}

// _________________________________________________________________
auto VocabularyMerger::mergeVocabulary(const std::string& basename,
                                       size_t numFiles,
                                       WordComparator auto comparator,
                                       WordCallback auto& wordCallback,
                                       ad_utility::MemorySize memoryToUse)
    -> VocabularyMetaData {
  // Return true iff p1 >= p2 according to the lexicographic order of the IRI
  // or literal.
  auto lessThan = [&comparator](const TripleComponentWithIndex& t1,
                                const TripleComponentWithIndex& t2) {
    return comparator(t1.iriOrLiteral_, t2.iriOrLiteral_);
  };
  auto lessThanForQueue = [&lessThan](const QueueWord& p1,
                                      const QueueWord& p2) {
    return lessThan(p1.entry_, p2.entry_);
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

  // Open and prepare all infiles and mmap output vectors.
  generators.reserve(numFiles);
  for (size_t i = 0; i < numFiles; i++) {
    generators.push_back(makeGenerator(i));
    idVecs_.emplace_back(0, absl::StrCat(basename, PARTIAL_MMAP_IDS, i));
  }

  std::vector<QueueWord> sortedBuffer;
  sortedBuffer.reserve(bufferSize_);

  std::future<void> writeFuture;

  // Some memory (that is hard to measure exactly) is used for the writing of a
  // batch of merged words, so we only give 80% of the total memory to the
  // merging. This is very approximate and should be investigated in more
  // detail.
  auto mergedWords =
      ad_utility::parallelMultiwayMerge<QueueWord, true,
                                        decltype(sizeOfQueueWord)>(
          0.8 * memoryToUse, generators, lessThanForQueue);
  ad_utility::ProgressBar progressBar{metaData_.numWordsTotal_,
                                      "Words merged: "};
  for (QueueWord& currentWord : std::views::join(mergedWords)) {
    // Accumulate the globally ordered queue words in a buffer.
    sortedBuffer.push_back(std::move(currentWord));

    if (sortedBuffer.size() >= bufferSize_) {
      // Wait for the (asynchronous) writing of the last batch of words, and
      // trigger the (again asynchronous) writing of the next batch.
      auto writeTask = [this, buffer = std::move(sortedBuffer), &wordCallback,
                        &lessThan, &progressBar]() {
        this->writeQueueWordsToIdVec(buffer, wordCallback, lessThan,
                                     progressBar);
      };
      sortedBuffer.clear();
      sortedBuffer.reserve(bufferSize_);
      // wait for the last batch

      LOG(TIMING) << "A new batch of words is ready" << std::endl;
      // First wait for the last batch to finish, that way there will be no race
      // conditions.
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
    writeQueueWordsToIdVec(sortedBuffer, wordCallback, lessThan, progressBar);
  }
  LOG(INFO) << progressBar.getFinalProgressString() << std::flush;

  auto metaData = std::move(metaData_);
  // completely reset all the inner state
  clear();
  return metaData;
}

// ________________________________________________________________________________
void VocabularyMerger::writeQueueWordsToIdVec(
    const std::vector<QueueWord>& buffer, WordCallback auto& wordCallback,
    std::predicate<TripleComponentWithIndex,
                   TripleComponentWithIndex> auto const& lessThan,
    ad_utility::ProgressBar& progressBar) {
  LOG(TIMING) << "Start writing a batch of merged words\n";

  // Smaller grained buffer for the actual inner write.
  auto bufSize = bufferSize_ / 5;
  std::future<void> writeFut;
  std::vector<std::pair<size_t, std::pair<size_t, Id>>> writeBuffer;
  writeBuffer.reserve(bufSize);

  // Iterate (avoid duplicates).
  for (auto& top : buffer) {
    if (!lastTripleComponent_.has_value() ||
        top.iriOrLiteral() != lastTripleComponent_.value().iriOrLiteral()) {
      if (lastTripleComponent_.has_value() &&
          !lessThan(lastTripleComponent_.value(), top.entry_)) {
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

      // Write the new word to the vocabulary.
      const auto& nextWord = lastTripleComponent_.value();
      if (nextWord.isBlankNode()) {
        lastTripleComponent_->index_ = metaData_.numBlankNodesTotal_;
        ++metaData_.numBlankNodesTotal_;
      } else {
        wordCallback(nextWord.iriOrLiteral(), nextWord.isExternal());

        metaData_.internalEntities_.addIfWordMatches(top.iriOrLiteral(),
                                                     nextWord.index_);
        metaData_.langTaggedPredicates_.addIfWordMatches(top.iriOrLiteral(),
                                                         nextWord.index_);
        metaData_.numWordsTotal_++;
      }
      if (progressBar.update()) {
        LOG(INFO) << progressBar.getProgressString() << std::flush;
      }
    } else {
      // If a word appears with different values for `isExternal`, then we
      // externalize it.
      bool& external = lastTripleComponent_.value().isExternal();
      external = external || top.isExternal();
    }
    const auto& word = lastTripleComponent_.value();
    Id targetId =
        word.isBlankNode()
            ? Id::makeFromBlankNodeIndex(BlankNodeIndex::make(word.index_))
            : Id::makeFromVocabIndex(VocabIndex::make(word.index_));
    // Write pair of local and global ID to buffer.
    writeBuffer.emplace_back(top.partialFileId_, std::pair{top.id(), targetId});

    if (writeBuffer.size() >= bufSize) {
      auto task = [this, buffer = std::move(writeBuffer)]() {
        this->doActualWrite(buffer);
      };
      if (writeFut.valid()) {
        writeFut.get();
      }
      writeFut = std::async(task);
      writeBuffer.clear();
      writeBuffer.reserve(bufSize);
    }
  }

  if (writeFut.valid()) {
    writeFut.get();
  }

  if (!writeBuffer.empty()) {
    doActualWrite(writeBuffer);
  }
}

// ____________________________________________________________________________
inline void VocabularyMerger::doActualWrite(
    const std::vector<std::pair<size_t, std::pair<size_t, Id>>>& buffer) {
  for (const auto& [id, value] : buffer) {
    idVecs_[id].push_back(
        {Id::makeFromVocabIndex(VocabIndex::make(value.first)), value.second});
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
  for (auto& [word, idAndSplitVal] : els) {
    auto& id = idAndSplitVal.id_;
    if (!first && lastWord != word) {
      nextWordId++;
      lastWord = word;
    }
    AD_CONTRACT_CHECK(!res.count(id));
    res[id] = nextWordId;
    id = nextWordId;
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
      ad_utility::parallel_sort(std::ranges::begin(els), std::ranges::end(els),
                                comp, ad_utility::parallel_tag(10));
    } else {
      std::ranges::sort(els, comp);
    }
  } else {
    std::ranges::sort(els, comp);
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
}  // namespace ad_utility::vocabulary_merger
