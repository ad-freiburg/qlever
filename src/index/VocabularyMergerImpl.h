// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H

#include <future>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/VocabularyMerger.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/ParallelMultiwayMerge.h"
#include "util/ProgressBar.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Timer.h"

namespace ad_utility::vocabulary_merger {
// _________________________________________________________________
template <typename W, typename C>
auto mergeVocabulary(const std::string& basename, size_t numFiles, W comparator,
                     C& internalWordCallback,
                     ad_utility::MemorySize memoryToUse)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  VocabularyMerger merger;
  return merger.mergeVocabulary(basename, numFiles, std::move(comparator),
                                internalWordCallback, memoryToUse);
}

// _________________________________________________________________
template <typename W, typename C>
auto VocabularyMerger::mergeVocabulary(const std::string& basename,
                                       size_t numFiles, W comparator,
                                       C& wordCallback,
                                       ad_utility::MemorySize memoryToUse)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
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

  // Open and prepare all infiles and file-based output vectors.
  auto makeWordRangeFromFile = [&basename](size_t fileIndex) {
    ad_utility::serialization::FileReadSerializer infile{
        absl::StrCat(basename, PARTIAL_VOCAB_WORDS_INFIX, fileIndex)};
    uint64_t numWords;
    infile >> numWords;

    return ad_utility::CachingTransformInputRange{
        ad_utility::integerRange(numWords),
        [fileIndex, infile{std::move(infile)}](
            [[maybe_unused]] const std::size_t i) mutable {
          TripleComponentWithIndex val;
          infile >> val;
          return QueueWord{std::move(val), fileIndex};
        }};
  };
  std::vector<decltype(makeWordRangeFromFile(0))> generators;
  generators.reserve(numFiles);

  for (std::size_t i : ad_utility::integerRange(numFiles)) {
    generators.push_back(makeWordRangeFromFile(i));
    idMaps_.emplace_back(absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i));
  }

  // Some memory (that is hard to measure exactly) is used for the writing of
  // a batch of merged words, so we only give 80% of the total memory to the
  // merging. This is very approximate and should be investigated in more
  // detail.
  auto mergedWords =
      ad_utility::parallelMultiwayMerge<QueueWord, true,
                                        decltype(sizeOfQueueWord)>(
          0.8 * memoryToUse, std::move(generators), lessThanForQueue);
  ad_utility::ProgressBar progressBar{metaData_.numWordsTotal(),
                                      "Words merged: "};
  for (std::vector<QueueWord>& currentWords : mergedWords) {
    writeQueueWordsToIdMap(currentWords, wordCallback, lessThan, progressBar);
  }

  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;

  auto metaData = std::move(metaData_);
  // completely reset all the inner state
  clear();
  return metaData;
}

// ________________________________________________________________________________
CPP_template_def(typename C, typename L)(
    requires WordCallback<C> CPP_and_def
        ranges::predicate<L, TripleComponentWithIndex,
                          TripleComponentWithIndex>) void VocabularyMerger::
    writeQueueWordsToIdMap(std::vector<QueueWord>& buffer, C& wordCallback,
                           const L& lessThan,
                           ad_utility::ProgressBar& progressBar) {
  AD_LOG_TIMING << "Start writing a batch of merged words\n";

  // Iterate (avoid duplicates).
  for (auto& top : buffer) {
    if (!lastTripleComponent_.has_value() ||
        top.iriOrLiteral() != lastTripleComponent_.value().iriOrLiteral()) {
      if (lastTripleComponent_.has_value() &&
          !lessThan(lastTripleComponent_.value(), top.entry_)) {
        AD_LOG_WARN << "Total vocabulary order violated for "
                    << lastTripleComponent_->iriOrLiteral() << " and "
                    << top.iriOrLiteral() << std::endl;
      }
      lastTripleComponent_ =
          TripleComponentWithIndex{std::move(top.iriOrLiteral()),
                                   top.isExternal(), metaData_.numWordsTotal()};

      // TODO<optimization> If we aim to further speed this up, we could
      // order all the write requests to _outfile _externalOutfile and all the
      // idVecs to have a more useful external access pattern.

      // Write the new word to the vocabulary.
      auto& nextWord = lastTripleComponent_.value();
      if (nextWord.isBlankNode()) {
        nextWord.index_ = metaData_.getNextBlankNodeIndex();
      } else {
        nextWord.index_ =
            wordCallback(nextWord.iriOrLiteral(), nextWord.isExternal());
        metaData_.addWord(nextWord.iriOrLiteral(), nextWord.index_);
      }
      if (progressBar.update()) {
        AD_LOG_INFO << progressBar.getProgressString() << std::flush;
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
    idMaps_[top.partialFileId_].push_back(
        {Id::makeFromVocabIndex(VocabIndex::make(top.id())), targetId});
  }
}

// ____________________________________________________________________________________________________________
inline HashMap<uint64_t, uint64_t> createInternalMapping(ItemVec& els) {
  HashMap<uint64_t, uint64_t> res;
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
    const std::vector<std::array<Id, NumColumnsIndexBuilding>>& input,
    const HashMap<uint64_t, uint64_t>& map,
    std::unique_ptr<TripleVec>* writePtr) {
  auto& vec = *(*writePtr);
  for (const auto& curTriple : input) {
    std::array<Id, NumColumnsIndexBuilding> mappedTriple;
    // for all triple elements find their mapping from partial to global ids
    for (size_t k = 0; k < NumColumnsIndexBuilding; ++k) {
      if (curTriple[k].getDatatype() != Datatype::VocabIndex) {
        mappedTriple[k] = curTriple[k];
        continue;
      }
      auto iterator = map.find(curTriple[k].getVocabIndex().get());
      if (iterator == map.end()) {
        AD_LOG_ERROR << "not found in partial local vocabulary: "
                     << curTriple[k] << std::endl;
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
                                         const std::string& fileName) {
  AD_LOG_DEBUG << "Writing partial vocabulary to: " << fileName << "\n";

  static constexpr size_t flushThreshold = 16ULL * 1024 * 1024;  // 16 MB

  ad_utility::serialization::FileWriteSerializer serializer{fileName};
  ad_utility::serialization::ByteBufferWriteSerializer byteBuffer;
  byteBuffer.reserve(flushThreshold + 1024);  // + slack for the last item

  uint64_t size = els.size();
  byteBuffer << size;

  auto flush = [&]() {
    ad_utility::TimeBlockAndLog t{"performing the actual write"};
    serializer.serializeBytes(byteBuffer.data().data(),
                              byteBuffer.data().size());
    byteBuffer.clear();
  };

  for (const auto& [word, idAndSplitVal] : els) {
    // When merging the vocabulary, we need the actual word, the (internal) id
    // we have assigned to this word, and the information, whether this word
    // belongs to the internal or external vocabulary.
    const auto& [id, splitVal] = idAndSplitVal;
    byteBuffer << word;
    byteBuffer << splitVal.isExternalized_;
    byteBuffer << id;

    if (byteBuffer.data().size() >= flushThreshold) {
      flush();
    }
  }

  // Flush remaining data.
  flush();
  serializer.close();

  AD_LOG_DEBUG << "Done writing partial vocabulary\n";
}

// __________________________________________________________________________________________________
inline ItemVec vocabMapsToVector(const ItemMapArray& map) {
  ItemVec els;
  std::array<size_t, std::tuple_size_v<ItemMapArray>> offsets;
  size_t totalEls = std::accumulate(
      map.begin(), map.end(), 0,
      [&offsets, idx = 0](const auto& x, const auto& y) mutable {
        offsets.at(idx) = x;
        idx++;
        return x + y.map_.size();
      });
  els.resize(totalEls);
  std::array<std::future<void>, std::tuple_size_v<ItemMapArray>> futures;
  size_t i = 0;
  for (const auto& singleMap : map) {
    futures.at(i) =
        std::async(std::launch::async, [&singleMap, &els, &offsets, i] {
          using T = ItemVec::value_type;
          ql::ranges::transform(singleMap.map_, els.begin() + offsets[i],
                                [](auto& el) -> T {
                                  return {el.first, std::move(el.second)};
                                });
        });
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
      ad_utility::parallel_sort(ql::ranges::begin(els), ql::ranges::end(els),
                                comp, ad_utility::parallel_tag(10));
    } else {
      ql::ranges::sort(els, comp);
    }
  } else {
    ql::ranges::sort(els, comp);
    (void)doParallelSort;  // avoid compiler warning for unused value.
  }
}

// _____________________________________________________________________
inline ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const std::string& filename) {
  ad_utility::HashMap<Id, Id> res;
  auto vec = getIdMapFromFile(filename);
  for (const auto& [partialId, globalId] : vec) {
    res[partialId] = globalId;
  }
  return res;
}
}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
