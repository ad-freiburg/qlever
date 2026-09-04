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
#include "util/Serializer/BufferedSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Timer.h"

namespace ad_utility::vocabulary_merger {
// _________________________________________________________________
template <typename W, typename C>
auto mergeVocabulary(
    const std::string& basename, size_t numFiles, W comparator, C& wordCallback,
    ad_utility::MemorySize memoryToUse,
    const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  using detail::QueueWord;
  // Return true iff `p1` is smaller than `p2` according to the order of the
  // IRI or literal.
  auto lessThanForQueue = [&comparator](const QueueWord& p1,
                                        const QueueWord& p2) {
    return comparator(p1.iriOrLiteral(), p1.isExternal(), p2.iriOrLiteral(),
                      p2.isExternal());
  };

  // Open and prepare all the input files.
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
  }

  // The stages of the pipeline. The `batchBuilder` (the first stage) runs on
  // this thread, the `pipeline` owns the three stages that run concurrently to
  // it.
  detail::VocabularyMergePipeline pipeline{basename, numFiles};
  detail::WordBatchBuilder batchBuilder;
  auto batchCallback = [&pipeline, &wordCallback,
                        &blankNodeIriRegexes](detail::WordBatch batch) {
    pipeline.push(std::move(batch), wordCallback, blankNodeIriRegexes);
  };

  // Some memory (that is hard to measure exactly) is used for the writing of
  // a batch of merged words, so we only give 80% of the total memory to the
  // merging. This is very approximate and should be investigated in more
  // detail.
  auto mergedWords =
      ad_utility::parallelMultiwayMerge<QueueWord, true,
                                        decltype(detail::sizeOfQueueWord)>(
          0.8 * memoryToUse, std::move(generators), lessThanForQueue);
  for (std::vector<QueueWord>& currentWords : mergedWords) {
    batchBuilder.addMergedWords(std::move(currentWords), comparator,
                                batchCallback);
  }
  // Hand the remaining words to the pipeline and wait until all of them have
  // actually been written.
  batchBuilder.finish(batchCallback);
  return pipeline.finish();
}
// ____________________________________________________________________________________________________________
inline HashMap<uint64_t, uint64_t> createInternalMapping(ItemVec& els) {
  HashMap<uint64_t, uint64_t> res;
  res.reserve(els.size());
  std::optional<std::string_view> lastWord;
  // This value will overflow on the first entry.
  size_t nextWordId = -1;
  for (auto& [word, idAndExternal] : els) {
    auto id = idAndExternal.id();
    if (lastWord != word) {
      nextWordId++;
      lastWord = word;
    }
    auto inserted = res.try_emplace(id, nextWordId).second;
    AD_CORRECTNESS_CHECK(inserted);
    idAndExternal = PartialVocabIndexWithExternalFlag{
        nextWordId, idAndExternal.isExternal()};
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

  // We buffer the data with our own buffer before passing it to the file in
  // large chunks. Despite `fwrite` (which is ultimately called by
  // `FileWriteSerializer::serializeBytes`) buffering data on its own, it is
  // faster to buffer with our own buffer, presumably because `fwrite` is
  // thread-safe and therefore has to acquire a mutex for every call.
  serialization::BufferedWriteSerializer serializer{
      serialization::FileWriteSerializer{fileName}, 16_MB};

  uint64_t size = els.size();
  serializer << size;

  // This is essentially a `VectorIncrementalSerializer` with a custom
  // serialization function, which the infrastructure currently does not
  // support.
  for (const auto& [word, idAndExternal] : els) {
    // When merging the vocabulary, we need the actual word, the (internal) id
    // we have assigned to this word, and the information, whether this word
    // belongs to the internal or external vocabulary.
    serializer << word;
    serializer << idAndExternal.isExternal();
    serializer << idAndExternal.id();
  }

  serializer.close();

  AD_LOG_DEBUG << "Done writing partial vocabulary\n";
}

// __________________________________________________________________________________________________
inline ItemVec vocabMapsToVector(const ItemMapArray& map) {
  ItemVec els;
  std::array<size_t, std::tuple_size_v<ItemMapArray>> offsets;
  // This is essentially `std::transform_exclusive_scan`, but GCC 8 doesn't
  // support this yet.
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
          ql::ranges::transform(
              singleMap.map_, els.begin() + offsets[i],
              [](auto& el) -> T { return {el.first, el.second}; });
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
  auto vec = getIdMapFromFile(filename);
  ad_utility::HashMap<Id, Id> map;
  map.reserve(vec.size());
  for (const auto& entry : vec) {
    map.emplace(Id::makeFromVocabIndex(VocabIndex::make(entry.localIndex_)),
                entry.globalId_);
  }
  return map;
}
}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
