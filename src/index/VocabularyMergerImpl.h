// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H

#include <cstdint>
#include <future>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/VocabularyMerger.h"
#include "util/Allocator.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/ParallelMultiwayMerge.h"
#include "util/Serializer/BufferedSerializer.h"
#include "util/Serializer/CompressedSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Timer.h"
#include "util/Views.h"

namespace ad_utility::vocabulary_merger {
// _________________________________________________________________
template <typename W, typename C>
auto mergeVocabulary(const std::string& basename, size_t numPartialVocabularies,
                     W comparator, C& wordCallback,
                     ad_utility::MemorySize memoryToUse,
                     const ad_utility::RegexSet& blankNodeIriRegexes)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  using detail::QueueWord;
  // Return true iff `p1` is smaller than `p2` according to the order of the
  // IRI or literal.
  auto lessThanForQueue = [&comparator](const QueueWord& p1,
                                        const QueueWord& p2) {
    return comparator(p1.iriOrLiteral(), p2.iriOrLiteral());
  };

  // Open and prepare all the input files.
  auto makeWordRangeFromFile = [&basename](size_t fileIndex) {
    ad_utility::serialization::FileReadSerializer infile{
        partialVocabularyWordsFilename(basename, fileIndex)};
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
  generators.reserve(numPartialVocabularies);
  // The index of the partial vocabulary that a merged word comes from is
  // stored in 32 bits (see `detail::LocalIdxToBatchMapping`). NOTE: This check
  // is done here (and not per merged word, which would be on the hot path of
  // the merging), because `partialFileId_` is always one of the indices below.
  AD_CORRECTNESS_CHECK(numPartialVocabularies <=
                       std::numeric_limits<uint32_t>::max());

  for (std::size_t i : ad_utility::integerRange(numPartialVocabularies)) {
    generators.push_back(makeWordRangeFromFile(i));
  }

  // The stages of the pipeline. The `batchBuilder` (the first stage) runs on
  // this thread, the `pipeline` owns the three stages that run concurrently to
  // it.
  detail::VocabularyMergePipeline pipeline{
      partialVocabularyIdMapFilenames(basename, numPartialVocabularies)};
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
    // Stop merging as soon as one of the stages of the pipeline has failed,
    // the exception is rethrown by `finish()` below.
    if (pipeline.hasFailed()) {
      break;
    }
    batchBuilder.addMergedWords(std::move(currentWords), comparator,
                                batchCallback);
  }
  // Hand the remaining words (including the one that is still held back) to
  // the pipeline and wait until all of them have actually been written.
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

// The serializer that is used to write the triples that were mapped using a
// single partial vocabulary to disk (see `writeMappedIdsToFile` below).
using TripleWriter = ad_utility::serialization::ZstdWriteSerializer<
    ad_utility::serialization::FileWriteSerializer>;

// The counterpart of `TripleWriter` that reads those triples back (see
// `readMappedIdsFromFile` below).
using TripleReader = ad_utility::serialization::ZstdReadSerializer<
    ad_utility::serialization::FileReadSerializer>;

// ________________________________________________________________________________________________________
inline void writeMappedIdsToFile(
    std::vector<std::array<Id, NumColumnsIndexBuilding>> input,
    const HashMap<uint64_t, uint64_t>& map, const std::string& filename) {
  for (auto& curTriple : input) {
    for (Id& id : curTriple) {
      if (id.getDatatype() != Datatype::VocabIndex) {
        continue;
      }
      // for all triple elements find their mapping from partial to global ids
      auto iterator = map.find(id.getVocabIndex().get());
      AD_CORRECTNESS_CHECK(iterator != map.end(), "VocabIndex ",
                           id.getVocabIndex().get(),
                           " not found in mapping for partial vocabulary");
      id = Id::makeFromVocabIndex(VocabIndex::make(iterator->second));
    }
  }
  TripleWriter writer{ad_utility::serialization::FileWriteSerializer{filename}};
  // Serialize the whole batch as a single vector. This prepends the number of
  // triples, so that `readMappedIdsFromFile` can read back exactly this batch
  // without any external bookkeeping.
  writer << input;
  // Flush the remaining buffered triples and close the file, so that it can be
  // read back.
  writer.close();
}

// ________________________________________________________________________________________________________
inline IdTableStatic<NumColumnsIndexBuilding> readMappedIdsFromFile(
    const std::string& filename) {
  TripleReader reader{ad_utility::serialization::FileReadSerializer{filename}};
  // The triples were written as a single vector, so their number precedes them
  // (see `writeMappedIdsToFile` above).
  size_t numTriples;
  reader >> numTriples;
  IdTableStatic<NumColumnsIndexBuilding> triples{
      ad_utility::makeUnlimitedAllocator<Id>()};
  triples.reserve(numTriples);
  for ([[maybe_unused]] size_t idx : ad_utility::integerRange(numTriples)) {
    std::array<Id, NumColumnsIndexBuilding> triple;
    reader >> triple;
    triples.push_back(triple);
  }
  return triples;
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
inline ItemVec vocabMapsToVector(const ItemMapAndBuffer& map) {
  ItemVec els;
  els.resize(map.map_.size());
  using T = ItemVec::value_type;
  ql::ranges::transform(map.map_, els.begin(),
                        [](auto& el) -> T { return {el.first, el.second}; });
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
inline ad_utility::HashMap<VocabIndex, Id> IdMapFromPartialIdMapFile(
    const std::string& filename) {
  auto vec = getIdMapFromFile(filename);
  ad_utility::HashMap<VocabIndex, Id> map;
  map.reserve(vec.size());
  for (const auto& entry : vec) {
    map.emplace(entry.localIndex_, entry.globalId_);
  }
  return map;
}
}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
