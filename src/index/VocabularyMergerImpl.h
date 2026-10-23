// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H

#include <algorithm>
#include <cstdint>
#include <future>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/VocabularyMerger.h"
#include "index/vocabulary_merger/PartialVocabularyFile.h"
#include "util/Allocator.h"
#include "util/Exception.h"
#include "util/GlobalExecutor.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/Serializer/CompressedSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Timer.h"
#include "util/Views.h"
#include "util/parallelBlockMerge/InMemoryBlockStorage.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

namespace ad_utility::vocabulary_merger {

// The options of the parallel block merge of the partial vocabularies, see
// `mergeVocabulary` below. The merge runs on the global thread pool.
//
// The chunks are cut such that each of them merges about
// `VOCAB_MERGER_INPUT_PER_CHUNK` of serialized input (see
// `PartialVocabularyRunsInput::blockWeight`), and a chunk keeps its slot among
// the chunks in flight until its output has been consumed (see
// `VOCAB_MERGER_NUM_BUFFERED_BLOCKS_PER_CHUNK`). The memory of the merge is
// therefore bounded by the chunks in flight: one decoded block per partial
// vocabulary plus the output of the chunk for each of them, so their number is
// capped such that this fits into the `memoryToUse`.
inline parallelBlockMerge::MergeOptions vocabularyMergeOptions(
    size_t numPartialVocabularies, size_t numInputBytes,
    ad_utility::MemorySize memoryToUse) {
  parallelBlockMerge::MergeOptions options;
  // The blocks of merged words are the batches of the pipeline behind the
  // merge, see `detail::WordBatchBuilder`.
  options.outputBlockSize = parallelBlockMerge::OutputBlockSize::both(
      VOCAB_MERGER_WORD_BATCH_SIZE, VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE);
  options.parallelismHint = ad_utility::globalExecutorNumThreads();
  const size_t inputPerChunk = VOCAB_MERGER_INPUT_PER_CHUNK.getBytes();
  const size_t numChunks =
      std::max<size_t>(1, (numInputBytes + inputPerChunk - 1) / inputPerChunk);
  options.targetChunksPerThread = std::max<size_t>(
      1, (numChunks + options.parallelism() - 1) / options.parallelism());
  // The blocks in memory are larger than their serialized form (a
  // `InputWord` per word next to the bytes, and the output of a chunk copies
  // the words), by up to a factor of two.
  constexpr size_t decodedFactor = 2;
  const uint64_t memoryPerChunk = (std::max<size_t>(numPartialVocabularies, 1) *
                                       PARTIAL_VOCAB_BLOCK_SIZE.getBytes() +
                                   inputPerChunk) *
                                  decodedFactor;
  // A chunk keeps its slot until the thread behind the merge has taken its
  // output, and that thread takes the chunks in order. With exactly one slot
  // per thread, one slow chunk therefore idles the whole pool: the chunks
  // behind it are done, hold their slots, and no new chunk may start. Several
  // slots per thread let the merge run ahead of a slow chunk, within the
  // memory limit.
  options.maxNumChunksInFlight = std::clamp<size_t>(
      memoryToUse.getBytes() / memoryPerChunk, 1,
      VOCAB_MERGER_CHUNKS_IN_FLIGHT_PER_THREAD * options.parallelism());
  return options;
}

// _________________________________________________________________
template <typename W, typename C>
auto mergeVocabulary(const std::string& basename, size_t numPartialVocabularies,
                     W comparator, C& wordCallback,
                     ad_utility::MemorySize memoryToUse,
                     const ad_utility::RegexSet& blankNodeIriRegexes,
                     std::function<uint64_t(std::string_view)> geoSortKeyFn)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  using detail::InputWord;
  using detail::MergedBlock;
  using detail::MergedWord;
  using detail::QueueWord;
  // The order of the merged vocabulary: by geo sort key first (all keys are 0
  // without a geo cell grid, see `geoSortKeyFn`), then by `comparator`.
  auto keyedLessThan = [&comparator](uint64_t key1, std::string_view word1,
                                     uint64_t key2, std::string_view word2) {
    if (key1 != key2) {
      return key1 < key2;
    }
    return comparator(word1, word2);
  };
  // Return true iff `p1` is smaller than `p2` in this order.
  auto lessThan = [&keyedLessThan](const InputWord& p1, const InputWord& p2) {
    return keyedLessThan(p1.geoSortKey_, p1.iriOrLiteral(), p2.geoSortKey_,
                         p2.iriOrLiteral());
  };
  // The index of the partial vocabulary that a merged word comes from is
  // stored in 32 bits (see `detail::InputWord` and
  // `detail::LocalIdxToBatchMapping`). NOTE: This check is done here (and not
  // per merged word, which would be on the hot path of the merging), because
  // `partialFileId_` is always one of the indices below.
  AD_CORRECTNESS_CHECK(numPartialVocabularies <=
                       std::numeric_limits<uint32_t>::max());

  // Merge the partial vocabularies in parallel, see
  // `util/parallelBlockMerge/ParallelBlockMerge.h`: the words are split into
  // ranges by the block index of the partial vocabularies, each range is merged
  // by a chunk of its own on the global thread pool, and this thread receives
  // the merged blocks in the order of the vocabulary. The blocks stay in
  // memory, and a chunk keeps its slot until this thread has taken its output,
  // see `vocabularyMergeOptions`.
  PartialVocabularyRunsInput input{basename, numPartialVocabularies,
                                   std::move(geoSortKeyFn)};
  const auto inputStatistics = input.statistics();
  auto options = vocabularyMergeOptions(numPartialVocabularies,
                                        inputStatistics.numBytes_, memoryToUse);
  AD_LOG_INFO << "Merging " << input.numRuns() << " partial vocabularies ("
              << inputStatistics.numWords_ << " words in "
              << inputStatistics.numBlocks_ << " blocks) in "
              << options.targetNumChunks() << " chunks, using "
              << options.parallelism() << " threads and up to "
              << options.maxNumChunksInFlight << " chunks in flight ..."
              << std::endl;
  auto mergedWords = parallelBlockMerge::parallelBlockMergeToRange<true>(
      ad_utility::globalExecutor(), std::move(input), lessThan,
      parallelBlockMerge::makeInMemoryStorageFactory<MergedBlock>(
          VOCAB_MERGER_NUM_BUFFERED_BLOCKS_PER_CHUNK,
          /*releaseChunkOnConsumption=*/true),
      std::move(options));

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

  for (MergedBlock& block : mergedWords) {
    // Stop merging as soon as one of the stages of the pipeline has failed,
    // the exception is rethrown by `finish()` below.
    if (pipeline.hasFailed()) {
      break;
    }
    // The pipeline consumes owning `QueueWord`s, one per occurrence of a word,
    // so the merged words are copied out of the block (and the block's buffers
    // go back to the pool). The occurrences that the merge folded into a word
    // (see `MergedBlock::addWord`) all get the external flag of that word,
    // which is the OR over them; the `batchBuilder` combines the flags of the
    // occurrences of a word in the same way.
    // TODO<joka921> This copy is temporary: the next slice of the parallel
    // vocabulary merge replaces the pipeline by one that works on the blocks.
    std::vector<QueueWord> currentWords;
    currentWords.reserve(block.numOccurrences());
    for (const MergedWord& word : block) {
      for (const auto& [partialFileId, localIndex] : block.occurrences(word)) {
        currentWords.emplace_back(
            TripleComponentWithIndex{std::string{word.iriOrLiteral()},
                                     word.isExternal(), localIndex},
            partialFileId, word.geoSortKey_);
      }
    }
    block.clear();
    batchBuilder.addMergedWords(std::move(currentWords), keyedLessThan,
                                batchCallback);
  }
  // Hand the remaining words (including the one that is still held back) to
  // the pipeline and wait until all of them have actually been written.
  batchBuilder.finish(batchCallback);
  return pipeline.finish();
}

// ____________________________________________________________________________________________________________
inline HashMap<uint64_t, uint64_t> createInternalMapping(ItemVec& entries) {
  HashMap<uint64_t, uint64_t> res;
  res.reserve(entries.size());
  std::optional<std::string_view> lastWord;
  // This value will overflow on the first entry.
  size_t nextWordId = -1;
  for (auto& entry : entries) {
    auto& indexAndFlag = entry.indexAndFlag_;
    auto index = indexAndFlag.index();
    if (lastWord != entry.word_) {
      nextWordId++;
      lastWord = entry.word_;
    }
    auto inserted = res.try_emplace(index, nextWordId).second;
    AD_CORRECTNESS_CHECK(inserted);
    indexAndFlag = PartialVocabIndexWithExternalFlag{nextWordId,
                                                     indexAndFlag.isExternal()};
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
    std::vector<std::array<Id, NumColumnsIndexBuilding>>& input,
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
  //
  // NOTE: We deliberately read the triples one by one instead of deserializing
  // them into a `std::vector` (`reader >> triples`) and copying that into the
  // `IdTable`. The vector and the table would be alive at the same time, which
  // would double the memory footprint of this step.
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
inline void writePartialVocabularyToFile(const ItemVec& entries,
                                         const std::string& fileName) {
  AD_LOG_DEBUG << "Writing partial vocabulary to: " << fileName << "\n";
  PartialVocabularyWriter writer{fileName};
  for (const auto& entry : entries) {
    writer(entry.word_, entry.indexAndFlag_);
  }
  writer.finish();
  AD_LOG_DEBUG << "Done writing partial vocabulary\n";
}

// __________________________________________________________________________________________________
template <typename GeoSortKeyFn>
ItemVec vocabMapsToVector(const ItemMapAndBuffer& map,
                          const GeoSortKeyFn& geoSortKeyFn) {
  ItemVec entries;
  entries.resize(map.map_.size());
  using T = ItemVec::value_type;
  ql::ranges::transform(map.map_, entries.begin(),
                        [&geoSortKeyFn](auto& el) -> T {
                          return {el.first, el.second, geoSortKeyFn(el.first)};
                        });
  return entries;
}

// _____________________________________________________________________________
inline ItemVec vocabMapsToVector(const ItemMapAndBuffer& map) {
  return vocabMapsToVector(map, [](std::string_view) { return uint64_t{0}; });
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
