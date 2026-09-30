// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/VocabularyMerger.h"
#include "index/vocabulary_merger/PartialVocabularyFile.h"
#include "index/vocabulary_merger/Segment.h"
#include "index/vocabulary_merger/SegmentCommitter.h"
#include "util/Allocator.h"
#include "util/Exception.h"
#include "util/GlobalExecutor.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/PostAndGetFuture.h"
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
  // The blocks of merged words are cut into segments behind the merge, see
  // `detail::buildSegment`.
  options.outputBlockSize = parallelBlockMerge::OutputBlockSize::both(
      VOCAB_MERGER_WORD_BATCH_SIZE, VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE);
  options.parallelismHint = ad_utility::globalExecutorNumThreads();
  const size_t inputPerChunk = VOCAB_MERGER_INPUT_PER_CHUNK.getBytes();
  const size_t numChunks =
      std::max<size_t>(1, (numInputBytes + inputPerChunk - 1) / inputPerChunk);
  options.targetChunksPerThread = std::max<size_t>(
      1, (numChunks + options.parallelism() - 1) / options.parallelism());
  // The decoded input blocks and the output of a chunk are several times
  // larger than their serialized form (every word is a `std::string`).
  constexpr size_t decodedFactor = 4;
  const uint64_t memoryPerChunk = (std::max<size_t>(numPartialVocabularies, 1) *
                                       PARTIAL_VOCAB_BLOCK_SIZE.getBytes() +
                                   inputPerChunk) *
                                  decodedFactor;
  options.maxNumChunksInFlight = std::clamp<size_t>(
      memoryToUse.getBytes() / memoryPerChunk, 1, options.parallelism());
  return options;
}

// The maximal number of segments (see `index/vocabulary_merger/Segment.h`)
// that are being built or waiting to be committed at the same time: two per
// thread, so that the pool is never starved, but no more than the memory
// limit allows (see `VOCAB_MERGER_MEMORY_PER_SEGMENT`).
inline size_t maxNumSegmentsInFlight(ad_utility::MemorySize memoryToUse) {
  size_t byThreads = 2 * std::max<size_t>(1, globalExecutorNumThreads());
  size_t byMemory =
      memoryToUse.getBytes() / VOCAB_MERGER_MEMORY_PER_SEGMENT.getBytes();
  return std::clamp<size_t>(byMemory, 1, byThreads);
}

// _________________________________________________________________
template <typename W>
auto mergeVocabulary(const std::string& basename, size_t numPartialVocabularies,
                     W comparator, ParallelWordWriterBase& writer,
                     ad_utility::MemorySize memoryToUse,
                     const ad_utility::RegexSet& blankNodeIriRegexes)
    -> CPP_ret(VocabularyMetaData)(requires WordComparator<W>) {
  using detail::QueueWord;
  using detail::Segment;
  // Return true iff `p1` is smaller than `p2` according to the order of the
  // IRI or literal.
  auto lessThanForQueue = [&comparator](const QueueWord& p1,
                                        const QueueWord& p2) {
    return comparator(p1.iriOrLiteral(), p2.iriOrLiteral());
  };
  AD_CORRECTNESS_CHECK(numPartialVocabularies <=
                       std::numeric_limits<uint32_t>::max());

  // Merge the partial vocabularies in parallel, see
  // `util/parallelBlockMerge/ParallelBlockMerge.h`: the words are split into
  // ranges by the block index of the partial vocabularies, each range is merged
  // by a chunk of its own on the global thread pool, and this thread receives
  // the merged blocks in the order of the vocabulary. The blocks stay in
  // memory, and a chunk keeps its slot until this thread has taken its output,
  // see `vocabularyMergeOptions`.
  PartialVocabularyRunsInput input{basename, numPartialVocabularies};
  size_t numBlocks = 0;
  size_t numInputWords = 0;
  size_t numInputBytes = 0;
  for (size_t run = 0; run < input.numRuns(); ++run) {
    for (size_t block = 0; block < input.numBlocks(run); ++block) {
      numInputWords += input.numElementsInBlock(run, block);
      numInputBytes += input.numBytesInBlock(run, block);
    }
    numBlocks += input.numBlocks(run);
  }
  auto options = vocabularyMergeOptions(numPartialVocabularies, numInputBytes,
                                        memoryToUse);
  const size_t maxSegmentsInFlight = maxNumSegmentsInFlight(memoryToUse);
  AD_LOG_INFO << "Merging " << input.numRuns() << " partial vocabularies ("
              << numInputWords << " words in " << numBlocks << " blocks) in "
              << options.targetNumChunks() << " chunks, using "
              << options.parallelism() << " threads, up to "
              << options.maxNumChunksInFlight << " chunks and up to "
              << maxSegmentsInFlight << " segments in flight ..." << std::endl;
  auto mergedWords = parallelBlockMerge::parallelBlockMergeToRange<true>(
      ad_utility::globalExecutor(), std::move(input), lessThanForQueue,
      parallelBlockMerge::makeInMemoryStorageFactory<
          PartialVocabularyRunsInput::Block>(
          VOCAB_MERGER_NUM_BUFFERED_BLOCKS_PER_CHUNK,
          /*releaseChunkOnConsumption=*/true),
      std::move(options));

  // The merged blocks are cut into segments, each of which is built by a task
  // on the global thread pool (see `detail::buildSegment`), and the finished
  // segments are committed in order (see `detail::SegmentCommitter`). A cut
  // between two blocks is only allowed if the last word of the one differs
  // from the first word of the other, so that no word spans two segments.
  detail::SegmentCommitter committer{
      writer,
      partialVocabularyIdMapFilenames(basename, numPartialVocabularies)};
  std::deque<std::future<std::shared_ptr<const Segment>>> segments;
  std::vector<std::vector<QueueWord>> currentBlocks;
  size_t currentNumWords = 0;
  ad_utility::Timer waitTimer{ad_utility::Timer::Started};
  ad_utility::Timer commitTimer{ad_utility::Timer::Stopped};
  std::atomic<uint64_t> segmentBusyMs{0};
  auto postSegment = [&]() {
    if (currentBlocks.empty() || committer.hasFailed()) {
      currentBlocks.clear();
      return;
    }
    segments.push_back(ad_utility::postAndGetFuture(
        ad_utility::globalExecutor(),
        [blocks = std::move(currentBlocks), &writer, &blankNodeIriRegexes,
         numPartialVocabularies, &comparator, &segmentBusyMs]() mutable {
          ad_utility::Timer timer{ad_utility::Timer::Started};
          auto segment = std::make_shared<const Segment>(detail::buildSegment(
              std::move(blocks), writer, blankNodeIriRegexes,
              numPartialVocabularies, comparator));
          segmentBusyMs += timer.msecs().count();
          return segment;
        }));
    currentBlocks.clear();
    currentNumWords = 0;
  };
  // Commit the segments that are done, in order, and at least one segment if
  // `waitForOne` is set.
  auto commitFinishedSegments = [&](bool waitForOne) {
    commitTimer.cont();
    while (!segments.empty()) {
      auto& next = segments.front();
      if (!waitForOne &&
          next.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        break;
      }
      committer.commit(next.get());
      segments.pop_front();
      waitForOne = false;
    }
    commitTimer.stop();
  };

  for (std::vector<QueueWord>& block : mergedWords) {
    waitTimer.stop();
    // Stop merging as soon as one of the stages has failed, the exception is
    // rethrown by `finish()` below.
    if (committer.hasFailed()) {
      break;
    }
    if (!block.empty()) {
      bool canCutHere = currentNumWords >= VOCAB_MERGER_SEGMENT_NUM_WORDS &&
                        currentBlocks.back().back().iriOrLiteral() !=
                            block.front().iriOrLiteral();
      if (canCutHere) {
        postSegment();
      }
      currentNumWords += block.size();
      currentBlocks.push_back(std::move(block));
    }
    commitFinishedSegments(segments.size() >= maxSegmentsInFlight);
    waitTimer.cont();
  }
  waitTimer.stop();
  postSegment();
  while (!segments.empty()) {
    commitFinishedSegments(true);
  }
  AD_LOG_INFO << "Time spent by the thread behind the merge: waiting for "
                 "merged blocks "
              << waitTimer.msecs() << ", committing the segments "
              << commitTimer.msecs() << "; building the segments "
              << segmentBusyMs << " ms" << std::endl;
  return committer.finish();
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
inline void writePartialVocabularyToFile(const ItemVec& els,
                                         const std::string& fileName) {
  AD_LOG_DEBUG << "Writing partial vocabulary to: " << fileName << "\n";
  PartialVocabularyWriter writer{fileName};
  for (const auto& [word, idAndExternal] : els) {
    writer(word, idAndExternal.isExternal(), idAndExternal.id());
  }
  writer.finish();
  AD_LOG_DEBUG << "Done writing partial vocabulary\n";
}

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
