// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H

#include <future>
#include <limits>
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
#include "util/Serializer/BufferedSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Timer.h"

namespace ad_utility::vocabulary_merger {
// _________________________________________________________________
template <typename W, typename C>
auto mergeVocabulary(
    const std::string& basename, size_t numFiles, W comparator,
    C& internalWordCallback, ad_utility::MemorySize memoryToUse,
    const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  VocabularyMerger merger;
  return merger.mergeVocabulary(basename, numFiles, std::move(comparator),
                                internalWordCallback, memoryToUse,
                                blankNodeIriRegexes);
}

// _________________________________________________________________
template <typename W, typename C>
auto VocabularyMerger::mergeVocabulary(
    const std::string& basename, size_t numFiles, W comparator, C& wordCallback,
    ad_utility::MemorySize memoryToUse,
    const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  // Return true iff p1 >= p2 according to the lexicographic order of the IRI
  // or literal.
  auto lessThanForQueue = [&comparator](const QueueWord& p1,
                                        const QueueWord& p2) {
    return comparator(p1.iriOrLiteral(), p1.isExternal(), p2.iriOrLiteral(),
                      p2.isExternal());
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

  // The index of the partial vocabulary is stored in a `uint32_t` for each of
  // the (very many) ID map entries, see `QueuedIdMapEntry`.
  AD_CORRECTNESS_CHECK(numFiles <= std::numeric_limits<uint32_t>::max());
  for (std::size_t i : ad_utility::integerRange(numFiles)) {
    generators.push_back(makeWordRangeFromFile(i));
    idMaps_.emplace_back(absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i));
  }

  ad_utility::ProgressBar progressBar{metaData_.numWordsTotal(),
                                      "Words merged: "};
  idMapWriterQueue_.emplace(queueSize, 1, "Writing the ID maps");
  mergedWordsDestructionQueue_.emplace(queueSize, 1,
                                       "Destroying the merged words");
  wordWriterQueue_.emplace(queueSize, 1, "Writing the merged vocabulary");
  processWordBatch_ = [this, &wordCallback, &blankNodeIriRegexes,
                       &progressBar](WordBatch batch) {
    writeUniqueWordsToVocabulary(std::move(batch), wordCallback,
                                 blankNodeIriRegexes, progressBar);
  };
  startNewBatch();

  // Some memory (that is hard to measure exactly) is used for the writing of
  // a batch of merged words, so we only give 80% of the total memory to the
  // merging. This is very approximate and should be investigated in more
  // detail.
  auto mergedWords =
      ad_utility::parallelMultiwayMerge<QueueWord, true,
                                        decltype(sizeOfQueueWord)>(
          0.8 * memoryToUse, std::move(generators), lessThanForQueue);
  for (std::vector<QueueWord>& currentWords : mergedWords) {
    // NOTE: The buffer is deliberately not consumed, but kept alive as part of
    // the batch, such that the merged words neither have to be moved nor
    // destroyed by this thread.
    currentBatch_.mergedWordBuffers_.push_back(std::move(currentWords));
    processMergedWords(currentBatch_.mergedWordBuffers_.back(), comparator);
  }
  // Hand the remaining words to the pipeline and wait until all of them have
  // actually been written. NOTE: The order is important, see the declaration of
  // the queues.
  flushBatch();
  wordWriterQueue_.value().finish();
  mergedWordsDestructionQueue_.value().finish();
  idMapWriterQueue_.value().finish();

  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;

  auto metaData = std::move(metaData_);
  // completely reset all the inner state
  clear();
  return metaData;
}

// ________________________________________________________________________________
CPP_template_def(typename W)(requires WordComparator<W>) void VocabularyMerger::
    processMergedWords(const std::vector<QueueWord>& buffer,
                       [[maybe_unused]] const W& comparator) {
  auto& idMapBatch = currentBatch_.idMapBatch_;
  auto& entries = idMapBatch.entries_;
  size_t& numEntries = idMapBatch.numEntries_;
  // Each of the `buffer`'s words yields exactly one entry. The entries are
  // allocated in advance (see `startNewBatch`), so the following `resize`
  // (which would have to copy the entries that are already in the buffer)
  // typically is a no-op, and the writes below are simple unchecked stores.
  if (entries.size() < numEntries + buffer.size()) {
    entries.resize(numEntries + buffer.size());
  }

  // Iterate (avoid duplicates).
  for (const auto& top : buffer) {
    if (!hasLastWord_ || top.iriOrLiteral() != lastWord_) {
      AD_EXPENSIVE_CHECK(
          !hasLastWord_ || comparator(lastWord_, lastWordIsExternal_,
                                      top.iriOrLiteral(), top.isExternal()),
          "Total vocabulary order violated for ", lastWord_, " and ",
          top.iriOrLiteral());
      // NOTE: The word is not written to the vocabulary here, but only
      // collected in the `currentBatch_`, such that the writing (which also
      // determines the global ID of the word) happens concurrently to the
      // merging of the next words.
      lastWord_ = top.iriOrLiteral();
      lastWordIsExternal_ = top.isExternal();
      hasLastWord_ = true;
      currentBatch_.uniqueWords_.push_back(
          UniqueWord{lastWord_, top.isExternal()});
      ++indexOfLastWordInBatch_;
    } else {
      // If a word appears with different values for `isExternal`, then we
      // externalize it.
      lastWordIsExternal_ = lastWordIsExternal_ || top.isExternal();
    }
    // Remember the local index of the word and the distinct word it belongs to;
    // the actual entry of the ID map is only created (and written) once the
    // global ID of that distinct word is known.
    entries[numEntries] =
        QueuedIdMapEntry{static_cast<uint32_t>(top.partialFileId_),
                         indexOfLastWordInBatch_, top.id()};
    ++numEntries;
  }

  if (numEntries >= idMapEntryBatchSize) {
    flushBatch();
  }
}

// ________________________________________________________________________________
inline void VocabularyMerger::flushBatch() {
  if (currentBatch_.idMapBatch_.numEntries_ == 0) {
    return;
  }
  // The `lastWord_` is a view into one of the buffers that are handed over to
  // the other threads, so we have to store our own copy of it. NOTE: The `if`
  // is important: if no new word was merged since the last flush, then the
  // `lastWord_` already is a view into the `lastWordStorage_`, and assigning a
  // string from a view into itself is undefined behavior.
  if (lastWord_.data() != lastWordStorage_.data()) {
    lastWordStorage_.assign(lastWord_.data(), lastWord_.size());
    lastWord_ = lastWordStorage_;
  }
  wordWriterQueue_.value().push(
      [this, batch = std::move(currentBatch_)]() mutable {
        processWordBatch_(std::move(batch));
      });
  startNewBatch();
}

// ________________________________________________________________________________
inline void VocabularyMerger::startNewBatch() {
  // NOTE: A moved-from vector is in a valid but unspecified state, so we have
  // to explicitly reset the batch.
  currentBatch_ = WordBatch{};
  // The entries are stored in a vector with a `default_init_allocator`, so this
  // `resize` is a plain allocation that doesn't touch the memory.
  currentBatch_.idMapBatch_.entries_.resize(idMapEntryBatchSize);
  // A word typically occurs in several of the partial vocabularies, so most of
  // the merged words are duplicates. This is only a rough estimate; the vector
  // grows if it doesn't suffice.
  currentBatch_.uniqueWords_.reserve(idMapEntryBatchSize / 4);
  indexOfLastWordInBatch_ = 0;
}

// ________________________________________________________________________________
CPP_template_def(typename C)(requires WordCallback<C>) void VocabularyMerger::
    writeUniqueWordsToVocabulary(
        WordBatch batch, C& wordCallback,
        const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes,
        ad_utility::ProgressBar& progressBar) {
  AD_LOG_TIMING << "Start writing a batch of merged words\n";

  // TODO<optimization> If we aim to further speed this up, we could
  // order all the write requests to _outfile _externalOutfile and all the
  // idVecs to have a more useful external access pattern.
  auto& globalIds = batch.idMapBatch_.globalIds_;
  globalIds.resize(batch.uniqueWords_.size() + 1);
  globalIds.at(0) = lastGlobalId_;
  size_t i = 1;
  for (const auto& uniqueWord : batch.uniqueWords_) {
    const auto& word = uniqueWord.word_;
    if (isBlankNode(word, blankNodeIriRegexes)) {
      globalIds[i] = Id::makeFromBlankNodeIndex(
          BlankNodeIndex::make(metaData_.getNextBlankNodeIndex()));
    } else {
      auto wordIndex = wordCallback(word, uniqueWord.isExternal_);
      metaData_.addWord(word, wordIndex);
      globalIds[i] = Id::makeFromVocabIndex(VocabIndex::make(wordIndex));
    }
    ++i;
    if (progressBar.update()) {
      AD_LOG_INFO << progressBar.getProgressString() << std::flush;
    }
  }
  lastGlobalId_ = globalIds.back();

  // The merged words are no longer needed. Their destruction (which involves
  // freeing one string per word) is expensive enough to be done by yet another
  // thread. NOTE: The `clear()` is the actual work of this task; it happens on
  // the queue's thread, as does the destruction of the (then empty) buffers.
  mergedWordsDestructionQueue_.value().push(
      [buffers = std::move(batch.mergedWordBuffers_)]() mutable {
        buffers.clear();
      });

  idMapWriterQueue_.value().push(
      [this, idMapBatch = std::move(batch.idMapBatch_)]() {
        AD_LOG_TIMING << "Start writing a batch of ID map entries\n";
        const auto& globalIds = idMapBatch.globalIds_;
        for (size_t j = 0; j < idMapBatch.numEntries_; ++j) {
          const auto& entry = idMapBatch.entries_[j];
          idMaps_[entry.partialFileId_].push_back(IdMapEntry{
              entry.localIndex_, globalIds[entry.indexOfWordInBatch_]});
        }
      });
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
