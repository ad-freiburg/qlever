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
namespace detail {

// ________________________________________________________________________________
CPP_template_def(typename W,
                 typename F)(requires WordComparator<W> CPP_and_def
                                 WordBatchCallback<F>) void WordBatchBuilder::
    addMergedWords(std::vector<QueueWord> buffer,
                   [[maybe_unused]] const W& comparator,
                   const F& batchCallback) {
  // NOTE: The buffer is deliberately not consumed, but kept alive as part of
  // the batch, such that the merged words neither have to be moved nor
  // destroyed by the merging thread.
  currentBatch_.mergedWordBuffers_.push_back(std::move(buffer));
  const auto& words = currentBatch_.mergedWordBuffers_.back();

  auto& idMapBatch = currentBatch_.idMapBatch_;
  auto& entries = idMapBatch.entries_;
  size_t& numEntries = idMapBatch.numEntries_;
  // Each of the `words` yields exactly one entry. The entries are allocated in
  // advance (see `startNewBatch`), so the following `resize` (which would have
  // to copy the entries that are already in the buffer) typically is a no-op,
  // and the writes below are simple unchecked stores.
  if (entries.size() < numEntries + words.size()) {
    entries.resize(numEntries + words.size());
  }

  // Iterate (avoid duplicates).
  for (const auto& top : words) {
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
    flush(batchCallback);
  }
}

// ________________________________________________________________________________
CPP_template_def(typename F)(
    requires WordBatchCallback<
        F>) void WordBatchBuilder::flush(const F& batchCallback) {
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
  batchCallback(std::move(currentBatch_));
  startNewBatch();
}

// ________________________________________________________________________________
inline void WordBatchBuilder::startNewBatch() {
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
CPP_template_def(typename C)(requires WordCallback<C>)
    IdMapBatch VocabularyWriter::writeWordsToVocabulary(
        const std::vector<UniqueWord>& uniqueWords, IdMapBatch idMapBatch,
        C& wordCallback,
        const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes) {
  AD_LOG_TIMING << "Start writing a batch of merged words\n";

  // TODO<optimization> If we aim to further speed this up, we could
  // order all the write requests to _outfile _externalOutfile and all the
  // idVecs to have a more useful external access pattern.
  auto& globalIds = idMapBatch.globalIds_;
  globalIds.resize(uniqueWords.size() + 1);
  globalIds.at(0) = lastGlobalId_;
  size_t i = 1;
  for (const auto& uniqueWord : uniqueWords) {
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
    if (progressBar_.update()) {
      AD_LOG_INFO << progressBar_.getProgressString() << std::flush;
    }
  }
  lastGlobalId_ = globalIds.back();
  return idMapBatch;
}

// ________________________________________________________________________________
inline void VocabularyWriter::logFinalProgress() {
  AD_LOG_INFO << progressBar_.getFinalProgressString() << std::flush;
}

// ________________________________________________________________________________
inline IdMapBatchWriter::IdMapBatchWriter(const std::string& basename,
                                          size_t numFiles) {
  // The index of the partial vocabulary is stored in a `uint32_t` for each of
  // the (very many) ID map entries, see `QueuedIdMapEntry`.
  AD_CORRECTNESS_CHECK(numFiles <= std::numeric_limits<uint32_t>::max());
  idMaps_.reserve(numFiles);
  for (size_t i : ad_utility::integerRange(numFiles)) {
    idMaps_.emplace_back(absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i));
  }
}

// ________________________________________________________________________________
inline void IdMapBatchWriter::writeBatch(const IdMapBatch& batch) {
  AD_LOG_TIMING << "Start writing a batch of ID map entries\n";
  const auto& globalIds = batch.globalIds_;
  for (size_t i = 0; i < batch.numEntries_; ++i) {
    const auto& entry = batch.entries_[i];
    idMaps_[entry.partialFileId_].push_back(
        IdMapEntry{entry.localIndex_, globalIds[entry.indexOfWordInBatch_]});
  }
}

// ________________________________________________________________________________
inline void IdMapBatchWriter::finish() {
  for (auto& idMap : idMaps_) {
    idMap.finish();
  }
}

// ________________________________________________________________________________
CPP_template_def(typename C)(
    requires WordCallback<C>) void VocabularyMergePipeline::
    push(WordBatch batch, C& wordCallback,
         const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes) {
  wordWriterQueue_.push([this, batch = std::move(batch), &wordCallback,
                         &blankNodeIriRegexes]() mutable {
    auto idMapBatch = vocabularyWriter_.writeWordsToVocabulary(
        batch.uniqueWords_, std::move(batch.idMapBatch_), wordCallback,
        blankNodeIriRegexes);

    // The merged words are no longer needed. Their destruction (which involves
    // freeing one string per word) is expensive enough to be done by yet
    // another thread. NOTE: The `clear()` is the actual work of this task; it
    // happens on the queue's thread, as does the destruction of the (then
    // empty) buffers.
    mergedWordsDestructionQueue_.push(
        [buffers = std::move(batch.mergedWordBuffers_)]() mutable {
          buffers.clear();
        });

    idMapWriterQueue_.push([this, idMapBatch = std::move(idMapBatch)]() {
      idMapBatchWriter_.writeBatch(idMapBatch);
    });
  });
}

// ________________________________________________________________________________
inline VocabularyMetaData VocabularyMergePipeline::finish() {
  // NOTE: The order is important, see the declaration of the members.
  wordWriterQueue_.finish();
  mergedWordsDestructionQueue_.finish();
  idMapWriterQueue_.finish();
  idMapBatchWriter_.finish();
  vocabularyWriter_.logFinalProgress();
  return std::move(vocabularyWriter_.metaData());
}
}  // namespace detail

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
  batchBuilder.flush(batchCallback);
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
