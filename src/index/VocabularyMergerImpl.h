// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H

#include <algorithm>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/VocabularyMerger.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Log.h"
#include "util/ParallelBlockMerge.h"
#include "util/ProgressBar.h"
#include "util/Serializer/BufferedSerializer.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Timer.h"
#include "util/Views.h"

namespace ad_utility::vocabulary_merger {
namespace detail {
// A read serializer for a partial vocabulary file that additionally tracks the
// byte offset of the next record. This is what makes the sequential fallback
// scan of `PartialVocabRunsInput` independent of the exact size of a single
// record.
//
// NOTE: The inheritance is deliberate and not only a shortcut: the
// serialization framework finds its `operator>>` (and the `operator|` and
// `serialize` that it calls) via argument-dependent lookup, for which the
// namespace of the base class has to be `ad_utility::serialization`.
class CountingFileReadSerializer
    : public ad_utility::serialization::CopyableFileReadSerializer {
 private:
  using Base = ad_utility::serialization::CopyableFileReadSerializer;
  // Seeking would silently invalidate the tracked position, so it is not part
  // of the interface of this class.
  using Base::setSerializationPosition;

  uint64_t position_;

 public:
  // Construct from the (shared) `file` and the byte offset at which the reading
  // starts.
  CountingFileReadSerializer(std::shared_ptr<ad_utility::File> file,
                             uint64_t position)
      : Base{std::move(file)}, position_{position} {
    Base::setSerializationPosition(position_);
  }

  // Read the next `numBytes` bytes and advance the position. This deliberately
  // hides (and does not override, the base class has no virtual functions) the
  // function of the base class; the serialization framework always calls it via
  // the static type and therefore always ends up here.
  void serializeBytes(char* bytePtr, size_t numBytes) {
    Base::serializeBytes(bytePtr, numBytes);
    position_ += numBytes;
  }

  // Return the byte offset of the next byte that will be read.
  uint64_t position() const { return position_; }
};
}  // namespace detail

// ____________________________________________________________________________
inline PartialVocabRunsInput::PartialVocabRunsInput(
    const std::string& basename, size_t numFiles,
    size_t fallbackSplitterInterval)
    : fallbackSplitterInterval_{fallbackSplitterInterval} {
  AD_CONTRACT_CHECK(fallbackSplitterInterval > 0);
  runs_.reserve(numFiles);
  for (size_t i = 0; i < numFiles; ++i) {
    runs_.push_back(
        readRun(absl::StrCat(basename, PARTIAL_VOCAB_WORDS_INFIX, i)));
  }
}

// ____________________________________________________________________________
inline size_t PartialVocabRunsInput::numElementsInBlock(size_t run,
                                                        size_t block) const {
  const auto& r = runs_.at(run);
  uint64_t end = block + 1 < r.blocks_.size()
                     ? r.blocks_[block + 1].numWordsBefore_
                     : r.numWords_;
  return static_cast<size_t>(end - r.blocks_.at(block).numWordsBefore_);
}

// ____________________________________________________________________________
inline PartialVocabRunsInput::Block PartialVocabRunsInput::readBlock(
    size_t run, size_t block) const {
  const auto& r = runs_.at(run);
  ad_utility::serialization::CopyableFileReadSerializer reader{r.file_};
  reader.setSerializationPosition(r.blocks_.at(block).byteOffset_);
  size_t numElements = numElementsInBlock(run, block);
  Block result;
  result.reserve(numElements);
  for (size_t i = 0; i < numElements; ++i) {
    TripleComponentWithIndex word;
    reader >> word;
    result.emplace_back(std::move(word), run);
  }
  return result;
}

// ____________________________________________________________________________
inline ad_utility::MemorySize PartialVocabRunsInput::maxBlockMemory() const {
  size_t result = 0;
  for (size_t run = 0; run < numRuns(); ++run) {
    const auto& r = runs_.at(run);
    for (size_t block = 0; block < r.blocks_.size(); ++block) {
      uint64_t end = block + 1 < r.blocks_.size()
                         ? r.blocks_[block + 1].byteOffset_
                         : r.wordsEnd_;
      // The words of a block occupy (slightly less than) as many bytes in
      // memory as they do in the file, plus the fixed size of a `QueueWord` for
      // each of them.
      size_t memory = static_cast<size_t>(end - r.blocks_[block].byteOffset_) +
                      numElementsInBlock(run, block) * sizeof(QueueWord);
      result = std::max(result, memory);
    }
  }
  return ad_utility::MemorySize::bytes(result);
}

// ____________________________________________________________________________
inline PartialVocabRunsInput::Run PartialVocabRunsInput::readRun(
    const std::string& filename) const {
  Run run;
  run.file_ = std::make_shared<ad_utility::File>(filename, "r");
  AD_CONTRACT_CHECK(run.file_->isOpen());
  auto fileSize = static_cast<uint64_t>(ql::filesystem::file_size(filename));
  AD_CORRECTNESS_CHECK(fileSize >= sizeof(uint64_t), "The partial vocabulary ",
                       filename, " is corrupted");
  ad_utility::serialization::CopyableFileReadSerializer reader{run.file_};
  reader >> run.numWords_;
  // The record of a single word consists of the size of the word, the word
  // itself, the `isExternal` flag, and the id, so the file has to hold at least
  // that many bytes per word. Checking this here turns a truncated file into a
  // proper error message instead of a read past the end of the file.
  constexpr uint64_t minRecordSize = 2 * sizeof(uint64_t) + sizeof(bool);
  AD_CORRECTNESS_CHECK(
      (fileSize - sizeof(uint64_t)) / minRecordSize >= run.numWords_,
      "The partial vocabulary ", filename, " is corrupted, it claims to hold ",
      run.numWords_, " words, but its size is only ", fileSize, " bytes");
  if (!readSplitterIndex(run, fileSize)) {
    buildIndexByScanning(run, fileSize);
  }
  return run;
}

// ____________________________________________________________________________
inline bool PartialVocabRunsInput::readSplitterIndex(Run& run,
                                                     uint64_t fileSize) {
  namespace ser = ad_utility::serialization;
  // The footer consists of the start offset of the index and the magic number.
  constexpr uint64_t footerSize = 2 * sizeof(uint64_t);
  // The size of the smallest possible entry of the splitter index, which is one
  // with two words of length zero.
  constexpr uint64_t minEntrySize = 4 * sizeof(uint64_t) + 2 * sizeof(bool);
  if (fileSize < footerSize) {
    return false;
  }
  ser::CopyableFileReadSerializer reader{run.file_};
  reader.setSerializationPosition(fileSize - footerSize);
  uint64_t indexStart = 0;
  uint64_t magic = 0;
  reader >> indexStart;
  reader >> magic;
  if (magic != PARTIAL_VOCAB_INDEX_MAGIC) {
    return false;
  }
  // The index has to start behind the `numWords` header, and it has to leave
  // room for its own number of entries and for the footer.
  if (indexStart < sizeof(uint64_t) ||
      indexStart + sizeof(uint64_t) > fileSize - footerSize) {
    return false;
  }

  // Read the complete index into memory first, such that a broken index can
  // never lead to a read past the end of the file.
  std::vector<char> buffer(fileSize - footerSize - indexStart);
  reader.setSerializationPosition(indexStart);
  reader.serializeBytes(buffer.data(), buffer.size());
  size_t bufferSize = buffer.size();
  ser::ByteBufferReadSerializer indexReader{std::move(buffer)};
  std::vector<BlockMetadata> blocks;
  try {
    uint64_t numEntries = 0;
    indexReader >> numEntries;
    if (numEntries > bufferSize / minEntrySize) {
      return false;
    }
    blocks.reserve(numEntries);
    for (uint64_t i = 0; i < numEntries; ++i) {
      BlockMetadata block;
      indexReader >> block.byteOffset_;
      indexReader >> block.numWordsBefore_;
      indexReader >> block.firstKey_.word_;
      indexReader >> block.firstKey_.isExternal_;
      indexReader >> block.lastKey_.word_;
      indexReader >> block.lastKey_.isExternal_;
      blocks.push_back(std::move(block));
    }
  } catch (const std::exception&) {
    return false;
  }

  // Check that the index is consistent with the rest of the file: the blocks
  // have to cover the words from the beginning on, and their byte offsets have
  // to point into the part of the file that holds the words.
  if (blocks.empty() != (run.numWords_ == 0)) {
    return false;
  }
  uint64_t previousNumWordsBefore = 0;
  uint64_t previousByteOffset = 0;
  for (const auto& block : blocks) {
    bool isFirst = &block == blocks.data();
    if (isFirst != (block.numWordsBefore_ == 0) ||
        block.numWordsBefore_ >= run.numWords_ ||
        block.byteOffset_ < sizeof(uint64_t) ||
        block.byteOffset_ >= indexStart ||
        (!isFirst && (block.numWordsBefore_ <= previousNumWordsBefore ||
                      block.byteOffset_ <= previousByteOffset))) {
      return false;
    }
    previousNumWordsBefore = block.numWordsBefore_;
    previousByteOffset = block.byteOffset_;
  }

  run.blocks_ = std::move(blocks);
  run.wordsEnd_ = indexStart;
  return true;
}

// ____________________________________________________________________________
inline void PartialVocabRunsInput::buildIndexByScanning(
    Run& run, uint64_t fileSize) const {
  AD_LOG_DEBUG << "The partial vocabulary " << run.file_->name()
               << " has no valid sparse index, building the block metadata by "
                  "a sequential scan"
               << std::endl;
  detail::CountingFileReadSerializer reader{run.file_, sizeof(uint64_t)};
  run.blocks_.clear();
  for (uint64_t i = 0; i < run.numWords_; ++i) {
    uint64_t byteOffset = reader.position();
    TripleComponentWithIndex word;
    reader >> word;
    Key key{std::move(word.iriOrLiteral_), word.isExternal_};
    if (i % fallbackSplitterInterval_ == 0) {
      run.blocks_.push_back(BlockMetadata{byteOffset, i, key, std::move(key)});
    } else {
      run.blocks_.back().lastKey_ = std::move(key);
    }
  }
  run.wordsEnd_ = reader.position();
  AD_CORRECTNESS_CHECK(run.wordsEnd_ <= fileSize);
}

// ____________________________________________________________________________
inline ad_utility::parallelBlockMerge::MergeOptions
mergeOptionsForPartialVocabularies(const PartialVocabRunsInput& input,
                                   ad_utility::MemorySize memoryToUse,
                                   size_t maxParallelism) {
  using ad_utility::MemorySize;
  ad_utility::parallelBlockMerge::MergeOptions options;
  maxParallelism = std::max<size_t>(1, maxParallelism);

  // Some memory (that is hard to measure exactly) is used for the writing of
  // a batch of merged words, so we only give 80% of the total memory to the
  // merging. This is very approximate and should be investigated in more
  // detail.
  MemorySize budget = 0.8 * memoryToUse;
  // One half of the budget is for the blocks that are read from the partial
  // vocabularies, the other half for the blocks of merged words.
  MemorySize inputBudget = budget / 2;

  // Every chunk that is in flight holds one input block per run at a time, so
  // bound the number of chunks accordingly. A single chunk is always allowed,
  // in which case the merge simply runs serially.
  size_t memoryPerChunk = input.maxBlockMemory().getBytes() * input.numRuns();
  size_t maxInFlight = maxParallelism;
  if (memoryPerChunk > 0) {
    maxInFlight =
        std::min(maxParallelism,
                 std::max<size_t>(1, inputBudget.getBytes() / memoryPerChunk));
  }
  options.maxInFlightChunks = maxInFlight;

  // Every chunk that is in flight may have `bufferedBlocksPerChunk` finished
  // output blocks waiting in the sink plus one that it is currently filling,
  // and the consumer holds one further block.
  size_t numLiveOutputBlocks =
      maxInFlight * (options.bufferedBlocksPerChunk + 1) + 1;
  // NOTE: The lower bound keeps the blocks (and therefore the calls to the
  // consumer of the merge) from becoming pathologically small for a very small
  // memory limit; it is never reached for the memory limits of an actual index
  // build.
  options.maxOutputBlockMemory = std::max(
      MemorySize::kilobytes(64), (budget - inputBudget) / numLiveOutputBlocks);
  // The memory limit above is what actually bounds the size of an output block,
  // so the limit on the number of elements is set to a value that is never
  // reached.
  options.outputBlockSize = std::numeric_limits<size_t>::max();
  return options;
}

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
  // Return true iff the first argument is smaller than the second one according
  // to the lexicographic order of the IRI or literal. This is deliberately
  // generic, because the parallel merge (see `ParallelBlockMerge.h`) compares
  // elements with elements, keys with keys, and elements with keys in both
  // orders, and because it is also used for the `TripleComponentWithIndex`es of
  // `writeQueueWordsToIdMap` below.
  auto lessThan = [&comparator](const auto& a, const auto& b) {
    return comparator(a.iriOrLiteral(), a.isExternal(), b.iriOrLiteral(),
                      b.isExternal());
  };
  // The comparator is copied into the merge, and from there into every single
  // chunk of the merge, so copying it has to stay cheap. In particular the
  // `TripleComponentComparator` of the index build holds six `icu::Collator`s,
  // all of which its copy constructor creates anew; the whole chain of captures
  // from `IndexImpl::passFileForVocabulary` down to here therefore passes it on
  // by reference. Note that this also means that all chunk threads share a
  // single comparator, which is correct because the comparators are thread-safe
  // as long as they are only used via their `const` interface.
  static_assert(sizeof(decltype(lessThan)) <= 2 * sizeof(void*),
                "The comparator of the vocabulary merge has become too large, "
                "see the comment above");

  // Open the partial vocabularies and prepare the file-based output vectors.
  PartialVocabRunsInput input{basename, numFiles};
  for (std::size_t i : ad_utility::integerRange(numFiles)) {
    idMaps_.emplace_back(absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i));
  }

  namespace pbm = ad_utility::parallelBlockMerge;
  auto scheduler = pbm::defaultMergeScheduler();
  auto options = mergeOptionsForPartialVocabularies(
      input, memoryToUse, scheduler->maxParallelism());
  auto mergedWords = pbm::parallelBlockMergeToRange</*moveElements=*/true>(
      std::move(input), lessThan, options, std::move(scheduler));
  ad_utility::ProgressBar progressBar{metaData_.numWordsTotal(),
                                      "Words merged: "};
  for (std::vector<QueueWord>& currentWords : mergedWords) {
    writeQueueWordsToIdMap(currentWords, wordCallback, lessThan,
                           blankNodeIriRegexes, progressBar);
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
    writeQueueWordsToIdMap(
        std::vector<QueueWord>& buffer, C& wordCallback, const L& lessThan,
        const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes,
        ad_utility::ProgressBar& progressBar) {
  AD_LOG_TIMING << "Start writing a batch of merged words\n";

  // Iterate (avoid duplicates).
  for (auto& top : buffer) {
    if (!lastTripleComponent_.has_value() ||
        top.iriOrLiteral() != lastTripleComponent_.value().iriOrLiteral()) {
      if (lastTripleComponent_.has_value()) {
        AD_CORRECTNESS_CHECK(lessThan(lastTripleComponent_.value(), top.entry_),
                             "Total vocabulary order violated for ",
                             lastTripleComponent_->iriOrLiteral(), " and ",
                             top.iriOrLiteral());
      }
      lastTripleComponent_ =
          TripleComponentWithIndex{std::move(top.iriOrLiteral()),
                                   top.isExternal(), metaData_.numWordsTotal()};
      lastTripleComponentIsBlankNode_ =
          lastTripleComponent_.value().isBlankNode(blankNodeIriRegexes);

      // TODO<optimization> If we aim to further speed this up, we could
      // order all the write requests to _outfile _externalOutfile and all the
      // idVecs to have a more useful external access pattern.

      // Write the new word to the vocabulary.
      auto& nextWord = lastTripleComponent_.value();
      if (lastTripleComponentIsBlankNode_) {
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
        lastTripleComponentIsBlankNode_
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
                                         const std::string& fileName,
                                         size_t splitterInterval) {
  AD_LOG_DEBUG << "Writing partial vocabulary to: " << fileName << "\n";
  AD_CONTRACT_CHECK(splitterInterval > 0);

  // A single entry of the sparse splitter index, holding the information about
  // one virtual block of `splitterInterval` consecutive words. The words are
  // stored as `std::string_view`s into `els`, which outlives this function.
  struct SplitterEntry {
    uint64_t byteOffsetOfFirstWordOfBlock_;
    uint64_t numWordsBeforeThisBlock_;
    std::string_view firstWord_;
    bool firstIsExternal_;
    std::string_view lastWord_;
    bool lastIsExternal_;
  };
  std::vector<SplitterEntry> splitters;
  splitters.reserve((els.size() + splitterInterval - 1) / splitterInterval);

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
  uint64_t wordIndex = 0;
  for (const auto& [word, idAndExternal] : els) {
    // Every `splitterInterval` words we remember the exact byte offset at which
    // the next word record starts, such that a reader can seek there directly.
    // Note that `getSerializationPosition` also accounts for the bytes that are
    // still sitting in the buffer of the `BufferedWriteSerializer`.
    if (wordIndex % splitterInterval == 0) {
      splitters.push_back(SplitterEntry{
          serializer.getSerializationPosition(), wordIndex, word,
          idAndExternal.isExternal(), word, idAndExternal.isExternal()});
    } else {
      splitters.back().lastWord_ = word;
      splitters.back().lastIsExternal_ = idAndExternal.isExternal();
    }
    ++wordIndex;

    // When merging the vocabulary, we need the actual word, the (internal) id
    // we have assigned to this word, and the information, whether this word
    // belongs to the internal or external vocabulary.
    serializer << word;
    serializer << idAndExternal.isExternal();
    serializer << idAndExternal.id();
  }

  // Append the sparse splitter index, followed by the 16-byte footer that
  // allows a reader to locate it (see the documentation of this function in
  // `VocabularyMerger.h` for the exact layout).
  uint64_t indexStartOffset = serializer.getSerializationPosition();
  serializer << static_cast<uint64_t>(splitters.size());
  for (const auto& splitter : splitters) {
    serializer << splitter.byteOffsetOfFirstWordOfBlock_;
    serializer << splitter.numWordsBeforeThisBlock_;
    serializer << splitter.firstWord_;
    serializer << splitter.firstIsExternal_;
    serializer << splitter.lastWord_;
    serializer << splitter.lastIsExternal_;
  }
  serializer << indexStartOffset;
  serializer << PARTIAL_VOCAB_INDEX_MAGIC;

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
  return ad_utility::HashMap<Id, Id>{vec.begin(), vec.end()};
}
}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
