// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H

#include <fstream>
#include <future>
#include <iostream>
#include <queue>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Vocabulary.h"
#include "index/VocabularyMerger.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Conversions.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/ParallelMultiwayMerge.h"
#include "util/ProgressBar.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/SourceLocation.h"
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

// ____________________________________________________________________________
// Helper: Create a generator that reads words from a partial vocabulary file.
inline auto VocabularyMerger::makePartialVocabGenerator(
    const std::string& basename, size_t fileIndex) {
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
}

// _________________________________________________________________
template <typename W, typename C>
auto VocabularyMerger::mergeVocabulary(const std::string& basename,
                                       size_t numFiles, W comparator,
                                       C& wordCallback,
                                       ad_utility::MemorySize memoryToUse)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  // If we have too many files, use two-stage merging to avoid hitting
  // file descriptor limits.
  if (numFiles > MAX_NUM_FILES_FOR_DIRECT_MERGE) {
    AD_LOG_INFO << "Using two-stage vocabulary merging for " << numFiles
                << " files (limit: " << MAX_NUM_FILES_FOR_DIRECT_MERGE << ")"
                << std::endl;
    return mergeTwoStage(basename, numFiles, std::move(comparator),
                         wordCallback, memoryToUse);
  }

  // Create comparators
  auto [lessThan, lessThanForQueue] = makeComparators(comparator);

  // Create input generators for all partial vocabulary files
  std::vector<decltype(makePartialVocabGenerator(basename, 0))> generators;
  generators.reserve(numFiles);

  for (std::size_t i : ad_utility::integerRange(numFiles)) {
    generators.push_back(makePartialVocabGenerator(basename, i));
    idMaps_.emplace_back(absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i));
  }

  std::vector<QueueWord> sortedBuffer;
  sortedBuffer.reserve(bufferSize_);

  std::future<void> writeFuture;

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
  for (QueueWord& currentWord : ql::views::join(mergedWords)) {
    // Accumulate the globally ordered queue words in a buffer.
    sortedBuffer.push_back(std::move(currentWord));

    if (sortedBuffer.size() >= bufferSize_) {
      // Wait for the (asynchronous) writing of the last batch of words, and
      // trigger the (again asynchronous) writing of the next batch.
      auto writeTask = [this, buffer = std::move(sortedBuffer), &wordCallback,
                        &lessThan, &progressBar]() {
        this->writeQueueWordsToIdMap(buffer, wordCallback, lessThan,
                                     progressBar);
      };
      sortedBuffer.clear();
      sortedBuffer.reserve(bufferSize_);
      // wait for the last batch

      AD_LOG_TIMING << "A new batch of words is ready" << std::endl;
      // First wait for the last batch to finish, that way there will be no
      // race conditions.
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
    writeQueueWordsToIdMap(sortedBuffer, wordCallback, lessThan, progressBar);
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
    writeQueueWordsToIdMap(const std::vector<QueueWord>& buffer,
                           C& wordCallback, const L& lessThan,
                           ad_utility::ProgressBar& progressBar) {
  AD_LOG_TIMING << "Start writing a batch of merged words\n";

  // Smaller grained buffer for the actual inner write.
  auto bufSize = bufferSize_ / 5;
  std::future<void> writeFut;
  std::vector<std::pair<size_t, std::pair<size_t, Id>>> writeBuffer;
  writeBuffer.reserve(bufSize);

  // Iterate (avoid duplicates).
  for (auto& top : buffer) {
    // Process the word and update metadata
    processQueueWord(top, wordCallback, lessThan, progressBar);

    // @Claude: Thee following code snippet (the getting of the target ID) is
    // duplicated with the 1-stage, and the 2-stage approach, please factor out.

    // Get the current word (which has been updated by processQueueWord)
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
    idMaps_[id].push_back(
        {Id::makeFromVocabIndex(VocabIndex::make(value.first)), value.second});
  }
}

// ____________________________________________________________________________
// Helper: Create comparator functions from a word comparator.
template <typename W>
auto VocabularyMerger::makeComparators(W comparator) {
  // Comparator for TripleComponentWithIndex
  auto lessThan = [comp = std::move(comparator)](
                      const TripleComponentWithIndex& t1,
                      const TripleComponentWithIndex& t2) {
    return comp(t1.iriOrLiteral_, t2.iriOrLiteral_);
  };

  // Comparator for QueueWord
  auto lessThanForQueue = [lessThan](const QueueWord& p1, const QueueWord& p2) {
    return lessThan(p1.entry_, p2.entry_);
  };

  return std::make_pair(std::move(lessThan), std::move(lessThanForQueue));
}

// ____________________________________________________________________________
// Helper: Create a generator that reads words from a batch vocabulary file.
inline auto VocabularyMerger::makeBatchVocabGenerator(
    const std::string& basename, size_t batchIndex) {
  ad_utility::serialization::FileReadSerializer infile{
      absl::StrCat(basename, BATCH_VOCAB_WORDS_INFIX, batchIndex)};
  uint64_t numWords;
  infile >> numWords;

  return ad_utility::CachingTransformInputRange{
      ad_utility::integerRange(numWords),
      [batchIndex, infile{std::move(infile)}](
          [[maybe_unused]] const std::size_t i) mutable {
        TripleComponentWithIndex val;
        infile >> val;
        return QueueWord{std::move(val), batchIndex};
      }};
}

// ____________________________________________________________________________
// Helper: Process a queue word and update metadata/mappings.
// Returns true if this is a new unique word.
template <typename C, typename L>
bool VocabularyMerger::processQueueWord(const QueueWord& word, C& wordCallback,
                                        const L& lessThan,
                                        ad_utility::ProgressBar& progressBar)
    requires WordCallback<C> && ranges::predicate<L, TripleComponentWithIndex,
                                                  TripleComponentWithIndex> {
  bool isNewWord = !lastTripleComponent_.has_value() ||
                   word.iriOrLiteral() != lastTripleComponent_->iriOrLiteral();

  if (isNewWord) {
    if (lastTripleComponent_.has_value() &&
        !lessThan(*lastTripleComponent_, word.entry_)) {
      AD_LOG_WARN << "Vocabulary order violated for "
                  << lastTripleComponent_->iriOrLiteral() << " and "
                  << word.iriOrLiteral() << std::endl;
    }
    lastTripleComponent_ = TripleComponentWithIndex{
        word.iriOrLiteral(), word.isExternal(), metaData_.numWordsTotal()};

    // Write the new word to the vocabulary.
    auto& nextWord = lastTripleComponent_.value();
    if (nextWord.isBlankNode()) {
      nextWord.index_ = metaData_.getNextBlankNodeIndex();
    } else {
      nextWord.index_ =
          wordCallback(nextWord.iriOrLiteral(), nextWord.isExternal());
      metaData_.addWord(word.iriOrLiteral(), nextWord.index_);
    }
    if (progressBar.update()) {
      AD_LOG_INFO << progressBar.getProgressString() << std::flush;
    }
  } else {
    // If a word appears with different values for `isExternal`, externalize it.
    bool& external = lastTripleComponent_->isExternal();
    external = external || word.isExternal();
  }

  return isNewWord;
}

// ________________________________________________________________________________
// Write queue words for two-stage merging. This is similar to
// writeQueueWordsToIdMap but writes to batch-to-global ID maps instead of
// per-file ID maps.
CPP_template_def(typename C, typename L)(
    requires WordCallback<C> CPP_and_def
        ranges::predicate<L, TripleComponentWithIndex,
                          TripleComponentWithIndex>) void VocabularyMerger::
    writeQueueWordsToIdMapTwoStage(
        const std::vector<QueueWord>& buffer, C& wordCallback,
        const L& lessThan, ad_utility::ProgressBar& progressBar,
        std::vector<IdMapWriter>& batchToGlobalIdMaps) {
  AD_LOG_TIMING << "Start writing a batch of merged words (two-stage)\n";

  for (auto& top : buffer) {
    // Process the word and update metadata
    processQueueWord(top, wordCallback, lessThan, progressBar);

    // Get the current word (which has been updated by processQueueWord)
    const auto& word = lastTripleComponent_.value();
    Id targetId =
        word.isBlankNode()
            ? Id::makeFromBlankNodeIndex(BlankNodeIndex::make(word.index_))
            : Id::makeFromVocabIndex(VocabIndex::make(word.index_));

    // Write to the batch-to-global ID map (batch index comes from
    // partialFileId_)
    size_t batchIdx = top.partialFileId_;
    batchToGlobalIdMaps[batchIdx].push_back(
        {Id::makeFromVocabIndex(VocabIndex::make(top.id())), targetId});
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
template <typename T>
inline void writeMappedIdsToExtVec(
    const T& input, const ad_utility::HashMap<uint64_t, uint64_t>& map,
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
  AD_LOG_DEBUG << "Done writing partial vocabulary\n";
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
          ql::ranges::transform(singleMap.map_, els.begin() + offsets[i],
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

// ____________________________________________________________________________
// Merge a single batch of partial vocabulary files into intermediate files.
// This creates files in the same format as the original partial vocab files,
// plus an internal mapping file that tracks how IDs within this batch are
// remapped.
template <typename W>
size_t VocabularyMerger::mergeSingleBatch(const std::string& basename,
                                          size_t batchIndex, size_t startFile,
                                          size_t endFile, W comparator,
                                          ad_utility::MemorySize memoryToUse) {
  AD_LOG_INFO << "Merging batch " << batchIndex << " (files " << startFile
              << " to " << endFile << ")" << std::endl;

  // Create comparators
  auto [lessThan, lessThanForQueue] = makeComparators(comparator);

  // Create input generators for this batch
  std::vector<decltype(makePartialVocabGenerator(basename, 0))> generators;
  generators.reserve(endFile - startFile);
  for (size_t i = startFile; i < endFile; ++i) {
    generators.push_back(makePartialVocabGenerator(basename, i));
  }

  // Open output file for merged batch vocabulary
  std::string batchVocabFile =
      absl::StrCat(basename, BATCH_VOCAB_WORDS_INFIX, batchIndex);
  ad_utility::serialization::FileWriteSerializer outfile{batchVocabFile};

  // Create ID map writers for the internal mappings (batch-local ID -> batch
  // ID)
  std::vector<IdMapWriter> internalIdMaps;
  internalIdMaps.reserve(endFile - startFile);
  for (size_t i = startFile; i < endFile; ++i) {
    internalIdMaps.emplace_back(absl::StrCat(
        basename, BATCH_VOCAB_INTERNAL_IDMAP_INFIX, batchIndex, ".", i));
  }

  // Perform the merge
  // @Claude: the setup of the parallelMultiwayMerge can be factored out into a
  // function, it is duplicated.
  auto mergedWords =
      ad_utility::parallelMultiwayMerge<QueueWord, true,
                                        decltype(sizeOfQueueWord)>(
          0.8 * memoryToUse, std::move(generators), lessThanForQueue);

  size_t batchLocalId = 0;
  std::optional<TripleComponentWithIndex> lastComponent = std::nullopt;

  // Buffer for writing
  ad_utility::serialization::ByteBufferWriteSerializer byteBuffer;
  byteBuffer.reserve(10'000'000);

  for (QueueWord& currentWord : ql::views::join(mergedWords)) {
    // Check if this is a new unique word
    bool isNewWord =
        !lastComponent.has_value() ||
        currentWord.iriOrLiteral() != lastComponent->iriOrLiteral();

    if (isNewWord) {
      if (lastComponent.has_value() &&
          !lessThan(*lastComponent, currentWord.entry_)) {
        AD_LOG_WARN << "Batch vocabulary order violated for "
                    << lastComponent->iriOrLiteral() << " and "
                    << currentWord.iriOrLiteral() << std::endl;
      }
      lastComponent = TripleComponentWithIndex{
          currentWord.iriOrLiteral(), currentWord.isExternal(), batchLocalId};

      // Write to batch vocabulary file
      byteBuffer << lastComponent->iriOrLiteral_;
      byteBuffer << lastComponent->isExternal();
      byteBuffer << batchLocalId;

      ++batchLocalId;
    } else {
      // If a word appears with different externalization values, externalize it
      lastComponent->isExternal() =
          lastComponent->isExternal() || currentWord.isExternal();
    }

    // Write to internal ID map (original file's local ID -> batch local ID)
    size_t internalMapIndex = currentWord.partialFileId_ - startFile;
    internalIdMaps[internalMapIndex].push_back(
        {Id::makeFromVocabIndex(VocabIndex::make(currentWord.id())),
         Id::makeFromVocabIndex(VocabIndex::make(lastComponent->index_))});
  }

  // Write the batch vocabulary file
  uint64_t numWords = batchLocalId;
  outfile << numWords;
  outfile.serializeBytes(byteBuffer.data().data(), byteBuffer.data().size());
  outfile.close();

  AD_LOG_INFO << "Batch " << batchIndex << " complete: " << numWords
              << " unique words" << std::endl;

  return batchLocalId;
}

// ____________________________________________________________________________
// Perform two-stage vocabulary merging when we have too many input files.
// Stage 1: Merge files in batches, creating intermediate batch files
// Stage 2: Merge the batch files to create final vocabulary and mappings
template <typename W, typename C>
auto VocabularyMerger::mergeTwoStage(const std::string& basename,
                                     size_t numFiles, W comparator,
                                     C& wordCallback,
                                     ad_utility::MemorySize memoryToUse)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  // Stage 1: Merge input files in batches
  const size_t batchSize = MAX_NUM_FILES_FOR_DIRECT_MERGE;
  const size_t numBatches = (numFiles + batchSize - 1) / batchSize;

  AD_LOG_INFO << "Stage 1: Merging " << numFiles << " files into " << numBatches
              << " batches" << std::endl;

  std::vector<size_t> batchWordCounts;
  batchWordCounts.reserve(numBatches);

  for (size_t batchIdx = 0; batchIdx < numBatches; ++batchIdx) {
    size_t startFile = batchIdx * batchSize;
    size_t endFile = std::min(startFile + batchSize, numFiles);
    size_t wordCount = mergeSingleBatch(basename, batchIdx, startFile, endFile,
                                        comparator, memoryToUse);
    batchWordCounts.push_back(wordCount);
  }

  // Stage 2: Merge the batch files to create final vocabulary
  AD_LOG_INFO << "Stage 2: Merging " << numBatches
              << " batch files into final vocabulary" << std::endl;

  // Create comparators
  auto [lessThan, lessThanForQueue] = makeComparators(comparator);

  // Create generators for batch files
  auto makeGenerator = [&basename](size_t batchIdx) {
    return makeBatchVocabGenerator(basename, batchIdx);
  };
  auto batchGenerators =
      ::ranges::to<std::vector>(ad_utility::integerRange(numBatches) |
                                ::ranges::views::transform(makeGenerator));

  // Create ID map writers for batch-local ID -> global ID
  auto makeIdMap = [&basename](size_t batchIndex) {
    return IdMapWriter{
        absl::StrCat(basename, ".batch-to-global-idmap.tmp.", batchIndex)};
  };
  auto batchToGlobalIdMaps =
      ::ranges::to<std::vector>(ad_utility::integerRange(numBatches) |
                                ::ranges::views::transform(makeIdMap));

  // Perform the final merge
  auto mergedWords =
      ad_utility::parallelMultiwayMerge<QueueWord, true,
                                        decltype(sizeOfQueueWord)>(
          0.8 * memoryToUse, std::move(batchGenerators), lessThanForQueue);

  std::vector<QueueWord> sortedBuffer;
  sortedBuffer.reserve(bufferSize_);
  std::future<void> writeFuture;

  ad_utility::ProgressBar progressBar{metaData_.numWordsTotal(),
                                      "Words merged: "};

  for (QueueWord& currentWord : ql::views::join(mergedWords)) {
    sortedBuffer.push_back(std::move(currentWord));

    if (sortedBuffer.size() >= bufferSize_) {
      auto writeTask = [this, buffer = std::move(sortedBuffer), &wordCallback,
                        &lessThan, &progressBar, &batchToGlobalIdMaps]() {
        this->writeQueueWordsToIdMapTwoStage(buffer, wordCallback, lessThan,
                                             progressBar, batchToGlobalIdMaps);
      };
      sortedBuffer.clear();
      sortedBuffer.reserve(bufferSize_);

      if (writeFuture.valid()) {
        writeFuture.get();
      }
      writeFuture = std::async(writeTask);
    }
  }

  if (writeFuture.valid()) {
    writeFuture.get();
  }

  if (!sortedBuffer.empty()) {
    writeQueueWordsToIdMapTwoStage(sortedBuffer, wordCallback, lessThan,
                                   progressBar, batchToGlobalIdMaps);
  }

  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;

  // @Claude: The following code can be a separate function.
  // Stage 3: Compose the ID mappings
  // For each original file: read internal map (original -> batch), read batch
  // map (batch -> global), compose them to create (original -> global)
  AD_LOG_INFO << "Stage 3: Composing ID mappings for " << numFiles << " files"
              << std::endl;

  for (size_t fileIdx = 0; fileIdx < numFiles; ++fileIdx) {
    size_t batchIdx = fileIdx / batchSize;

    // Read internal map: original file's local ID -> batch local ID
    std::string internalMapFile = absl::StrCat(
        basename, BATCH_VOCAB_INTERNAL_IDMAP_INFIX, batchIdx, ".", fileIdx);
    auto internalMap = getIdMapFromFile(internalMapFile);

    // Read batch map: batch local ID -> global ID
    std::string batchMapFile =
        absl::StrCat(basename, ".batch-to-global-idmap.tmp.", batchIdx);
    auto batchToGlobalMap = IdMapFromPartialIdMapFile(batchMapFile);

    // Compose: original -> global
    IdMapWriter finalMap(
        absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, fileIdx));
    for (const auto& [originalId, batchId] : internalMap) {
      Id globalId = batchToGlobalMap.at(batchId);
      finalMap.push_back({originalId, globalId});
    }

    // Clean up internal map file
    ad_utility::deleteFile(internalMapFile);
  }

  // Clean up batch files and batch-to-global maps
  for (size_t batchIdx = 0; batchIdx < numBatches; ++batchIdx) {
    ad_utility::deleteFile(
        absl::StrCat(basename, BATCH_VOCAB_WORDS_INFIX, batchIdx));
    ad_utility::deleteFile(
        absl::StrCat(basename, ".batch-to-global-idmap.tmp.", batchIdx));
  }

  auto metaData = std::move(metaData_);
  clear();
  return metaData;
}

}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARYMERGERIMPL_H
