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
#include "util/Serializer/BufferedSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Timer.h"

namespace ad_utility::vocabulary_merger {
// _________________________________________________________________
template <typename W, typename C>
auto mergeVocabulary(const std::string& basename,
                     const std::vector<std::string>& partialVocabularySuffixes,
                     W comparator, C& internalWordCallback,
                     ad_utility::MemorySize memoryToUse,
                     const ad_utility::RegexSet& blankNodeIriRegexes)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  VocabularyMerger merger;
  return merger.mergeVocabulary(basename, partialVocabularySuffixes,
                                std::move(comparator), internalWordCallback,
                                memoryToUse, blankNodeIriRegexes);
}

// _________________________________________________________________
template <typename W, typename C>
auto VocabularyMerger::mergeVocabulary(
    const std::string& basename,
    const std::vector<std::string>& partialVocabularySuffixes, W comparator,
    C& wordCallback, ad_utility::MemorySize memoryToUse,
    const ad_utility::RegexSet& blankNodeIriRegexes)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>) {
  // Return true iff `p1` is smaller than `p2` according to the order of the
  // IRI or literal.
  auto lessThanForQueue = [&comparator](const QueueWord& p1,
                                        const QueueWord& p2) {
    return comparator(p1.iriOrLiteral(), p2.iriOrLiteral());
  };

  // Open and prepare all infiles and file-based output vectors.
  auto makeWordRangeFromFile = [&basename,
                                &partialVocabularySuffixes](size_t fileIndex) {
    ad_utility::serialization::FileReadSerializer infile{
        absl::StrCat(basename, PARTIAL_VOCAB_WORDS_INFIX,
                     partialVocabularySuffixes.at(fileIndex))};
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
  generators.reserve(partialVocabularySuffixes.size());

  for (std::size_t i :
       ad_utility::integerRange(partialVocabularySuffixes.size())) {
    generators.push_back(makeWordRangeFromFile(i));
    idMapWriters_.push_back(makeIdMapWriter(absl::StrCat(
        basename, PARTIAL_VOCAB_IDMAP_INFIX, partialVocabularySuffixes.at(i))));
  }

  // Some memory (that is hard to measure exactly) is used for the writing of
  // a batch of merged words, so we only give 80% of the total memory to the
  // merging. This is very approximate and should be investigated in more
  // detail.
  auto mergedWords =
      ad_utility::parallelMultiwayMerge<QueueWord, true,
                                        decltype(detail::sizeOfQueueWord)>(
          0.8 * memoryToUse, std::move(generators), lessThanForQueue);
  // Hand each complete batch of merged words to the writing thread. NOTE: The
  // `wordCallback` and the `blankNodeIriRegexes` are captured by reference
  // into the queued task, so both of them have to stay alive until the
  // `wordBatchQueue_` has been finished below.
  auto batchCallback = [this, &wordCallback,
                        &blankNodeIriRegexes](detail::WordBatch batch) {
    wordBatchQueue_.push([this, batch = std::move(batch), &wordCallback,
                          &blankNodeIriRegexes]() mutable {
      writeWordBatch(batch, wordCallback, blankNodeIriRegexes);
    });
  };
  for (std::vector<QueueWord>& currentWords : mergedWords) {
    batchBuilder_.addMergedWords(std::move(currentWords), comparator,
                                 batchCallback);
  }
  // Hand the remaining words (including the one that is still held back) to
  // the writing thread and wait until all of them have actually been written.
  batchBuilder_.finish(batchCallback);
  wordBatchQueue_.finish();

  AD_LOG_INFO << progressBar_.getFinalProgressString() << std::flush;

  auto metaData = std::move(metaData_);
  // completely reset all the inner state
  clear();
  return metaData;
}

// _____________________________________________________________________________
CPP_template_def(typename C)(requires WordCallback<C>) void VocabularyMerger::
    writeWordBatch(const detail::WordBatch& batch, C& wordCallback,
                   const ad_utility::RegexSet& blankNodeIriRegexes) {
  AD_LOG_TIMING << "Start writing a batch of merged words\n";

  // TODO<optimization> If we aim to further speed this up, we could
  // order all the write requests to _outfile _externalOutfile and all the
  // idVecs to have a more useful external access pattern.

  // Write the distinct words of the batch to the vocabulary, which determines
  // their global IDs.
  std::vector<Id> globalIds;
  globalIds.reserve(batch.uniqueWords_.size());
  for (const auto& uniqueWord : batch.uniqueWords_) {
    const auto& word = uniqueWord.word_;
    if (isBlankNode(word, blankNodeIriRegexes)) {
      globalIds.push_back(Id::makeFromBlankNodeIndex(
          BlankNodeIndex::make(metaData_.getNextBlankNodeIndex())));
    } else {
      auto wordIndex = wordCallback(word, uniqueWord.isExternal_);
      metaData_.addWord(word, wordIndex);
      globalIds.push_back(Id::makeFromVocabIndex(VocabIndex::make(wordIndex)));
    }
    if (progressBar_.update()) {
      AD_LOG_INFO << progressBar_.getProgressString() << std::flush;
    }
  }

  // Write the mapping from the local index to the global ID to the ID map of
  // the partial vocabulary that each occurrence of a word came from.
  const auto& localIdxMappings = batch.localIdxMappings_;
  for (size_t i = 0; i < localIdxMappings.numMappings_; ++i) {
    const auto& mapping = localIdxMappings.mappings_[i];
    idMapWriters_[mapping.partialVocabularyIndex_].push(
        IdMapEntry{mapping.indexOfWordInPartialVocabulary_,
                   globalIds[mapping.indexOfWordInBatch_]});
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
    const HashMap<uint64_t, uint64_t>& map, TripleVec& vec) {
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
