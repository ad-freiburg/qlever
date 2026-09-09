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
  // The order of the merge: by geo sort key first (all keys are 0 without a
  // geo cell grid), then by `comparator`.
  auto lessThan = [&comparator](
                      uint64_t key1, const TripleComponentWithIndex& t1,
                      uint64_t key2, const TripleComponentWithIndex& t2) {
    if (key1 != key2) {
      return key1 < key2;
    }
    return comparator(t1.iriOrLiteral_, t1.isExternal_, t2.iriOrLiteral_,
                      t2.isExternal_);
  };
  auto lessThanForQueue = [&lessThan](const QueueWord& p1,
                                      const QueueWord& p2) {
    return lessThan(p1.geoSortKey_, p1.entry_, p2.geoSortKey_, p2.entry_);
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
          uint64_t geoSortKey;
          infile >> geoSortKey;
          return QueueWord{std::move(val), fileIndex, geoSortKey};
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
        ranges::predicate<L, uint64_t, TripleComponentWithIndex, uint64_t,
                          TripleComponentWithIndex>) void VocabularyMerger::
    writeQueueWordsToIdMap(std::vector<QueueWord>& buffer, C& wordCallback,
                           const L& lessThan,
                           const ad_utility::RegexSet& blankNodeIriRegexes,
                           ad_utility::ProgressBar& progressBar) {
  AD_LOG_TIMING << "Start writing a batch of merged words\n";

  // Iterate (avoid duplicates).
  for (auto& top : buffer) {
    if (!lastTripleComponent_.has_value() ||
        top.iriOrLiteral() != lastTripleComponent_.value().iriOrLiteral()) {
      if (lastTripleComponent_.has_value()) {
        AD_CORRECTNESS_CHECK(
            lessThan(lastGeoSortKey_, lastTripleComponent_.value(),
                     top.geoSortKey_, top.entry_),
            "Total vocabulary order violated for ",
            lastTripleComponent_->iriOrLiteral(), " and ", top.iriOrLiteral());
      }
      lastTripleComponent_ =
          TripleComponentWithIndex{std::move(top.iriOrLiteral()),
                                   top.isExternal(), metaData_.numWordsTotal()};
      lastGeoSortKey_ = top.geoSortKey_;
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
    // Write the mapping from the local index to the global ID to the ID map
    // of the partial vocabulary that this occurrence of the word came from.
    idMapWriters_[top.partialFileId_].push(
        IdMapEntry{VocabIndex::make(top.id()), targetId});
  }
}

// ____________________________________________________________________________________________________________
inline HashMap<uint64_t, uint64_t> createInternalMapping(ItemVec& entries) {
  HashMap<uint64_t, uint64_t> res;
  res.reserve(entries.size());
  std::optional<std::string_view> lastWord;
  // This value will overflow on the first entry.
  size_t nextWordId = -1;
  for (auto& entry : entries) {
    auto& idAndExternal = entry.idAndFlag_;
    auto id = idAndExternal.id();
    if (lastWord != entry.word_) {
      nextWordId++;
      lastWord = entry.word_;
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
inline void writePartialVocabularyToFile(const ItemVec& entries,
                                         const std::string& fileName) {
  AD_LOG_DEBUG << "Writing partial vocabulary to: " << fileName << "\n";

  // We buffer the data with our own buffer before passing it to the file in
  // large chunks. Despite `fwrite` (which is ultimately called by
  // `FileWriteSerializer::serializeBytes`) buffering data on its own, it is
  // faster to buffer with our own buffer, presumably because `fwrite` is
  // thread-safe and therefore has to acquire a mutex for every call.
  serialization::BufferedWriteSerializer serializer{
      serialization::FileWriteSerializer{fileName}, 16_MB};

  uint64_t size = entries.size();
  serializer << size;

  // This is essentially a `VectorIncrementalSerializer` with a custom
  // serialization function, which the infrastructure currently does not
  // support.
  for (const auto& entry : entries) {
    // When merging the vocabulary, we need the actual word, the (internal) id
    // we have assigned to this word, the information, whether this word
    // belongs to the internal or external vocabulary, and its geo sort key.
    serializer << entry.word_;
    serializer << entry.idAndFlag_.isExternal();
    serializer << entry.idAndFlag_.id();
    serializer << entry.geoSortKey_;
  }

  serializer.close();

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

// _______________________________________________________________________________________________________________________
template <class StringSortComparator>
void sortVocabVector(ItemVec* vecPtr, StringSortComparator comp,
                     const bool doParallelSort) {
  auto& entries = *vecPtr;
  if constexpr (USE_PARALLEL_SORT) {
    if (doParallelSort) {
      ad_utility::parallel_sort(ql::ranges::begin(entries),
                                ql::ranges::end(entries), comp,
                                ad_utility::parallel_tag(10));
    } else {
      ql::ranges::sort(entries, comp);
    }
  } else {
    ql::ranges::sort(entries, comp);
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
