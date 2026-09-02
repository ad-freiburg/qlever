// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGER_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGER_H

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexBuilderTypes.h"
#include "index/vocabulary/Vocabulary.h"
#include "util/HashMap.h"
#include "util/ProgressBar.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializePair.h"
#include "util/Serializer/SerializeVector.h"
#include "util/TaskQueue.h"
#include "util/TypeTraits.h"

// Writes pairs of (partial ID, global ID) incrementally to a file.
class IdMapWriter {
 private:
  std::string filename_;
  using Serializer = ad_utility::serialization::VectorIncrementalSerializer<
      std::pair<Id, Id>, ad_utility::serialization::FileWriteSerializer>;
  std::unique_ptr<Serializer> serializer_;

 public:
  explicit IdMapWriter(const std::string& filename) : filename_(filename) {
    serializer_ = std::make_unique<Serializer>(filename);
  }

  void push_back(const std::pair<Id, Id>& pair) { serializer_->push(pair); }
};

// Get a vector of pairs of (partial ID, global ID) deserialized from a file
// that has previously been written using the `IdMapWriter` class above.
using IdMap = std::vector<std::pair<Id, Id>>;
inline IdMap getIdMapFromFile(const std::string& filename) {
  IdMap idMap;
  ad_utility::serialization::FileReadSerializer serializer(filename);
  serializer >> idMap;
  return idMap;
}

using TripleVec =
    ad_utility::CompressedExternalIdTable<NumColumnsIndexBuilding>;

namespace ad_utility::vocabulary_merger {
// Concept for a callback that can be called with a `string_view` and a `bool`.
// If the `bool` is true, then the word is to be stored in the external
// vocabulary else in the internal vocabulary.
template <typename T>
CPP_concept WordCallback =
    ad_utility::InvocableWithExactReturnType<T, uint64_t, std::string_view,
                                             bool>;
// Concept for a callable that compares two `string_view`s with respective
// `isExternal` flags.
template <typename T>
CPP_concept WordComparator =
    ranges::predicate<T, std::string_view, bool, std::string_view, bool>;

// The result of a call to `mergeVocabulary` (see below).
struct VocabularyMetaData {
  // This struct is used to incrementally construct the range of IDs that
  // correspond to a given prefix. To use it, all the words from the
  // vocabulary must be passed to the member function `addIfWordMatches` in
  // sorted order. After that, the range `[begin(), end())` is the range of
  // all the words that start with the prefix.
  struct IdRangeForPrefix {
    explicit IdRangeForPrefix(std::string prefix)
        : prefix_{std::move(prefix)} {}
    // Check if `word` starts with the `prefix_`. If so, `wordIndex`
    // will become part of the range that this struct represents. The function
    // returns `true` in this case, else `false`. For this to work, all the
    // words that start with the `prefix_` have to be passed in consecutively
    // and their indices have to be consecutive and ascending.
    bool addIfWordMatches(std::string_view word, size_t wordIndex) {
      if (!ql::starts_with(word, prefix_)) {
        return false;
      }
      if (!beginWasSeen_) {
        begin_ = Id::makeFromVocabIndex(VocabIndex::make(wordIndex));
        beginWasSeen_ = true;
      }
      end_ = Id::makeFromVocabIndex(VocabIndex::make(wordIndex + 1));
      return true;
    }

    Id begin() const { return begin_; }
    Id end() const { return end_; }

    // Return true if the `id` belongs to this range.
    bool contains(Id id) const { return begin_ <= id && id < end_; }

   private:
    Id begin_ = Id::makeUndefined();
    Id end_ = Id::makeUndefined();
    std::string prefix_;
    bool beginWasSeen_ = false;
  };

 public:
  // This function has to be called for every *DISTINCT* word (IRI or literal,
  // not blank nodes) and the index, which is assigned to this word by the merge
  // procedure. It automatically updates the various prefix ranges, the total
  // number of words, and the mapping of the special IDs.
  void addWord(std::string_view word, size_t wordIndex) {
    ++numWordsTotal_;
    if (langTaggedPredicates_.addIfWordMatches(word, wordIndex)) {
      return;
    }
    if (internalEntities_.addIfWordMatches(word, wordIndex)) {
      if (globalSpecialIds_->contains(word)) {
        specialIdMapping_[std::string{word}] =
            Id::makeFromVocabIndex(VocabIndex::make(wordIndex));
      }
    }
  }

  // Return the index of the next blank node and increment the internal counter
  // of blank nodes. This has to be called for every distinct blank node that
  // is encountered.
  size_t getNextBlankNodeIndex() {
    auto res = numBlankNodesTotal_;
    ++numBlankNodesTotal_;
    return res;
  }

  // The mapping from the `qlever::specialIds` to their actual IDs.
  // This is created on the fly by the calls to `addWord`.
  const auto& specialIdMapping() const { return specialIdMapping_; }
  // The prefix range for the `@en@<predicate` style predicates.
  const auto& langTaggedPredicates() const { return langTaggedPredicates_; }
  // The prefix range for the internal IRIs in the `ql:` namespace.
  const auto& internalEntities() const { return internalEntities_; }
  // The number of words for which `addWord()` has been called. Needs to return
  // a reference to be used in combination with a `ProgressBar`.
  const size_t& numWordsTotal() const { return numWordsTotal_; }

  // Return true iff the `id` belongs to one of the two ranges that contain
  // the internal IDs that were added by QLever and were not part of the
  // input.
  bool isQleverInternalId(Id id) const {
    return internalEntities_.contains(id) || langTaggedPredicates_.contains(id);
  }

 private:
  // The number of distinct words (size of the created vocabulary).
  size_t numWordsTotal_ = 0;
  // The number of distinct blank nodes that were found and immediately
  // converted to an ID without becoming part of the vocabulary.
  size_t numBlankNodesTotal_ = 0;
  IdRangeForPrefix langTaggedPredicates_{
      std::string{ad_utility::languageTaggedPredicatePrefix}};
  IdRangeForPrefix internalEntities_{
      std::string{QLEVER_INTERNAL_PREFIX_IRI_WITHOUT_CLOSING_BRACKET}};

  ad_utility::HashMap<std::string, Id> specialIdMapping_;
  const ad_utility::HashMap<std::string, Id>* globalSpecialIds_ =
      &qlever::specialIds();
};
// The internal helper types and the individual stages of the merging pipeline
// (see the comment above `mergeVocabulary` below). None of them is part of the
// public interface of this header.
namespace detail {

// Helper `struct` for a word from a partial vocabulary.
struct QueueWord {
  QueueWord() = default;
  QueueWord(TripleComponentWithIndex&& v, size_t file)
      : entry_(std::move(v)), partialFileId_(file) {}
  TripleComponentWithIndex entry_;  // the word, its local ID and the
                                    // information if it will be externalized
  size_t partialFileId_;  // from which partial vocabulary did this word come

  [[nodiscard]] const bool& isExternal() const { return entry_.isExternal(); }
  [[nodiscard]] bool& isExternal() { return entry_.isExternal(); }

  [[nodiscard]] const std::string& iriOrLiteral() const {
    return entry_.iriOrLiteral();
  }

  [[nodiscard]] std::string& iriOrLiteral() { return entry_.iriOrLiteral(); }

  [[nodiscard]] const auto& id() const { return entry_.index_; }
};

// Compute the memory footprint of a `QueueWord`, which the parallel merging
// needs to limit its memory consumption.
struct SizeOfQueueWord {
  ad_utility::MemorySize operator()(const QueueWord& q) const {
    return ad_utility::MemorySize::bytes(sizeof(QueueWord) +
                                         q.entry_.iriOrLiteral().size());
  }
};
inline constexpr SizeOfQueueWord sizeOfQueueWord{};

// A word that occurs in the merged vocabulary for the first time, together
// with the information whether it is to be externalized.
struct UniqueWord {
  // NOTE: This is a view into one of the `mergedWordBuffers_` of the
  // `WordBatch` that this word belongs to. Those buffers are deliberately
  // kept alive until the batch has been written to the vocabulary, such that
  // the merging thread never has to copy or move a single word.
  std::string_view word_;
  bool isExternal_;
};

// A single entry of one of the partial ID maps, as it is created by the
// merging thread. NOTE: At that point, the global ID of the corresponding
// word is not yet known (it is only determined when the word is written to
// the vocabulary), so the entry instead stores the index of the word within
// its batch (see `IdMapBatch::globalIds_`).
struct QueuedIdMapEntry {
  uint32_t partialFileId_;
  uint32_t indexOfWordInBatch_;
  uint64_t localIndex_;
};

// All the ID map entries of a single batch, together with the global IDs
// that those entries refer to.
struct IdMapBatch {
  std::vector<QueuedIdMapEntry> entries_;
  // The global IDs of the distinct words of the batch. The element at index
  // `0` is the global ID of the *last* distinct word of the *previous*
  // batch, because the first words of a batch may well be further
  // occurrences of that word.
  std::vector<Id> globalIds_;
};

// A batch of merged words, as it is handed from the merging thread to the
// thread that writes the words to the vocabulary.
struct WordBatch {
  std::vector<UniqueWord> uniqueWords_;
  IdMapBatch idMapBatch_;
  // The buffers of merged words that back the `string_view`s of the
  // `uniqueWords_` (see there).
  std::vector<std::vector<QueueWord>> mergedWordBuffers_;
};

// Concept for a callback that consumes a complete `WordBatch`.
template <typename T>
CPP_concept WordBatchCallback = std::is_invocable_v<const T&, WordBatch>;

// The number of ID map entries (which is the same as the number of merged
// words) that are collected in a single batch. A single buffer of merged
// words only contains a rather small number of words (currently 100), which
// would be much too fine-grained for a task queue.
inline constexpr size_t idMapEntryBatchSize = 100'000;

// The maximal number of batches that may be waiting in the queue of the
// pipeline. NOTE: A batch keeps all the merged words alive that it was created
// from (typically a few megabytes, see `idMapEntryBatchSize`), so this also
// determines the memory footprint of the pipeline.
inline constexpr size_t queueSize = 3;

// The first stage of the merging pipeline: eliminate the duplicates from the
// merged words and collect the distinct words as well as the entries for the
// partial ID maps in batches.
//
// NOTE: This class is used exclusively by the merging thread; the complete
// `WordBatch`es are the only thing that it hands on to the other stages.
class WordBatchBuilder {
 private:
  // The word that was merged last. It is a view into one of the
  // `mergedWordBuffers_` of the `currentBatch_`, or, as soon as that batch has
  // been handed on to the next stage, into `lastWordStorage_`.
  std::string_view lastWord_;
  std::string lastWordStorage_;
  bool hasLastWord_ = false;
  // Whether any of the occurrences of the `lastWord_` was marked as external.
  // NOTE: This is only used for the check of the vocabulary order. A word is
  // written to the vocabulary as soon as its first occurrence is seen, so a
  // later occurrence can no longer change its `isExternal` flag.
  bool lastWordIsExternal_ = false;
  // The index of the `lastWord_` within the `currentBatch_`, where `0` means
  // that it is the last distinct word of the previous batch (see
  // `IdMapBatch::globalIds_`).
  uint32_t indexOfLastWordInBatch_ = 0;
  // The batch that is currently being filled.
  WordBatch currentBatch_;

 public:
  WordBatchBuilder() { startNewBatch(); }

  // Eliminate the duplicates from a `buffer` of merged words and add the
  // resulting distinct words as well as one ID map entry per word to the
  // current batch. Whenever a batch is full, it is handed to the
  // `batchCallback`. The `QueueWord`s must be passed in alphabetical order wrt
  // the `comparator` (also across multiple calls), which is checked.
  CPP_template(typename W, typename F)(
      requires WordComparator<W> CPP_and WordBatchCallback<
          F>) void addMergedWords(std::vector<QueueWord> buffer,
                                  const W& comparator, const F& batchCallback);

  // Hand the current (typically only partially filled) batch to the
  // `batchCallback` and start a new batch. Do nothing if the current batch is
  // empty.
  CPP_template(typename F)(requires WordBatchCallback<F>) void flush(
      const F& batchCallback);

 private:
  // Reset the `currentBatch_` and allocate its buffers.
  void startNewBatch();
};

// The second stage of the merging pipeline: write the distinct words of a
// batch to the vocabulary (via the word callback) and thereby determine their
// global IDs.
//
// NOTE: This class is used exclusively by the worker thread of the
// `VocabularyMergePipeline` below.
class VocabularyWriter {
 private:
  // The metadata of the merged vocabulary, which is built up incrementally as
  // the words are written.
  VocabularyMetaData metaData_;
  // The global ID of the word that was written to the vocabulary last (see
  // `IdMapBatch::globalIds_`).
  Id lastGlobalId_ = Id::makeUndefined();
  ad_utility::ProgressBar progressBar_{metaData_.numWordsTotal(),
                                       "Words merged: "};

 public:
  // Write the `uniqueWords` to the vocabulary, store their global IDs in the
  // `idMapBatch`, and return that batch (which is then complete and can be
  // written to the ID maps).
  CPP_template(typename C)(requires WordCallback<C>) IdMapBatch
      writeWordsToVocabulary(
          const std::vector<UniqueWord>& uniqueWords, IdMapBatch idMapBatch,
          C& wordCallback,
          const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes);

  // The metadata, which is complete as soon as all the batches have been
  // written.
  VocabularyMetaData& metaData() { return metaData_; }

  // Log the final state of the progress bar. This has to be called exactly
  // once, after the last batch has been written.
  void logFinalProgress();
};

// The third stage of the merging pipeline: write the entries of a complete
// `IdMapBatch` to the partial ID maps, one of which is created per partial
// vocabulary.
//
// NOTE: This class is used exclusively by the worker thread of the
// `VocabularyMergePipeline` below.
class IdMapBatchWriter {
 private:
  // The ID maps, one per partial vocabulary.
  std::vector<IdMapWriter> idMaps_;

 public:
  // Create the ID map for each of the `numFiles` partial vocabularies. The
  // filenames are `basename + PARTIAL_VOCAB_IDMAP_INFIX + i`.
  IdMapBatchWriter(const std::string& basename, size_t numFiles);

  // Write all the entries of the `batch` to their respective ID maps.
  void writeBatch(const IdMapBatch& batch);

  // Close all the ID maps. After this, no more batches may be written.
  void finish();
};

// The stages of the merging pipeline that run asynchronously to the merging
// thread (stages 2 and 3 in the comment above `mergeVocabulary` below).
//
// NOTE: The queue has exactly one worker thread, so the batches are processed
// in exactly the order in which the merging thread creates them, and the state
// of the individual stages requires no further synchronization.
class VocabularyMergePipeline {
 private:
  // NOTE: The order of the following declarations is important, because the
  // members are destroyed in the reverse order of their declaration, and the
  // destructor of the queue blocks until all its pending tasks have been run.
  // The tasks write to both the `vocabularyWriter_` and the
  // `idMapBatchWriter_`, so the queue has to be destroyed before them.
  IdMapBatchWriter idMapBatchWriter_;
  VocabularyWriter vocabularyWriter_;
  ad_utility::TaskQueue<false> queue_{
      queueSize, 1, "Writing the merged vocabulary and the ID maps"};

 public:
  // Create the pipeline. The `basename` and `numFiles` determine the files of
  // the partial ID maps (see `IdMapBatchWriter`).
  VocabularyMergePipeline(const std::string& basename, size_t numFiles)
      : idMapBatchWriter_{basename, numFiles} {}

  // Asynchronously process a single `batch` of merged words: write its
  // distinct words to the vocabulary (via the `wordCallback` and the
  // `blankNodeIriRegexes`), then write its ID map entries and destroy the
  // merged words that it was created from. Block if the pipeline is busy.
  CPP_template(typename C)(requires WordCallback<C>) void push(
      WordBatch batch, C& wordCallback,
      const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes);

  // Wait until all the batches that were pushed have been processed
  // completely, close the ID maps, and return the metadata of the merged
  // vocabulary. After this, no more batches may be pushed.
  VocabularyMetaData finish();
};
}  // namespace detail

// _______________________________________________________________
// Merge the partial vocabularies in the  binary files
// `basename + PARTIAL_VOCAB_WORDS_INFIX + to_string(i)`
// where `0 <= i < numFiles`.
// Return the number of total Words merged and the lower and upper bound of
// language tagged predicates. Argument `comparator` gives the way to order
// strings (case-sensitive or not). Argument `wordCallback`
// is called for each merged word in the vocabulary in the order of their
// appearance. Argument `blankNodeIriRegexes` is a (possibly empty) list of
// compiled regexes; IRIs that are fully matched by any of them are treated as
// blank nodes (see `TripleComponentWithIndex::isBlankNode`). The regexes are
// compiled by the caller (see `IndexImpl::setBlankNodeIriRegexes`).
//
// The merging is organized as a pipeline of two threads, which communicate via
// a task queue, such that both of them can work concurrently:
//
// 1. The thread that calls `mergeVocabulary` obtains the merged words in
//    sorted order and eliminates the duplicates (a word typically occurs in
//    many of the partial vocabularies). It collects the distinct words as well
//    as the entries for the partial ID maps in batches (see
//    `detail::WordBatchBuilder`) and hands each batch to the second thread.
// 2. The second thread writes the distinct words of a batch to the vocabulary
//    (via the `wordCallback`) and thereby determines their global IDs (see
//    `detail::VocabularyWriter`). It then writes the entries of the partial ID
//    maps, which only now know their global IDs (see
//    `detail::IdMapBatchWriter`), and finally destroys the merged words of the
//    batch (which involves freeing one string per word).
//
// The second of those stages is owned by the
// `detail::VocabularyMergePipeline`.
template <typename W, typename C>
auto mergeVocabulary(
    const std::string& basename, size_t numFiles, W comparator, C& wordCallback,
    ad_utility::MemorySize memoryToUse,
    const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes = {})
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>);

// ____________________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const std::string& filename);

/**
 * @brief Create a hashMap that maps the Id of the pair<string, Id> to the
 * position of the string in the vector. The resulting ids will be ascending and
 * duplicates strings that appear adjacent to each other will be given the same
 * ID. If Input is sorted this will mean if result[x] == result[y] then the
 * strings that were connected to x and y in the input were identical. Also
 * modifies the input Ids to their mapped values.
 *
 * @param els  Must be sorted(at least duplicates must be adjacent) according to
 * the strings and the Ids must be unique to work correctly.
 */
ad_utility::HashMap<uint64_t, uint64_t> createInternalMapping(ItemVec& els);

/**
 * @brief for each of the IdTriples in <input>: map the three Ids using the
 * <map> and write the resulting Id triple to <*writePtr>
 */
void writeMappedIdsToExtVec(
    const std::vector<std::array<Id, NumColumnsIndexBuilding>>& input,
    const HashMap<Id, Id>& map, std::unique_ptr<TripleVec>* writePtr);

/**
 * @brief Serialize a std::vector<std::pair<string, Id>> to a binary file
 *
 * For each string first writes the size of the string (64 bits). Then the
 * actual string content (no trailing zero) and then the Id (sizeof(Id)
 *
 * @param els The input
 * @param fileName will write to this file. If it exists it will be overwritten
 */
void writePartialVocabularyToFile(const ItemVec& els,
                                  const std::string& fileName);

/**
 * @brief Take an Array of HashMaps of strings to Ids and insert all the
 * elements from all the hashMaps into a single vector No reordering or
 * deduplication is done, so result.size() == summed size of all the hash maps
 */
ItemVec vocabMapsToVector(const ItemMapArray& map);

// _____________________________________________________________________________________________________________
/**
 * @brief Sort the input in-place according to the strings as compared by the
 * StringComparator
 * @tparam A binary Function object to compare strings (e.g.
 * std::less<std::string>())
 * @param doParallelSort if true and USE_PARALLEL_SORT is true, use the gnu
 * parallel extension for sorting.
 */
template <class StringSortComparator>
void sortVocabVector(ItemVec* vecPtr, StringSortComparator comp,
                     bool doParallelSort);
}  // namespace ad_utility::vocabulary_merger

#include "index/VocabularyMergerImpl.h"

#endif  // QLEVER_SRC_INDEX_VOCABULARYMERGER_H
