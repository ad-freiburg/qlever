// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGER_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGER_H

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
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
#include "util/ExceptionHandling.h"
#include "util/HashMap.h"
#include "util/ProgressBar.h"
#include "util/Serializer/BufferedSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializePair.h"
#include "util/Serializer/SerializeVector.h"
#include "util/TaskQueue.h"
#include "util/TypeTraits.h"
#include "util/UninitializedAllocator.h"

// A single entry of an ID map (see `IdMapWriter` below): the index that a word
// has inside a partial vocabulary, and the global ID that the vocabulary merger
// has assigned to that word.
//
// NOTE: This deliberately is a plain struct and not a `std::pair`. libstdc++'s
// `std::pair` has user-provided assignment operators and hence is not trivially
// copyable, which would make a `std::vector` of them neither trivially
// serializable (it would then be written and read one member at a time) nor
// bitwise relocatable.
struct IdMapEntry {
  // NOTE: The local index deliberately is a plain index and not an `Id`. Inside
  // a partial vocabulary a word is always a `VocabIndex`, so the datatype bits
  // of an `Id` would carry no information, but computing them for each of the
  // (very many) entries would cost time in the vocabulary merger.
  uint64_t localIndex_;
  Id globalId_;

  bool operator==(const IdMapEntry& other) const {
    return localIndex_ == other.localIndex_ && globalId_ == other.globalId_;
  }
  bool operator!=(const IdMapEntry& other) const { return !(*this == other); }

  // Enable the serialization of an `IdMapEntry` (and of contiguous ranges of
  // them) in the `ad_utility::serialization` framework.
  template <typename T>
  friend std::true_type allowTrivialSerialization(IdMapEntry, T);

  // Make the output of failed tests readable.
  friend std::ostream& operator<<(std::ostream& str, const IdMapEntry& entry) {
    return str << '{' << entry.localIndex_ << ", " << entry.globalId_ << '}';
  }
};
static_assert(
    ad_utility::serialization::TriviallySerializable<IdMapEntry>,
    "An `IdMapEntry` has to be trivially serializable, else a whole `IdMap` "
    "would be written and read one member at a time");

// Writes `IdMapEntry`s incrementally to a file. The entries are buffered and
// only handed to the file in blocks, because a single entry is only 16 bytes
// and writing each of them directly to the file would be very inefficient. The
// resulting file has exactly the format of a serialized `IdMap`.
class IdMapWriter {
 public:
  // The amount of data that is buffered before it is written to the file.
  // NOTE: There is one `IdMapWriter` per partial vocabulary, of which there can
  // be thousands, so this must not be too large: it is not only the memory
  // footprint, but also the cache and TLB pressure of the writes, which are
  // scattered over all the writers.
  static constexpr ad_utility::MemorySize bufferSize =
      ad_utility::MemorySize::kilobytes(16);

 private:
  using Serializer = ad_utility::serialization::BufferedWriteSerializer<
      ad_utility::serialization::FileWriteSerializer>;
  // NOTE: The indirection via the `unique_ptr` makes this class movable, which
  // is required because the `IdMapWriter`s are stored in a `std::vector`.
  std::unique_ptr<Serializer> serializer_;
  // The number of entries that have been pushed so far. It is written to the
  // beginning of the file by `finish()`.
  uint64_t numEntries_ = 0;

 public:
  explicit IdMapWriter(const std::string& filename)
      : serializer_{std::make_unique<Serializer>(
            ad_utility::serialization::FileWriteSerializer{filename},
            bufferSize)} {
    // Write a placeholder for the number of entries, which is only known once
    // `finish()` is called.
    *serializer_ << numEntries_;
  }

  // This class is move-only. NOTE: There deliberately is no move assignment,
  // as it would have to deal with the (currently never occurring) case that the
  // assigned-to writer has not been finished yet.
  IdMapWriter(const IdMapWriter&) = delete;
  IdMapWriter& operator=(const IdMapWriter&) = delete;
  IdMapWriter(IdMapWriter&&) noexcept = default;

  ~IdMapWriter() {
    ad_utility::terminateIfThrows([this]() { finish(); },
                                  "The closing of an `IdMapWriter` failed");
  }

  // Append a single entry.
  void push_back(const IdMapEntry& entry) {
    *serializer_ << entry;
    ++numEntries_;
  }

  // Flush the buffer, write the total number of entries to the beginning of
  // the file, and close the file. This is automatically called by the
  // destructor. After a call to `finish()`, no more calls to `push_back()` are
  // allowed.
  void finish() {
    if (!serializer_) {
      return;
    }
    auto file = std::move(*serializer_).underlyingSerializer();
    serializer_.reset();
    file.setSerializationPosition(0);
    file << numEntries_;
    file.close();
  }
};

// Get the `IdMapEntry`s deserialized from a file that has previously been
// written using the `IdMapWriter` class above.
using IdMap = std::vector<IdMapEntry>;
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
template <typename W, typename C>
auto mergeVocabulary(
    const std::string& basename, size_t numFiles, W comparator, C& wordCallback,
    ad_utility::MemorySize memoryToUse,
    const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes = {})
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>);

// A helper class that implements the `mergeVocabulary` function (see
// above). Everything in this class is private and only the
// `mergeVocabulary` function is a friend.
//
// The merging is organized as a pipeline of four threads, which communicate
// via task queues, such that all of them can work concurrently:
//
// 1. The thread that calls `mergeVocabulary` obtains the merged words in
//    sorted order and eliminates the duplicates (a word typically occurs in
//    many of the partial vocabularies). It collects the distinct words as well
//    as the entries for the partial ID maps in batches (see `WordBatch`) and
//    hands each batch to the second thread.
// 2. The `wordWriterQueue_`'s thread writes the distinct words of a batch to
//    the vocabulary (via the `wordCallback`) and thereby determines their
//    global IDs.
// 3. The `idMapWriterQueue_`'s thread writes the entries of the partial ID
//    maps (which only now know their global IDs) to the `idMaps_`.
// 4. The `mergedWordsDestructionQueue_`'s thread destroys the merged words of
//    a batch (which involves freeing one string per word) once they have been
//    written to the vocabulary.
//
// NOTE: Each of the queues has exactly one worker thread, so the batches are
// processed in exactly the order in which they are created by the merging
// thread, and the `metaData_` as well as the `idMaps_` require no further
// synchronization.
class VocabularyMerger {
 private:
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

  struct SizeOfQueueWord {
    ad_utility::MemorySize operator()(const QueueWord& q) const {
      return ad_utility::MemorySize::bytes(sizeof(QueueWord) +
                                           q.entry_.iriOrLiteral().size());
    }
  };
  constexpr static SizeOfQueueWord sizeOfQueueWord{};

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
    // The entries. NOTE: The vector is allocated (but not initialized) in
    // advance, and only the first `numEntries_` of its elements are valid, see
    // `VocabularyMerger::startNewBatch`.
    ad_utility::UninitializedVector<QueuedIdMapEntry> entries_;
    size_t numEntries_ = 0;
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

  // The maximal number of batches that may be waiting in each of the queues
  // below. NOTE: A batch keeps all the merged words alive that it was created
  // from (typically a few megabytes, see `idMapEntryBatchSize`), so this also
  // determines the memory footprint of the pipeline, which currently is in the
  // order of a hundred megabytes.
  static constexpr size_t queueSize = 3;
  // The number of ID map entries (which is the same as the number of merged
  // words) that are collected in a single batch. A single call to
  // `processMergedWords` only deals with a rather small number of words
  // (currently 100), which would be much too fine-grained for a task queue.
  static constexpr size_t idMapEntryBatchSize = 100'000;

  // The result (mostly metadata) which we'll return. NOTE: While the merging
  // is running, this is only touched by the `wordWriterQueue_`'s thread.
  VocabularyMetaData metaData_;

  // The word that was merged last. It is a view into one of the
  // `mergedWordBuffers_` of the `currentBatch_`, or, as soon as that batch has
  // been handed to the `wordWriterQueue_`, into `lastWordStorage_`.
  std::string_view lastWord_;
  std::string lastWordStorage_;
  bool hasLastWord_ = false;
  // Whether any of the occurrences of the `lastWord_` was marked as external.
  // NOTE: This is only used for the (expensive) check of the vocabulary order.
  // A word is written to the vocabulary as soon as its first occurrence is
  // seen, so a later occurrence can no longer change its `isExternal` flag.
  bool lastWordIsExternal_ = false;
  // The index of the `lastWord_` within the `currentBatch_`, where `0` means
  // that it is the last distinct word of the previous batch (see
  // `IdMapBatch::globalIds_`).
  uint32_t indexOfLastWordInBatch_ = 0;
  // The batch that the merging thread is currently filling.
  WordBatch currentBatch_;
  // The global ID of the word that was written to the vocabulary last. NOTE:
  // This is only touched by the `wordWriterQueue_`'s thread.
  Id lastGlobalId_ = Id::makeUndefined();

  // The action that is performed by the `wordWriterQueue_` for a single batch.
  // It is set up by `mergeVocabulary`, because it requires access to the word
  // callback and to the regexes for blank nodes.
  //
  // NOTE: This has to be declared before the queues, because it is used by the
  // `wordWriterQueue_`'s thread and hence may only be destroyed after that
  // thread has been joined.
  std::function<void(WordBatch)> processWordBatch_;

  // we will store pairs of <partialId, globalId>
  std::vector<IdMapWriter> idMaps_;

  // The queues that make up the pipeline (see the comment above the class).
  //
  // NOTE: The order of the declarations is important, because the members are
  // destroyed in the reverse order of their declaration, and the destructor of
  // a queue blocks until all its pending tasks have been run. The
  // `wordWriterQueue_` pushes to the two other queues, and the
  // `idMapWriterQueue_` writes to the `idMaps_`, so this is the only order in
  // which no task can be pushed to (or run on) an already destroyed object.
  std::optional<ad_utility::TaskQueue<false>> idMapWriterQueue_;
  std::optional<ad_utility::TaskQueue<false>> mergedWordsDestructionQueue_;
  std::optional<ad_utility::TaskQueue<false>> wordWriterQueue_;

  // Friend declaration for the publicly available function.
  template <typename W, typename C>
  friend auto mergeVocabulary(
      const std::string& basename, size_t numFiles, W comparator,
      C& wordCallback, ad_utility::MemorySize memoryToUse,
      const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes)
      -> CPP_ret(VocabularyMetaData)(
          requires WordComparator<W>&& WordCallback<C>);
  VocabularyMerger() = default;

  // _______________________________________________________________
  // The function that performs the actual merge. See the static global
  // `mergeVocabulary` function for details.
  template <typename W, typename C>
  auto mergeVocabulary(
      const std::string& basename, size_t numFiles, W comparator,
      C& wordCallback, ad_utility::MemorySize memoryToUse,
      const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes)
      -> CPP_ret(VocabularyMetaData)(
          requires WordComparator<W>&& WordCallback<C>);

  // Eliminate the duplicates from a buffer of merged words and add the
  // resulting distinct words as well as one ID map entry per word to the
  // `currentBatch_`, which is handed to the `wordWriterQueue_` as soon as it is
  // full. The `QueueWord`s must be passed in alphabetical order wrt the
  // `comparator` (also across multiple calls). NOTE: This order is only checked
  // if the expensive checks are enabled (see `AD_EXPENSIVE_CHECK`), because the
  // additional comparison per word is rather costly.
  CPP_template(typename W)(requires WordComparator<W>) void processMergedWords(
      const std::vector<QueueWord>& buffer, const W& comparator);

  // Hand the `currentBatch_` to the `wordWriterQueue_` (which then
  // asynchronously writes the words to the vocabulary) and start a new batch.
  void flushBatch();

  // Reset the `currentBatch_` and allocate its buffers.
  void startNewBatch();

  // Write the distinct words of the `batch` to the vocabulary, determine their
  // global IDs, and hand the batch on to the `idMapWriterQueue_` and to the
  // `mergedWordsDestructionQueue_`. This runs on the `wordWriterQueue_`'s
  // thread (see `processWordBatch_`).
  CPP_template(typename C)(
      requires WordCallback<
          C>) void writeUniqueWordsToVocabulary(WordBatch batch,
                                                C& wordCallback,
                                                const std::vector<
                                                    std::unique_ptr<re2::RE2>>&
                                                    blankNodeIriRegexes,
                                                ad_utility::ProgressBar&
                                                    progressBar);

  // Close all associated files and file-based vectors and reset all internal
  // variables.
  void clear() {
    // NOTE: The order is important, see the declaration of the queues above.
    wordWriterQueue_.reset();
    mergedWordsDestructionQueue_.reset();
    idMapWriterQueue_.reset();
    processWordBatch_ = {};
    idMaps_.clear();
    metaData_ = VocabularyMetaData{};
    currentBatch_ = WordBatch{};
    lastWord_ = {};
    lastWordStorage_.clear();
    hasLastWord_ = false;
    lastWordIsExternal_ = false;
    indexOfLastWordInBatch_ = 0;
    lastGlobalId_ = Id::makeUndefined();
  }
};

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
