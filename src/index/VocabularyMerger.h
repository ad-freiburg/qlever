// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGER_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGER_H

#include <memory>
#include <optional>
#include <string>
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

// A single entry of an ID map (see `IdMapWriter` below): the ID that a word has
// inside a partial vocabulary, and the global ID that the vocabulary merger has
// assigned to that word.
//
// NOTE: This deliberately is a plain struct and not a `std::pair<Id, Id>`.
// libstdc++'s `std::pair` has user-provided assignment operators and hence is
// not trivially copyable, which would make a `std::vector` of them neither
// trivially serializable (it would then be written and read one `Id` at a time)
// nor bitwise relocatable.
struct IdMapEntry {
  Id localId_;
  Id globalId_;

  bool operator==(const IdMapEntry& other) const {
    return localId_ == other.localId_ && globalId_ == other.globalId_;
  }
  bool operator!=(const IdMapEntry& other) const { return !(*this == other); }

  // Enable the serialization of an `IdMapEntry` (and of contiguous ranges of
  // them) in the `ad_utility::serialization` framework.
  template <typename T>
  friend std::true_type allowTrivialSerialization(IdMapEntry, T);

  // Make the output of failed tests readable.
  friend std::ostream& operator<<(std::ostream& str, const IdMapEntry& entry) {
    return str << '{' << entry.localId_ << ", " << entry.globalId_ << '}';
  }
};
static_assert(
    ad_utility::serialization::TriviallySerializable<IdMapEntry>,
    "An `IdMapEntry` has to be trivially serializable, else a whole `IdMap` "
    "would be written and read one `Id` at a time");

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
class VocabularyMerger {
 private:
  // private data members

  // The result (mostly metadata) which we'll return.
  VocabularyMetaData metaData_;
  std::optional<TripleComponentWithIndex> lastTripleComponent_ = std::nullopt;
  // Whether `lastTripleComponent_` is a blank node. Cached here so that
  // `isBlankNode` (which may run a set of regexes) is evaluated only once per
  // distinct word.
  bool lastTripleComponentIsBlankNode_ = false;
  // The global ID of `lastTripleComponent_`. Cached here because it is the same
  // for all the (possibly many) occurrences of a word in the partial
  // vocabularies, and computing it involves a branch and a range check.
  Id lastTargetId_ = Id::makeUndefined();
  // we will store pairs of <partialId, globalId>
  std::vector<IdMapWriter> idMaps_;

  // An `IdMapEntry` together with the index of the partial vocabulary it has to
  // be written to.
  struct QueuedIdMapEntry {
    size_t partialFileId_;
    IdMapEntry entry_;
  };

  // The `QueuedIdMapEntry`s are not written by the thread that performs the
  // merge,
  // but by a single separate thread, such that the (rather expensive) merging
  // of the words and the writing of the ID maps happen concurrently.
  std::optional<ad_utility::TaskQueue<false>> idMapWriterQueue_;
  // The maximal number of batches that may be waiting in the
  // `idMapWriterQueue_`.
  static constexpr size_t idMapWriterQueueSize = 5;
  // The entries are collected in this buffer before they are handed to the
  // `idMapWriterQueue_`. This is necessary because a single call to
  // `writeQueueWordsToIdMap` only deals with a rather small number of words
  // (currently 100), which is much too fine-grained for a task queue.
  std::vector<QueuedIdMapEntry> idMapEntryBuffer_;
  // The number of entries that are collected in the `idMapEntryBuffer_` before
  // they are handed to the `idMapWriterQueue_` as a single task.
  static constexpr size_t idMapEntryBatchSize = 100'000;

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

  // Write the queue words in the buffer to their corresponding `idMaps`.
  // The `QueueWord`s must be passed in alphabetical order wrt `lessThan` (also
  // across multiple calls). NOTE: This order is only checked if the expensive
  // checks are enabled (see `AD_EXPENSIVE_CHECK`), because the additional
  // comparison per word is rather costly.
  // clang-format off
    CPP_template(typename C, typename L)(
      requires WordCallback<C> CPP_and ranges::predicate<
          L, TripleComponentWithIndex, TripleComponentWithIndex>)
      // clang-format on
      void writeQueueWordsToIdMap(
          std::vector<QueueWord>& buffer, C& wordCallback,
          [[maybe_unused]] const L& lessThan,
          const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes,
          ad_utility::ProgressBar& progressBar);

  // Hand the contents of the `idMapEntryBuffer_` to the `idMapWriterQueue_`,
  // which then asynchronously writes them to the corresponding `idMaps_`.
  void flushIdMapEntries();

  // Close all associated files and file-based vectors and reset all internal
  // variables.
  void clear() {
    metaData_ = VocabularyMetaData{};
    lastTripleComponent_ = std::nullopt;
    lastTripleComponentIsBlankNode_ = false;
    lastTargetId_ = Id::makeUndefined();
    idMapEntryBuffer_.clear();
    // NOTE: The order is important. The destructor of the `idMapWriterQueue_`
    // blocks until all pending entries have been written, so the `idMaps_` may
    // only be destroyed afterwards.
    idMapWriterQueue_.reset();
    idMaps_.clear();
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
