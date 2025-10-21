// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGER_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGER_H

#include <optional>
#include <string>
#include <utility>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexBuilderTypes.h"
#include "index/Vocabulary.h"
#include "util/HashMap.h"
#include "util/ProgressBar.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializePair.h"
#include "util/Serializer/SerializeVector.h"
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
// Concept for a callable that compares two `string_view`s.
template <typename T>
CPP_concept WordComparator =
    ranges::predicate<T, std::string_view, std::string_view>;

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
// appearance. Argument `maxFilesPerBatch` controls the maximum number of files
// to merge simultaneously (to avoid file descriptor limits).
template <typename W, typename C>
auto mergeVocabulary(const std::string& basename, size_t numFiles, W comparator,
                     C& wordCallback, ad_utility::MemorySize memoryToUse,
                     size_t maxFilesPerBatch = MAX_NUM_FILES_FOR_DIRECT_MERGE)
    -> CPP_ret(VocabularyMetaData)(
        requires WordComparator<W>&& WordCallback<C>);

// Helper structure that holds information about batch merging.
struct BatchInfo {
  size_t batchIndex_;           // Which meta-batch (0, 1, 2, ...)
  size_t originalFileIndex_;    // Original file index in the full set

  BatchInfo(size_t batchIndex, size_t originalFileIndex)
      : batchIndex_(batchIndex), originalFileIndex_(originalFileIndex) {}
};

// A helper class that implements the `mergeVocabulary` function (see
// above). Everything in this class is private and only the
// `mergeVocabulary` function is a friend.
class VocabularyMerger {
 private:
  // private data members

  // The result (mostly metadata) which we'll return.
  VocabularyMetaData metaData_;
  /**
   * @brief Struct to manage duplicate occurrences of words during merging of
   *        partial vocabularies.
   * @detail This struct is needed for the correct semantics of isExternal.
   *         If multiple occurrences of the same iriOrLiteral appear, they are
   *         flattened to one vocab entry. If those occurrences have different
   *         values for isExternal the logic is as follows:
   *         For isExternal: If one of the occurrences has this set to true,
   *         this should be set to true later on after merging.
   *
   *         This struct is used to collect all multiple occurrences of
   *         iriOrLiterals and once all are collected, the writing happens. The
   *         reason the collection works is the QueueWords being sorted.
   */
  struct EqualWords {
    std::string iriOrLiteral_;
    bool isExternal_;
    // The targetId_ the Id the words are mapped to by vocabulary. This is the
    // same for equal words since they are later written as one.
    Id targetId_;
    // partialVocabAndPartialFileIds
    struct PartialIds {
      size_t fileId_;
      size_t localIndex_;
    };
    std::vector<PartialIds> partialIds_;

    EqualWords(std::string iriOrLiteral, bool isExternal,
               size_t fileId, size_t localIndex)
        : iriOrLiteral_(std::move(iriOrLiteral)),
          isExternal_(isExternal),
          partialIds_({{fileId, localIndex}}) {
      targetId_ = Id::makeUndefined();
    }

    bool isBlankNode() const { return iriOrLiteral_.starts_with("_:"); }
  };
  std::optional<EqualWords> currentWord_ = std::nullopt;
  // we will store pairs of <partialId, globalId>
  std::vector<IdMapWriter> idMaps_;

  const size_t bufferSize_ = BATCH_SIZE_VOCABULARY_MERGE;

  // Friend declaration for the publicly available function.
  template <typename W, typename C>
  friend auto mergeVocabulary(const std::string& basename, size_t numFiles,
                              W comparator, C& wordCallback,
                              ad_utility::MemorySize memoryToUse)
      -> CPP_ret(VocabularyMetaData)(
          requires WordComparator<W>&& WordCallback<C>);
  VocabularyMerger() = default;

  // _______________________________________________________________
  // The function that performs the actual merge. See the static global
  // `mergeVocabulary` function for details.
  template <typename W, typename C>
  auto mergeVocabulary(const std::string& basename, size_t numFiles,
                       W comparator, C& wordCallback,
                       ad_utility::MemorySize memoryToUse,
                       size_t maxFilesPerBatch)
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

    [[nodiscard]] const auto& id() const { return entry_.index_; }
  };

  constexpr static auto sizeOfQueueWord = [](const QueueWord& q) {
    return ad_utility::MemorySize::bytes(sizeof(QueueWord) +
                                         q.entry_.iriOrLiteral().size());
  };

  // Write the queue words in the buffer to their corresponding `idMaps`.
  // The `QueueWord`s must be passed in alphabetical order wrt `lessThan` (also
  // across multiple calls).
  // clang-format off
    CPP_template(typename C, typename L)(
      requires WordCallback<C> CPP_and ranges::predicate<
          L, TripleComponentWithIndex, TripleComponentWithIndex>)
      // clang-format on
      void writeQueueWordsToIdMap(const std::vector<QueueWord>& buffer,
                                  C& wordCallback, const L& lessThan,
                                  ad_utility::ProgressBar& progressBar);

  // Write queue words for two-stage merging (writes to batch ID maps instead of
  // final ID maps).
  // clang-format off
    CPP_template(typename C, typename L)(
      requires WordCallback<C> CPP_and ranges::predicate<
          L, TripleComponentWithIndex, TripleComponentWithIndex>)
      // clang-format on
      void writeQueueWordsToIdMapTwoStage(
          const std::vector<QueueWord>& buffer, C& wordCallback,
          const L& lessThan, ad_utility::ProgressBar& progressBar,
          std::vector<IdMapWriter>& batchToGlobalIdMaps);

  // This function has to be called for a word once all of its occurrences in
  // the different partial vocabularies have been processed by the queue. It
  // gets the index and target index for EqualWords. This target index has to be
  // set only once for EqualWords. The function isn't const since `metaData_` is
  // modified.
  CPP_template(typename C)(
      requires WordCallback<C>) void writeAndGetEqualWordIds(EqualWords&
                                                                 equalWords,
                                                             C& wordCallback);

  // Close all associated files and file-based vectors and reset all internal
  // variables.
  void clear() {
    metaData_ = VocabularyMetaData{};
    currentWord_ = std::nullopt;
    idMaps_.clear();
  }

  // Inner helper function for the parallel pipeline, which performs the actual
  // write to the IdMaps. Format of argument is `<mapToWriteTo<internalId,
  // globalId>>`.
  void doActualWrite(
      const std::vector<std::pair<size_t, std::pair<size_t, Id>>>& buffer);

  // Helper: Create comparator functions from a word comparator.
  template <typename W>
  static auto makeComparators(W comparator);

  // Helper: Create a generator that reads words from a partial vocabulary file.
  static auto makePartialVocabGenerator(const std::string& basename,
                                        size_t fileIndex);

  // Helper: Create a generator that reads words from a batch vocabulary file.
  static auto makeBatchVocabGenerator(const std::string& basename,
                                      size_t batchIndex);

  // Helper: Process a queue word and update metadata/mappings.
  // Returns true if this is a new unique word.
  template <typename C, typename L>
  bool processQueueWord(const QueueWord& word, C& wordCallback,
                        const L& lessThan, ad_utility::ProgressBar& progressBar)
      requires WordCallback<C> &&
               ranges::predicate<L, TripleComponentWithIndex,
                                 TripleComponentWithIndex>;

  // Helper: Compute the target ID for the last processed triple component.
  Id getTargetIdForLastComponent() const;

  // Helper: Setup and execute parallel multiway merge for generators.
  template <typename GeneratorRange, typename Comparator>
  static auto setupParallelMerge(GeneratorRange&& generators,
                                 const Comparator& lessThanForQueue,
                                 ad_utility::MemorySize memoryToUse);

  // Helper: Compose ID mappings in Stage 3 of two-stage merge.
  void composeIdMappings(const std::string& basename, size_t numFiles,
                         size_t batchSize, size_t numBatches);

  // Merge a batch of partial vocabulary files into a single batch file.
  // Returns the number of words in the merged batch.
  template <typename W>
  size_t mergeSingleBatch(const std::string& basename, size_t batchIndex,
                          size_t startFile, size_t endFile, W comparator,
                          ad_utility::MemorySize memoryToUse);

  // Perform two-stage merge when the number of files exceeds the limit.
  template <typename W, typename C>
  auto mergeTwoStage(const std::string& basename, size_t numFiles, W comparator,
                     C& wordCallback, ad_utility::MemorySize memoryToUse,
                     size_t maxFilesPerBatch)
      -> CPP_ret(VocabularyMetaData)(
          requires WordComparator<W>&& WordCallback<C>);
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
ad_utility::HashMap<uint64_t, uint64_t> createInternalMapping(ItemVec* els);

/**
 * @brief for each of the IdTriples in <input>: map the three Ids using the
 * <map> and write the resulting Id triple to <*writePtr>
 */
template <typename T>
void writeMappedIdsToExtVec(const T& input,
                            const ad_utility::HashMap<Id, Id>& map,
                            std::unique_ptr<TripleVec>* writePtr);

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
ItemVec vocabMapsToVector(ItemMapArray& map);

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

#include "VocabularyMergerImpl.h"

#endif  // QLEVER_SRC_INDEX_VOCABULARYMERGER_H
