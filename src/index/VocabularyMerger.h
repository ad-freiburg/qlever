// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYMERGER_H
#define QLEVER_SRC_INDEX_VOCABULARYMERGER_H

#include <cstdint>
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
#include "util/HashMap.h"
#include "util/ParallelBlockMerge.h"
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

// A single word of a partial vocabulary, together with the index of the
// partial vocabulary that it came from. This is the element type of the merge
// of the partial vocabularies (see `PartialVocabRunsInput` below).
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

// Return the memory that a single `QueueWord` occupies.
struct SizeOfQueueWord {
  ad_utility::MemorySize operator()(const QueueWord& q) const {
    return ad_utility::MemorySize::bytes(sizeof(QueueWord) +
                                         q.entry_.iriOrLiteral().size());
  }
};
constexpr inline SizeOfQueueWord sizeOfQueueWord{};

// The input policy (see `ad_utility::parallelBlockMerge::BlockedRunsInput`) for
// the merge of the partial vocabularies in the files
// `basename + PARTIAL_VOCAB_WORDS_INFIX + to_string(i)` for `0 <= i <
// numFiles`. Every such file is one presorted run, and the blocks of that run
// are exactly the virtual blocks that are described by the sparse splitter
// index at the end of the file (see `writePartialVocabularyToFile`). The
// splitter index gives us the number of words as well as the first and the last
// word of every block without any I/O, which is exactly what the parallel merge
// needs to split its work into independent chunks.
//
// If a file has no (valid) splitter index, then the index is built by a single
// sequential scan of that file, see `buildIndexByScanning`.
class PartialVocabRunsInput {
 public:
  using value_type = QueueWord;

  // The key by which the words are ordered. It deliberately has the same
  // accessors as `QueueWord`, such that a single generic comparator can compare
  // keys and elements in any combination.
  struct Key {
    std::string word_;
    bool isExternal_ = false;

    std::string_view iriOrLiteral() const { return word_; }
    bool isExternal() const { return isExternal_; }
  };
  using Block = std::vector<QueueWord>;

  // The metadata of a single block of a single run, which is the information
  // that the merge requires without performing any I/O.
  struct BlockMetadata {
    // The byte offset at which the record of the first word of the block
    // starts.
    uint64_t byteOffset_ = 0;
    // The number of words in all the previous blocks of the same run.
    uint64_t numWordsBefore_ = 0;
    Key firstKey_;
    Key lastKey_;
  };

 private:
  // A single presorted run, that is a single partial vocabulary file.
  struct Run {
    // The `File` is shared, because `readBlock` creates a `pread`-based
    // serializer on it for every single call, possibly from several threads at
    // the same time.
    std::shared_ptr<ad_utility::File> file_;
    // The total number of words in this run.
    uint64_t numWords_ = 0;
    // The byte offset directly behind the record of the last word, that is the
    // end of the part of the file that holds the words.
    uint64_t wordsEnd_ = 0;
    std::vector<BlockMetadata> blocks_;
  };

  std::vector<Run> runs_;
  // The number of words per block that is used when a file has no valid
  // splitter index and the metadata has to be built by a sequential scan.
  size_t fallbackSplitterInterval_;

 public:
  // Open the `numFiles` partial vocabularies and read (or, as a fallback,
  // compute) their block metadata. Only unit tests should ever change the
  // `fallbackSplitterInterval` from its default.
  explicit PartialVocabRunsInput(
      const std::string& basename, size_t numFiles,
      size_t fallbackSplitterInterval = PARTIAL_VOCAB_SPLITTER_INTERVAL);

  // ______________________________________________________________________
  size_t numRuns() const { return runs_.size(); }

  // ______________________________________________________________________
  size_t numBlocks(size_t run) const { return runs_.at(run).blocks_.size(); }

  // ______________________________________________________________________
  size_t numElementsInBlock(size_t run, size_t block) const;

  // ______________________________________________________________________
  const Key& firstKey(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).firstKey_;
  }

  // ______________________________________________________________________
  const Key& lastKey(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).lastKey_;
  }

  // Read the words of a single block from the file of the given run. This is
  // the only function that performs I/O, and it is thread-safe, because it uses
  // a `CopyableFileReadSerializer` (which is based on `pread` and therefore
  // does not touch the file position) that is local to the call.
  Block readBlock(size_t run, size_t block) const;

  // ______________________________________________________________________
  Block makeEmptyBlock() const { return Block{}; }

  // ______________________________________________________________________
  template <typename T>
  void appendToBlock(Block& block, T&& element) const {
    block.push_back(std::forward<T>(element));
  }

  // ______________________________________________________________________
  ad_utility::MemorySize memorySizeOfElement(const QueueWord& element) const {
    return sizeOfQueueWord(element);
  }

  // Return an estimate of the memory that the largest block of any run occupies
  // once it has been read into a `Block`. The merge uses this to bound the
  // number of chunks that it keeps in flight, because every such chunk reads up
  // to one block per run at a time.
  ad_utility::MemorySize maxBlockMemory() const;

  // Return the metadata of all the blocks of the given run. This is only needed
  // for testing.
  const std::vector<BlockMetadata>& blockMetadata(size_t run) const {
    return runs_.at(run).blocks_;
  }

 private:
  // Open the file with the given name and read its block metadata, either from
  // the splitter index at the end of the file, or, if that index is missing or
  // broken, by a sequential scan.
  Run readRun(const std::string& filename) const;

  // Try to read the block metadata of the `run` from the sparse splitter index
  // at the end of its file. Return false (and leave the `run` unchanged) if the
  // index is missing or not consistent with the rest of the file, in which case
  // the caller has to fall back to `buildIndexByScanning`.
  static bool readSplitterIndex(Run& run, uint64_t fileSize);

  // Build the block metadata of the `run` by a single sequential scan of its
  // file. This is the fallback for files that were written by hand (as in some
  // unit tests) or by an older version of QLever, and it is therefore also the
  // path that defines the *meaning* of the metadata.
  void buildIndexByScanning(Run& run, uint64_t fileSize) const;
};

// Return the options for the parallel merge of the partial vocabularies of the
// given `input` (see `ParallelBlockMerge.h`), such that the merge stays within
// the given memory budget. The `maxParallelism` is the number of chunks that
// the scheduler can run at the same time.
ad_utility::parallelBlockMerge::MergeOptions mergeOptionsForPartialVocabularies(
    const PartialVocabRunsInput& input, ad_utility::MemorySize memoryToUse,
    size_t maxParallelism);

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
  // we will store pairs of <partialId, globalId>
  std::vector<IdMapWriter> idMaps_;

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

  // Write the queue words in the buffer to their corresponding `idMaps`.
  // The `QueueWord`s must be passed in alphabetical order wrt `lessThan` (also
  // across multiple calls).
  // clang-format off
    CPP_template(typename C, typename L)(
      requires WordCallback<C> CPP_and ranges::predicate<
          L, TripleComponentWithIndex, TripleComponentWithIndex>)
      // clang-format on
      void writeQueueWordsToIdMap(
          std::vector<QueueWord>& buffer, C& wordCallback, const L& lessThan,
          const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes,
          ad_utility::ProgressBar& progressBar);

  // Close all associated files and file-based vectors and reset all internal
  // variables.
  void clear() {
    metaData_ = VocabularyMetaData{};
    lastTripleComponent_ = std::nullopt;
    lastTripleComponentIsBlankNode_ = false;
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
 * The exact layout of the resulting file is the following:
 *
 *     [uint64 numWords]                                  <- offset 0
 *     numWords x { [word][isExternal][uint64 id] }
 *     ---- the sparse splitter index ----
 *     [uint64 numIndexEntries]                           <- byte offset `I`
 *     numIndexEntries x { [uint64 byteOffsetOfFirstWordOfBlock]
 *                         [uint64 numWordsBeforeThisBlock]
 *                         [firstWord][firstIsExternal]
 *                         [lastWord][lastIsExternal] }
 *     [uint64 I]                                         <- 16-byte footer
 *     [uint64 PARTIAL_VOCAB_INDEX_MAGIC]
 *
 * Entry `j` of the splitter index describes the virtual block of words with the
 * indices `[j * splitterInterval, min((j + 1) * splitterInterval, numWords))`.
 * It stores the first and the last word of that block (together with their
 * `isExternal` flags), such that a reader has the first and the last key of
 * each block available without any additional I/O, as well as the exact byte
 * offset at which the first word record of the block starts, such that a reader
 * can seek there directly. There are always `ceil(numWords / splitterInterval)`
 * entries; in particular a vocabulary with zero words leads to zero entries,
 * but the footer is written in any case.
 *
 * NOTE: All the parts before the splitter index are unchanged from the previous
 * format, so a reader that reads `numWords` and then exactly that many word
 * records still works and simply ignores the trailing bytes. Conversely, a
 * reader that wants to use the splitter index has to check the last 8 bytes of
 * the file for `PARTIAL_VOCAB_INDEX_MAGIC` and fall back to a sequential scan
 * if the magic is absent (e.g. for files that were written by hand or by an
 * older version of QLever).
 *
 * @param els The input
 * @param fileName will write to this file. If it exists it will be overwritten
 * @param splitterInterval The number of words per virtual block, see above.
 * Only unit tests should ever change this from its default.
 */
void writePartialVocabularyToFile(
    const ItemVec& els, const std::string& fileName,
    size_t splitterInterval = PARTIAL_VOCAB_SPLITTER_INTERVAL);

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
