// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>
#pragma once

#include <string>
#include <stxxl/vector>
#include <utility>

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/HashMap.h"
#include "../util/MmapVector.h"
#include "./ConstantsIndexBuilding.h"
#include "./IndexBuilderTypes.h"
#include "Vocabulary.h"
#include "engine/idTable/CompressedExternalIdTable.h"

using IdPairMMapVec = ad_utility::MmapVector<std::pair<Id, Id>>;
using IdPairMMapVecView = ad_utility::MmapVectorView<std::pair<Id, Id>>;
using std::string;

using TripleVec = ad_utility::CompressedExternalIdTable<3>;

/**
 * Class for merging the partial vocabularies. The main function is still in the
 * `mergeVocabulary` function, but the parallel pipeline is easier when this is
 * encapsulated within a class.
 */
class VocabularyMerger {
 public:
  // If this is set, then we will only output the internal vocabulary.
  // This is useful for the prefix compression, where we don't need the
  // external part of the vocabulary and the mapping from local to global IDs.
  bool _noIdMapsAndIgnoreExternalVocab = false;
  // result of a call to mergeVocabulary
  struct VocabularyMetaData {
    // This struct is used to incrementally construct the range of IDs that
    // correspond to a given prefix. To use it, all the words from the
    // vocabulary must be passed to the member function `addIfWordMatches` in
    // sorted order. After that, the range `[begin(), end())` is the range of
    // all the words that start with the prefix.
    struct IdRangeForPrefix {
      IdRangeForPrefix(std::string prefix) : prefix_{std::move(prefix)} {}
      // Check if `word` starts with the `prefix_`. If so, `wordIndex`
      // will become part of the range that this struct represents.
      // For this to work, all the words that start with the `prefix_` have to
      // be passed in consecutively and their indices have to be consecutive
      // and ascending.
      void addIfWordMatches(std::string_view word, size_t wordIndex) {
        if (!word.starts_with(prefix_)) {
          return;
        }
        if (!beginWasSeen_) {
          begin_ = Id::makeFromVocabIndex(VocabIndex::make(wordIndex));
          beginWasSeen_ = true;
        }
        end_ = Id::makeFromVocabIndex(VocabIndex::make(wordIndex + 1));
      }

      Id begin() const { return begin_; }
      Id end() const { return end_; }

     private:
      Id begin_ = ID_NO_VALUE;
      Id end_ = ID_NO_VALUE;
      std::string prefix_;
      bool beginWasSeen_ = false;
    };

    size_t numWordsTotal_ = 0;  // that many distinct words were found (size of
                                // the vocabulary)
    IdRangeForPrefix langTaggedPredicates_{"@"};
    IdRangeForPrefix internalEntities_{INTERNAL_ENTITIES_URI_PREFIX};
  };

 private:
  // private data members

  // The result (mostly metadata) which we'll return.
  VocabularyMetaData metaData_;
  std::optional<TripleComponentWithIndex> lastTripleComponent_ = std::nullopt;
  std::ofstream outfileExternal_;
  // we will store pairs of <partialId, globalId>
  std::vector<IdPairMMapVec> idVecs_;

  const size_t _bufferSize = 10000000;

 public:
  VocabularyMerger() = default;

  // _______________________________________________________________
  // merge the partial vocabularies in the  binary files
  // fileIdx + PARTIAL_VOCAB_FILE_NAME + to_string(i)
  // where 0 <= i < numFiles
  // Directly Writes .vocabulary file at fileIdx (no more need to pass
  // through Vocabulary class
  // Writes file "externalTextFile" which can be used to directly write external
  // Literals
  // Returns the number of total Words merged and via the parameters
  // the lower and upper bound of language tagged predicates
  // Argument comparator gives the way to order strings (case-sensitive or not)
  // This automatically resets the inner members after finishing, to leave the
  // external interface stateless
  template <typename Comp, typename InternalVocabularyAction>
  VocabularyMetaData mergeVocabulary(const std::string& fileIdx,
                                     size_t numFiles, Comp comparator,
                                     InternalVocabularyAction& action,
                                     ad_utility::MemorySize memToUse);

 private:
  // helper struct used in the priority queue for merging.
  // represents tokens/words in a certain partial vocabulary
  struct QueueWord {
    QueueWord() = default;
    QueueWord(TripleComponentWithIndex&& v, size_t file)
        : _entry(std::move(v)), _partialFileId(file) {}
    TripleComponentWithIndex _entry;  // the word, its local ID and the
                                      // information if it will be externalized
    size_t _partialFileId;  // from which partial vocabulary did this word come

    [[nodiscard]] const bool& isExternal() const { return _entry.isExternal(); }
    [[nodiscard]] bool& isExternal() { return _entry.isExternal(); }

    [[nodiscard]] const std::string& iriOrLiteral() const {
      return _entry.iriOrLiteral();
    }
    [[nodiscard]] std::string& iriOrLiteral() { return _entry.iriOrLiteral(); }

    [[nodiscard]] const auto& id() const { return _entry.index_; }
    [[nodiscard]] auto& id() { return _entry.index_; }
  };

  constexpr static auto sizeOfQueueWord = [](const QueueWord& q) {
    return ad_utility::MemorySize::bytes(sizeof(QueueWord) +
                                         q._entry.iriOrLiteral().size());
  };

  // write the queu words in the buffer to their corresponding idPairVecs.
  // Requires that all the QueueWords that are ever passed are ordered
  // alphabetically (Also across multiple calls)
  template <typename InternalVocabularyAction>
  void writeQueueWordsToIdVec(
      const std::vector<QueueWord>& buffer,
      InternalVocabularyAction& internalVocabularyAction, const auto& lessThan);

  // close all associated files and MmapVectors and reset all internal variables
  void clear() {
    metaData_ = VocabularyMetaData{};
    lastTripleComponent_ = std::nullopt;
    outfileExternal_ = std::ofstream();
    idVecs_.clear();
  }

  // inner helper function for the parallel pipeline. perform the actual write
  // to the IdPairVecs. Format of argument is <vecToWriteTo<internalId,
  // globalId>>
  void doActualWrite(
      const std::vector<std::pair<size_t, std::pair<size_t, size_t>>>& buffer);
};

// ______________
// TODO<joka921> is this even used
// anymore?___________________________________________________________________________
template <class Comp>
void writePartialIdMapToBinaryFileForMerging(
    std::shared_ptr<const ItemMapArray> map, const string& fileName, Comp comp,
    bool doParallelSort);

// _________________________________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const string& mmapFilename);

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
void writeMappedIdsToExtVec(const auto& input,
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
void writePartialVocabularyToFile(const ItemVec& els, const string& fileName);

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

#include "VocabularyGeneratorImpl.h"
