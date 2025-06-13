// Copyright 2025 University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include <variant>

#include "global/ValueId.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/BitUtils.h"
#include "util/Exception.h"
#include "util/HashSet.h"

// The signature of the SplitFunction for a SplitVocabulary. For each literal or
// IRI, it should return a marker index which of the underlying vocabularies of
// the SplitVocabulary should be used. The underlying vocabularies except 0
// should not hold conventional string literals (that is, without a special data
// type) or IRIs. Thus the function should return 0 for these inputs.
template <typename T>
CPP_concept SplitFunctionT =
    ad_utility::InvocableWithExactReturnType<T, uint8_t, std::string_view>;

// The signature of the SplitFilenameFunction for a SplitVocabulary. For a given
// base filename the function should construct readable filenames for each of
// the underlying vocabularies. This should usually happen by appending a suffix
// for each vocabulary.
template <typename T, uint8_t N>
CPP_concept SplitFilenameFunctionT =
    ad_utility::InvocableWithExactReturnType<T, std::array<std::string, N>,
                                             std::string_view>;

// A SplitVocabulary is a vocabulary layer that divides words into different
// underlying vocabularies. It is templated on the UnderlyingVocabularies as
// well as a SplitFunction that decides which underlying vocabulary is used for
// each word and a SplitFilenameFunction that assigns filenames to underlying
// vocabularies.
template <typename SplitFunction, typename SplitFilenameFunction,
          typename... UnderlyingVocabularies>
requires SplitFunctionT<SplitFunction> &&
         SplitFilenameFunctionT<SplitFilenameFunction,
                                sizeof...(UnderlyingVocabularies)>
class SplitVocabulary {
 public:
  // A SplitVocabulary must have at least two and at most 255 underlying
  // vocabularies. Note that this limit is very large and there should not be a
  // need for this many vocabularies. Two or three should suffice for reasonable
  // use cases.
  static_assert(sizeof...(UnderlyingVocabularies) >= 2 &&
                sizeof...(UnderlyingVocabularies) <= 255);
  static constexpr uint8_t numberOfVocabs =
      static_cast<uint8_t>(sizeof...(UnderlyingVocabularies));

  // Assuming we only make use of methods that all UnderlyingVocabularies
  // provide, we simplify this class by using an array over a variant instead of
  // a tuple.
  using AnyUnderlyingVocab =
      ad_utility::UniqueVariant<UnderlyingVocabularies...>;
  using UnderlyingVocabsArray = std::array<AnyUnderlyingVocab, numberOfVocabs>;
  using AnyUnderlyingWordWriterPtr = std::unique_ptr<WordWriterBase>;
  using UnderlyingWordWriterPtrsArray =
      std::array<AnyUnderlyingWordWriterPtr, numberOfVocabs>;

  // Bit masks for extracting and adding marker and vocabIndex bits
  static constexpr uint64_t markerBitMaskSize = ad_utility::bitMaskSizeForValue(
      numberOfVocabs - 1);  // Range of marker: [0..numberOfVocabs-1]
  static constexpr uint64_t markerBitMask =
      ad_utility::bitMaskForHigherBits(ValueId::numDatatypeBits +
                                       markerBitMaskSize) &
      ad_utility::bitMaskForLowerBits(ValueId::numDataBits);
  static constexpr uint64_t markerShift =
      ValueId::numDataBits - markerBitMaskSize;
  static constexpr uint64_t vocabIndexBitMask =
      ad_utility::bitMaskForLowerBits(markerShift);

  // Instances of the functions used for implementing the specific split logic
  static constexpr SplitFunction splitFunction_{};
  static constexpr SplitFilenameFunction splitFilenameFunction_{};

 private:
  // Array that holds all underlying vocabularies.
  UnderlyingVocabsArray underlying_;

 public:
  // Check validity of vocabIndex and marker, then return a new 64 bit index
  // that contains the marker and vocabIndex. The result is guaranteed to be
  // zero in all ValueId datatype bits.
  static uint64_t addMarker(uint64_t vocabIndex, uint8_t marker) {
    AD_CORRECTNESS_CHECK(marker < numberOfVocabs &&
                         vocabIndex <= vocabIndexBitMask);
    return vocabIndex | (static_cast<uint64_t>(marker) << markerShift);
  };

  // Extract the marker from a full 64 bit index.
  static constexpr uint8_t getMarker(uint64_t indexWithMarker) {
    uint64_t marker = (indexWithMarker & markerBitMask) >> markerShift;
    AD_CORRECTNESS_CHECK(marker < numberOfVocabs);
    return static_cast<uint8_t>(marker);
  }

  // Use the SplitFunction to determine the marker for a given word (that is, in
  // which vocabulary this word would go)
  static uint8_t getMarkerForWord(const std::string_view& word) {
    return splitFunction_(word);
  };

  // Helper to detect if a "special" vocabulary is used.
  static constexpr bool isSpecialVocabIndex(uint64_t indexWithMarker) {
    return getMarker(indexWithMarker) != 0;
  }

  // Extract only the vocab index bits and remove ValueId datatype and marker
  // bits.
  static constexpr uint64_t getVocabIndex(uint64_t indexWithMarker) {
    return indexWithMarker & vocabIndexBitMask;
  };

  // Close all underlying vocabularies.
  void close();

  // Read the vocabulary from files: all underlying vocabularies will be read
  // using the filenames returned by SplitFilenameFunction for the given base
  // filename.
  void readFromFile(const std::string& filename);

  // The item-at operator retrieves a word by a given index. The index is
  // expected to have the marker bits set to indicate which underlying
  // vocabulary is to be used.
  // Note: The item-at operator needs to be defined in header to avoid some
  // serious compiler trouble.
  decltype(auto) operator[](uint64_t idx) const {
    // Check marker bit to determine which vocabulary to use
    auto unmarkedIdx = getVocabIndex(idx);
    auto marker = getMarker(idx);

    // Retrieve the word from the indicated underlying vocabulary
    return std::visit(
        [&unmarkedIdx](auto& vocab) {
          AD_CORRECTNESS_CHECK(unmarkedIdx < vocab.size());
          // TODO<ullingerc>: How to handle if the different underlying
          // vocabularies return different types (std::string / std::string_view
          // / ...) on their operator[] implementations? A variant will probably
          // cause trouble in the Vocabulary class.
          return vocab[unmarkedIdx];
        },
        underlying_[marker]);
  }

  // The size of a SplitVocabulary is the sum of the sizes of the underlying
  // vocabularies.
  [[nodiscard]] uint64_t size() const {
    uint64_t total = 0;
    for (auto& vocab : underlying_) {
      total += std::visit([](auto& v) { return v.size(); }, vocab);
    }
    return total;
  }

  // Perform a search for upper or lower bound on the underlying vocabulary
  // given by the marker parameter. By default this is the "main" vocabulary
  // (first).
  template <typename InternalStringType, typename Comparator,
            bool getUpperBound>
  WordAndIndex boundImpl(const InternalStringType& word, Comparator comparator,
                         uint8_t marker = 0) const {
    AD_CORRECTNESS_CHECK(marker < numberOfVocabs);
    WordAndIndex subResult = std::visit(
        [&](auto& v) {
          if constexpr (getUpperBound) {
            return v.upper_bound(word, comparator);
          } else {
            return v.lower_bound(word, comparator);
          }
        },
        underlying_[marker]);
    if (subResult.isEnd()) {
      return subResult;
    }
    return {subResult.word(), addMarker(subResult.index(), marker)};
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator, uint8_t marker = 0) const {
    return boundImpl<InternalStringType, Comparator, false>(word, comparator,
                                                            marker);
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator, uint8_t marker = 0) const {
    return boundImpl<InternalStringType, Comparator, true>(word, comparator,
                                                           marker);
  }

  // Shortcut to retrieve the first underlying vocabulary
  AnyUnderlyingVocab& getUnderlyingMainVocabulary() { return underlying_[0]; }
  const AnyUnderlyingVocab& getUnderlyingMainVocabulary() const {
    return underlying_[0];
  }

  // Retrieve a reference to any of the underlying vocabularies
  AnyUnderlyingVocab& getUnderlyingVocabulary(uint8_t marker) {
    AD_CORRECTNESS_CHECK(marker < numberOfVocabs);
    return underlying_[marker];
  }
  const AnyUnderlyingVocab& getUnderlyingVocabulary(uint8_t marker) const {
    AD_CORRECTNESS_CHECK(marker < numberOfVocabs);
    return underlying_[marker];
  }

  // Load from file: open all underlying vocabularies on the corresponding
  // result of SplitFilenameFunction for the given base filename.
  void open(const std::string& filename);

  // This word writer writes words to different vocabularies depending on the
  // result of SplitFunction.
  class WordWriter : public WordWriterBase {
   private:
    UnderlyingWordWriterPtrsArray underlyingWordWriters_;

   public:
    // Construct a WordWriter for each vocabulary in the given array. Determine
    // filenames of underlying vocabularies using the SplitFilenameFunction.
    WordWriter(const UnderlyingVocabsArray& underlyingVocabularies,
               const std::string& filename);

    // Add the next word to the vocabulary and return its index.
    uint64_t operator()(std::string_view word, bool isExternal) override;

    // Finish the writing on all underlying word writers. After this no more
    // calls to `operator()` are allowed.
    void finishImpl() override;
  };

  // Construct a SplitVocabulary::WordWriter that creates WordWriters on all
  // underlying vocabularies and calls the appropriate one depending on the
  // result of SplitFunction for the given word.
  std::unique_ptr<WordWriter> makeDiskWriterPtr(
      const std::string& filename) const {
    return std::make_unique<WordWriter>(underlying_, filename);
  }
};

// Concrete implementations of split function and split filename function
namespace detail::splitVocabulary {

// Split function for Well-Known Text Literals: All words are written to
// vocabulary 0 except WKT literals, which go to vocabulary 1.
[[maybe_unused]] inline auto geoSplitFunc =
    [](std::string_view word) -> uint8_t {
  return word.starts_with("\"") && word.ends_with(GEO_LITERAL_SUFFIX);
};

// Split filename function for Well-Known Text Literals: The vocabulary 0 is
// saved under the base filename and WKT literals are saved with a suffix
// ".geometry"
[[maybe_unused]] inline auto geoFilenameFunc =
    [](std::string_view base) -> std::array<std::string, 2> {
  return {std::string(base), absl::StrCat(base, ".geometry")};
};

}  // namespace detail::splitVocabulary

// A SplitGeoVocabulary splits only Well-Known Text literals to their own
// vocabulary. This can be used for precomputations for spatial features.
// TODO<ullingerc>: Switch 2nd Vocab to GeoVocabulary<UnderlyingVocabulary>
// after merge of #1951
template <class UnderlyingVocabulary>
using SplitGeoVocabulary =
    SplitVocabulary<decltype(detail::splitVocabulary::geoSplitFunc),
                    decltype(detail::splitVocabulary::geoFilenameFunc),
                    UnderlyingVocabulary, UnderlyingVocabulary>;

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H
