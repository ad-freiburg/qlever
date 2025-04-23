// Copyright 2025 University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>

#include "global/ValueId.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/HashSet.h"

// Func. to decide where a literal goes
template <const auto& T>
CPP_concept SplitFunctionT =
    ad_utility::InvocableWithExactReturnType<decltype(T), bool,
                                             std::string_view>;

// Func. to get filenames for each vocabulary
template <const auto& T>
CPP_concept SplitFilenameFunctionT = ad_utility::InvocableWithExactReturnType<
    decltype(T), std::array<std::string, 2>, std::string>;

CPP_template(class MainVocabulary, class SpecialVocabulary,
             const auto& SplitFunction, const auto& SplitFilenameFunction)(
    requires SplitFunctionT<SplitFunction> CPP_and
        SplitFilenameFunctionT<SplitFilenameFunction>) class SplitVocabulary {
 private:
  MainVocabulary underlyingMain_;
  SpecialVocabulary underlyingSpecial_;  // Should not hold conventional string
                                         // literals (that is, without a special
                                         // data type) or IRIs

  // The 5th highest bit of the vocabulary index is used as a marker to
  // determine whether the word is stored in the normal vocabulary or the
  // special vocabulary.
  static constexpr uint64_t specialVocabMarker = 1ull
                                                 << (ValueId::numDataBits - 1);
  static constexpr uint64_t specialVocabIndexMask =
      ad_utility::bitMaskForLowerBits(ValueId::numDataBits - 1);
  static constexpr uint64_t maxVocabIndex = specialVocabMarker - 1;

 public:
  static uint64_t makeSpecialVocabIndex(uint64_t vocabIndex);

  static bool isSpecialVocabIndex(uint64_t index) {
    return static_cast<bool>(index & specialVocabMarker);
  }

  static bool isSpecialLiteral(const std::string& input) {
    return SplitFunction(input);
  };

  void build(const std::vector<std::string>& words,
             const std::string& filename);

  void close();

  // Read the vocabulary from files.
  void readFromFile(const std::string& filename);

  // Problem with getId is that it needs the Comparator which is from the
  // UnicodeVocab above this splitvocab for the correct lower_bound. -> we can't
  // do it here
  // bool getId(std::string_view word, uint64_t* idx) const;

  // needs to be defined in header otherwise we get serious compiler trouble
  decltype(auto) operator[](uint64_t idx) const {
    // Check marker bit to determine which vocabulary to use
    if (isSpecialVocabIndex(idx)) {
      // The requested word is stored in the special vocabulary
      uint64_t unmarkedIdx = idx & specialVocabIndexMask;
      AD_CONTRACT_CHECK(unmarkedIdx <= underlyingSpecial_.size());
      return underlyingSpecial_[unmarkedIdx];
    } else {
      // The requested word is stored in the vocabulary for normal words
      AD_CONTRACT_CHECK(idx <= maxVocabIndex && idx <= underlyingMain_.size());
      return underlyingMain_[idx];
    }
  }

  [[nodiscard]] uint64_t size() const {
    return underlyingMain_.size() + underlyingSpecial_.size();
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator,
                           bool useSpecial = false) const {
    if (useSpecial) {
      WordAndIndex subResult = underlyingSpecial_.lower_bound(word, comparator);
      if (subResult.isEnd()) {
        return subResult;
      }
      return {subResult.word(), subResult.index() | specialVocabMarker};
    } else {
      return underlyingMain_.lower_bound(word, comparator);
    }
  }
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator,
                           bool useSpecial = false) const {
    if (useSpecial) {
      WordAndIndex subResult = underlyingSpecial_.upper_bound(word, comparator);
      if (subResult.isEnd()) {
        return subResult;
      }
      return {subResult.word(), subResult.index() | specialVocabMarker};
    } else {
      return underlyingMain_.upper_bound(word, comparator);
    }
  }

  MainVocabulary& getUnderlyingMainVocabulary() { return underlyingMain_; }

  const MainVocabulary& getUnderlyingMainVocabulary() const {
    return underlyingMain_;
  }

  SpecialVocabulary& getUnderlyingSpecialVocabulary() {
    return underlyingSpecial_;
  }

  const SpecialVocabulary& getUnderlyingSpecialVocabulary() const {
    return underlyingSpecial_;
  }

  void open(const std::string& filename);

  // This word writer writes words to different vocabularies depending on their
  // content.
  using MainWWPtr = std::unique_ptr<typename MainVocabulary::WordWriter>;
  using SpecialWWPtr = std::unique_ptr<typename SpecialVocabulary::WordWriter>;
  class WordWriter {
   private:
    MainWWPtr underlyingWordWriter_;
    SpecialWWPtr underlyingSpecialWordWriter_;
    std::string readableName_ = "";

   public:
    WordWriter(const MainVocabulary& mainVocabulary,
               const SpecialVocabulary& specialVocabulary,
               const std::string& filename);

    // Add the next word to the vocabulary and return its index.
    uint64_t operator()(std::string_view word, bool isExternal);

    // Finish the writing on both underlying word writers. After this no more
    // calls to `operator()` are allowed.
    void finish();

    std::string& readableName() { return readableName_; }
  };

  using WWPtr = std::unique_ptr<WordWriter>;
  WWPtr makeWordWriterPtr(const std::string& filename) const {
    return std::make_unique<WordWriter>(underlyingMain_, underlyingSpecial_,
                                        filename);
  }
};

// Concrete implementations of split function and split filename function

static constexpr std::string_view GEO_LITERAL_SUFFIX =
    ad_utility::constexprStrCat<"\"^^<", GEO_WKT_LITERAL, ">">();

// Split function
inline bool geoSplitFunc(std::string_view word) {
  return word.starts_with("\"") && word.ends_with(GEO_LITERAL_SUFFIX);
};

// Split filename function
inline std::array<std::string, 2> geoFilenameFunc(std::string base) {
  return {base, base + ".geometry"};
};

template <class UnderlyingVocabulary>
using SplitGeoVocabulary =
    SplitVocabulary<UnderlyingVocabulary, UnderlyingVocabulary, geoSplitFunc,
                    geoFilenameFunc>;

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H
