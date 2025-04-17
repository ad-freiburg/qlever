// Copyright 2025 University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H

#include <cstdint>
#include <functional>
#include <string_view>

#include "global/ValueId.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/HashSet.h"

// Func. to decide where a literal goes

// Func. to get filenames for each vocabulary

// using SplitFunctionT = const std::function<bool(std::string_view)>&;
// using SplitFilenameFunctionT =
//     const std::function<std::array<std::string, 2>(std::string)>&;

template <typename StringType, typename ComparatorType, typename IndexT,
          class MainVocabulary, class SpecialVocabulary,
          const auto& SplitFunction, const auto& SplitFilenameFunction>
class SplitVocabulary {
 private:
  MainVocabulary underlyingMain_;
  SpecialVocabulary underlyingSpecial_;

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

  void build(const std::vector<std::string>& words,
             const std::string& filename);

  void close();

  // Read the vocabulary from files.
  void readFromFile(const std::string& filename);

  // needs to be defined in header otherwise we get serious compiler trouble
  decltype(auto) operator[](uint64_t idx) const {
    // Check marker bit to determine which vocabulary to use
    if (isSpecialVocabIndex(idx)) {
      // The requested word is stored in the special vocabulary
      uint64_t unmarkedIdx = idx & specialVocabIndexMask;
      return underlyingSpecial_[unmarkedIdx];
    } else {
      // The requested word is stored in the vocabulary for normal words
      AD_CONTRACT_CHECK(idx <= maxVocabIndex);
      return underlyingMain_[idx];
    }
  }

  [[nodiscard]] uint64_t size() const {
    return underlyingMain_.size() + underlyingSpecial_.size();
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    // TODO
    return underlyingMain_.lower_bound(word, comparator);
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    // TODO
    return underlyingMain_.upper_bound(word, comparator);
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
  class WordWriter {
   private:
    MainVocabulary::WordWriter underlyingWordWriter_;
    SpecialVocabulary::WordWriter underlyingSpecialWordWriter_;
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

  WordWriter makeWordWriter(const std::string& filename) const {
    return {underlyingMain_, underlyingSpecial_, filename};
  }

  void createFromSet(const ad_utility::HashSet<std::string>& set,
                     const std::string& filename);

  bool getId(std::string_view word, uint64_t* idx) const;
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

template <typename StringType, typename ComparatorType, typename IndexT,
          class UnderlyingVocabulary>
using SplitGeoVocabulary =
    SplitVocabulary<StringType, ComparatorType, IndexT, UnderlyingVocabulary,
                    UnderlyingVocabulary, geoSplitFunc, geoFilenameFunc>;

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H
