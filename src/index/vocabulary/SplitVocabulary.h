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

using SplitFunctionT = const std::function<bool(std::string_view)>&;
using SplitFilenameFunctionT =
    const std::function<std::array<std::string, 2>(std::string)>&;

template <class MainVocabulary, class SpecialVocabulary,
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

  bool isSpecialLiteral(uint64_t index) const {
    return static_cast<bool>(index & specialVocabMarker);
  }

  // Read the vocabulary from files.
  void readFromFile(const std::string& filename);

  decltype(auto) operator[](uint64_t id) const;

  [[nodiscard]] uint64_t size() const {
    return underlyingMain_.size() + underlyingSpecial_.size();
  }

  // TODO
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return underlyingMain_.lower_bound(word, comparator);
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return underlyingSpecial_.upper_bound(word, comparator);
  }

  MainVocabulary& getUnderlyingMainVocabulary() { return underlyingMain_; }

  const MainVocabulary& getUnderlyingMainVocabulary() const {
    return underlyingMain_;
  }

  SpecialVocabulary& getUnderlyingSpecialVocabulary() {
    return underlyingSpecial_;
  }

  const SpecialVocabulary& getUnderlyingVocabulary() const {
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
    WordWriter(
        const SplitVocabulary<MainVocabulary, SpecialVocabulary, SplitFunction,
                              SplitFilenameFunction>& vocabulary,
        const std::string& filename);

    // Add the next word to the vocabulary and return its index.
    uint64_t operator()(std::string_view word, bool isExternal);

    // Finish the writing on both underlying word writers. After this no more
    // calls to `operator()` are allowed.
    void finish();

    std::string& readableName() { return readableName_; }
  };

  WordWriter makeWordWriter(const std::string& filename) const {
    return {*this, filename};
  }

  void createFromSet(const ad_utility::HashSet<std::string>& set,
                     const std::string& filename);

  bool getId(std::string_view word, uint64_t* idx) const;
};

// Concrete implementations of split function and split filename function

static constexpr std::string_view GEO_LITERAL_SUFFIX =
    ad_utility::constexprStrCat<"\"^^<", GEO_WKT_LITERAL, ">">();

// Split function
const auto geoSplitFunc = [](std::string_view word) -> bool {
  return word.starts_with("\"") && word.ends_with(GEO_LITERAL_SUFFIX);
};
// Split filename function
const auto geoFilenameFunc =
    [](std::string base) -> std::array<std::string, 2> {
  return {base, base + ".geometry"};
};

template <class UnderlyingVocabulary>
using SplitGeoVocabulary =
    SplitVocabulary<UnderlyingVocabulary, UnderlyingVocabulary, geoSplitFunc,
                    geoFilenameFunc>;

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARY_H
