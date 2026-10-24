// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_HASWORDNORMALIZATION_H
#define QLEVER_SRC_INDEX_HASWORDNORMALIZATION_H

#include <array>
#include <cstdint>
#include <string_view>

#include "util/EnumWithStrings.h"

namespace ad_utility {

enum class HasWordNormalizationEnum : uint8_t { Lowercase, LowercaseNoAccents };

// The normalization that is applied to the words of the `ql:has-word` triples
// (option `--has-word-normalization` of `qlever-index`). In all modes, literals
// are split at non-alphanumeric characters and the words are lowercased. With
// `LowercaseNoAccents`, additionally all combining accents are removed from the
// words (e.g. "Café" becomes "cafe").
class HasWordNormalization
    : public EnumWithStrings<HasWordNormalization, HasWordNormalizationEnum> {
 public:
  using Enum = HasWordNormalizationEnum;

  static constexpr std::array<std::pair<Enum, std::string_view>, 2>
      descriptions_{{{Enum::Lowercase, "lowercase"},
                     {Enum::LowercaseNoAccents, "lowercase-no-accents"}}};

  static const HasWordNormalization Lowercase;
  static const HasWordNormalization LowercaseNoAccents;

  static constexpr std::string_view typeName() {
    return "has-word normalization";
  }

  using EnumWithStrings::EnumWithStrings;
};

const inline HasWordNormalization HasWordNormalization::Lowercase{
    HasWordNormalization::Enum::Lowercase};
const inline HasWordNormalization HasWordNormalization::LowercaseNoAccents{
    HasWordNormalization::Enum::LowercaseNoAccents};

// The key under which the normalization is stored in the configuration of an
// index.
inline constexpr std::string_view HAS_WORD_NORMALIZATION_KEY =
    "has-word-normalization";

}  // namespace ad_utility

#endif  // QLEVER_SRC_INDEX_HASWORDNORMALIZATION_H
