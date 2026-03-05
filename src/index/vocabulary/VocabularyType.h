// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPE_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPE_H

#include "util/EnumWithStrings.h"

namespace ad_utility {

// A lightweight enum for the different implementation strategies of the
// `PolymorphicVocabulary`. Also includes operations for conversion to and from
// string.
class VocabularyType : public EnumWithStrings<VocabularyType> {
 public:
  // The different vocabulary implementations.
  enum struct Enum {
    InMemoryUncompressed,
    OnDiskUncompressed,
    InMemoryCompressed,
    OnDiskCompressed,
    OnDiskCompressedGeoSplit
  };

  static constexpr size_t numValues_ = 5;
  static constexpr std::array<Enum, numValues_> all_{
      Enum::InMemoryUncompressed, Enum::OnDiskUncompressed,
      Enum::InMemoryCompressed, Enum::OnDiskCompressed,
      Enum::OnDiskCompressedGeoSplit};

  static constexpr std::array<std::string_view, numValues_> descriptions_{
      "in-memory-uncompressed", "on-disk-uncompressed", "in-memory-compressed",
      "on-disk-compressed", "on-disk-compressed-geo-split"};

  static constexpr std::string_view typeName() { return "vocabulary type"; }

  using EnumWithStrings::EnumWithStrings;
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPE_H
