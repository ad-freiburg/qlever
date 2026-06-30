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
namespace detail {
enum struct VocabularyTypeEnum {
  InMemoryUncompressed,
  OnDiskUncompressed,
  InMemoryCompressed,
  OnDiskCompressed,
  OnDiskCompressedGeoSplit,
  OnDiskCompressedEmbSplit,
  OnDiskCompressedGeoEmbSplit
};

}
class VocabularyType
    : public EnumWithStrings<VocabularyType, detail::VocabularyTypeEnum> {
 public:
  // The different vocabulary implementations.
  using Enum = detail::VocabularyTypeEnum;

  static constexpr std::array<std::pair<Enum, std::string_view>, 7>
      descriptions_{
          {{Enum::InMemoryUncompressed, "in-memory-uncompressed"},
           {Enum::OnDiskUncompressed, "on-disk-uncompressed"},
           {Enum::InMemoryCompressed, "in-memory-compressed"},
           {Enum::OnDiskCompressed, "on-disk-compressed"},
           {Enum::OnDiskCompressedGeoSplit, "on-disk-compressed-geo-split"},
           {Enum::OnDiskCompressedEmbSplit, "on-disk-compressed-emb-split"},
           {Enum::OnDiskCompressedGeoEmbSplit,
            "on-disk-compressed-geo-emb-split"}}};
  static const VocabularyType InMemoryUncompressed;
  static const VocabularyType OnDiskUncompressed;
  static const VocabularyType InMemoryCompressed;
  static const VocabularyType OnDiskCompressed;
  static const VocabularyType OnDiskCompressedGeoSplit;
  static const VocabularyType OnDiskCompressedEmbSplit;
  static const VocabularyType OnDiskCompressedGeoEmbSplit;

  static constexpr std::string_view typeName() { return "vocabulary type"; }

  using EnumWithStrings::EnumWithStrings;
};

const inline VocabularyType VocabularyType::InMemoryUncompressed{
    VocabularyType::Enum::InMemoryUncompressed};
const inline VocabularyType VocabularyType::OnDiskUncompressed{
    VocabularyType::Enum::OnDiskUncompressed};
const inline VocabularyType VocabularyType::InMemoryCompressed{
    VocabularyType::Enum::InMemoryCompressed};
const inline VocabularyType VocabularyType::OnDiskCompressed{
    VocabularyType::Enum::OnDiskCompressed};
const inline VocabularyType VocabularyType::OnDiskCompressedGeoSplit{
    VocabularyType::Enum::OnDiskCompressedGeoSplit};
const inline VocabularyType VocabularyType::OnDiskCompressedEmbSplit{
    VocabularyType::Enum::OnDiskCompressedEmbSplit};
const inline VocabularyType VocabularyType::OnDiskCompressedGeoEmbSplit{
    VocabularyType::Enum::OnDiskCompressedGeoEmbSplit};
}  // namespace ad_utility

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPE_H
