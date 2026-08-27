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
#include "util/Random.h"

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
  // NOTE: The "with holes" variants are not used for regular index building
  // (they cannot even be built word by word, see
  // `VocabularyInMemoryBinSearch`). They are only used for vocabularies that
  // were exported from a larger vocabulary with some of the entries excluded,
  // such that the indices of the remaining entries are no longer contiguous.
  InMemoryUncompressedWithHoles,
  InMemoryCompressedWithHoles
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
           {Enum::InMemoryUncompressedWithHoles,
            "in-memory-uncompressed-with-holes"},
           {Enum::InMemoryCompressedWithHoles,
            "in-memory-compressed-with-holes"}}};
  static const VocabularyType InMemoryUncompressed;
  static const VocabularyType OnDiskUncompressed;
  static const VocabularyType InMemoryCompressed;
  static const VocabularyType OnDiskCompressed;
  static const VocabularyType OnDiskCompressedGeoSplit;
  static const VocabularyType InMemoryUncompressedWithHoles;
  static const VocabularyType InMemoryCompressedWithHoles;

  static constexpr std::string_view typeName() { return "vocabulary type"; }

  using EnumWithStrings::EnumWithStrings;

  // Return the vocabulary types that can be used to build a regular index,
  // i.e. all types but the "with holes" variants (see above).
  static constexpr std::array<Enum, 5> allForIndexBuilding() {
    return {Enum::InMemoryUncompressed, Enum::OnDiskUncompressed,
            Enum::InMemoryCompressed, Enum::OnDiskCompressed,
            Enum::OnDiskCompressedGeoSplit};
  }

  // Return the vocabulary types that can be used to build a regular index (see
  // `allForIndexBuilding`) as a comma-separated single string. This is the
  // counterpart of the inherited `getListOfSupportedValues`, which also
  // includes the "with holes" variants.
  static std::string getListOfValuesForIndexBuilding() {
    return absl::StrJoin(
        allForIndexBuilding() | ql::views::transform([](Enum type) {
          return VocabularyType{type}.toString();
        }),
        ", ");
  }

  // Return a random vocabulary type that can be used to build a regular index
  // (see `allForIndexBuilding`), useful for fuzz testing. This is the
  // counterpart of the inherited `random()`, which may also return one of the
  // "with holes" variants.
  static VocabularyType randomForIndexBuilding() {
    thread_local ad_utility::FastRandomIntGenerator<size_t> generator;
    constexpr auto types = allForIndexBuilding();
    return VocabularyType{types.at(generator() % types.size())};
  }
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
const inline VocabularyType VocabularyType::InMemoryUncompressedWithHoles{
    VocabularyType::Enum::InMemoryUncompressedWithHoles};
const inline VocabularyType VocabularyType::InMemoryCompressedWithHoles{
    VocabularyType::Enum::InMemoryCompressedWithHoles};
}  // namespace ad_utility

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPE_H
