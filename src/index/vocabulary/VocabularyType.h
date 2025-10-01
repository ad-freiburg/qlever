//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPE_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPE_H

#include <absl/strings/str_join.h>

#include <array>
#include <string_view>

#include "backports/three_way_comparison.h"
#include "util/Random.h"
#include "util/json.h"

namespace ad_utility {

// A lightweight enum for the different implementation strategies of the
// `PolymorphicVocabulary`. Also includes operations for conversion to and from
// string.
// TODO<joka921> Implement a generic mixin that can also be used for other
// enums, especially such used in command-line interfaces.
class VocabularyType {
 public:
  // The different vocabulary implementations;
  enum struct Enum {
    InMemoryUncompressed,
    OnDiskUncompressed,
    InMemoryCompressed,
    OnDiskCompressed,
    OnDiskCompressedGeoSplit
  };

 private:
  Enum value_ = Enum::InMemoryUncompressed;

  static constexpr size_t numValues_ = 5;
  // All possible values.
  static constexpr std::array<Enum, numValues_> all_{
      Enum::InMemoryUncompressed, Enum::OnDiskUncompressed,
      Enum::InMemoryCompressed, Enum::OnDiskCompressed,
      Enum::OnDiskCompressedGeoSplit};

  // The string representations of the enum values.
  static constexpr std::array<std::string_view, numValues_> descriptions_{
      "in-memory-uncompressed", "on-disk-uncompressed", "in-memory-compressed",
      "on-disk-compressed", "on-disk-compressed-geo-split"};

  static_assert(all_.size() == descriptions_.size());

 public:
  // Constructors
  VocabularyType() = default;
  explicit VocabularyType(Enum value) : value_{value} {}

  // Create from a string. The string must be one of the `descriptions_`,
  // otherwise a `runtime_error_` is thrown.
  static VocabularyType fromString(std::string_view description) {
    auto it = ql::ranges::find(descriptions_, description);
    if (it == descriptions_.end()) {
      throw std::runtime_error{
          absl::StrCat("\"", description,
                       "\" is not a valid vocabulary type. The currently "
                       "supported vocabulary types are ",
                       getListOfSupportedValues())};
    }
    return VocabularyType{all().at(it - descriptions_.begin())};
  }

  // Return all the possible enum values as a comma-separated single string.
  static std::string getListOfSupportedValues() {
    return absl::StrJoin(descriptions_, ", ");
  }

  // Convert the enum to the corresponding string.
  std::string_view toString() const {
    return descriptions_.at(static_cast<size_t>(value_));
  }

  // Return the actual enum value.
  Enum value() const { return value_; }

  // Return a list of all the enum values.
  static constexpr const std::array<Enum, numValues_>& all() { return all_; }

  // Conversion To JSON.
  friend void to_json(nlohmann::json& j, const VocabularyType& vocabEnum) {
    j = vocabEnum.toString();
  }

  // Conversion from JSON.
  friend void from_json(const nlohmann::json& j, VocabularyType& vocabEnum) {
    vocabEnum = VocabularyType::fromString(static_cast<std::string>(j));
  }

  // Get a random value, useful for fuzz testing. In particular, each time an
  // index is built for testing in `IndexTestHelpers.cpp` we assign it a random
  // vocabulary type (repeating all these tests for all types exhaustively would
  // be infeasible).
  static VocabularyType random() {
    static thread_local ad_utility::FastRandomIntGenerator<size_t> r;
    return VocabularyType{static_cast<Enum>(r() % numValues_)};
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(VocabularyType, value_)
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPE_H
