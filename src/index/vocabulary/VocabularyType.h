//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <string_view>

#include "util/json.h"

namespace ad_utility {
class VocabularyEnum {
 public:
  enum struct Enum { InMemory, OnDisk, CompressedInMemory, CompressedOnDisk };

 private:
  Enum value_ = Enum::InMemory;

  static constexpr std::array<std::string_view, 4> descriptions{
      "in-memory-uncompressed", "on-disk-uncompressed", "in-memory-compressed",
      "on-disk-compressed"};

 public:
  VocabularyEnum() = default;
  explicit VocabularyEnum(Enum value) : value_{value} {}

  static VocabularyEnum fromString(std::string_view description) {
    auto it = ql::ranges::find(descriptions, description);
    if (it == descriptions.end()) {
      throw std::runtime_error{
          absl::StrCat("\"", description,
                       "\" is not a valid vocabulary type. The currently "
                       "supported vocabulary types are ",
                       absl::StrJoin(descriptions, ", "))};
      ;
    }
    return VocabularyEnum{static_cast<Enum>(it - descriptions.begin())};
  }
  std::string_view toString() const {
    return descriptions.at(static_cast<size_t>(value_));
  }

  Enum value() const { return value_; }

  // Conversion To JSON.
  friend void to_json(nlohmann::json& j, const VocabularyEnum& vocabEnum) {
    j = vocabEnum.toString();
  }

  // Conversion from JSON.
  friend void from_json(const nlohmann::json& j, VocabularyEnum& vocabEnum) {
    vocabEnum = VocabularyEnum::fromString(static_cast<std::string>(j));
  }
};
}  // namespace ad_utility
