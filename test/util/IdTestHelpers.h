//  Copyright 2023, University of Freiburg,
//  //              Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "global/Id.h"

// Lambdas to simply create an `Id` with a given value and type during unit
// tests.
namespace ad_utility::testing {
inline auto IntId = [](const auto& i) { return Id::makeFromInt(i); };

inline auto DoubleId = [](const auto& d) { return Id::makeFromDouble(d); };
inline auto BoolId = [](bool b) { return Id::makeFromBool(b); };

inline auto VocabId = [](const auto& v) {
  return Id::makeFromVocabIndex(VocabIndex::make(v));
};

inline auto LocalVocabId = []<typename T>(const T& v) {
  if constexpr (std::integral<T>) {
    return Id::makeFromLocalVocabIndex(
        std::bit_cast<LocalVocabIndex>(static_cast<uint64_t>(v)));
  } else {
    return Id::makeFromLocalVocabIndex(std::bit_cast<LocalVocabIndex>(v));
  }
};

inline auto TextRecordId = [](const auto& t) {
  return Id::makeFromTextRecordIndex(TextRecordIndex ::make(t));
};

inline auto WordVocabId = [](const auto& t) {
  return Id::makeFromWordVocabIndex(WordVocabIndex ::make(t));
};
}  // namespace ad_utility::testing
