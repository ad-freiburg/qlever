// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTIDBITREPRESENTATION_H
#define QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTIDBITREPRESENTATION_H

#include <cstdint>
#include <functional>
#include <limits>
#include <ostream>
#include <string>
#include <type_traits>

#include "backports/three_way_comparison.h"

namespace columnBasedIdTable::splitLayout {

// The raw bit representation of an `Id` in the experimental split layout: a
// datatype byte and a full 64-bit payload word, kept apart (unlike the legacy
// `ValueId`, which packs both into a single 64-bit word). The comparison is
// datatype-major (first by `datatype_`, then by `payload_`), which is
// consistent with the ordering of the legacy `ValueId`s (as long as no
// `LocalVocabIndex` is involved).
struct SplitLayoutIdBitRepresentation {
  uint8_t datatype_;
  uint64_t payload_;

  // The default three-way comparison is datatype-major because `datatype_`
  // is declared before `payload_`.
  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(
      SplitLayoutIdBitRepresentation, datatype_, payload_)

  // Return the bit representation of the smallest `Id` that is greater
  // than the `Id` of this bit representation. The overflow of the
  // payload carries into the datatype byte, analogously to the increment of
  // the single integer in the packed 64-bit representation of the legacy
  // `ValueId`.
  [[nodiscard]] constexpr SplitLayoutIdBitRepresentation incremented() const {
    if (payload_ == std::numeric_limits<uint64_t>::max()) {
      return {static_cast<uint8_t>(datatype_ + 1), 0};
    }
    return {datatype_, payload_ + 1};
  }

  // Enable hashing in abseil (required by `ad_utility::HashSet` and
  // `ad_utility::HashMap`).
  template <typename H>
  friend H AbslHashValue(H h, const SplitLayoutIdBitRepresentation& rep) {
    return H::combine(std::move(h), rep.datatype_, rep.payload_);
  }

  // The bit representation can be serialized by simply copying its bytes.
  // Note: The padding bytes between `datatype_` and `payload_` are not
  // guaranteed to be zero, so the byte-level output is not deterministic,
  // but roundtripping works correctly.
  template <typename U>
  friend std::true_type allowTrivialSerialization(
      SplitLayoutIdBitRepresentation, U);

  // Support for `absl::StrCat` etc., analogous to the stringification of the
  // legacy single-integer bit representation.
  template <typename Sink>
  friend void AbslStringify(Sink& sink,
                            const SplitLayoutIdBitRepresentation& rep) {
    sink.Append(std::to_string(rep.datatype_));
    sink.Append(":");
    sink.Append(std::to_string(rep.payload_));
  }

  // This operator is only used for debugging and testing.
  friend std::ostream& operator<<(std::ostream& ostr,
                                  const SplitLayoutIdBitRepresentation& rep) {
    return ostr << static_cast<int>(rep.datatype_) << ':' << rep.payload_;
  }
};
}  // namespace columnBasedIdTable::splitLayout

// Make `SplitLayoutIdBitRepresentation` usable as a key of `std` hash
// containers.
template <>
struct std::hash<
    columnBasedIdTable::splitLayout::SplitLayoutIdBitRepresentation> {
  size_t operator()(
      const columnBasedIdTable::splitLayout::SplitLayoutIdBitRepresentation&
          rep) const noexcept {
    return std::hash<uint64_t>{}(rep.payload_ ^
                                 (static_cast<uint64_t>(rep.datatype_) << 56));
  }
};

#endif  // QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTIDBITREPRESENTATION_H
