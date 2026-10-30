// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTIDBITREPRESENTATION_H
#define QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTIDBITREPRESENTATION_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <ostream>
#include <string>
#include <type_traits>

#include "backports/three_way_comparison.h"

namespace columnBasedIdTable::splitLayout {

// The raw bit representation of an `Id` in the experimental split layout: a
// full 64-bit payload word and a datatype byte, kept apart (unlike the legacy
// `ValueId`, which packs both into a single 64-bit word). The comparison is
// datatype-major (first by `datatype_`, then by `payload_`), which is
// consistent with the ordering of the legacy `ValueId`s (as long as no
// `LocalVocabIndex` is involved).
struct SplitLayoutIdBitRepresentation {
  using PayloadType = uint64_t;
  using DatatypeType = uint8_t;

  PayloadType payload_;
  DatatypeType datatype_;
  // Explicit padding bytes, which are always set to zero by the constructor
  // below, s.t. the bytes of a bit representation are deterministic (e.g. for
  // writing them to disk). With them, the layout is the same as the one of a
  // `SplitLayoutId`.
  std::array<uint8_t, sizeof(PayloadType) - sizeof(DatatypeType)> padding_;

  // Default construction of an uninitialized bit representation, like for an
  // `Id`.
  SplitLayoutIdBitRepresentation() = default;

  constexpr SplitLayoutIdBitRepresentation(DatatypeType datatype,
                                           PayloadType payload)
      : payload_{payload}, datatype_{datatype}, padding_{} {}

  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(
      SplitLayoutIdBitRepresentation)

  // The datatype-major comparison: first compare the datatype byte, then the
  // payload bits as unsigned integers, analogously to the bitwise comparison
  // of a `SplitLayoutId`. NOTE: The defaulted comparison would compare the
  // members in the order of their declaration, so first the `payload_`.
  [[nodiscard]] constexpr ql::strong_ordering compareThreeWay(
      const SplitLayoutIdBitRepresentation& other) const {
    const auto typeComparison = ql::compareThreeWay(datatype_, other.datatype_);
    return typeComparison != 0 ? typeComparison
                               : ql::compareThreeWay(payload_, other.payload_);
  }

  // Compare the datatype first and then the payload, like `compareThreeWay`
  // above. NOTE: Not defaulted, because in C++20 the defaulted equality would
  // compare the members in the order of their declaration and also the
  // `padding_`, but in C++17 (the macro with a tie) only the listed members,
  // and both should behave exactly the same.
  constexpr bool operator==(const SplitLayoutIdBitRepresentation& other) const {
    return datatype_ == other.datatype_ && payload_ == other.payload_;
  }
#ifdef QLEVER_CPP_17
  constexpr bool operator!=(const SplitLayoutIdBitRepresentation& other) const {
    return !(*this == other);
  }
#endif

  // Return the bit representation of the smallest `Id` that is greater
  // than the `Id` of this bit representation. The overflow of the
  // payload carries into the datatype byte, analogously to the increment of
  // the single integer in the packed 64-bit representation of the legacy
  // `ValueId`.
  [[nodiscard]] constexpr SplitLayoutIdBitRepresentation incremented() const {
    if (payload_ == std::numeric_limits<PayloadType>::max()) {
      return {static_cast<DatatypeType>(datatype_ + 1), 0};
    }
    return {datatype_, payload_ + 1};
  }

  // Enable hashing in abseil (required by `ad_utility::HashSet` and
  // `ad_utility::HashMap`).
  template <typename H>
  friend H AbslHashValue(H h, const SplitLayoutIdBitRepresentation& rep) {
    return H::combine(std::move(h), rep.datatype_, rep.payload_);
  }

  // The bit representation can be serialized by simply copying its bytes
  // (including the zeroed padding).
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

// The `payload_` is followed by the `datatype_` and the explicit padding up to
// the size of two payloads, and there are no bytes that the compiler adds
// implicitly (and leaves uninitialized).
static_assert(offsetof(SplitLayoutIdBitRepresentation, payload_) == 0);
static_assert(offsetof(SplitLayoutIdBitRepresentation, datatype_) ==
              sizeof(SplitLayoutIdBitRepresentation::PayloadType));
static_assert(
    std::has_unique_object_representations_v<SplitLayoutIdBitRepresentation>);
}  // namespace columnBasedIdTable::splitLayout

// Make `SplitLayoutIdBitRepresentation` usable as a key of `std` hash
// containers.
template <>
struct std::hash<
    columnBasedIdTable::splitLayout::SplitLayoutIdBitRepresentation> {
  size_t operator()(
      const columnBasedIdTable::splitLayout::SplitLayoutIdBitRepresentation&
          rep) const noexcept {
    using PayloadType = columnBasedIdTable::splitLayout::
        SplitLayoutIdBitRepresentation::PayloadType;
    return std::hash<PayloadType>{}(
        rep.payload_ ^ (static_cast<PayloadType>(rep.datatype_) << 56));
  }
};

#endif  // QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTIDBITREPRESENTATION_H
