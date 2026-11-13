// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTID_H
#define QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTID_H

#include <limits>

#include "backports/three_way_comparison.h"
#include "engine/idTable/splitLayout/SplitLayoutIdBitRepresentation.h"
#include "global/IdBase.h"
#include "util/NBitInteger.h"

namespace columnBasedIdTable::splitLayout {
// Encode values of different types (the types from the `Datatype` enum in
// `Datatype.h`) using a full 64-bit word for the value and a separate byte for
// the datatype. Unlike the legacy `ValueId`, which packs both into a single
// 64-bit word, no bits of the value are lost. Everything that does not depend
// on how the datatype and the payload are stored is implemented in the base
// class `qlever::IdBase`.
class SplitLayoutId
    : public qlever::IdBase<SplitLayoutId, SplitLayoutIdBitRepresentation,
                            SplitLayoutIdBitRepresentation::PayloadType,
                            SplitLayoutIdBitRepresentation::DatatypeType> {
  friend IdBase;

 public:
  using IdBase::fromBits;

  // ___________________________________________________________________________
  // The layout: a full 64-bit word for the payload and one byte for the
  // datatype.

  static constexpr DatatypeType numDatatypeBits = 8;
  static constexpr PayloadType numDataBits = 64;
  // The number of bits of a vocabulary index, including the markers in its
  // topmost bits (see `SplitVocabulary` and `GeoCellGrid`). Not the whole
  // payload, so that `SegmentLocalId` can store the index of the
  // sub-vocabulary in the remaining 4 bits of a single 64-bit word.
  // TODO<pas-kes> Decouple `SegmentLocalId` from the `Id`, then use all 64
  // bits here.
  static constexpr PayloadType numVocabIndexBits = 60;

  using IntegerType = ad_utility::NBitInteger<numDataBits>;

  // The maximum value for the unsigned index types (see `global/IndexTypes.h`).
  // Since the payload consists of a full 64-bit word, all values of
  // `PayloadType` are valid indices.
  static constexpr PayloadType maxIndex =
      std::numeric_limits<PayloadType>::max();

  // The smallest double > 0 that will not be rounded to zero. As the payload
  // stores all bits of a `double`, this is the smallest subnormal `double`.
  // Symmetrically, `-minPositiveDouble` is the largest double < 0 that will not
  // be rounded to zero.
  // Note: This constant is currently only used in unit tests.
  static constexpr double minPositiveDouble =
      std::numeric_limits<double>::denorm_min();

 private:
  // The actual data: a full 64-bit word of payload, a single byte for the
  // datatype and the zeroed padding, together 16 bytes.
  T bits_;

  // The datatype byte has exactly `numDatatypeBits` bits, so every value of
  // `DatatypeType` fits into the datatype bits (see `fromBits`).
  static_assert(numDatatypeBits == 8 * sizeof(DatatypeType));

 public:
  // Default construction of an uninitialized id.
  SplitLayoutId() = default;

  // Get the underlying bit representation, e.g. for compression etc.
  [[nodiscard]] constexpr T getBits() const noexcept { return bits_; }

  // Get the datatype bits.
  [[nodiscard]] constexpr DatatypeType getDatatypeBits() const noexcept {
    return bits_.datatype_;
  }

  // Get the payload bits (without the datatype byte).
  [[nodiscard]] constexpr PayloadType getPayloadBits() const noexcept {
    return bits_.payload_;
  }

  // Construct from the underlying bit representation. `bits` must have been
  // obtained by a call to `getBits()` on a valid `SplitLayoutId`.
  static constexpr SplitLayoutId fromBits(T bits) noexcept {
    return SplitLayoutId{bits};
  }

  // Combine datatype and payload bits (inverse of `getDatatypeBits` and
  // `getPayloadBits`). As for the legacy `ValueId`, the `datatype` only has to
  // fit into the datatype bits, which every `DatatypeType` does (see the
  // `static_assert` at `bits_`). It is not checked that the datatype exists.
  static constexpr SplitLayoutId fromBits(const DatatypeType datatype,
                                          const PayloadType payload) noexcept {
    return SplitLayoutId{T{datatype, payload}};
  }

  // Return the smallest and largest possible `SplitLayoutId` wrt the underlying
  // representation. Note: The datatype byte of `max()` is `0xFF`, which is
  // not a valid `Datatype`. Such IDs must only be used as sentinel values
  // for comparisons, never as actual values.
  constexpr static SplitLayoutId min() noexcept {
    return fromBits(DatatypeType{0}, std::numeric_limits<PayloadType>::min());
  }

  constexpr static SplitLayoutId max() noexcept {
    return fromBits(std::numeric_limits<DatatypeType>::max(),
                    std::numeric_limits<PayloadType>::max());
  }

  // Return the smallest `Id` that is greater than this one (see
  // `SplitLayoutIdBitRepresentation::incremented`).
  [[nodiscard]] constexpr SplitLayoutId incremented() const {
    return SplitLayoutId{bits_.incremented()};
  }

  // Enable the serialization of `SplitLayoutId` in the
  // `ad_utility::serialization` framework by simply copying the 16 raw bytes
  // (including the zeroed padding). This is much faster than serializing the
  // datatype and payload separately, especially for large contiguous arrays of
  // IDs.
  //
  // TODO<pas-kes> This also persists the 7 padding bytes, which a custom
  // serialization function will avoid.
  template <typename U>
  friend void allowTrivialSerialization(SplitLayoutId, U);

 private:
  // The datatype-major bitwise comparison of the bits. This is only correct if
  // neither of the involved IDs has the `LocalVocabIndex` datatype (see
  // `compareThreeWay` in `IdBase`).
  [[nodiscard]] constexpr ql::strong_ordering compareBitwiseImpl(
      const SplitLayoutId& other) const {
    return bits_.compareThreeWay(other.bits_);
  }

  // Private constructor from the bit representation, which is used in the
  // implementation of the static factory methods `fromBits()` etc. The bit
  // representation always has zeroed padding bytes, so that all valid
  // `SplitLayoutId`s have a deterministic byte representation.
  constexpr explicit SplitLayoutId(T bits) : bits_{bits} {
    // A `SplitLayoutId` consists of its bits only, without additional bytes.
    static_assert(sizeof(SplitLayoutId) == sizeof(T));
  }
};

static_assert(SplitLayoutId::checkDerived());
}  // namespace columnBasedIdTable::splitLayout

#endif  // QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTID_H
