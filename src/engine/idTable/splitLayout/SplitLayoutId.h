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

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "backports/three_way_comparison.h"
#include "engine/idTable/splitLayout/SplitLayoutIdBitRepresentation.h"
#include "global/Datatype.h"
#include "global/IdBase.h"
#include "util/BitUtils.h"
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
  // The actual data: a full 64-bit word of payload and a single byte for the
  // datatype. Together with the explicitly stored `padding_` an `Id` occupies
  // 16 bytes.
  PayloadType payload_;
  DatatypeType datatype_;

  // The `datatype_` has exactly `numDatatypeBits` bits, so every value of
  // `DatatypeType` fits into the datatype bits (see `fromBits`).
  static_assert(numDatatypeBits == 8 * sizeof(DatatypeType));

  // The number of bytes of the two data members above, and the number of bytes
  // that fill an `Id` up to a multiple of the alignment of the payload.
  static constexpr size_t numSemanticallyUsedBytes =
      sizeof(PayloadType) + sizeof(DatatypeType);
  static constexpr size_t numPaddingBytes =
      ad_utility::alignUp(numSemanticallyUsedBytes, alignof(PayloadType)) -
      numSemanticallyUsedBytes;

  // Explicit padding bytes. They are always set to zero by all factory
  // functions, s.t. all valid `SplitLayoutId`s have a deterministic byte
  // representation (e.g. for hashing raw bytes or writing them to disk).
  // Note: Default-constructed `Id`s are deliberately uninitialized (see
  // the default constructor below).
  std::array<uint8_t, numPaddingBytes> padding_;

 public:
  // Default construction of an uninitialized id.
  SplitLayoutId() = default;

  // Get the underlying bit representation, e.g. for compression etc.
  [[nodiscard]] constexpr SplitLayoutIdBitRepresentation getBits()
      const noexcept {
    return {datatype_, payload_};
  }

  // Get the datatype bits.
  [[nodiscard]] constexpr DatatypeType getDatatypeBits() const noexcept {
    return datatype_;
  }

  // Get the payload bits (without the datatype byte).
  [[nodiscard]] constexpr PayloadType getPayloadBits() const noexcept {
    return payload_;
  }

  // Construct from the underlying bit representation. `bits` must have been
  // obtained by a call to `getBits()` on a valid `SplitLayoutId`.
  static constexpr SplitLayoutId fromBits(T bits) noexcept {
    return {bits.payload_, bits.datatype_};
  }

  // Combine datatype and payload bits (inverse of `getDatatypeBits` and
  // `getPayloadBits`). As for the legacy `ValueId`, the `datatype` only has to
  // fit into the datatype bits, which every `DatatypeType` does (see the
  // `static_assert` at `datatype_`). It is not checked that the datatype
  // exists.
  static constexpr SplitLayoutId fromBits(const DatatypeType datatype,
                                          const PayloadType payload) noexcept {
    return {payload, datatype};
  }

  // Create an `Id` of type `Undefined` with the given `payload`. Unlike the
  // `fromBits` of the legacy `ValueId`, the argument contains no datatype bits.
  // This is used for the special `Id`s, which are not `UNDEF` (see
  // `SpecialIds.h`).
  static constexpr SplitLayoutId fromBits(PayloadType payload) noexcept {
    return {payload, Datatype::Undefined};
  }

  // Return the smallest and largest possible `SplitLayoutId` wrt the underlying
  // representation. Note: The datatype byte of `max()` is `0xFF`, which is
  // not a valid `Datatype`. Such IDs must only be used as sentinel values
  // for comparisons, never as actual values.
  constexpr static SplitLayoutId min() noexcept {
    return {std::numeric_limits<PayloadType>::min(), DatatypeType{0}};
  }

  constexpr static SplitLayoutId max() noexcept {
    return {std::numeric_limits<PayloadType>::max(), DatatypeType{0xFF}};
  }

  // Return the smallest `Id` that is greater than this one. The overflow of the
  // payload carries into the datatype byte, analogously to the increment of
  // the single integer in the packed 64-bit representation of the legacy
  // `ValueId`.
  [[nodiscard]] constexpr SplitLayoutId incremented() const {
    if (payload_ == std::numeric_limits<PayloadType>::max()) {
      return {0, static_cast<DatatypeType>(datatype_ + 1)};
    }
    return {payload_ + 1, datatype_};
  }

  // Enable the serialization of `SplitLayoutId` in the
  // `ad_utility::serialization` framework by simply copying the 16 raw bytes
  // (including the zeroed padding). This is much faster than serializing the
  // datatype and payload separately, especially for large contiguous arrays of
  // IDs.
  // TODO<pas-kes>: Is this wasteful to persist, with 7 unused bytes?
  template <typename U>
  friend void allowTrivialSerialization(SplitLayoutId, U);

 private:
  // The datatype-major bitwise comparison: first compare the datatype byte,
  // then the payload bits as unsigned integers. This is only correct if
  // neither of the involved IDs has the `LocalVocabIndex` datatype (see
  // `compareThreeWay` in `IdBase`).
  [[nodiscard]] constexpr ql::strong_ordering compareBitwiseImpl(
      const SplitLayoutId& other) const {
    const auto typeComparison = ql::compareThreeWay(datatype_, other.datatype_);
    return typeComparison != 0 ? typeComparison
                               : ql::compareThreeWay(payload_, other.payload_);
  }

  // Private constructors from the payload bits and the datatype (byte). They
  // are used in the implementation of the static factory methods `fromBits()`
  // etc. Note: The explicit `padding_` bytes are always initialized to zero,
  // s.t. all valid `SplitLayoutId`s have a deterministic byte representation.
  constexpr SplitLayoutId(PayloadType payload, DatatypeType datatype)
      : payload_{payload}, datatype_{datatype}, padding_{} {
    // There is no padding left that the compiler adds implicitly (and leaves
    // uninitialized). If this fails, `numPaddingBytes` is wrong, e.g. because
    // it is zero (an `std::array` of size 0 is not empty).
    static_assert(sizeof(SplitLayoutId) ==
                  numSemanticallyUsedBytes + numPaddingBytes);
  }

  constexpr SplitLayoutId(PayloadType payload, Datatype datatype)
      : SplitLayoutId{payload, static_cast<DatatypeType>(datatype)} {}
};

static_assert(SplitLayoutId::checkDerived());
}  // namespace columnBasedIdTable::splitLayout

#endif  // QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTID_H
