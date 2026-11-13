//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_GLOBAL_FOLDEDVALUEID_H
#define QLEVER_SRC_GLOBAL_FOLDEDVALUEID_H

#include <absl/base/casts.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <cstdint>
#include <exception>
#include <limits>
#include <string>

#include "backports/three_way_comparison.h"
#include "global/Datatype.h"
#include "global/IdBase.h"
#include "rdfTypes/GeoPoint.h"
#include "util/BitUtils.h"
#include "util/Exception.h"
#include "util/NBitInteger.h"
#include "util/SourceLocation.h"

namespace foldedValueId {

// Encode values of different types (the types from the `Datatype` enum in
// `Datatype.h`) using 4 bits for the datatype and 60 bits for the value, packed
// into a single 64-bit word. Everything that does not depend on this packing is
// implemented in the base class `qlever::IdBase`.
class ValueId : public qlever::IdBase<ValueId, uint64_t, uint64_t, uint8_t> {
  friend IdBase;

 public:
  using IdBase::fromBits;

  // ___________________________________________________________________________
  // The layout: 4 bits for the datatype and 60 bits for the payload.

  static constexpr T numDatatypeBits = 4;
  static constexpr T numDataBits = 64 - numDatatypeBits;
  // The number of bits of a vocabulary index, including the markers in its
  // topmost bits (see `SplitVocabulary` and `GeoCellGrid`). For this `Id`, a
  // vocabulary index uses the whole payload.
  static constexpr T numVocabIndexBits = numDataBits;

  using IntegerType = ad_utility::NBitInteger<numDataBits>;

  // The maximum value for the unsigned index types (see `global/IndexTypes.h`).
  // A larger index throws an `IndexTooLargeException`.
  static constexpr T maxIndex = (1ull << numDataBits) - 1;

  // The smallest double > 0 that will not be rounded to zero by the precision
  // loss of `FoldedId`. Symmetrically, `-minPositiveDouble` is the largest
  // double <0 that will not be rounded to zero.
  // Note: This constant is currently only used in unit tests, and cannot be
  // computed at compile time in C++17.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  static constexpr double minPositiveDouble =
      absl::bit_cast<double>(1ull << numDatatypeBits);
#endif

  // Assert that the size of an encoded GeoPoint equals the available bits in a
  // ValueId.
  static_assert(numDataBits == GeoPoint::numDataBits);

  // This exception is thrown if we try to store a value of an index type
  // (VocabIndex, LocalVocabIndex, TextRecordIndex) that is larger than
  // `maxIndex`.
  struct IndexTooLargeException : public std::exception {
   private:
    std::string errorMessage_;

   public:
    explicit IndexTooLargeException(
        T tooBigValue,
        ad_utility::source_location s = AD_CURRENT_SOURCE_LOC()) {
      errorMessage_ = absl::StrCat(
          s.file_name(), ", line ", s.line(), ": The given value ", tooBigValue,
          " is bigger than what the maxIndex of ValueId allows.");
    }

    const char* what() const noexcept override { return errorMessage_.c_str(); }
  };

 private:
  // The actual bits.
  T _bits;

 public:
  // Default construction of an uninitialized id.
  ValueId() = default;

  // ___________________________________________________________________________
  // The access to the bits.

  // Get the underlying bit representation, e.g. for compression etc.
  [[nodiscard]] constexpr T getBits() const noexcept { return _bits; }

  // Get the datatype bits.
  [[nodiscard]] constexpr DatatypeType getDatatypeBits() const noexcept {
    return static_cast<DatatypeType>(_bits >> numDataBits);
  }

  // Get the payload bits.
  [[nodiscard]] constexpr PayloadType getPayloadBits() const noexcept {
    return _bits & ad_utility::bitMaskForLowerBits(numDataBits);
  }

  // Construct from the underlying bit representation. `bits` must have been
  // obtained by a call to `getBits()` on a valid `ValueId`.
  static constexpr ValueId fromBits(T bits) noexcept { return {bits}; }

  // Combine datatype and payload bits (inverse of `getDatatypeBits` and
  // `getPayloadBits`). `payload` must not have any datatype bits set.
  static constexpr ValueId fromBits(const T datatype,
                                    const T payload) noexcept {
    AD_EXPENSIVE_CHECK(datatype < (T{1} << numDatatypeBits));
    AD_EXPENSIVE_CHECK(payload <= maxIndex);
    return {payload | (datatype << numDataBits)};
  }

  // ___________________________________________________________________________
  // The order of the bits.

  // Return the smallest and largest possible `ValueId` wrt the underlying
  // representation. Note: The datatype bits of `max()` are `15`, which is not
  // a valid `Datatype`. Such IDs must only be used as sentinel values for
  // comparisons, never as actual values.
  constexpr static ValueId min() noexcept {
    return {std::numeric_limits<T>::min()};
  }
  constexpr static ValueId max() noexcept {
    return {std::numeric_limits<T>::max()};
  }

  // Return the smallest `Id` that is greater than this one. The overflow of
  // the payload carries into the datatype, as the bits form one integer.
  [[nodiscard]] constexpr ValueId incremented() const { return _bits + 1; }

  // Enable the serialization of `ValueId` in the `ad_utility::serialization`
  // framework.
  template <typename T>
  friend void allowTrivialSerialization(ValueId, T);

 private:
  // The comparison of two `Id`s that are bitwise comparable (see
  // `compareThreeWay` in `IdBase`). Because the type bits are the most
  // significant bits, the comparison of the bits as one unsigned integer
  // compares by the datatype first.
  [[nodiscard]] constexpr ql::strong_ordering compareBitwiseImpl(
      const ValueId& other) const {
    return ql::compareThreeWay(_bits, other._bits);
  }

  // Private constructor that implicitly converts from the underlying
  // representation. Used in the implementation of the static factory methods
  // `fromBits()` etc.
  constexpr ValueId(T bits) : _bits{bits} {}
};

static_assert(ValueId::checkDerived());

}  // namespace foldedValueId

#endif  // QLEVER_SRC_GLOBAL_FOLDEDVALUEID_H
