// Copyright 2026 The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_GLOBAL_IDBASE_H
#define QLEVER_SRC_GLOBAL_IDBASE_H

#include <absl/base/casts.h>
#include <absl/strings/str_format.h>

#include <array>
#include <cstdint>
#include <functional>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "backports/algorithm.h"
#include "backports/keywords.h"
#include "backports/three_way_comparison.h"
#include "global/Datatype.h"
#include "global/IndexTypes.h"
#include "global/TripleTermIndex.h"
#include "index/IdBitRepresentation.h"
#include "rdfTypes/GeoPoint.h"
#include "util/Algorithm.h"
#include "util/DateYearDuration.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

namespace qlever {

// The common part of the two implementations of `Id` (the legacy
// `foldedValueId::ValueId` and
// `columnBasedIdTable::splitLayout::SplitLayoutId`). Both encode a value of one
// of the types from the `Datatype` enum as a datatype and a payload. Everything
// that only depends on these two is implemented here, once for both (CRTP).
//
// The template parameters are the `Derived` class itself, its bit
// representation `T` and the unsigned types of the payload and the datatype.
// The types are template parameters and not members of `Derived`, because
// `Derived` is still incomplete at the point where it derives from this class.
//
// The `Derived` class defines only how the datatype and the payload are stored
// (see also `checkDerived`):
// - The layout: `numDatatypeBits`, `numDataBits`, `numVocabIndexBits`,
//   `IntegerType`, `maxIndex` and `minPositiveDouble`. A payload with fewer
//   than 64 bits stores a 64-bit value (a `double` or a pointer) with its
//   lowest `64 - numDataBits` bits stripped (see `numStrippedBits`).
// - The access to the bits: `getBits()`, `getDatatypeBits()`,
//   `getPayloadBits()` and the static `fromBits(T)` and
//   `fromBits(DatatypeType, PayloadType)`. This class adds
//   `fromBits(Datatype, PayloadType)`, so the `Derived` class needs
//   `using IdBase::fromBits;`, otherwise its own `fromBits` overloads hide it.
// - The order of the bits: `compareBitwiseImpl`, `min()`, `max()` and
//   `incremented()`.
template <typename Derived, typename BitRepresentationT, typename PayloadTypeT,
          typename DatatypeTypeT>
class IdBase {
 public:
  // ___________________________________________________________________________
  // Types and constants.

  using T = BitRepresentationT;
  using PayloadType = PayloadTypeT;
  using DatatypeType = DatatypeTypeT;
  static_assert(std::is_unsigned_v<PayloadType>);
  static_assert(std::is_unsigned_v<DatatypeType>);

  // The largest representable integer value, see the `IntegerType` of the
  // `Derived` class. NOTE: This member may use `Derived`, because its
  // initializer is only instantiated where it is used.
  static constexpr int64_t maxInt = Derived::IntegerType::max();

  // A struct that represents the single undefined value. This is required for
  // generic code like in the `visit` method.
  struct UndefinedType {};

  // ___________________________________________________________________________
  // The datatypes.

  // All types that store strings. Together, the IDs of all the items of these
  // types form a consecutive range of IDs when sorted. Within this range, the
  // IDs are ordered by their string values, not by their IDs (and hence also
  // not by their types).
  // TODO<joka921> `SecondaryVocabIndex` also stores strings, but it is
  // deliberately not listed here (yet): the IDs of that type are directly
  // adjacent to the ones listed here, but they are *not* ordered by their
  // string value, so listing them here would break the semantic (that is, by
  // string value) comparison of IDs in `ValueIdComparators.h`. Teaching that
  // comparison about the secondary vocabulary is a separate step.
  static constexpr std::array<Datatype, 2> stringTypes_{
      Datatype::VocabIndex, Datatype::LocalVocabIndex};

  // Assert that the types in `stringTypes_` are directly adjacent. This is
  // required to make the comparison of IDs in `ValueIdComparators.h` work.
  static constexpr Datatype maxStringType_ = ql::ranges::max(stringTypes_);
  static constexpr Datatype minStringType_ = ql::ranges::min(stringTypes_);
  static_assert(static_cast<size_t>(maxStringType_) -
                    static_cast<size_t>(minStringType_) + 1 ==
                stringTypes_.size());

  // The datatypes that the position of a word in the vocabularies of an index
  // can have, see `LocalVocabEntry::positionInVocab()`. The comparison of an
  // `Id` of type `LocalVocabIndex` with an `Id` of one of these types requires
  // that position (which may be expensive to compute); for all other datatypes
  // the comparison of the bits already yields the correct result, see
  // `compareThreeWay` below.
  //
  // NOTE: `EncodedVal` is not among them, because an IRI that is encoded
  // directly in an `Id` is never stored in a `LocalVocabEntry` (see
  // `toIdOrLocalVocabEntry`).
  static constexpr std::array<Datatype, 2> datatypesOfPositionInVocab_{
      Datatype::VocabIndex, Datatype::SecondaryVocabIndex};

  // Assert that `VocabIndex`, `LocalVocabIndex`, and `SecondaryVocabIndex` are
  // directly adjacent, in this order. The comparison of an `Id` of type
  // `LocalVocabIndex` with an `Id` of an unrelated datatype relies on this, see
  // `compareThreeWay` below.
  static_assert(static_cast<size_t>(Datatype::LocalVocabIndex) ==
                static_cast<size_t>(Datatype::VocabIndex) + 1);
  static_assert(static_cast<size_t>(Datatype::SecondaryVocabIndex) ==
                static_cast<size_t>(Datatype::LocalVocabIndex) + 1);

  // A mapping that decides if a Datatype is bitwise comparable or not. See
  // `canBeComparedBitwise()` below.
  static constexpr std::array<bool, qlever::enumSize<Datatype>>
      isTypeBitwiseComparable_{true, true, true, true, true, false, true,
                               true, true, true, true, true, true,  true};

  // Constant to be used in `static_assert` statements to indicate that
  // behavior is relying on the local vocab entry to be the only datatype that
  // is not bitwise comparable.
  static constexpr bool isOnlyLocalVocabNotBitwiseComparable =
      !isTypeBitwiseComparable_.at(
          static_cast<size_t>(Datatype::LocalVocabIndex));

  // The datatype, as the `Datatype` enum.
  [[nodiscard]] constexpr Datatype getDatatype() const noexcept {
    return static_cast<Datatype>(derived().getDatatypeBits());
  }

  // An ID is considered trivial, if its datatype is trivial (see
  // `isDatatypeTrivial` in `Datatype.h`).
  constexpr bool isTrivial() const { return isDatatypeTrivial(getDatatype()); }

  // Return true iff `datatype` is one of the datatypes that the position of a
  // word in the vocabularies can have, see `datatypesOfPositionInVocab_`.
  static constexpr bool isDatatypeOfPositionInVocab(Datatype datatype) {
    return ad_utility::contains(datatypesOfPositionInVocab_, datatype);
  }

  // An `Id` is considered bitwise comparable if the mapping at
  // `isTypeBitwiseComparable_` says so. This is currently the case for all
  // datatypes except for the local vocab index.
  constexpr bool canBeComparedBitwise() const {
    static_assert(isOnlyLocalVocabNotBitwiseComparable);
    return isTypeBitwiseComparable_.at(static_cast<size_t>(getDatatype()));
  }

  // ___________________________________________________________________________
  // The values: the factory functions and the getters for each datatype.

  // Combine the datatype and the payload bits, see
  // `Derived::fromBits(DatatypeType, PayloadType)`.
  static constexpr Derived fromBits(Datatype datatype,
                                    PayloadType payload) noexcept {
    return Derived::fromBits(static_cast<DatatypeType>(datatype), payload);
  }

  // Create an `Id` of the `Undefined` type. There is only one such ID and it is
  // guaranteed to be smaller than all IDs of other types. This helps
  // implementing the correct join behavior in presence of undefined values.
  static constexpr Derived makeUndefined() noexcept {
    return Derived::fromBits(Datatype::Undefined, 0);
  }

  // Returns an object of `UndefinedType`. In many scenarios this function is
  // unnecessary because `getDatatype() == Undefined` already identifies the
  // single undefined value correctly, but it is very useful for generic code
  // like the `visit` member function.
  [[nodiscard]] constexpr UndefinedType getUndefined() const noexcept {
    return {};
  }

  [[nodiscard]] QL_CONSTEXPR bool isUndefined() const noexcept {
    return derived() == makeUndefined();
  }

  // Create an `Id` for a double value. If the payload has fewer than 64 bits,
  // the lowest bits of the mantissa are stripped (see `numStrippedBits`), so
  // that the precision is reduced from 53 to `53 - numStrippedBits()`
  // significant bits.
  // NOTE: This function and `getDouble` below are only `constexpr` in C++20
  // mode, where `absl::bit_cast` is `std::bit_cast`. In C++17 mode it falls
  // back to a `memcpy`.
  static QL_CONSTEXPR Derived makeFromDouble(double d) {
    return Derived::fromBits(
        Datatype::Double,
        static_cast<PayloadType>(absl::bit_cast<uint64_t>(d) >>
                                 numStrippedBits()));
  }

  // Obtain the `double` that this `Id` encodes. If `getDatatype() != Double`
  // then the result is unspecified.
  [[nodiscard]] QL_CONSTEXPR double getDouble() const noexcept {
    return absl::bit_cast<double>(
        static_cast<uint64_t>(derived().getPayloadBits()) << numStrippedBits());
  }

  // Create an `Id` for a signed integer value. The integers that the
  // `IntegerType` of the `Derived` class can represent are stored exactly
  // (60 bits for the legacy `ValueId`, the full `int64_t` for the
  // `SplitLayoutId`). Other integers overflow according to the semantics of
  // `NBitInteger`.
  static constexpr Derived makeFromInt(int64_t i) {
    return Derived::fromBits(Datatype::Int, Derived::IntegerType::toNBit(i));
  }

  // Obtain the signed integer that this `Id` encodes. If `getDatatype() != Int`
  // then the result is unspecified.
  [[nodiscard]] constexpr int64_t getInt() const noexcept {
    return Derived::IntegerType::fromNBit(derived().getPayloadBits());
  }

  // Create an `Id` for a boolean value.
  static constexpr Derived makeFromBool(bool b) noexcept {
    return Derived::fromBits(Datatype::Bool, static_cast<PayloadType>(b));
  }

  // Create an `Id` for a boolean value, represented as "0" or "1" instead of
  // "false" or "true".
  static constexpr Derived makeBoolFromZeroOrOne(bool b) noexcept {
    auto bits = static_cast<PayloadType>(b);
    bits |= static_cast<PayloadType>(true) << 1;
    return Derived::fromBits(Datatype::Bool, bits);
  }

  // Obtain the boolean value.
  [[nodiscard]] constexpr bool getBool() const noexcept {
    return static_cast<bool>(derived().getPayloadBits() & 1);
  }

  // Obtain the boolean value as a string view. In particular, return either
  // `true`, `false`, `0` , or `1`, depending on whether the value was created
  // via `makeFromBool` or `makeBoolFromZeroOrOne` (see above).
  [[nodiscard]] constexpr std::string_view getBoolLiteral() const noexcept {
    if (derived().getPayloadBits() & 0b10) {
      return getBool() ? "1" : "0";
    }
    return getBool() ? "true" : "false";
  }

  // Create an `Id` for an unsigned index of one of the index types (see
  // `global/IndexTypes.h`). These types can represent values in the range
  // [0, `maxIndex`]. When `index` is outside of this range, an
  // `IndexTooLargeException` is thrown.
  static constexpr Derived makeFromTripleTermIndex(TripleTermIndex index) {
    return makeFromIndex(index.get(), Datatype::TripleTermIndex);
  }

  static constexpr Derived makeFromVocabIndex(VocabIndex index) {
    return makeFromIndex(index.get(), Datatype::VocabIndex);
  }

  static constexpr Derived makeFromEncodedVal(PayloadType idx) {
    return makeFromIndex(idx, Datatype::EncodedVal);
  }

  static constexpr Derived makeFromTextRecordIndex(TextRecordIndex index) {
    return makeFromIndex(index.get(), Datatype::TextRecordIndex);
  }

  static Derived makeFromLocalVocabIndex(LocalVocabIndex index) {
    // The lowest `numStrippedBits()` bits of a `LocalVocabIndex` are always
    // zero, because of its alignment, so they can be stripped.
    // TODO<pas-kes> With the split layout nothing is stripped, so the
    // `alignas(16)` of `LocalVocabEntry` is only needed for the legacy layout.
    static_assert(alignof(decltype(*index)) >=
                  (size_t{1} << numStrippedBits()));
    return Derived::fromBits(
        Datatype::LocalVocabIndex,
        static_cast<PayloadType>(reinterpret_cast<uintptr_t>(index) >>
                                 numStrippedBits()));
  }

  static constexpr Derived makeFromWordVocabIndex(WordVocabIndex index) {
    return makeFromIndex(index.get(), Datatype::WordVocabIndex);
  }

  static constexpr Derived makeFromBlankNodeIndex(BlankNodeIndex index) {
    return makeFromIndex(index.get(), Datatype::BlankNodeIndex);
  }

  static constexpr Derived makeFromSecondaryVocabIndex(
      SecondaryVocabIndex index) {
    return makeFromIndex(index.get(), Datatype::SecondaryVocabIndex);
  }

  // Obtain the unsigned index that this `Id` encodes. If `getDatatype()` is not
  // the index type that the respective getter below is named after, then the
  // result is unspecified.
  [[nodiscard]] constexpr TripleTermIndex getTripleTermIndex() const noexcept {
    return TripleTermIndex::make(derived().getPayloadBits());
  }

  [[nodiscard]] constexpr VocabIndex getVocabIndex() const noexcept {
    return VocabIndex::make(derived().getPayloadBits());
  }

  [[nodiscard]] constexpr PayloadType getEncodedVal() const noexcept {
    return derived().getPayloadBits();
  }

  [[nodiscard]] constexpr TextRecordIndex getTextRecordIndex() const noexcept {
    return TextRecordIndex::make(derived().getPayloadBits());
  }

  [[nodiscard]] LocalVocabIndex getLocalVocabIndex() const noexcept {
    return reinterpret_cast<LocalVocabIndex>(
        static_cast<uintptr_t>(derived().getPayloadBits())
        << numStrippedBits());
  }

  [[nodiscard]] constexpr WordVocabIndex getWordVocabIndex() const noexcept {
    return WordVocabIndex::make(derived().getPayloadBits());
  }

  [[nodiscard]] constexpr BlankNodeIndex getBlankNodeIndex() const noexcept {
    return BlankNodeIndex::make(derived().getPayloadBits());
  }

  [[nodiscard]] constexpr SecondaryVocabIndex getSecondaryVocabIndex()
      const noexcept {
    return SecondaryVocabIndex::make(derived().getPayloadBits());
  }

  // Store or load a `Date` object.
  // NOTE: These functions cannot be `constexpr`, because
  // `DateYearOrDuration` is not a literal type.
  // TODO<pas-kes> The split layout has 4 more payload bits, which could give a
  // `Date` a higher precision (a finer time or a larger range of years).
  static Derived makeFromDate(DateYearOrDuration d) noexcept {
    return Derived::fromBits(Datatype::Date, absl::bit_cast<uint64_t>(d));
  }

  DateYearOrDuration getDate() const noexcept {
    return absl::bit_cast<DateYearOrDuration>(
        static_cast<uint64_t>(derived().getPayloadBits()));
  }

  // Create an `Id` for a GeoPoint object (representing a POINT from WKT).
  static Derived makeFromGeoPoint(GeoPoint p) {
    return Derived::fromBits(Datatype::GeoPoint, p.toBitRepresentation());
  }

  // Obtain a new `GeoPoint` object representing the pair of coordinates that
  // this `Id` encodes. If `getDatatype() != GeoPoint` then the result is
  // unspecified.
  GeoPoint getGeoPoint() const {
    return GeoPoint::fromBitRepresentation(derived().getPayloadBits());
  }

  // Similar to `std::visit` for `std::variant`. First gets the datatype and
  // then calls `visitor(getTYPE)` where `getTYPE` is the correct getter method
  // for the datatype (e.g. `getDouble` for `Datatype::Double`). Visitor must
  // be callable with all of the possible return types of the `getTYPE`
  // functions.
  // TODO<joka921> This currently still has limited functionality because
  // VocabIndex, LocalVocabIndex, TextRecordIndex,  and EncodedVal are all of
  // the same type `uint64_t` and the visitor cannot distinguish between them.
  // Create strong types for these indices and make the `Id` class use them.
  template <typename Visitor>
  decltype(auto) visit(Visitor&& visitor) const {
    switch (getDatatype()) {
      case Datatype::Undefined:
        return std::invoke(visitor, getUndefined());
      case Datatype::Bool:
        return std::invoke(visitor, getBool());
      case Datatype::Double:
        return std::invoke(visitor, getDouble());
      case Datatype::Int:
        return std::invoke(visitor, getInt());
      case Datatype::EncodedVal:
        return std::invoke(visitor, getEncodedVal());
      case Datatype::VocabIndex:
        return std::invoke(visitor, getVocabIndex());
      case Datatype::LocalVocabIndex:
        return std::invoke(visitor, getLocalVocabIndex());
      case Datatype::TextRecordIndex:
        return std::invoke(visitor, getTextRecordIndex());
      case Datatype::WordVocabIndex:
        return std::invoke(visitor, getWordVocabIndex());
      case Datatype::Date:
        return std::invoke(visitor, getDate());
      case Datatype::GeoPoint:
        return std::invoke(visitor, getGeoPoint());
      case Datatype::BlankNodeIndex:
        return std::invoke(visitor, getBlankNodeIndex());
      case Datatype::SecondaryVocabIndex:
        return std::invoke(visitor, getSecondaryVocabIndex());
      case Datatype::TripleTermIndex:
        return std::invoke(visitor, getTripleTermIndex());
    }
    AD_FAIL();
  }

  // ___________________________________________________________________________
  // Comparison.

  // Comparison is performed on the datatype first and then on the payload bits
  // (as unsigned integers), see `compareBitwiseImpl` of the `Derived` class.
  // All values of the same `Datatype` are therefore adjacent to each other.
  // Unsigned index types are also ordered correctly. Signed integers are
  // ordered as follows: first the positive integers in order and then the
  // negative integers in order. For doubles it is first the positive doubles in
  // order, then the negative doubles in reversed order. This is a direct
  // consequence of comparing the bit representation of these values as
  // unsigned integers.
  // NOTE: An `Id` of type `LocalVocabIndex` does not carry its value in its
  // bits (they are a pointer), so it is ordered by the position that its word
  // has in the vocabularies of the index (see
  // `LocalVocabEntry::positionInVocab`). That position only has to be looked up
  // if the other `Id` is of one of the datatypes that such a position can have
  // (`datatypesOfPositionInVocab_`, see there). For all other datatypes the
  // comparison of the bits already yields the same result, because
  // `LocalVocabIndex` is directly adjacent to `VocabIndex` and
  // `SecondaryVocabIndex` (see the `static_assert`s above), so an `Id` of any
  // other datatype compares to the datatype bits of a `LocalVocabIndex` exactly
  // like it compares to the datatype bits of the position.
  // NOTE: This is only `constexpr` in C++20 mode. GCC 8, which the C++17 CI job
  // uses, rejects the branches below in a relaxed-constexpr function
  // ("expression `<statement>` is not a constant expression"), and nothing
  // compares `Id`s at compile time anyway.
  [[nodiscard]] QL_CONSTEXPR auto compareThreeWay(const Derived& other) const {
    using enum Datatype;
    const auto type = getDatatype();
    const auto otherType = other.getDatatype();
    if (type != LocalVocabIndex && otherType != LocalVocabIndex) {
      return derived().compareBitwiseImpl(other);
    }
    if (type == LocalVocabIndex && otherType == LocalVocabIndex) [[unlikely]] {
      return ql::compareThreeWay(*getLocalVocabIndex(),
                                 *other.getLocalVocabIndex());
    }

    // Exactly one of the types is `LocalVocabIndex`. If the other one is a type
    // that a position in the vocabularies can have, then compare that position,
    // else fall through to the comparison of the bits, see the note above.
    // NOTE: The position is stored as an `Id` of the current build (see
    // `LocalVocabEntry::IdProxy`). The `else` branch only exists because the
    // tests also compile the other `Id` type, without these comparisons.
    // TODO<pas-kes> Template `LocalVocabEntry` on the `Id` (too intrusive for
    // this PR) and remove the `if constexpr`.
    if constexpr (std::is_same_v<T, IdBitRepresentation>) {
      // NOTE: GCC 11 issues a false positive warning here, so we try to avoid
      // it by being over-explicit about the branches here.
      if (isDatatypeOfPositionInVocab(type) && otherType == LocalVocabIndex) {
        return comparePositionInVocabToLocalVocab(
            LocalVocabEntry::IdProxy::make(derived().getBits()),
            other.getLocalVocabIndex());
      }
      if (type == LocalVocabIndex && isDatatypeOfPositionInVocab(otherType)) {
        auto inverseOrder = comparePositionInVocabToLocalVocab(
            LocalVocabEntry::IdProxy::make(other.getBits()),
            getLocalVocabIndex());
        return ql::compareThreeWay(0, inverseOrder);
      }
    } else {
      AD_THROW(
          "Only the `Id` of the current build can compare a `LocalVocabIndex` "
          "with an `Id` of another datatype");
    }

    // One of the types is `LocalVocabIndex`, and the other one is a non-string
    // type like `Int` or `Undefined`. Then the comparison of the bits
    // automatically compares by the datatype.
    return derived().compareBitwiseImpl(other);
  }

  // The same as a static function with two arguments, for
  // `QL_DEFINE_CUSTOM_THREEWAY_OPERATOR` below.
  static QL_CONSTEXPR auto compareThreeWay(const Derived& left,
                                           const Derived& right) {
    return left.compareThreeWay(right);
  }

  // The comparison operators. They are hidden friends with two `Derived`
  // arguments and not members of this class: in C++20 a member `operator<=>`
  // with a `Derived` argument is ambiguous with its reversed candidate (both
  // need the conversion of one `Derived` to this base class), which breaks
  // `std::three_way_comparable` and thus the comparison of tuples of `Id`s.
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR(Derived)

  friend QL_CONSTEXPR bool operator==(const Derived& left,
                                      const Derived& right) {
    return compareThreeWay(left, right) == 0;
  }

  friend QL_CONSTEXPR bool operator!=(const Derived& left,
                                      const Derived& right) {
    return !(left == right);
  }

  // When there are no local vocab entries, then comparison can only be done
  // on the underlying bits, which allows much better code generation (e.g.
  // vectorization). In particular, this method should for example be used
  // during index building.
  // NOTE: This is only `constexpr` in C++20 mode, for the same reason as
  // `compareThreeWay` above.
  QL_CONSTEXPR auto compareWithoutLocalVocab(const Derived& other) const {
    // NOTE: If this static assertion is violated at some point, make sure to
    // check all callers of this function if they are still correct.
    static_assert(isOnlyLocalVocabNotBitwiseComparable);
    AD_EXPENSIVE_CHECK(canBeComparedBitwise());
    AD_EXPENSIVE_CHECK(other.canBeComparedBitwise());
    return derived().compareBitwiseImpl(other);
  }

  // ___________________________________________________________________________
  // Hashing and output.

  // Enable hashing in abseil for `Id` (required by `ad_utility::HashSet` and
  // `ad_utility::HashMap`).
  template <typename H>
  friend H AbslHashValue(H h, const Derived& id) {
    // Adding 0/1 to the hash is required to ensure that for two unequal
    // elements the hash expansions of neither is a suffix of the other. This is
    // a property that absl requires for hashes. The hash expansion is the list
    // of simpler values actually being hashed (here: bits or hash expansion of
    // the LocalVocabEntry).
    if (id.getDatatype() != Datatype::LocalVocabIndex) {
      static_assert(isOnlyLocalVocabNotBitwiseComparable);
      return H::combine(std::move(h), id.getBits(), 0);
    }
    auto [lower, upper] = id.getLocalVocabIndex()->positionInVocab();
    if (upper != lower) {
      return H::combine(std::move(h), lower.get(), 0);
    }
    return H::combine(std::move(h), *id.getLocalVocabIndex(), 1);
  }

  // This operator is only for debugging and testing. It returns a
  // human-readable representation.
  friend std::ostream& operator<<(std::ostream& ostr, const Derived& id) {
    ostr << toString(id.getDatatype())[0] << ':';
    if (id.getDatatype() == Datatype::Undefined) {
      return ostr << id.getPayloadBits();
    }

    auto visitor = [&ostr](auto&& value) {
      using ValueType = decltype(value);
      if constexpr (ad_utility::isSimilar<ValueType, UndefinedType>) {
        // already handled above
        AD_FAIL();
      } else if constexpr (ad_utility::isSimilar<ValueType, double>) {
        // NOTE: `%f` is what `std::to_string` did for floating-point values
        // before C++26. Since C++26, it produces the shortest round-trip
        // representation instead (`42` instead of `42.000000`).
        ostr << absl::StrFormat("%f", value);
      } else if constexpr (ad_utility::SimilarToAny<ValueType, int64_t,
                                                    uint64_t>) {
        ostr << std::to_string(value);
      } else if constexpr (ad_utility::isSimilar<ValueType, bool>) {
        ostr << (value ? "true" : "false");
      } else if constexpr (ad_utility::isSimilar<ValueType,
                                                 DateYearOrDuration>) {
        ostr << value.toStringAndType().first;
      } else if constexpr (ad_utility::isSimilar<ValueType, GeoPoint>) {
        ostr << value.toStringRepresentation();
      } else if constexpr (ad_utility::isSimilar<ValueType, LocalVocabIndex>) {
        AD_CORRECTNESS_CHECK(value != nullptr);
        ostr << value->toStringRepresentation();
      } else {
        // `ValueType` is one of the remaining index types, all of which wrap a
        // `uint64_t` (see `global/IndexTypes.h`).
        ostr << std::to_string(value.get());
      }
    };
    id.visit(visitor);
    return ostr;
  }

  // ___________________________________________________________________________
  // Functions for algorithms.

  // Lambdas that forward to the member function of the same name (without the
  // suffix `L`). Unlike a pointer-to-member like `&Id::isUndefined`, they also
  // work for proxy types that provide the same member functions, like the
  // elements of a column view that does not store `Id`s.
  static constexpr auto isUndefinedL = [](const auto& id) {
    return id.isUndefined();
  };

  static constexpr auto isDefinedL = [](const auto& id) {
    return !id.isUndefined();
  };

  static constexpr auto getBitsL = [](const auto& id) { return id.getBits(); };

  static constexpr auto getDatatypeL = [](const auto& id) {
    return id.getDatatype();
  };

  // `fromBits(T)` as a single function and not an overload set, so that it can
  // be passed to an algorithm directly, like the lambdas above. It only takes
  // the bit representation `T`.
  static constexpr Derived fromBitsL(T bits) { return Derived::fromBits(bits); }

  // ___________________________________________________________________________
  // Check what the `Derived` class has to define, see the comment at the
  // beginning of the class. This is a function, because `Derived` is incomplete
  // at the point where it derives from this class, so the `Derived` classes
  // call it in a `static_assert` after their definition.
  static constexpr bool checkDerived() {
    static_assert(std::is_base_of_v<IdBase, Derived>);
    static_assert(Derived::numDataBits <= 8 * sizeof(PayloadType));
    static_assert(Derived::numDatatypeBits <= 8 * sizeof(DatatypeType));
    static_assert(Derived::numVocabIndexBits <= Derived::numDataBits);
    static_assert(Derived::numDataBits >= GeoPoint::numDataBits);
    return true;
  }

 private:
  // Access the `Derived` class (CRTP).
  constexpr const Derived& derived() const {
    return static_cast<const Derived&>(*this);
  }

  // The number of lowest bits of a 64-bit value (a `double` or a pointer) that
  // are stripped to store it in the payload: 4 for the legacy `ValueId` with
  // its 60 payload bits, 0 for the `SplitLayoutId`.
  static constexpr int numStrippedBits() {
    // A payload with more than 64 bits would need a different storage of these
    // 64-bit values, so it is not supported.
    static_assert(Derived::numDataBits <= 64);
    return 64 - static_cast<int>(Derived::numDataBits);
  }

  // Helper function for the implementation of the unsigned index types. The
  // check is only needed if an index can be larger than `maxIndex`, and only
  // then the `Derived` class has to define the `IndexTooLargeException`.
  template <typename Index>
  static constexpr Derived makeFromIndex(Index id, Datatype type) {
    if constexpr (Derived::maxIndex < std::numeric_limits<Index>::max()) {
      if (id > Derived::maxIndex) {
        throw typename Derived::IndexTooLargeException(id);
      }
    }
    return Derived::fromBits(type, static_cast<PayloadType>(id));
  }

  // Compare the bits of an `Id` whose datatype is one of
  // `datatypesOfPositionInVocab_` with a `LocalVocabIndex`. For details see
  // `LocalVocabEntry::positionInVocab()`.
  static ql::strong_ordering comparePositionInVocabToLocalVocab(
      LocalVocabEntry::IdProxy otherId, ::LocalVocabIndex localVocabIndex) {
    auto [lowerBound, upperBound] = localVocabIndex->positionInVocab();
    if (otherId < lowerBound) {
      return ql::strong_ordering::less;
    }
    if (otherId >= upperBound) {
      return ql::strong_ordering::greater;
    }
    return ql::strong_ordering::equal;
  }
};

}  // namespace qlever

#endif  // QLEVER_SRC_GLOBAL_IDBASE_H
