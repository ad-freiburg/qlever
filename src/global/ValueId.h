//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_GLOBAL_VALUEID_H
#define QLEVER_SRC_GLOBAL_VALUEID_H

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <limits>

#include "backports/functional.h"
#include "backports/keywords.h"
#include "backports/three_way_comparison.h"
#include "global/Constants.h"
#include "global/IndexTypes.h"
#include "global/ValueIdBitRepresentation.h"
#include "rdfTypes/GeoPoint.h"
#include "util/Algorithm.h"
#include "util/BitUtils.h"
#include "util/DateYearDuration.h"
#include "util/NBitInteger.h"
#include "util/Serializer/Serializer.h"
#include "util/SourceLocation.h"

// The different Datatypes that a `ValueId` (see below) can encode.
// Note: If you add a datatype, make sure to update the `MaxValue` if necessary,
// and check whether you have to add it to the `isDatatypeTrivial` function
// directly below.
enum struct Datatype {
  Undefined = 0,
  Bool,
  Int,
  Double,
  VocabIndex,
  LocalVocabIndex,
  TextRecordIndex,
  Date,
  GeoPoint,
  WordVocabIndex,
  BlankNodeIndex,
  EncodedVal,
  MaxValue = EncodedVal
  // Note: Unfortunately, we cannot easily get the size of an enum.
  // If members are added to this enum, then the `MaxValue`
  // alias must always be equal to the last member,
  // else other code breaks with out-of-bounds accesses.
};

// Return true iff the `datatype` is a trivial datatype. This means that IDs
// with this datatype directly encode the value they represent and do not point
// to an external resource. In other words: These IDs can safely be shared
// across different QLever indices without having to rewrite them. Note:
// `BlankNodeIndex` is deliberately NOT considiered trivial, as blank nodes
// depend on the context, in particular they have to be remapped when results
// from different  RDF sources are merged. Same goes for `EncodedVal` which
// depends on the (configurable!) prefixes for the encoding.
constexpr bool isDatatypeTrivial(Datatype datatype) {
  using enum Datatype;
  constexpr std::array trivialDatatypes{Undefined, Bool, Int,
                                        Double,    Date, GeoPoint};
  return ad_utility::contains(trivialDatatypes, datatype);
}

/// Convert the `Datatype` enum to the corresponding string
inline QL_CONSTEXPR std::string_view toString(Datatype type) {
  switch (type) {
    case Datatype::Undefined:
      return "Undefined";
    case Datatype::Bool:
      return "Bool";
    case Datatype::Double:
      return "Double";
    case Datatype::Int:
      return "Int";
    case Datatype::EncodedVal:
      return "EncodedIri";
    case Datatype::VocabIndex:
      return "VocabIndex";
    case Datatype::LocalVocabIndex:
      return "LocalVocabIndex";
    case Datatype::TextRecordIndex:
      return "TextRecordIndex";
    case Datatype::WordVocabIndex:
      return "WordVocabIndex";
    case Datatype::Date:
      return "Date";
    case Datatype::GeoPoint:
      return "GeoPoint";
    case Datatype::BlankNodeIndex:
      return "BlankNodeIndex";
  }
  // This line is reachable if we cast an arbitrary invalid int to this enum
  AD_FAIL();
}

/// Encode values of different types (the types from the `Datatype` enum above)
/// using a full byte for the datatype and a full 64-bit word for the value.
/// The ordering of `ValueId`s is datatype-major: IDs are first ordered by
/// their datatype byte and then by their payload bits (interpreted as an
/// unsigned 64-bit integer).
class ValueId {
 public:
  // The type of the payload bits.
  using T = uint64_t;
  static constexpr T numDatatypeBits = 8;
  static constexpr T numDataBits = 64;

  using IntegerType = ad_utility::NBitInteger<numDataBits>;

  /// The maximum value for the unsigned types that are used as indices
  /// (currently VocabIndex, LocalVocabIndex and Text). Since the payload now
  /// consists of a full 64-bit word, all values of `T` are valid indices.
  static constexpr T maxIndex = std::numeric_limits<T>::max();

  // The largest representable integer value. As the payload is a full 64-bit
  // word, this is the full range of `int64_t`.
  static constexpr int64_t maxInt = IntegerType::max();
  // All types that store strings. Together, the IDs of all the items of these
  // types form a consecutive range of IDs when sorted. Within this range, the
  // IDs are ordered by their string values, not by their IDs (and hence also
  // not by their types).
  static constexpr std::array<Datatype, 2> stringTypes_{
      Datatype::VocabIndex, Datatype::LocalVocabIndex};

  // A mapping that decides if a Datatype is bitwise comparable or not. See
  // `canBeComparedBitwise()` below.
  static constexpr std::array<bool, 12> isTypeBitwiseComparable_{
      true, true, true, true, true, false, true, true, true, true, true, true};

  // Assert that the types in `stringTypes_` are directly adjacent. This is
  // required to make the comparison of IDs in `ValueIdComparators.h` work.
  static constexpr Datatype maxStringType_ = ql::ranges::max(stringTypes_);
  static constexpr Datatype minStringType_ = ql::ranges::min(stringTypes_);
  static_assert(static_cast<size_t>(maxStringType_) -
                    static_cast<size_t>(minStringType_) + 1 ==
                stringTypes_.size());

  // Assert that an encoded `GeoPoint` fits into the payload of a `ValueId`.
  static_assert(numDataBits >= GeoPoint::numDataBits);

  /// A struct that represents the single undefined value. This is required for
  /// generic code like in the `visit` method.
  struct UndefinedType {};

  /// The raw bit representation of a `ValueId`: a single datatype byte and a
  /// full 64-bit word of payload. See `ValueIdBitRepresentation.h` for
  /// details.
  using BitRepresentation = ValueIdBitRepresentation;

 private:
  // The actual data: a full 64-bit word of payload and a single byte for the
  // datatype. Together with the explicitly stored `padding_` this makes a
  // `ValueId` occupy 16 bytes when stored directly. The `IdTable` therefore
  // stores the payload and datatype in separate columns to avoid wasting
  // memory for the padding.
  T payload_;
  uint8_t datatype_;
  // Explicit padding bytes. They are always set to zero by all factory
  // functions, s.t. all valid `ValueId`s have a deterministic byte
  // representation (e.g. for hashing raw bytes or writing them to disk).
  // Note: Default-constructed `ValueId`s are deliberately uninitialized (see
  // the default constructor below).
  std::array<uint8_t, 7> padding_;

 public:
  /// Default construction of an uninitialized id.
  ValueId() = default;

  /// Comparison is performed datatype-major, i.e. first on the datatype byte
  /// and then on the payload bits (as unsigned integers). All values of the
  /// same `Datatype` are therefore adjacent to each other. Unsigned index
  /// types are also ordered correctly. Signed integers are ordered as follows:
  /// first the positive integers in order and then the negative integers in
  /// order. For doubles it is first the positive doubles in order, then the
  /// negative doubles in reversed order. This is a direct consequence of
  /// comparing the bit representation of these values as unsigned integers.
  constexpr auto compareThreeWay(const ValueId& other) const {
    using enum Datatype;
    auto type = getDatatype();
    auto otherType = other.getDatatype();
    if (type != LocalVocabIndex && otherType != LocalVocabIndex) {
      return compareBitwiseImpl(other);
    }
    if (type == LocalVocabIndex && otherType == LocalVocabIndex) [[unlikely]] {
      return ql::compareThreeWay(*getLocalVocabIndex(),
                                 *other.getLocalVocabIndex());
    }

    // GCC 11 issues a false positive warning here, so we try to avoid it by
    // being over-explicit about the branches here.
    if ((type == VocabIndex || type == EncodedVal) &&
        otherType == LocalVocabIndex) {
      return compareVocabAndLocalVocab(
          LocalVocabEntry::IdProxy::make(getBits()),
          other.getLocalVocabIndex());
    } else if (type == LocalVocabIndex &&
               (otherType == VocabIndex || otherType == EncodedVal)) {
      auto inverseOrder = compareVocabAndLocalVocab(
          LocalVocabEntry::IdProxy::make(other.getBits()),
          getLocalVocabIndex());

      return ql::compareThreeWay(0, inverseOrder);
    }

    // One of the types is `LocalVocab`, and the other one is a non-string
    // type like `Integer` or `Undefined. Then the comparison by bits
    // automatically compares by the datatype.
    return compareBitwiseImpl(other);
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(ValueId)

  friend constexpr bool operator==(const ValueId& left, const ValueId& right) {
    return ql::compareThreeWay(left, right) == 0;
  }
  friend constexpr bool operator!=(const ValueId& left, const ValueId& right) {
    return !(left == right);
  }

  // When there are no local vocab entries, then comparison can only be done
  // on the underlying bits, which allows much better code generation (e.g.
  // vectorization). In particular, this method should for example be used
  // during index building.
  constexpr auto compareWithoutLocalVocab(const ValueId& other) const {
    // NOTE: If this static assertion is violated at some point, make sure to
    // check all callers of this function if they are still correct.
    static_assert(isOnlyLocalVocabNotBitwiseComparable);
    AD_EXPENSIVE_CHECK(canBeComparedBitwise());
    AD_EXPENSIVE_CHECK(other.canBeComparedBitwise());
    return compareBitwiseImpl(other);
  }

  /// Get the underlying bit representation, e.g. for compression etc.
  [[nodiscard]] constexpr BitRepresentation getBits() const noexcept {
    return {datatype_, payload_};
  }
  /// Get only the payload bits (without the datatype byte).
  [[nodiscard]] constexpr T getPayloadBits() const noexcept { return payload_; }
  /// Construct from the underlying bit representation. `bits` must have been
  /// obtained by a call to `getBits()` on a valid `ValueId`.
  static constexpr ValueId fromBits(BitRepresentation bits) noexcept {
    return {bits.payload_, bits.datatype_};
  }

  /// Get the datatype.
  [[nodiscard]] constexpr Datatype getDatatype() const noexcept {
    return static_cast<Datatype>(datatype_);
  }

  /// Create a `ValueId` of the `Undefined` type. There is only one such ID and
  /// it is guaranteed to be smaller than all IDs of other types. This helps
  /// implementing the correct join behavior in presence of undefined values.
  constexpr static ValueId makeUndefined() noexcept { return {0, 0}; }

  /// Returns an object of `UndefinedType`. In many scenarios this function is
  /// unnecessary because `getDatatype() == Undefined` already identifies the
  /// single undefined value correctly, but it is very useful for generic code
  /// like the `visit` member function.
  [[nodiscard]] UndefinedType getUndefined() const noexcept { return {}; }
  constexpr bool isUndefined() const noexcept {
    return *this == makeUndefined();
  }

  /// Create a `ValueId` for a double value. The full precision of the IEEE
  /// double precision floating point number is preserved.
  static ValueId makeFromDouble(double d) {
    return {absl::bit_cast<T>(d), Datatype::Double};
  }
  /// Obtain the `double` that this `ValueId` encodes. If `getDatatype() !=
  /// Double` then the result is unspecified.
  [[nodiscard]] double getDouble() const noexcept {
    return absl::bit_cast<double>(payload_);
  }

  /// Create a `ValueId` for a signed integer value. The full range of
  /// `int64_t` can be represented.
  static ValueId makeFromInt(int64_t i) noexcept {
    return {IntegerType::toNBit(i), Datatype::Int};
  }

  /// Obtain the signed integer that this `ValueId` encodes. If `getDatatype()
  /// != Int` then the result is unspecified.
  [[nodiscard]] int64_t getInt() const noexcept {
    return IntegerType::fromNBit(payload_);
  }

  /// Create a `ValueId` for a boolean value.
  static constexpr ValueId makeFromBool(bool b) noexcept {
    return {static_cast<T>(b), Datatype::Bool};
  }

  /// Create a `ValueId` for a boolean value, represented as "0" or "1" instead
  /// of "false" or "true".
  static constexpr ValueId makeBoolFromZeroOrOne(bool b) noexcept {
    auto bits = static_cast<T>(b);
    bits |= static_cast<T>(true) << 1;
    return {bits, Datatype::Bool};
  }

  // Obtain the boolean value.
  [[nodiscard]] constexpr bool getBool() const noexcept {
    return static_cast<bool>(payload_ & 1);
  }

  // Obtain the boolean value as a string view. In particular, return either
  // `true`, `false`, `0` , or `1`, depending on whether the value was created
  // via `makeFromBool` or `makeBoolFromZeroOrOne` (see above).
  std::string_view getBoolLiteral() const noexcept {
    bool value = getBool();
    if (payload_ & 0b10) {
      return value ? "1" : "0";
    }
    return value ? "true" : "false";
  }

  /// Create a `ValueId` for an unsigned index of type
  /// `VocabIndex|TextRecordIndex|LocalVocabIndex`. These types can
  /// represent all values of `uint64_t`.
  static constexpr ValueId makeFromVocabIndex(VocabIndex index) noexcept {
    return {index.get(), Datatype::VocabIndex};
  }

  static constexpr ValueId makeFromEncodedVal(uint64_t idx) noexcept {
    return {idx, Datatype::EncodedVal};
  }

  static constexpr ValueId makeFromTextRecordIndex(
      TextRecordIndex index) noexcept {
    return {index.get(), Datatype::TextRecordIndex};
  }
  static ValueId makeFromLocalVocabIndex(LocalVocabIndex index) noexcept {
    // The pointer is stored verbatim in the payload.
    return {reinterpret_cast<T>(index), Datatype::LocalVocabIndex};
  }
  static constexpr ValueId makeFromWordVocabIndex(
      WordVocabIndex index) noexcept {
    return {index.get(), Datatype::WordVocabIndex};
  }
  static constexpr ValueId makeFromBlankNodeIndex(
      BlankNodeIndex index) noexcept {
    return {index.get(), Datatype::BlankNodeIndex};
  }

  /// Obtain the unsigned index that this `ValueId` encodes. If `getDatatype()
  /// != [VocabIndex|TextRecordIndex|LocalVocabIndex]` then the result is
  /// unspecified.
  [[nodiscard]] constexpr VocabIndex getVocabIndex() const noexcept {
    return VocabIndex::make(payload_);
  }

  [[nodiscard]] constexpr uint64_t getEncodedVal() const noexcept {
    return payload_;
  }

  [[nodiscard]] constexpr TextRecordIndex getTextRecordIndex() const noexcept {
    return TextRecordIndex::make(payload_);
  }
  [[nodiscard]] LocalVocabIndex getLocalVocabIndex() const noexcept {
    return reinterpret_cast<LocalVocabIndex>(payload_);
  }
  [[nodiscard]] constexpr WordVocabIndex getWordVocabIndex() const noexcept {
    return WordVocabIndex::make(payload_);
  }

  [[nodiscard]] constexpr BlankNodeIndex getBlankNodeIndex() const noexcept {
    return BlankNodeIndex::make(payload_);
  }

  // Store or load a `Date` object.
  static ValueId makeFromDate(DateYearOrDuration d) noexcept {
    return {absl::bit_cast<uint64_t>(d), Datatype::Date};
  }

  DateYearOrDuration getDate() const noexcept {
    return absl::bit_cast<DateYearOrDuration>(payload_);
  }

  /// Create a `ValueId` for a GeoPoint object (representing a POINT from WKT).
  static ValueId makeFromGeoPoint(GeoPoint p) {
    return {p.toBitRepresentation(), Datatype::GeoPoint};
  }

  /// Obtain a new `GeoPoint` object representing the pair of coordinates that
  /// this `ValueId` encodes. If `getDatatype() != GeoPoint` then the result
  /// is unspecified.
  GeoPoint getGeoPoint() const {
    return GeoPoint::fromBitRepresentation(payload_);
  }

  // An ID is considered trivial, if its datatype is trivial (see
  // `isDatatypeTrivial` above).
  constexpr bool isTrivial() const { return isDatatypeTrivial(getDatatype()); }

  // An `Id` is considered bitwise comparable if the mapping at
  // `isTypeBitwiseComparable_` says so. This is currently the case for all
  // datatypes except for the local vocab index.
  constexpr bool canBeComparedBitwise() const {
    static_assert(isOnlyLocalVocabNotBitwiseComparable);
    return isTypeBitwiseComparable_.at(static_cast<size_t>(getDatatype()));
  }

  // Constant to be used in `static_assert` statements to indicate that
  // behavior is relying on the local vocab entry to be the only datatype that
  // is not bitwise comparable.
  constexpr static bool isOnlyLocalVocabNotBitwiseComparable =
      isTypeBitwiseComparable_.size() ==
          static_cast<size_t>(Datatype::MaxValue) + 1 &&
      !isTypeBitwiseComparable_.at(
          static_cast<size_t>(Datatype::LocalVocabIndex));

  /// Return the smallest and largest possible `ValueId` wrt the underlying
  /// representation. Note: The datatype byte of `max()` is `0xFF`, which is
  /// not a valid `Datatype`. Such IDs must only be used as sentinel values
  /// for comparisons, never as actual values.
  constexpr static ValueId min() noexcept { return {0, 0}; }
  constexpr static ValueId max() noexcept {
    return {std::numeric_limits<T>::max(), uint8_t{0xFF}};
  }

  /// Enable hashing in abseil for `ValueId` (required by `ad_utility::HashSet`
  /// and `ad_utility::HashMap`
  template <typename H>
  friend H AbslHashValue(H h, const ValueId& id) {
    // Adding 0/1 to the hash is required to ensure that for two unequal
    // elements the hash expansions of neither is a suffix of the other.This is
    // a property that absl requires for hashes. The hash expansion is the list
    // of simpler values actually being hashed (here: bits or hash expansion of
    // the LocalVocabEntry).
    if (id.getDatatype() != Datatype::LocalVocabIndex) {
      static_assert(isOnlyLocalVocabNotBitwiseComparable);
      return H::combine(std::move(h), id.datatype_, id.payload_, 0);
    }
    auto [lower, upper] = id.getLocalVocabIndex()->positionInVocab();
    if (upper != lower) {
      return H::combine(std::move(h), lower.get(), 0);
    }
    return H::combine(std::move(h), *id.getLocalVocabIndex(), 1);
  }

  /// Enable the serialization of `ValueId` in the `ad_utility::serialization`
  /// framework by simply copying the 16 raw bytes (including the zeroed
  /// padding). This is much faster than serializing the datatype and payload
  /// separately, especially for large contiguous arrays of IDs.
  template <typename U>
  friend std::true_type allowTrivialSerialization(ValueId, U);

  /// Similar to `std::visit` for `std::variant`. First gets the datatype and
  /// then calls `visitor(getTYPE)` where `getTYPE` is the correct getter method
  /// for the datatype (e.g. `getDouble` for `Datatype::Double`). Visitor must
  /// be callable with all of the possible return types of the `getTYPE`
  /// functions.
  /// TODO<joka921> This currently still has limited functionality because
  /// VocabIndex, LocalVocabIndex, TextRecordIndex,  and EncodedVal are all of
  /// the same type `uint64_t` and the visitor cannot distinguish between them.
  /// Create strong types for these indices and make the `ValueId` class use
  /// them.
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
    }
    AD_FAIL();
  }

  /// This operator is only for debugging and testing. It returns a
  /// human-readable representation.
  friend std::ostream& operator<<(std::ostream& ostr, const ValueId& id) {
    ostr << toString(id.getDatatype())[0] << ':';
    if (id.getDatatype() == Datatype::Undefined) {
      return ostr << id.getPayloadBits();
    }

    auto visitor = [&ostr](auto&& value) {
      using T = decltype(value);
      if constexpr (ad_utility::isSimilar<T, UndefinedType>) {
        // already handled above
        AD_FAIL();
      } else if constexpr (ad_utility::SimilarToAny<T, double, int64_t,
                                                    uint64_t>) {
        ostr << std::to_string(value);
      } else if constexpr (ad_utility::isSimilar<T, bool>) {
        ostr << (value ? "true" : "false");
      } else if constexpr (ad_utility::isSimilar<T, DateYearOrDuration>) {
        ostr << value.toStringAndType().first;
      } else if constexpr (ad_utility::isSimilar<T, GeoPoint>) {
        ostr << value.toStringRepresentation();
      } else if constexpr (ad_utility::isSimilar<T, LocalVocabIndex>) {
        AD_CORRECTNESS_CHECK(value != nullptr);
        ostr << value->toStringRepresentation();
      } else {
        // T is `VocabIndex | TextRecordIndex`
        ostr << std::to_string(value.get());
      }
    };
    id.visit(visitor);
    return ostr;
  }

 private:
  // Compares a vocabulary index with a local vocabulary index range.
  static ql::strong_ordering compareVocabAndLocalVocab(
      LocalVocabEntry::IdProxy vocabIndex, ::LocalVocabIndex localVocabIndex) {
    auto [lowerBound, upperBound] = localVocabIndex->positionInVocab();
    if (vocabIndex < lowerBound) {
      return ql::strong_ordering::less;
    } else if (vocabIndex >= upperBound) {
      return ql::strong_ordering::greater;
    } else {
      return ql::strong_ordering::equal;
    }
  }

  // The datatype-major bitwise comparison: first compare the datatype byte,
  // then the payload bits as unsigned integers. This is only correct if
  // neither of the involved IDs has the `LocalVocabIndex` datatype.
  constexpr ql::strong_ordering compareBitwiseImpl(const ValueId& other) const {
    auto typeComparison = ql::compareThreeWay(datatype_, other.datatype_);
    return typeComparison != 0 ? typeComparison
                               : ql::compareThreeWay(payload_, other.payload_);
  }

  // Private constructors from the payload bits and the datatype (byte). They
  // are used in the implementation of the static factory methods
  // `makeFromDouble()`, `makeFromInt()` etc. Note: The explicit `padding_`
  // bytes are always initialized to zero, s.t. all valid `ValueId`s have a
  // deterministic byte representation.
  constexpr ValueId(T payload, uint8_t datatype)
      : payload_{payload}, datatype_{datatype}, padding_{} {}
  constexpr ValueId(T payload, Datatype datatype)
      : ValueId{payload, static_cast<uint8_t>(datatype)} {}
};

// The materialized `ValueId` must be trivially copyable (e.g. for the
// serialization) and 16 bytes in size.
static_assert(std::is_trivially_copyable_v<ValueId>);
static_assert(sizeof(ValueId) == 16);

#endif  // QLEVER_SRC_GLOBAL_VALUEID_H
