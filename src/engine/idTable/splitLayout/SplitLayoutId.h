// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTID_H
#define QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTID_H
#include <boost/math/tools/numeric_limits.hpp>

#include "SplitLayoutIdBitRepresentation.h"
#include "global/Datatype.h"
#include "global/IndexTypes.h"
#include "rdfTypes/GeoPoint.h"
#include "util/DateYearDuration.h"
#include "util/NBitInteger.h"

namespace columnBasedIdTable::splitLayout {

class SplitLayoutId {
 public:
  using T = SplitLayoutIdBitRepresentation;
  using PayloadType = T::PayloadType;
  using DatatypeType = T::DatatypeType;

  static constexpr DatatypeType numDatatypeBits = 8;
  static constexpr PayloadType numDataBits = 64;
  // The number of bits of a vocabulary index, including the markers in its
  // topmost bits (see `SplitVocabulary` and `GeoCellGrid`). Not the whole
  // payload, so that `SegmentLocalId` can store the index of the
  // sub-vocabulary in the remaining 4 bits of a single 64-bit word.
  static constexpr PayloadType numVocabIndexBits = 60;
  static_assert(numVocabIndexBits <= numDataBits);

  using IntegerType = ad_utility::NBitInteger<numDataBits>;

  // The maximum value for the unsigned types that are used as indices
  // (currently VocabIndex, LocalVocabIndex and Text). Since the payload now
  // consists of a full 64-bit word, all values of `PayloadType` are valid
  // indices.
  static constexpr PayloadType maxIndex =
      std::numeric_limits<PayloadType>::max();

  // The smallest double > 0 that will not be rounded to zero. As the payload
  // stores all bits of a `double`, this is the smallest subnormal `double`.
  // Symmetrically, `-minPositiveDouble` is the largest double < 0 that will not
  // be rounded to zero.
  // Note: This constant is currently only used in unit tests.
  static constexpr double minPositiveDouble =
      std::numeric_limits<double>::denorm_min();

  // The largest representable integer value.
  static constexpr int64_t maxInt = IntegerType::max();
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

  // The datatypes that the position of a word in the vocabularies of an index
  // can have, see `LocalVocabEntry::positionInVocab()`. The comparison of an
  // `Id` of type `LocalVocabIndex` with an `Id` of one of these types requires
  // that position (which may be expensive to compute); for all other datatypes
  // the comparison of the bits already yields the correct result, see
  // `compareThreeWay` below.
  static constexpr std::array<Datatype, 3> datatypesOfPositionInVocab_{
      Datatype::VocabIndex, Datatype::SecondaryVocabIndex,
      Datatype::EncodedVal};

  // A mapping that decides if a Datatype is bitwise comparable or not. See
  // `canBeComparedBitwise()` below.
  static constexpr std::array<bool, 13> isTypeBitwiseComparable_{
      true, true, true, true, true, false, true,
      true, true, true, true, true, true};

  // Assert that the types in `stringTypes_` are directly adjacent. This is
  // required to make the comparison of IDs in `ValueIdComparators.h` work.
  static constexpr Datatype maxStringType_ = ql::ranges::max(stringTypes_);
  static constexpr Datatype minStringType_ = ql::ranges::min(stringTypes_);
  static_assert(static_cast<size_t>(maxStringType_) -
                    static_cast<size_t>(minStringType_) + 1 ==
                stringTypes_.size());

  // Assert that `VocabIndex`, `LocalVocabIndex`, and `SecondaryVocabIndex` are
  // directly adjacent, in this order. The comparison of an `Id` of type
  // `LocalVocabIndex` with an `Id` of an unrelated datatype relies on this, see
  // `compareThreeWay` below.
  static_assert(static_cast<size_t>(Datatype::LocalVocabIndex) ==
                static_cast<size_t>(Datatype::VocabIndex) + 1);
  static_assert(static_cast<size_t>(Datatype::SecondaryVocabIndex) ==
                static_cast<size_t>(Datatype::LocalVocabIndex) + 1);

  // Assert that an encoded `GeoPoint` fits into the payload of a `ValueId`.
  static_assert(numDataBits >= GeoPoint::numDataBits);

  // A struct that represents the single undefined value. This is required for
  // generic code like in the `visit` method.
  struct UndefinedType {};

 private:
  // The actual data: a full 64-bit word of payload and a single byte for the
  // datatype. Together with the explicitly stored `padding_` an `Id` occupies
  // 16 bytes.
  PayloadType payload_;
  DatatypeType datatype_;

  // The `datatype_` must be able to hold all `numDatatypeBits` bits.
  static_assert(numDatatypeBits <= 8 * sizeof(DatatypeType));

  // The number of bytes of the two data members above, and the number of bytes
  // that fill an `Id` up to a multiple of the alignment of the payload.
  static constexpr size_t numDataBytes =
      sizeof(PayloadType) + sizeof(DatatypeType);
  // NOTE: Plain arithmetic on purpose: CLion  cannot evaluate a function
  // call (e.g. `ad_utility::alignUp`) in the size of `padding_` and marks
  // every `static_assert` on an `Id` as failed.
  static constexpr size_t numPaddingBytes =
      (alignof(PayloadType) - numDataBytes % alignof(PayloadType)) %
      alignof(PayloadType);

  // Explicit padding bytes. They are always set to zero by all factory
  // functions, s.t. all valid `ValueId`s have a deterministic byte
  // representation (e.g. for hashing raw bytes or writing them to disk).
  // Note: Default-constructed `Id`s are deliberately uninitialized (see
  // the default constructor below).
  std::array<uint8_t, numPaddingBytes> padding_;

 public:
  SplitLayoutId() = default;

  // Get the underlying bit representation, e.g. for compression etc.
  [[nodiscard]] constexpr SplitLayoutIdBitRepresentation getBits()
      const noexcept {
    return {datatype_, payload_};
  }

  [[nodiscard]] constexpr Datatype getDatatype() const noexcept {
    return static_cast<Datatype>(datatype_);
  }

  [[nodiscard]] constexpr DatatypeType getDatatypeBits() const noexcept {
    return datatype_;
  }

  // Get only the payload bits (without the datatype byte).
  [[nodiscard]] constexpr PayloadType getPayloadBits() const noexcept {
    return payload_;
  }

  // Construct from the underlying bit representation. `bits` must have been
  // obtained by a call to `getBits()` on a valid `SplitLayoutId`.
  static constexpr SplitLayoutId fromBits(
      SplitLayoutIdBitRepresentation bits) noexcept {
    return {bits.payload_, bits.datatype_};
  }

  static constexpr SplitLayoutId fromBits(const DatatypeType datatype,
                                          const PayloadType payload) noexcept {
    AD_EXPENSIVE_CHECK(
        datatype <= static_cast<DatatypeType>(Datatype::MaxValue),
        "The datatype does not exist");
    return {payload, datatype};
  }

  // The same with the `Datatype` instead of its bits.
  static constexpr SplitLayoutId fromBits(const Datatype datatype,
                                          const PayloadType payload) noexcept {
    return fromBits(static_cast<DatatypeType>(datatype), payload);
  }

  // Create an `Id` of type `Undefined` with the given `payload`. Unlike the
  // `fromBits` of the legacy `ValueId`, the argument contains no datatype bits.
  // This is used for the special `Id`s, which are not `UNDEF` (see
  // `SpecialIds.h`).
  static constexpr SplitLayoutId fromBits(uint64_t payload) noexcept {
    return {payload, Datatype::Undefined};
  }

  // Create a `SplitLayoutId` of the `Undefined` type. There is only one such ID
  // and it is guaranteed to be smaller than all IDs of other types. This helps
  // implementing the correct join behavior in presence of undefined values.
  constexpr static SplitLayoutId makeUndefined() noexcept {
    return {0, Datatype::Undefined};
  }

  // Returns an object of `UndefinedType`. In many scenarios this function is
  // unnecessary because `getDatatype() == Undefined` already identifies the
  // single undefined value correctly, but it is very useful for generic code
  // like the `visit` member function.
  [[nodiscard]] constexpr UndefinedType getUndefined() const noexcept {
    return {};
  }

  [[nodiscard]] QL_CONSTEXPR bool isUndefined() const noexcept {
    return *this == makeUndefined();
  }

  // Create a `SplitLayoutId` for a double value. The full precision of the IEEE
  // double precision floating point number is preserved.
  // NOTE: This function and `getDouble` below are only `constexpr` in C++20
  // mode, where `absl::bit_cast` is `std::bit_cast`. In C++17 mode it falls
  // back to a `memcpy`.
  static QL_CONSTEXPR SplitLayoutId makeFromDouble(double d) {
    return {absl::bit_cast<PayloadType>(d), Datatype::Double};
  }

  // Obtain the `double` that this `SplitLayoutId` encodes. If `getDatatype() !=
  // Double` then the result is unspecified.
  [[nodiscard]] QL_CONSTEXPR double getDouble() const noexcept {
    return absl::bit_cast<double>(payload_);
  }

  // Create a `SplitLayoutId` for a signed integer value. The full range of
  // `int64_t` can be represented.
  static constexpr SplitLayoutId makeFromInt(int64_t i) noexcept {
    return {IntegerType::toNBit(i), Datatype::Int};
  }

  // Obtain the signed integer that this `SplitLayoutId` encodes. If
  // `getDatatype()
  // != Int` then the result is unspecified.
  [[nodiscard]] constexpr int64_t getInt() const noexcept {
    return IntegerType::fromNBit(payload_);
  }

  // Create a `SplitLayoutId` for a boolean value.
  static constexpr SplitLayoutId makeFromBool(bool b) noexcept {
    return {static_cast<PayloadType>(b), Datatype::Bool};
  }

  // Create a `ValueId` for a boolean value, represented as "0" or "1" instead
  // of "false" or "true".
  static constexpr SplitLayoutId makeBoolFromZeroOrOne(bool b) noexcept {
    auto bits = static_cast<PayloadType>(b);
    bits |= static_cast<PayloadType>(true) << 1;
    return {bits, Datatype::Bool};
  }

  // Obtain the boolean value.
  [[nodiscard]] constexpr bool getBool() const noexcept {
    return static_cast<bool>(payload_ & 1);
  }

  // Obtain the boolean value as a string view. In particular, return either
  // `true`, `false`, `0` , or `1`, depending on whether the value was created
  // via `makeFromBool` or `makeBoolFromZeroOrOne` (see above).
  [[nodiscard]] constexpr std::string_view getBoolLiteral() const noexcept {
    if (payload_ & 0b10) {
      return getBool() ? "1" : "0";
    }
    return getBool() ? "true" : "false";
  }

  // Create a `SplitLayoutId` for an unsigned index of type
  // `VocabIndex|TextRecordIndex|LocalVocabIndex`. These types can
  // represent all values of `uint64_t`.
  static constexpr SplitLayoutId makeFromVocabIndex(VocabIndex index) noexcept {
    return {index.get(), Datatype::VocabIndex};
  }

  static constexpr SplitLayoutId makeFromEncodedVal(PayloadType idx) noexcept {
    return {idx, Datatype::EncodedVal};
  }

  static constexpr SplitLayoutId makeFromTextRecordIndex(
      TextRecordIndex index) noexcept {
    return {index.get(), Datatype::TextRecordIndex};
  }

  static SplitLayoutId makeFromLocalVocabIndex(LocalVocabIndex index) noexcept {
    // The pointer is stored verbatim in the payload.
    return {reinterpret_cast<PayloadType>(index), Datatype::LocalVocabIndex};
  }

  static constexpr SplitLayoutId makeFromWordVocabIndex(
      WordVocabIndex index) noexcept {
    return {index.get(), Datatype::WordVocabIndex};
  }

  static constexpr SplitLayoutId makeFromBlankNodeIndex(
      BlankNodeIndex index) noexcept {
    return {index.get(), Datatype::BlankNodeIndex};
  }

  static constexpr SplitLayoutId makeFromSecondaryVocabIndex(
      SecondaryVocabIndex index) noexcept {
    return {index.get(), Datatype::SecondaryVocabIndex};
  }

  // Obtain the unsigned index that this `SplitLayoutId` encodes. If
  // `getDatatype()` is not the index type that the respective getter below is
  // named after, then the result is unspecified.
  [[nodiscard]] constexpr VocabIndex getVocabIndex() const noexcept {
    return VocabIndex::make(payload_);
  }

  [[nodiscard]] constexpr PayloadType getEncodedVal() const noexcept {
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

  [[nodiscard]] constexpr SecondaryVocabIndex getSecondaryVocabIndex()
      const noexcept {
    return SecondaryVocabIndex::make(payload_);
  }

  // Store or load a `Date` object.
  // NOTE: These functions cannot be `constexpr`, because
  // `DateYearOrDuration` is not a literal type.
  static SplitLayoutId makeFromDate(DateYearOrDuration d) noexcept {
    return {absl::bit_cast<PayloadType>(d), Datatype::Date};
  }

  DateYearOrDuration getDate() const noexcept {
    return absl::bit_cast<DateYearOrDuration>(payload_);
  }

  // TODO<joka921> implement dates

  // Create a `SplitLayoutId` for a GeoPoint object (representing a POINT from
  // WKT).
  static SplitLayoutId makeFromGeoPoint(const GeoPoint p) {
    return {p.toBitRepresentation(), Datatype::GeoPoint};
  }

  // Obtain a new `GeoPoint` object representing the pair of coordinates that
  // this `SplitLayoutId` encodes. If `getDatatype() != GeoPoint` then the
  // result is unspecified.
  GeoPoint getGeoPoint() const {
    return GeoPoint::fromBitRepresentation(payload_);
  }

  // An ID is considered trivial, if its datatype is trivial (see
  // `isDatatypeTrivial` above).
  constexpr bool isTrivial() const { return isDatatypeTrivial(getDatatype()); }

  // Return true iff `datatype` is one of the datatypes that the position of a
  // word in the vocabularies can have, see `datatypesOfPositionInVocab_`.
  constexpr static bool isDatatypeOfPositionInVocab(Datatype datatype) {
    return ad_utility::contains(datatypesOfPositionInVocab_, datatype);
  }

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

  // Return the smallest and largest possible `ValueId` wrt the underlying
  // representation. Note: The datatype byte of `max()` is `0xFF`, which is
  // not a valid `Datatype`. Such IDs must only be used as sentinel values
  // for comparisons, never as actual values.
  constexpr static SplitLayoutId min() noexcept {
    return {std::numeric_limits<PayloadType>::min(), DatatypeType{0}};
  }

  constexpr static SplitLayoutId max() noexcept {
    return {std::numeric_limits<PayloadType>::max(), DatatypeType{0xFF}};
  }

  // Enable hashing in abseil for `SplitLayoutId` (required by
  // `ad_utility::HashSet` and `ad_utility::HashMap`
  template <typename H>
  friend H AbslHashValue(H h, const SplitLayoutId& id) {
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

  // Enable the serialization of `SplitLayoutId` in the
  // `ad_utility::serialization` framework by simply copying the 16 raw bytes
  // (including the zeroed padding). This is much faster than serializing the
  // datatype and payload separately, especially for large contiguous arrays of
  // IDs.
  // TODO<pas-kes>: Is this wasteful to persist, with 7 unused bytes?
  template <typename U>
  friend std::true_type allowTrivialSerialization(SplitLayoutId, U);

  // Similar to `std::visit` for `std::variant`. First gets the datatype and
  // then calls `visitor(getTYPE)` where `getTYPE` is the correct getter method
  // for the datatype (e.g. `getDouble` for `Datatype::Double`). Visitor must
  // be callable with all of the possible return types of the `getTYPE`
  // functions.
  // TODO<joka921> This currently still has limited functionality because
  // VocabIndex, LocalVocabIndex, TextRecordIndex,  and EncodedVal are all of
  // the same type `uint64_t` and the visitor cannot distinguish between them.
  // Create strong types for these indices and make the `ValueId` class use
  // them.
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
    }
    AD_FAIL();
  }

  // This operator is only for debugging and testing. It returns a
  // human-readable representation.
  friend std::ostream& operator<<(std::ostream& ostr, const SplitLayoutId& id) {
    ostr << toString(id.getDatatype())[0] << ':';
    if (id.getDatatype() == Datatype::Undefined) {
      return ostr << id.getPayloadBits();
    }

    auto visitor = [&ostr](auto&& value) {
      using ValueType = decltype(value);
      if constexpr (ad_utility::isSimilar<ValueType, UndefinedType>) {
        // already handled above
        AD_FAIL();
      } else if constexpr (ad_utility::SimilarToAny<ValueType, double, int64_t,
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

  // Comparison is performed datatype-major, i.e. first on the datatype byte
  // and then on the payload bits (as unsigned integers). All values of the
  // same `Datatype` are therefore adjacent to each other. Unsigned index
  // types are also ordered correctly. Signed integers are ordered as follows:
  // first the positive integers in order and then the negative integers in
  // order. For doubles it is first the positive doubles in order, then the
  // negative doubles in reversed order. This is a direct consequence of
  // comparing the bit representation of these values as unsigned integers.
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
  // like it compares to the datatype bits of the position. NOTE: The single
  // exception is a word that is stored as an encoded IRI (see
  // `LocalVocabContext::encodeAsId`), whose position is of type `EncodedVal`,
  // which is *not* adjacent to `LocalVocabIndex`. Comparing such a word to an
  // `Id` of an unrelated datatype hence deviates from comparing the positions.
  // This is a pre-existing inconsistency of the encoded IRIs that is tracked
  // separately, see the note at `valueIdComparators::detail::compareIdsImpl`.
  // NOTE: This is only `constexpr` in C++20 mode. GCC 8, which the C++17 CI job
  // uses, rejects the branches below in a relaxed-constexpr function
  // ("expression `<statement>` is not a constant expression"), and nothing
  // compares `Id`s at compile time anyway.
  [[nodiscard]] QL_CONSTEXPR auto compareThreeWay(
      const SplitLayoutId& other) const {
    using enum Datatype;
    const auto type = getDatatype();
    const auto otherType = other.getDatatype();
    if (type != LocalVocabIndex && otherType != LocalVocabIndex) {
      return compareBitwiseImpl(other);
    }
    if (type == LocalVocabIndex && otherType == LocalVocabIndex) [[unlikely]] {
      return ql::compareThreeWay(*getLocalVocabIndex(),
                                 *other.getLocalVocabIndex());
    }

    // Exactly one of the types is `LocalVocabIndex`. If the other one is a type
    // that a position in the vocabularies can have, then compare that position,
    // else fall through to the comparison of the bits, see the note above.
    // NOTE: GCC 11 issues a false positive warning here, so we try to avoid it
    // by being over-explicit about the branches here.
    if (isDatatypeOfPositionInVocab(type) && otherType == LocalVocabIndex) {
      return comparePositionInVocabToLocalVocab(
          LocalVocabEntry::IdProxy::make(getBits()),
          other.getLocalVocabIndex());
    }

    if (type == LocalVocabIndex && isDatatypeOfPositionInVocab(otherType)) {
      auto inverseOrder = comparePositionInVocabToLocalVocab(
          LocalVocabEntry::IdProxy::make(other.getBits()),
          getLocalVocabIndex());

      return ql::compareThreeWay(0, inverseOrder);
    }

    // One of the types is `LocalVocab`, and the other one is a non-string
    // type like `Integer` or `Undefined. Then the comparison by bits
    // automatically compares by the datatype.
    return compareBitwiseImpl(other);
  }

  // NOTE: All the comparison operators are `QL_CONSTEXPR` and not `constexpr`,
  // because they call `compareThreeWay` above, see the note there.
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_QL_CONSTEXPR(SplitLayoutId)

  friend QL_CONSTEXPR bool operator==(const SplitLayoutId& left,
                                      const SplitLayoutId& right) {
    return ql::compareThreeWay(left, right) == 0;
  }

  friend QL_CONSTEXPR bool operator==(const SplitLayoutId& left,
                                      const uint64_t& right) {
    return left.datatype_ == right;
  }

  friend QL_CONSTEXPR bool operator!=(const SplitLayoutId& left,
                                      const SplitLayoutId& right) {
    return !(left == right);
  }

  // When there are no local vocab entries, then comparison can only be done
  // on the underlying bits, which allows much better code generation (e.g.
  // vectorization). In particular, this method should for example be used
  // during index building.
  // NOTE: This is only `constexpr` in C++20 mode, for the same reason as
  // `compareThreeWay` above.
  QL_CONSTEXPR auto compareWithoutLocalVocab(const SplitLayoutId& other) const {
    // NOTE: If this static assertion is violated at some point, make sure to
    // check all callers of this function if they are still correct.
    static_assert(isOnlyLocalVocabNotBitwiseComparable);
    AD_EXPENSIVE_CHECK(canBeComparedBitwise());
    AD_EXPENSIVE_CHECK(other.canBeComparedBitwise());
    return compareBitwiseImpl(other);
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

  static constexpr auto fromBitsL = [](const auto& idBitRep) {
    return fromBits(idBitRep);
  };

  static constexpr auto getBitsL = [](const auto& id) { return id.getBits(); };

  static constexpr auto getDatatypeL = [](const auto& id) {
    return id.getDatatype();
  };

 private:
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

  // The datatype-major bitwise comparison: first compare the datatype byte,
  // then the payload bits as unsigned integers. This is only correct if
  // neither of the involved IDs has the `LocalVocabIndex` datatype.
  // NOTE: `QL_CONSTEXPR`, not `constexpr`, for consistency with its two
  // callers above (`compareThreeWay`/`compareWithoutLocalVocab`), which are
  // only `constexpr` in C++20 mode.
  [[nodiscard]] QL_CONSTEXPR ql::strong_ordering compareBitwiseImpl(
      const SplitLayoutId& other) const {
    const auto typeComparison = ql::compareThreeWay(datatype_, other.datatype_);
    return typeComparison != 0 ? typeComparison
                               : ql::compareThreeWay(payload_, other.payload_);
  }

  // Private constructors from the payload bits and the datatype (byte). They
  // are used in the implementation of the static factory methods
  // `makeFromDouble()`, `makeFromInt()` etc. Note: The explicit `padding_`
  // bytes are always initialized to zero, s.t. all valid `ValueId`s have a
  // deterministic byte representation.
  constexpr SplitLayoutId(PayloadType payload, DatatypeType datatype)
      : payload_{payload}, datatype_{datatype}, padding_{} {}

  constexpr SplitLayoutId(PayloadType payload, Datatype datatype)
      : SplitLayoutId{payload, static_cast<DatatypeType>(datatype)} {}
};
}  // namespace columnBasedIdTable::splitLayout

// There is no padding left that the compiler adds implicitly (and leaves
// uninitialized). If this fails, `numPaddingBytes` is wrong, e.g. because it is
// zero (an `std::array` of size 0 is not empty).
static_assert(std::has_unique_object_representations_v<
              columnBasedIdTable::splitLayout::SplitLayoutId>);

#endif  // QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_SPLITLAYOUTID_H
