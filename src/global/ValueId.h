//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VALUEID_H
#define QLEVER_VALUEID_H

#include <cstdint>
#include <limits>

#include "../util/BitUtils.h"
#include "../util/NBitInteger.h"

/// The different Datatypes that a `ValueId` (see below) can encode.
enum struct Datatype {
  Undefined = 0,
  Int,
  Double,
  VocabIndex,
  LocalVocabIndex,
  TextIndex,
  // TODO<joka921> At least "date" is missing and not yet folded.
};

/// Convert the `Datatype` enum to the corresponding string
constexpr std::string_view toString(Datatype type) {
  switch (type) {
    case Datatype::Undefined:
      return "Undefined";
    case Datatype::Double:
      return "Double";
    case Datatype::Int:
      return "Int";
    case Datatype::VocabIndex:
      return "VocabIndex";
    case Datatype::LocalVocabIndex:
      return "LocalVocabIndex";
    case Datatype::TextIndex:
      return "TextIndex";
  }
  // This line is reachable if we cast an arbitrary invalid int to this enum
  throw std::runtime_error("should be unreachable");
}

/// Encode values of different types (the types from the `Datatype` enum above)
/// using 4 bits for the datatype and 60 bits for the value.
class ValueId {
 public:
  using T = uint64_t;
  static constexpr T numDatatypeBits = 4;
  static constexpr T numDataBits = 64 - numDatatypeBits;

  using IntegerType = ad_utility::NBitInteger<numDataBits>;

  /// The maximum value for the unsigned types that are used as indices
  /// (currently VocabIndex, LocalVocabIndex and Text).
  static constexpr T maxIndex = 1ull << (numDataBits - 1);

  /// The smallest double > 0 that will not be rounded to zero by the precision
  /// loss of `FoldedId`. Symmetrically, `-minPositiveDouble` is the largest
  /// double <0 that will not be rounded to zero.
  static constexpr double minPositiveDouble =
      std::bit_cast<double>(1ull << numDatatypeBits);

  /// This exception is thrown if we try to store a value of an index type
  /// (VocabIndex, LocalVocabIndex, TextIndex) that is larger than
  /// `maxIndex`.
  struct IndexTooLargeException : public std::exception {};

  /// A struct that represents the single undefined value. This is required for
  /// generic code like in the `visit` method.
  struct UndefinedType {};

 private:
  // The actual bits.
  T _bits;

 public:
  /// Default construction of an uninitialized id.
  ValueId() = default;

  /// Equality comparison is performed directly on the underlying
  /// representation.
  constexpr bool operator==(const ValueId&) const = default;

  /// Comparison is performed directly on the underlying representation. Note
  /// that because the type bits are the most significant bits, all values of
  /// the same `Datatype` will be adjacent to each other. Unsigned index types
  /// are also ordered correctly. Signed integers are ordered as follows: first
  /// the positive integers in order and then the negative integers in order.
  /// For doubles it is first the positive doubles in order, then the negative
  /// doubles in reversed order. This is a direct consequence of comparing the
  /// bit representation of these values as unsigned integers.
  /// TODO<joka921> Is this ordering also consistent for corner cases of doubles
  /// (inf, nan, denormalized numbers, negative zero)?
  constexpr auto operator<=>(const ValueId& other) const {
    return _bits <=> other._bits;
  }

  /// Get the datatype.
  [[nodiscard]] constexpr Datatype getDatatype() const noexcept {
    return static_cast<Datatype>(_bits >> numDataBits);
  }

  /// Create a `ValueId` of the `Undefined` type. There is only one such ID and
  /// it is guaranteed to be smaller than all IDs of other types. This helps
  /// implementing the correct join behavior in presence of undefined values.
  constexpr static ValueId makeUndefined() noexcept { return {0}; }

  /// Returns an object of `UndefinedType`. In many scenarios this function is
  /// unnecessary because `getDatatype() == Undefined` already identifies the
  /// single undefined value correctly, but it is very useful for generic code
  /// like the `visit` member function.
  [[nodiscard]] UndefinedType getUndefined() const noexcept { return {}; }

  /// Create a `ValueId` for a double value. The conversion will reduce the
  /// precision of the mantissa of an IEEE double precision floating point
  /// number from 53 to 49 significant bits.
  static ValueId makeFromDouble(double d) {
    auto shifted = std::bit_cast<T>(d) >> numDatatypeBits;
    return addDatatypeBits(shifted, Datatype::Double);
  }
  /// Obtain the `double` that this `ValueId` encodes. If `getDatatype() !=
  /// Double` then the result is unspecified.
  [[nodiscard]] double getDouble() const noexcept {
    return std::bit_cast<double>(_bits << numDatatypeBits);
  }

  /// Create a `ValueId` for a signed integer value. Integers in the range
  /// [-2^59, 2^59-1] can be represented. Integers outside of this range will
  /// overflow according to the semantics of `NBitInteger<60>`.
  static ValueId makeFromInt(int64_t i) noexcept {
    auto nbit = IntegerType::toNBit(i);
    return addDatatypeBits(nbit, Datatype::Int);
  }

  /// Obtain the signed integer that this `ValueId` encodes. If `getDatatype()
  /// != Int` then the result is unspecified.
  [[nodiscard]] int64_t getInt() const noexcept {
    return IntegerType::fromNBit(_bits);
  }

  /// Create a `ValueId` for an unsigned index of type
  /// `VocabIndex|TextIndex|LocalVocabIndex`. These types can
  /// represent values in the range [0, 2^60]. When `index` is outside of this
  /// range, and `IndexTooLargeException` is thrown.
  static ValueId makeFromVocabIndex(T index) {
    return makeFromIndex(index, Datatype::VocabIndex);
  }
  static ValueId makeFromTextIndex(T index) {
    return makeFromIndex(index, Datatype::TextIndex);
  }
  static ValueId makeFromLocalVocabIndex(T index) {
    return makeFromIndex(index, Datatype::LocalVocabIndex);
  }

  /// Obtain the unsigned index that this `ValueId` encodes. If `getDatatype()
  /// != [VocabIndex|TextIndex|LocalVocabIndex]` then the result is
  /// unspecified.
  [[nodiscard]] constexpr T getVocabIndex() const noexcept {
    return removeDatatypeBits(_bits);
  }
  [[nodiscard]] constexpr T getTextIndex() const noexcept {
    return removeDatatypeBits(_bits);
  }
  [[nodiscard]] constexpr T getLocalVocabIndex() const noexcept {
    return removeDatatypeBits(_bits);
  }

  // TODO<joka921> implement dates

  /// Enable hashing in abseil for `ValueId` (required by `ad_utility::HashSet`
  /// and `ad_utility::HashMap`
  template <typename H>
  friend H AbslHashValue(H h, const ValueId& id) {
    return H::combine(std::move(h), id._bits);
  }

  /// Enable the serialization of `ValueId` in the `ad_utility::serialization`
  /// framework.
  template <typename Serializer>
  friend void serialize(Serializer& serializer, ValueId& id) {
    serializer | id._bits;
  }

  /// Similar to `std::visit` for `std::variant`. First gets the datatype and
  /// then calls `visitor(getTYPE)` where `getTYPE` is the correct getter method
  /// for the datatype (e.g. `getDouble` for `Datatype::Double`). Visitor must
  /// be callable with all of the possible return types of the `getTYPE`
  /// functions.
  /// TODO<joka921> This currently still has limited functionality because
  /// VocabIndex, LocalVocabIndex and TextIndex are all of the same type
  /// `uint64_t` and the visitor cannot distinguish between them. Create strong
  /// types for these indices and make the `ValueId` class use them.
  template <typename Visitor>
  decltype(auto) visit(Visitor&& visitor) const {
    switch (getDatatype()) {
      case Datatype::Undefined:
        return std::invoke(visitor, getUndefined());
      case Datatype::Double:
        return std::invoke(visitor, getDouble());
      case Datatype::Int:
        return std::invoke(visitor, getInt());
      case Datatype::VocabIndex:
        return std::invoke(visitor, getVocabIndex());
      case Datatype::LocalVocabIndex:
        return std::invoke(visitor, getLocalVocabIndex());
      case Datatype::TextIndex:
        return std::invoke(visitor, getTextIndex());
    }
  }

  /// This operator is only for debugging and testing. It returns a
  /// human-readable representation.
  friend std::ostream& operator<<(std::ostream& ostr, const ValueId& id) {
    ostr << toString(id.getDatatype()) << ':';
    auto visitor = [&ostr]<typename T>(T&& value) {
      if constexpr (ad_utility::isSimilar<T, ValueId::UndefinedType>) {
        ostr << "Undefined";
      } else {
        ostr << std::to_string(value);
      }
    };
    id.visit(visitor);
    return ostr;
  }

 private:
  // Private constructor that implicitly converts from the underlying
  // representation. Used in the implementation of the static factory methods
  // `Double()`, `Int()` etc.
  constexpr ValueId(T bits) : _bits{bits} {}

  // Set the first 4 bits of `bits` to a 4-bit representation of `type`.
  // Requires that the first four bits of `bits` are all zero.
  static constexpr ValueId addDatatypeBits(T bits, Datatype type) {
    auto mask = static_cast<T>(type) << numDataBits;
    return {bits | mask};
  }

  // Set the datatype bits of `bits` to zero.
  static constexpr T removeDatatypeBits(T bits) noexcept {
    auto mask = ad_utility::bitMaskForLowerBits(numDataBits);
    return bits & mask;
  }

  // Helper function for the implementation of the unsigned index types.
  static constexpr ValueId makeFromIndex(T id, Datatype type) {
    if (id > maxIndex) {
      throw IndexTooLargeException();
    }
    return addDatatypeBits(id, type);
  }
};

#endif  // QLEVER_VALUEID_H
