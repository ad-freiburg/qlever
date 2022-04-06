//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_FOLDEDID_H
#define QLEVER_FOLDEDID_H

#include <cstdint>
#include <limits>

#include "../util/BitUtils.h"
#include "../util/NBitInteger.h"

/// The different Datatypes that a `FoldedId` (see below) can encode.
enum struct Datatype {
  Undefined = 0,
  Int,
  Double,
  Vocab,
  LocalVocab,
  Text,
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
    case Datatype::Vocab:
      return "Vocab";
    case Datatype::LocalVocab:
      return "LocalVocab";
    case Datatype::Text:
      return "Text";
  }
}

/// Encode values of different types (the types from the `Datatype` enum above)
/// using 4 bits for the datatype and 60 bits for the value.
class FoldedId {
 public:
  using T = uint64_t;
  static constexpr T numTypeBits = 4;
  static constexpr T numDataBits = 64 - numTypeBits;

  using IntegerType = ad_utility::NBitInteger<numDataBits>;

  /// The maximum value for the unsigned "index" types (currently Vocab,
  /// LocalVocab and Text).
  static constexpr T maxIndex = 1ull << (numDataBits - 1);

  /// This exception is thrown if we try to store a value of an index type
  /// (Vocab, LocalVocab, Text) that is larger than `maxIndex`.
  struct IndexTooLargeException : public std::exception {};

  /// A struct that represents the single undefined value. This is required for
  /// generic code like in the `visit` method.
  struct UndefinedT {};

 private:
  // The actual bits.
  T _bits;

 public:
  /// Default construction of an uninitialized id.
  FoldedId() {}

  /// Equality comparison is performed directly on the underlying representation
  constexpr bool operator==(const FoldedId&) const = default;

  /// Comparison is performed directly on the underlying representation. This
  /// means that all values of the same `Datatype` will be adjacent to each
  /// other. Unsigned index types are also ordered correctly.
  /// TODO<joka921> Should we give additional guarantees on the other types?
  constexpr auto operator<=>(const FoldedId& other) const {
    return _bits <=> other._bits;
  }

  /// Get the datatype.
  [[nodiscard]] constexpr Datatype getDatatype() const noexcept {
    return static_cast<Datatype>(_bits >> numDataBits);
  }

  /// Create a `FoldedId` of the `Undefined` type. There is only one such ID and
  /// it is guaranteed to be smaller than all Ids of other types.
  constexpr static FoldedId Undefined() noexcept { return {0}; }

  /// Returns an object of `UndefinedT`. In many scenarios this function is
  /// unnecessary because `getDatatype() == Undefined` already identifies the
  /// single undefined value correctly, but it is very useful for generic code
  /// like the `visit` member function.
  [[nodiscard]] UndefinedT getUndefined() const noexcept { return {}; }

  /// Create a `FoldedId` for a double value. The conversion will reduce the
  /// precision of the mantissa of an IEEE double precision floating point
  /// number from 53 to 49 significant bits.
  static FoldedId Double(double d) {
    auto shifted = std::bit_cast<T>(d) >> numTypeBits;
    return addMask(shifted, Datatype::Double);
  }
  /// Obtain the `double` that this `FoldedId` encodes. If `getDatatype() !=
  /// Double` then the result is unspecified.
  [[nodiscard]] double getDouble() const noexcept {
    return std::bit_cast<double>(_bits << numTypeBits);
  }

  /// Create a `FoldedId` for a signed integer value. Integers in the range
  /// [-2^59, 2^59-1] can be represented. Integers outside of this range will
  /// overflow according to the semantics of `NBitInteger<60>`.
  static FoldedId Int(int64_t i) noexcept {
    auto nbit = IntegerType::toNBit(i);
    return addMask(nbit, Datatype::Int);
  }

  /// Obtain the signed integer that this `FoldedId` encodes. If `getDatatype()
  /// != Int` then the result is unspecified.
  [[nodiscard]] int64_t getInt() const noexcept {
    return IntegerType::fromNBit(_bits);
  }

  /// Create a `FoldedId` for an unsigned index of type `Vocab|Text|LocalVocab`.
  /// These types can represent values in the range [0, 2^60]. When `index` is
  /// outside of this range, and `IndexTooLargeException` is thrown.
  static FoldedId Vocab(T index) {
    return makeUnsigned(index, Datatype::Vocab);
  }
  static FoldedId Text(T index) { return makeUnsigned(index, Datatype::Text); }
  static FoldedId LocalVocab(T index) {
    return makeUnsigned(index, Datatype::LocalVocab);
  }

  /// Obtain the unsigned index that this `FoldedId` encodes. If `getDatatype()
  /// != [Vocab|Text|LocalVocab]` then the result is unspecified.
  [[nodiscard]] constexpr T getVocab() const noexcept {
    return removeMask(_bits);
  }
  [[nodiscard]] constexpr T getText() const noexcept {
    return removeMask(_bits);
  }
  [[nodiscard]] constexpr T getLocalVocab() const noexcept {
    return removeMask(_bits);
  }

  // TODO<joka921> implement dates

  /// Enable hashing in abseil for `FoldedId` (required by `ad_utility::HashSet`
  /// and `ad_utility::HashMap`
  template <typename H>
  friend H AbslHashValue(H h, const FoldedId& id) {
    return H::combine(std::move(h), id._bits);
  }

  /// Enable the serialization of `FoldedId` in the `ad_utility::serialization`
  /// framework.
  template <typename Serializer>
  friend void serialize(Serializer& serializer, FoldedId& id) {
    serializer | id._bits;
  }

  /// Similar to `std::visit` for `std::variant`. First gets the datatype and
  /// then calls `visitor(getTYPE)` where `getTYPE` is the correct getter method
  /// for the datatype (e.g. `getDouble` for `Datatype::Double`). Visitor must
  /// be callable with all of the possible return types of the `getTYPE`
  /// functions.
  template <typename Visitor>
  decltype(auto) visit(Visitor&& visitor) const {
    switch (getDatatype()) {
      case Datatype::Undefined:
        return std::invoke(visitor, getUndefined());
      case Datatype::Double:
        return std::invoke(visitor, getDouble());
      case Datatype::Int:
        return std::invoke(visitor, getInt());
      case Datatype::Vocab:
        return std::invoke(visitor, getVocab());
      case Datatype::LocalVocab:
        return std::invoke(visitor, getLocalVocab());
      case Datatype::Text:
        return std::invoke(visitor, getText());
    }
  }

  /// This operator is only for debugging and testing. It returns a
  /// human-readable representation.
  friend std::ostream& operator<<(std::ostream& ostr, const FoldedId& id) {
    ostr << toString(id.getDatatype()) << ':';
    auto visitor = [&ostr]<typename T>(T&& value) {
      if constexpr (ad_utility::isSimilar<T, FoldedId::UndefinedT>) {
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
  constexpr FoldedId(T bits) : _bits{bits} {}

  // Set the first 4 bits of `bits` to a 4-bit representation of `type`.
  // Requires that the first four bits of `bits` are all zero.
  static constexpr T addMask(T bits, Datatype type) {
    auto mask = static_cast<T>(type) << numDataBits;
    return bits | mask;
  }

  // Set the first 4 bits of `bits` to zero.
  static constexpr T removeMask(T bits) noexcept {
    auto mask = ad_utility::bitMaskForLowerBits(numDataBits);
    return bits & mask;
  }

  // Helper function for the implementation of the unsigned index types
  static constexpr FoldedId makeUnsigned(T id, Datatype type) {
    if (id > maxIndex) {
      throw IndexTooLargeException();
    }
    return addMask(id, type);
  }
};

#endif  // QLEVER_FOLDEDID_H
