//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VALUEID_H
#define QLEVER_VALUEID_H

#include <absl/strings/str_cat.h>

#include <bit>
#include <cstdint>
#include <functional>
#include <limits>

#include "global/IndexTypes.h"
#include "util/BitUtils.h"
#include "util/DateYearDuration.h"
#include "util/NBitInteger.h"
#include "util/Serializer/Serializer.h"
#include "util/SourceLocation.h"

/// The different Datatypes that a `ValueId` (see below) can encode.
enum struct Datatype {
  Undefined = 0,
  Bool,
  Int,
  Double,
  VocabIndex,
  LocalVocabIndex,
  TextRecordIndex,
  Date,
  WordVocabIndex,
  BlankNodeIndex,
  MaxValue = BlankNodeIndex
  // Note: Unfortunately we cannot easily get the size of an enum.
  // If members are added to this enum, then the `MaxValue`
  // alias must always be equal to the last member,
  // else other code breaks with out-of-bounds accesses.
};

/// Convert the `Datatype` enum to the corresponding string
constexpr std::string_view toString(Datatype type) {
  switch (type) {
    case Datatype::Undefined:
      return "Undefined";
    case Datatype::Bool:
      return "Bool";
    case Datatype::Double:
      return "Double";
    case Datatype::Int:
      return "Int";
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
    case Datatype::BlankNodeIndex:
      return "BlankNodeIndex";
  }
  // This line is reachable if we cast an arbitrary invalid int to this enum
  AD_FAIL();
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

  // The largest representable integer value.
  static constexpr int64_t maxInt = IntegerType::max();

  /// This exception is thrown if we try to store a value of an index type
  /// (VocabIndex, LocalVocabIndex, TextRecordIndex) that is larger than
  /// `maxIndex`.
  struct IndexTooLargeException : public std::exception {
   private:
    std::string errorMessage_;

   public:
    IndexTooLargeException(T tooBigValue,
                           ad_utility::source_location s =
                               ad_utility::source_location::current()) {
      errorMessage_ = absl::StrCat(
          s.file_name(), ", line ", s.line(), ": The given value ", tooBigValue,
          " is bigger than what the maxIndex of ValueId allows.");
    }

    const char* what() const noexcept override { return errorMessage_.c_str(); }
  };

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
  // NOTE: (Also for the operator<=> below: These comparisons only work
  // correctly if we only store entries in the local vocab that are NOT part of
  // the vocabulary. This is currently not true for expression results from
  // GROUP BY and BIND operations (for performance reasons). So a join with such
  // results will currently lead to wrong results.
  constexpr bool operator==(const ValueId& other) const {
    if (getDatatype() == Datatype::LocalVocabIndex &&
        other.getDatatype() == Datatype::LocalVocabIndex) [[unlikely]] {
      return *getLocalVocabIndex() == *other.getLocalVocabIndex();
    }
    return _bits == other._bits;
  }

  /// Comparison is performed directly on the underlying representation. Note
  /// that because the type bits are the most significant bits, all values of
  /// the same `Datatype` will be adjacent to each other. Unsigned index types
  /// are also ordered correctly. Signed integers are ordered as follows: first
  /// the positive integers in order and then the negative integers in order.
  /// For doubles it is first the positive doubles in order, then the negative
  /// doubles in reversed order. This is a direct consequence of comparing the
  /// bit representation of these values as unsigned integers.
  constexpr auto operator<=>(const ValueId& other) const {
    if (getDatatype() == Datatype::LocalVocabIndex &&
        other.getDatatype() == Datatype::LocalVocabIndex) [[unlikely]] {
      return *getLocalVocabIndex() <=> *other.getLocalVocabIndex();
    }
    return _bits <=> other._bits;
  }

  /// Get the underlying bit representation, e.g. for compression etc.
  [[nodiscard]] constexpr T getBits() const noexcept { return _bits; }
  /// Construct from the underlying bit representation. `bits` must have been
  /// obtained by a call to `getBits()` on a valid `ValueId`.
  static constexpr ValueId fromBits(T bits) noexcept { return {bits}; }

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
  bool isUndefined() const noexcept { return *this == makeUndefined(); }

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

  /// Create a `ValueId` for a boolean value.
  static constexpr ValueId makeFromBool(bool b) noexcept {
    auto bits = static_cast<T>(b);
    return addDatatypeBits(bits, Datatype::Bool);
  }

  // Obtain the boolean value.
  [[nodiscard]] bool getBool() const noexcept {
    return static_cast<bool>(removeDatatypeBits(_bits));
  }

  /// Create a `ValueId` for an unsigned index of type
  /// `VocabIndex|TextRecordIndex|LocalVocabIndex`. These types can
  /// represent values in the range [0, 2^60]. When `index` is outside of this
  /// range, and `IndexTooLargeException` is thrown.
  static ValueId makeFromVocabIndex(VocabIndex index) {
    return makeFromIndex(index.get(), Datatype::VocabIndex);
  }
  static ValueId makeFromTextRecordIndex(TextRecordIndex index) {
    return makeFromIndex(index.get(), Datatype::TextRecordIndex);
  }
  static ValueId makeFromLocalVocabIndex(LocalVocabIndex index) {
    // The last `numDatatypeBits` of a `LocalVocabIndex` are always zero, so we
    // can reuse them for the datatype.
    static_assert(alignof(decltype(*index)) >= (1u << numDatatypeBits));
    return makeFromIndex(reinterpret_cast<T>(index) >> numDatatypeBits,
                         Datatype::LocalVocabIndex);
  }
  static ValueId makeFromWordVocabIndex(WordVocabIndex index) {
    return makeFromIndex(index.get(), Datatype::WordVocabIndex);
  }
  static ValueId makeFromBlankNodeIndex(BlankNodeIndex index) {
    return makeFromIndex(index.get(), Datatype::BlankNodeIndex);
  }

  /// Obtain the unsigned index that this `ValueId` encodes. If `getDatatype()
  /// != [VocabIndex|TextRecordIndex|LocalVocabIndex]` then the result is
  /// unspecified.
  [[nodiscard]] constexpr VocabIndex getVocabIndex() const noexcept {
    return VocabIndex::make(removeDatatypeBits(_bits));
  }
  [[nodiscard]] constexpr TextRecordIndex getTextRecordIndex() const noexcept {
    return TextRecordIndex::make(removeDatatypeBits(_bits));
  }
  [[nodiscard]] LocalVocabIndex getLocalVocabIndex() const noexcept {
    return reinterpret_cast<LocalVocabIndex>(_bits << numDatatypeBits);
  }
  [[nodiscard]] constexpr WordVocabIndex getWordVocabIndex() const noexcept {
    return WordVocabIndex::make(removeDatatypeBits(_bits));
  }

  [[nodiscard]] constexpr BlankNodeIndex getBlankNodeIndex() const noexcept {
    return BlankNodeIndex::make(removeDatatypeBits(_bits));
  }

  // Store or load a `Date` object.
  static ValueId makeFromDate(DateYearOrDuration d) noexcept {
    return addDatatypeBits(std::bit_cast<uint64_t>(d), Datatype::Date);
  }

  DateYearOrDuration getDate() const noexcept {
    return std::bit_cast<DateYearOrDuration>(removeDatatypeBits(_bits));
  }

  // TODO<joka921> implement dates

  /// Return the smallest and largest possible `ValueId` wrt the underlying
  /// representation
  constexpr static ValueId min() noexcept {
    return {std::numeric_limits<T>::min()};
  }
  constexpr static ValueId max() noexcept {
    return {std::numeric_limits<T>::max()};
  }

  /// Enable hashing in abseil for `ValueId` (required by `ad_utility::HashSet`
  /// and `ad_utility::HashMap`
  template <typename H>
  friend H AbslHashValue(H h, const ValueId& id) {
    return H::combine(std::move(h), id._bits);
  }

  /// Enable the serialization of `ValueId` in the `ad_utility::serialization`
  /// framework.
  AD_SERIALIZE_FRIEND_FUNCTION(ValueId) { serializer | arg._bits; }

  /// Similar to `std::visit` for `std::variant`. First gets the datatype and
  /// then calls `visitor(getTYPE)` where `getTYPE` is the correct getter method
  /// for the datatype (e.g. `getDouble` for `Datatype::Double`). Visitor must
  /// be callable with all of the possible return types of the `getTYPE`
  /// functions.
  /// TODO<joka921> This currently still has limited functionality because
  /// VocabIndex, LocalVocabIndex and TextRecordIndex are all of the same type
  /// `uint64_t` and the visitor cannot distinguish between them. Create strong
  /// types for these indices and make the `ValueId` class use them.
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
      return ostr << id.getBits();
    }

    auto visitor = [&ostr]<typename T>(T&& value) {
      if constexpr (ad_utility::isSimilar<T, ValueId::UndefinedType>) {
        // already handled above
        AD_FAIL();
      } else if constexpr (ad_utility::isSimilar<T, double> ||
                           ad_utility::isSimilar<T, int64_t>) {
        ostr << std::to_string(value);
      } else if constexpr (ad_utility::isSimilar<T, bool>) {
        ostr << (value ? "true" : "false");
      } else if constexpr (ad_utility::isSimilar<T, DateYearOrDuration>) {
        ostr << value.toStringAndType().first;
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
      throw IndexTooLargeException(id);
    }
    return addDatatypeBits(id, type);
  }
};

#endif  // QLEVER_VALUEID_H
