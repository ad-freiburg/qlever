//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_FOLDEDID_H
#define QLEVER_FOLDEDID_H

#include <cstdint>
#include <limits>

#include "../util/BitUtils.h"
#include "../util/NBitInteger.h"

enum struct Datatype {
  Undefined = 0,
  Int,
  Double,
  Date,
  Vocab,
  LocalVocab,
  Text,
};

class FoldedId {
 public:
  using T = uint64_t;
  static constexpr T numTypeBits = 4;
  static constexpr T numDataBits = 64 - numTypeBits;

  using IntegerType = ad_utility::NBitInteger<numDataBits>;

  static constexpr T maxId = 1ull << (numDataBits - 1);

  struct IdTooLargeException : public std::exception {};

 private:
  T _bits;

 public:
  constexpr bool operator==(const FoldedId&) const = default;
  constexpr auto operator<=>(const FoldedId& other) const {
    return _bits <=> other._bits;
  }

  [[nodiscard]] constexpr Datatype getDatatype() const noexcept {
    return static_cast<Datatype>(_bits >> numDataBits);
  }

  static FoldedId Undefined() { return {0}; }
  static FoldedId Double(double d) {
    auto shifted = std::bit_cast<T>(d) >> numTypeBits;
    return addMask(shifted, Datatype::Double);
  }

  [[nodiscard]] double getDouble() const noexcept {
    return std::bit_cast<double>(_bits << numTypeBits);
  }

  static FoldedId Int(int64_t i) noexcept {
    auto nbit = IntegerType::toNBit(i);
    return addMask(nbit, Datatype::Int);
  }

  [[nodiscard]] int64_t getInt() const noexcept {
    return IntegerType::fromNBit(_bits);
  }

  static FoldedId Vocab(T index) {
    return makeUnsigned(index, Datatype::Vocab);
  }
  static FoldedId Text(T index) { return makeUnsigned(index, Datatype::Text); }
  static FoldedId LocalVocab(T index) {
    return makeUnsigned(index, Datatype::LocalVocab);
  }

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

 private:
  constexpr FoldedId(T bits) : _bits{bits} {}

  static constexpr T addMask(T bits, Datatype type) {
    auto mask = static_cast<T>(type) << numDataBits;
    return bits | mask;
  }
  static constexpr T removeMask(T bits) noexcept {
    auto mask = ad_utility::bitMaskForLowerBits(numDataBits);
    return bits & mask;
  }

  static constexpr FoldedId makeUnsigned(T id, Datatype type) {
    if (id > maxId) {
      throw IdTooLargeException();
    }
    return addMask(id, type);
  }
};

#endif  // QLEVER_FOLDEDID_H
