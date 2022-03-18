//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_DATATYPES_H
#define QLEVER_DATATYPES_H

#include <Exceptions.h>

#include <bit>
#include <variant>

#include "./BitUtils.h"
#include "./Date.h"
#include "./BoundedInteger.h"

namespace ad_utility::datatypes {

// TODO<joka921> For now, double/float/decimals all will become "doubles" without the possibility of converting them back.
enum struct Datatype {
  Undefined,
  Int,
  Bool,
  Double,
  Date,
  Vocab,
};
class FancyId {
  using T = uint64_t;

  static constexpr size_t MASK_SIZE_INT = 2;
  static constexpr size_t numBits = 64 - MASK_SIZE_INT;
  using N = ad_utility::NBitInteger<numBits>;

 public:
  static constexpr FancyId Undefined() {return {0ul};}

  static constexpr FancyId Double(double d) {
    auto asBits = std::bit_cast<T>(d);
    asBits = asBits >> MASK_SIZE_DOUBLE;
    asBits |= DoubleMask;
    return FancyId{asBits};
  }

  [[nodiscard]] constexpr bool isDouble() const {
    constexpr T HigherBits = bitMaskForHigherBits(MASK_SIZE_DOUBLE);
    return (_data & HigherBits) == DoubleMask;
  }

  [[nodiscard]] constexpr double getDoubleUnchecked() const {
    return std::bit_cast<double>(_data << MASK_SIZE_DOUBLE);
  }

  static constexpr FancyId Integer(int64_t i) {
    return {IntMask | N::toNBit(i)};
  }

  [[nodiscard]] constexpr int64_t getIntegerUnchecked() const {
    // This automatically gets rid of the Mask in the higher bits.
    return N::fromNBit(_data);
  }

  [[nodiscard]] constexpr bool isInteger() const {
    constexpr T HigherBits = bitMaskForHigherBits(MASK_SIZE_INT);
    return (_data & HigherBits) == IntMask;
  }

  constexpr static auto MinInteger() {
    return N::MinInteger();
  }
  constexpr static auto MaxInteger() {
    return N::MaxInteger();
  }

  // TODO::Implement all the other datatypes.

  [[nodiscard]] constexpr T data() const { return _data; }

 private:
  T _data;
  constexpr FancyId(uint64_t data) : _data{data} {};

  static constexpr size_t MASK_SIZE_DOUBLE = 1;
  constexpr static uint64_t DoubleMask = T(0b1000'0000) << 56;

  constexpr static uint64_t IntMask = T(0b0100'0000) << 56;

  static constexpr size_t MASK_SIZE_VOCAB = 3;
  constexpr static uint64_t VocabMask = T(0b0010'0000) << 56;

  static constexpr size_t MASK_SIZE_DATE = 4;
  constexpr static uint64_t DateMask = T(0b0001'0000) << 56;

  static constexpr size_t MASK_SIZE_BOOL = 5;
  constexpr static uint64_t BoolMask = T(0b0000'1000) << 56;
};

}  // namespace ad_utility::datatypes

#endif  // QLEVER_DATATYPES_H
