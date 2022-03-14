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
enum struct Datatype {
  Undefined,
  Int,
  Bool,
  Decimal,
  Double,
  Float,
  Date,
  Vocab,
  LocalVocab
};
class FancyId {
  using T = uint64_t;

 public:
  // static NBitInteger Undefined() {return {0ul};}
  static constexpr FancyId Double(double d) {
    auto asBits = std::bit_cast<T>(d);
    asBits = asBits >> MASK_SIZE;
    asBits |= DoubleMask;
    return FancyId{asBits};
  }

  static constexpr FancyId Integer(int64_t i) {
    // propagate the sign.
    constexpr int64_t signBit = 1l << MASK_SHIFT;
    auto highestBitShifted =
        ((signBit & i) << (MASK_SIZE - 1)) >> (MASK_SIZE - 1);
    constexpr T mask = std::numeric_limits<T>::max() >> MASK_SIZE;
    return {static_cast<T>((i & mask) | highestBitShifted)};
  }
  static constexpr FancyId MaxInteger() {
    // propagate the sign.
    return {std::numeric_limits<T>::max() >> MASK_SIZE};
  }
  static constexpr FancyId MinInteger() {
    constexpr int64_t signBit = 1l << MASK_SHIFT;
    auto highestBitShifted = ((signBit) << (MASK_SIZE - 1)) >> (MASK_SIZE - 1);
    return {static_cast<T>(highestBitShifted)};
  }

  [[nodiscard]] constexpr double getDoubleUnchecked() const {
    return std::bit_cast<double>(_data << MASK_SIZE);
  }

  [[nodiscard]] constexpr int64_t getIntegerUnchecked() const {
    return static_cast<int64_t>(_data);
  }

  [[nodiscard]] constexpr T data() const { return _data; }

 private:
  T _data;
  static constexpr size_t MASK_SIZE = 4;
  static constexpr size_t MASK_SHIFT = 8 * sizeof(T) - MASK_SIZE;
  constexpr FancyId(uint64_t data) : _data{data} {};

  constexpr static uint64_t DoubleMask = 1ul << MASK_SHIFT;
  constexpr static uint64_t DecimalMask = 3ul << MASK_SHIFT;
  constexpr static uint64_t FloatMask = 5ul << MASK_SHIFT;

  constexpr static uint64_t PositiveIntMask = 0ul << MASK_SHIFT;
  constexpr static uint64_t NegativeIntMask = 16ul << MASK_SHIFT;
  constexpr static uint64_t BoolMask = 6ul << MASK_SHIFT;

  constexpr static uint64_t VocabMask = 4ul << MASK_SHIFT;
  constexpr static uint64_t LocalVocabMask = 8ul << MASK_SHIFT;
  constexpr static uint64_t DateMask = 12ul << MASK_SHIFT;
};

namespace fancyIdLimits {

static constexpr int64_t maxInteger =
    FancyId::MaxInteger().getIntegerUnchecked();
static constexpr int64_t minInteger =
    FancyId::MinInteger().getIntegerUnchecked();
}  // namespace fancyIdLimits

}  // namespace ad_utility::datatypes

#endif  // QLEVER_DATATYPES_H
