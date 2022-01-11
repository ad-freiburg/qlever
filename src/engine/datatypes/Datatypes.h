//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_DATATYPES_H
#define QLEVER_DATATYPES_H

#include <variant>
#include <bit>
#include "../../util/AllocatorWithLimit.h"

namespace ad_utility {
enum struct Datatype {
  Undefined, Int, Bool, Decimal, Double, Float, Date,
  Vocab, LocalVocab
};



class FancyId {
  using T = uint64_t;
 public:
  static FancyId Undefined() {return {0ul};}
  static FancyId Double(double d) {
    auto asBits = std::bit_cast<T>(d);
    asBits = asBits >> MASK_SIZE;
    asBits |= DoubleMask;
    return FancyId{asBits};
  }

  static FancyId Integer(int64_t i) {
    return {static_cast<T>(i)};
  }

  [[nodiscard]] double getDoubleUnchecked() const {
    return std::bit_cast<double>(_data << MASK_SIZE);
  }

  [[nodiscard]] double getDoubleUnchecked() const {
    return std::bit_cast<double>(_data << MASK_SIZE);
  }

  [[nodiscard]] T data() const {
    return _data;
  }

 private:
  T _data;
  static constexpr size_t MASK_SIZE = 4;
  static constexpr size_t MASK_SHIFT = 8 * sizeof(T) - MASK_SIZE;
  FancyId(uint64_t data): _data{data}{};

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

}

#endif  // QLEVER_DATATYPES_H
