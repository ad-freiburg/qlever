//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BOUNDEDINTEGER_H
#define QLEVER_BOUNDEDINTEGER_H

namespace ad_utility {
struct BoundedIntegerOutOfRangeError : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

template <int64_t Min, int64_t Max>
requires(Max > Min) struct BoundedInteger {
  constexpr static auto numBits = numBitsRequired(Max - Min + 1);
  using T = unsignedTypeForNumberOfBits<numBits>;

 private:
  T _data;

 public:
  constexpr static auto min = Min;
  constexpr static auto max = Max;

  explicit constexpr BoundedInteger(int64_t value) : _data{value - Min} {
    if (value < Min || value > Max) {
      // TODO<joka921> exception message
      throw BoundedIntegerOutOfRangeError{"value out of range"};
    }
  }

  [[nodiscard]] constexpr int64_t get() const noexcept {
    return static_cast<int64_t>(_data) + Min;
  }

  [[nodiscard]] constexpr uint64_t toBits() const noexcept { return _data; }

  static constexpr BoundedInteger fromUncheckedBits(uint64_t bits) {
    return {UncheckedBitsTag{}, bits};
  }

 private:
  struct UncheckedBitsTag {};
  BoundedInteger(UncheckedBitsTag, uint64_t bits)
      : _data{static_cast<T>(bits)} {}
};

// TODO<joka921> requires clause
template <typename... Bounded>
struct CombinedBoundedIntegers {
  std::tuple<Bounded...> _integers;

  // TODO<joka921> requires clause
  constexpr CombinedBoundedIntegers(auto... values) { ... }
};
}  // namespace ad_utility

#endif  // QLEVER_BOUNDEDINTEGER_H
