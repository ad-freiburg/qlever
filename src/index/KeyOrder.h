// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_KEYORDER_H
#define QLEVER_SRC_INDEX_KEYORDER_H

#include <array>
#include <cstddef>
#include <cstdint>

#include "backports/algorithm.h"
#include "util/Exception.h"

namespace qlever {

// A strong type for a permutation of the integers `0, 1, 2, 3`. This is used
// to determine the permutation of a quad (0 = S, 1 = P, 2 = O, 3 = G). For
// example, `1, 0, 2, 3` represents the permutation `PSOG`.
class KeyOrder {
 public:
  using T = uint8_t;
  static constexpr size_t NumKeys = 4;
  using Array = std::array<T, NumKeys>;

 private:
  Array keys_;

 public:
  // Construct from four numbers. If `(a b c d)` is not a permutation of the
  // numbers `0, 1, 2, 3`, then an `AD_CONTRACT_CHECK` will fail.
  KeyOrder(T a, T b, T c, T d) : keys_{a, b, c, d} { validate(); }

  // Get access to the keys.
  const auto& keys() const { return keys_; }

  // Apply the permutation specified by this `KeyOrder` to the `input`.
  // The elements of the input are copied into the result.
  template <typename T>
  std::array<T, NumKeys> permuteTuple(
      const std::array<T, NumKeys>& input) const {
    return permuteImpl(input, std::make_index_sequence<NumKeys>());
  }

  // Check that `keys()[3] == 3`, i.e that the first three keys specify a
  // permutation of the numbers `[0..3]`. Then apply this permutation to the
  // `input` same as in `permute` above. This function is sometimes used in code
  // for permutations where the graph is the last variable. It will be removed
  // in the future when we have more proper support for named graphs.
  template <typename T>
  std::array<T, 3> permuteTriple(const std::array<T, 3>& input) const {
    AD_CONTRACT_CHECK(keys_[3] == 3);
    return permuteImpl(input, std::make_index_sequence<3>());
  }

 private:
  // Check the invariants.
  void validate() const {
    AD_CONTRACT_CHECK(
        ql::ranges::all_of(keys_, [](size_t x) { return x < NumKeys; }),
        "Keys are out of range");
    auto keys = keys_;
    ql::ranges::sort(keys);
    AD_CONTRACT_CHECK(std::unique(keys.begin(), keys.end()) == keys.end(),
                      "Keys are not unique.");
  }

  // The internal implementation of `permute` above.
  CPP_variadic_template(typename T, size_t N, size_t... Indexes)(requires(
      sizeof...(Indexes) ==
      N)) auto permuteImpl(const std::array<T, N>& input,
                           std::integer_sequence<size_t, Indexes...>) const
      -> std::array<T, N> {
    return {input[std::get<Indexes>(keys_)]...};
  }
};
}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_KEYORDER_H
