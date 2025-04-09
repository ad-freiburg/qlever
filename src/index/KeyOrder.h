// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_KEYORDER_H
#define QLEVER_SRC_INDEX_KEYORDER_H

#include <array>
#include <cstddef>

#include "backports/algorithm.h"
#include "util/Exception.h"

namespace qlever {
// A strong type for a permutation of the integers `0, 1, 2, 3` that is used to
// determine a permutation of triples (0 = S, 1 = P, 2 = O, 3 = G), so
// `1 0 2 3` represents the `PSOG` permutation.
class KeyOrder {
 public:
  static constexpr size_t NumKeys = 4;
  using Array = std::array<size_t, NumKeys>;

 private:
  Array keys_;

 public:
  // Construct from four numbers. If `(a b c d)` is not a permutation of the
  // numbers `[0...4]`, then an `AD_CONTRACT_CHECK` will fail.
  KeyOrder(size_t a, size_t b, size_t c, size_t d) : keys_{a, b, c, d} {
    validate();
  }

  // Get access to the permutation.
  const Array& keys() const { return keys_; }

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
};
}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_KEYORDER_H
