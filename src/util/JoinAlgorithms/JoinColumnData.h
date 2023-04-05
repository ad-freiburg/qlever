//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <cstdint>
#include <vector>

#include "util/Algorithm.h"

namespace ad_utility {
// The implementation of the join algorithms (merge/zipper join, galloping join)
// in `JoinAlgorithms.h` assume that in each of their inputs the first columns
// are the (ordered) join columns. They also write a result where the join
// columns come first, then the non-join columns of the left input and then the
// non-join columns of the right input. This format is different from the
// formats that the join operations (`Join`, `OptionalJoin`, `MultiColumnJoin`
// etc.) provide and expect: The columns of the input are arbitrarily ordered
// and there is an explicit data structure that contains the information which
// columns are the join columns. The output must be first all columns from the
// left input (in the same order) and then all the columns in the right input,
// but without the join columns (also in the same order). The following struct
// stores the information, which permutations have to be applied to the input
// and the result to convert from one of those formats to the other.
class JoinColumnData {
 private:
  // For a documentation of those members, see the getter function with the same
  // name below.
  std::vector<size_t> jcsLeft_;
  std::vector<size_t> jcsRight_;
  std::vector<size_t> permutationLeft_;
  std::vector<size_t> permutationRight_;
  std::vector<size_t> permutationResult_;

 public:
  // The join columns in the left input. For example if `jcsLeft()` is `[3, 0]`
  // this means that the primary join column is the third column of the left
  // input, and the secondary join column is the 0-th column of the left input.
  const std::vector<size_t>& jcsLeft() const { return jcsLeft_; }
  // The same, but for the right input.
  const std::vector<size_t>& jcsRight() const { return jcsRight_; }

  // This permutation has to be applied to the left input to obtain the column
  // order expected by the join algorihtms (see above for details on the
  // different orderings).
  const std::vector<size_t>& permutationLeft() const {
    return permutationLeft_;
  };
  // The same, but for the right input.
  const std::vector<size_t>& permutationRight() const {
    return permutationRight_;
  }
  // This permutation has to be applied to the result of the join algorithms to
  // obtain the column order that the `Join` subclasses of `Operation` expect.
  const std::vector<size_t>& permutationResult() const {
    return permutationResult_;
  }

  JoinColumnData(const std::vector<std::array<size_t, 2>>& joinColumns,
                 size_t numColsA, size_t numColsB) {
    permutationResult_.resize(numColsA + numColsB - joinColumns.size());
    for (auto [colA, colB] : joinColumns) {
      permutationResult_.at(colA) = jcsLeft_.size();
      jcsLeft_.push_back(colA);
      jcsRight_.push_back(colB);
    }

    permutationLeft_ = jcsLeft_;
    permutationRight_ = jcsRight_;

    for (size_t i = 0; i < numColsA; ++i) {
      if (!ad_utility::contains(jcsLeft_, i)) {
        permutationResult_.at(i) = permutationLeft_.size();
        permutationLeft_.push_back(i);
      }
    }

    size_t numSkippedJoinColumns = 0;
    for (size_t i = 0; i < numColsB; ++i) {
      if (!ad_utility::contains(jcsRight_, i)) {
        permutationResult_.at(i - numSkippedJoinColumns + numColsA) =
            i - numSkippedJoinColumns + numColsA;
        permutationRight_.push_back(i);
      } else {
        ++numSkippedJoinColumns;
      }
    }
  }
};
}  // namespace ad_utility
