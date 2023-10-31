//  Copyright 2023. University of Freiburg,
//                  Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VECTORWITHELEMENTWISEMOVE_H
#define QLEVER_VECTORWITHELEMENTWISEMOVE_H

#include <vector>

#include "util/ExceptionHandling.h"

namespace columnBasedIdTable::detail {
// A class that inherits from a `vector<T>`. This class changes the move
// operators of the underlying `vector` as follows: Instead of moving the vector
// as a whole, only the individual elements are moved. This is used for the
// column-based `IdTables` where we move the individual columns, but still want
// a table that was moved from to have the same number of columns as before, but
// with the columns now being empty.

// Note: This class is an implementation detail of the column-based ID
// tables. Its semantics are very particular, so we don't expect it to have a
// use case outside the `IdTable` module.
template <typename T>
struct VectorWithElementwiseMove : public std::vector<T> {
  using Base = std::vector<T>;
  using Base::Base;
  // Defaulted copy operations, inherited from the base class.
  VectorWithElementwiseMove(const VectorWithElementwiseMove&) = default;
  VectorWithElementwiseMove& operator=(const VectorWithElementwiseMove&) =
      default;

  // Move operations with the specified semantics.
  VectorWithElementwiseMove(VectorWithElementwiseMove&& other) noexcept {
    moveImpl(std::move(other));
  }
  VectorWithElementwiseMove& operator=(
      VectorWithElementwiseMove&& other) noexcept {
    this->clear();
    moveImpl(std::move(other));
    return *this;
  }
  // Nothing changes for the destructors, but we obey the rule of 5.
  ~VectorWithElementwiseMove() = default;

 private:
  // The common implementation of the move semantics, move-insert the elements
  // of `other` into `this`. Terminate with a readable error message in the
  // unlikely case of `std::bad_alloc`.
  void moveImpl(VectorWithElementwiseMove&& other) noexcept {
    ad_utility::terminateIfThrows(
        [&other, self = this] {
          AD_CORRECTNESS_CHECK(self->empty());
          self->insert(self->end(), std::make_move_iterator(other.begin()),
                       std::make_move_iterator(other.end()));
        },
        "Error happened during the move construction or move assignment of an "
        "IdTable");
  }
};
}  // namespace columnBasedIdTable::detail

#endif  // QLEVER_VECTORWITHELEMENTWISEMOVE_H
