//  Copyright 2023. University of Freiburg,
//                  Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_RESIZEWHENMOVEVECTOR_H
#define QLEVER_RESIZEWHENMOVEVECTOR_H

#include <vector>

#include "util/ExceptionHandling.h"

namespace columnBasedIdTable::detail {
// Disclaimer: This class is an implementation detail of the column based ID
// tables. Its semantics are very particular, so we don't expect it to have a
// use case outside of the `IdTable` module.
// A class that inherits from a `vector<T>`, where `T`.
// This class changes the move operators of the underlying
// `vector` as follows: Instead of moving the vector as a whole, only the
// indidual elements are moved. This is used for the column based `IdTables`
// where we move the indivdual columns, but still want a moved from table to
// have the same number of columns as before, but with the columns now being
// empty.
template <typename T>
struct ResizeWhenMoveVector : public std::vector<T> {
  using Base = std::vector<T>;
  using Base::Base;
  // Defaulted copy operations
  ResizeWhenMoveVector(const ResizeWhenMoveVector&) = default;
  ResizeWhenMoveVector& operator=(const ResizeWhenMoveVector&) = default;

  // Move operations with the specified semantics.
  ResizeWhenMoveVector(ResizeWhenMoveVector&& other) noexcept {
    moveImpl(std::move(other));
  }
  ResizeWhenMoveVector& operator=(ResizeWhenMoveVector&& other) noexcept {
    this->clear();
    moveImpl(std::move(other));
    return *this;
  }

 private:
  // The common implementation, move the elements of `other` into `this`.
  // Terminate with a readable error message in the unlikely case of
  // `std::bad_alloc`.
  void moveImpl(ResizeWhenMoveVector&& other) noexcept {
    ad_utility::terminateIfThrows(
        [&other, self = this] {
          self->insert(self->end(), std::make_move_iterator(other.begin()),
                       std::make_move_iterator(other.end()));
        },
        "Error happened during the move construction or move assignment of an "
        "IdTable");
  }
};
}  // namespace columnBasedIdTable::detail

#endif  // QLEVER_RESIZEWHENMOVEVECTOR_H
