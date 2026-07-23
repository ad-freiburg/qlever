// Copyright 2021 - 2026, University of Freiburg, Chair of Algorithms and Data
// Structures.
//
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_VECTORWITHMEMORYLIMIT_H
#define QLEVER_SRC_UTIL_VECTORWITHMEMORYLIMIT_H

#include <initializer_list>
#include <vector>

#include "backports/concepts.h"
#include "backports/keywords.h"
#include "backports/type_traits.h"
#include "util/AllocatorWithLimit.h"
#include "util/Forward.h"
#include "util/TypeTraits.h"

namespace ad_utility {

// A `std::vector<T, AllocatorWithLimit>` with deleted copy constructor and copy
// assignment. Used whenever we want no accidental copies of large intermediate
// results (originally in the `SparqlExpression` module). Copying is only
// possible via the explicit `clone()` member function.
template <typename T>
class VectorWithMemoryLimit
    : public std::vector<T, ad_utility::AllocatorWithLimit<T>> {
 public:
  using Allocator = ad_utility::AllocatorWithLimit<T>;
  using Base = std::vector<T, ad_utility::AllocatorWithLimit<T>>;

 private:
  struct CloneTag {};

 public:
  // The `AllocatorWithLimit` is not default-constructible (on purpose).
  // Unfortunately, the support for such allocators is not really great in the
  // standard library. In particular, the type trait
  // `std::default_initializable<std::vector<T, Alloc>>` will be true, even if
  // the `Alloc` is not default-initializable, which leads to hard compile
  // errors with the ranges library. For this reason we cannot simply inherit
  // all the constructors from `Base`, but explicitly have to forward all but
  // the default constructor. In particular, we only forward constructors that
  // have
  // * at least one argument
  // * the first argument must not be similar to `std::vector` or
  // `VectorWithMemoryLimit` to not hide copy or move constructors
  // * the last argument must be `AllocatorWithLimit` (all constructors to
  // `vector` take the allocator as a last parameter)
  // * there must be a constructor of `Base` for the given arguments.
  CPP_template(typename... Args)(
      requires(sizeof...(Args) > 0) CPP_and CPP_NOT(
          concepts::derived_from<ql::remove_cvref_t<ad_utility::First<Args...>>,
                                 Base>)
          CPP_and concepts::convertible_to<ad_utility::Last<Args...>, Allocator>
              CPP_and concepts::constructible_from<Base, Args&&...>)
      QL_EXPLICIT(sizeof...(Args) == 1) VectorWithMemoryLimit(Args&&... args)
      : Base{AD_FWD(args)...} {}

  // We have to explicitly forward the `initializer_list` constructor because it
  // for some reason is not covered by the above generic mechanism.
  VectorWithMemoryLimit(std::initializer_list<T> init, const Allocator& alloc)
      : Base(init, alloc) {}

  // Disable copy constructor and copy assignment operator (copying is too
  // expensive in the setting where we want to use this class and not
  // necessary).
 public:
  VectorWithMemoryLimit& operator=(const VectorWithMemoryLimit&) = delete;
  VectorWithMemoryLimit(const VectorWithMemoryLimit&) = delete;
  // Moving is fine.
  VectorWithMemoryLimit(VectorWithMemoryLimit&&) noexcept = default;
  VectorWithMemoryLimit& operator=(VectorWithMemoryLimit&&) noexcept = default;

  // Allow copying via an explicit clone() function.
  [[nodiscard]] VectorWithMemoryLimit clone() const {
    // Call the private copy constructor.
    return VectorWithMemoryLimit(CloneTag{}, *this);
  }

 private:
  // Constructor for copying, used to implement the `clone` function.
  // We use the explicit tag, s.t. this constructor doesn't match the signature
  // of a copy constructor, because otherwise type traits like `copyable` or
  // `constructible_form` would be misled (they might return true for certain
  // compilers in C++17 if the copy constructor is present but private.
  VectorWithMemoryLimit(CloneTag, const VectorWithMemoryLimit& other)
      : Base{static_cast<const Base&>(other)} {}
};

static_assert(!ql::concepts::default_initializable<VectorWithMemoryLimit<int>>);
static_assert(!ql::concepts::copyable<VectorWithMemoryLimit<int>>);

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VECTORWITHMEMORYLIMIT_H
