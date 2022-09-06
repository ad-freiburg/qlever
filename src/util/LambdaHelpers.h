//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_LAMBDAHELPERS_H
#define QLEVER_LAMBDAHELPERS_H

#include "./Forward.h"

namespace ad_utility {

namespace detail {
// Helper type for `makeAssignableLambda`.
template <typename Lambda>
struct AssignableLambdaImpl {
 private:
  Lambda _lambda;

 public:
  explicit AssignableLambdaImpl(Lambda lambda) : _lambda{std::move(lambda)} {}

  decltype(auto) operator()(auto&&... args) noexcept(
      noexcept(_lambda(AD_FWD(args)...))) {
    return _lambda(AD_FWD(args)...);
  }

  decltype(auto) operator()(auto&&... args) const
      noexcept(noexcept(_lambda(AD_FWD(args)...))) {
    return _lambda(AD_FWD(args)...);
  }

  AssignableLambdaImpl& operator=(const AssignableLambdaImpl& other) requires
      std::is_copy_constructible_v<AssignableLambdaImpl> {
    std::destroy_at(&_lambda);
    std::construct_at(&_lambda, other._lambda);
    return *this;
  }

  AssignableLambdaImpl& operator=(AssignableLambdaImpl&& other) noexcept
      requires std::is_move_constructible_v<AssignableLambdaImpl> {
    std::destroy_at(&_lambda);
    std::construct_at(&_lambda, std::move(other._lambda));
    return *this;
  }

  AssignableLambdaImpl(const AssignableLambdaImpl&) = default;
  AssignableLambdaImpl(AssignableLambdaImpl&&) noexcept = default;
};
}  // namespace detail

/// The closure types created by lambda expressions have copy and move
/// constructors, but no assignment operators. This function takes a lambda
/// expression and returns an object that behaves like that lambda, but
/// additionally has copy and move assignment. This is useful e.g. to implement
/// iterator types that store lambda expressions (see Iterators.h), because many
/// STL algorithms require iterators to be assignable. The assignment operators
/// are implemented using `destroy_at` and `construct_at`.
auto makeAssignableLambda(auto lambda) {
  return detail::AssignableLambdaImpl{std::move(lambda)};
}
}  // namespace ad_utility

#endif  // QLEVER_LAMBDAHELPERS_H
