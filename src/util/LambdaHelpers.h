//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_LAMBDAHELPERS_H
#define QLEVER_LAMBDAHELPERS_H

#include "./Forward.h"
#include "backports/concepts.h"

namespace ad_utility {

namespace detail {
// Helper type for `makeAssignableLambda`.
template <typename Lambda, bool Movable, bool Copyable>
struct AssignableLambdaImpl;

template <typename Lambda>
struct AssignableLambdaImpl<Lambda, false, false> {
 private:
  Lambda _lambda;

 protected:
  Lambda& lambda() { return _lambda; }
  const Lambda& lambda() const { return _lambda; }

 public:
  explicit constexpr AssignableLambdaImpl(Lambda lambda)
      : _lambda{std::move(lambda)} {}
  AssignableLambdaImpl() = default;

  template <typename... Args>
  decltype(auto) operator()(Args&&... args) noexcept(
      noexcept(_lambda(AD_FWD(args)...))) {
    return _lambda(AD_FWD(args)...);
  }

  template <typename... Args>
  decltype(auto) constexpr operator()(Args&&... args) const
      noexcept(noexcept(_lambda(AD_FWD(args)...))) {
    return _lambda(AD_FWD(args)...);
  }
};

template <typename Lambda>
struct AssignableLambdaImpl<Lambda, true, false>
    : public AssignableLambdaImpl<Lambda, false, false> {
 private:
  using Base = AssignableLambdaImpl<Lambda, false, false>;

 public:
  using Base::Base;
  using Base::operator();

 protected:
  using Base::lambda;

 public:
  AssignableLambdaImpl& operator=(AssignableLambdaImpl&& other) noexcept
      QL_CONCEPT_OR_NOTHING(requires std::is_move_constructible_v<Lambda>) {
    std::destroy_at(&lambda());
    new (&lambda()) Lambda(std::move(other.lambda()));
    return *this;
  }

  AssignableLambdaImpl(const AssignableLambdaImpl&) = default;
  AssignableLambdaImpl(AssignableLambdaImpl&&) noexcept = default;
};

template <typename Lambda>
struct AssignableLambdaImpl<Lambda, true, true>
    : private AssignableLambdaImpl<Lambda, true, false> {
 private:
  using Base = AssignableLambdaImpl<Lambda, true, false>;
  using Base::lambda;

 public:
  using Base::Base;
  using Base::operator();

  AssignableLambdaImpl& operator=(const AssignableLambdaImpl& other)
      QL_CONCEPT_OR_NOTHING(requires std::is_copy_constructible_v<Lambda>) {
    std::destroy_at(&lambda());
    new (&lambda()) Lambda(other.lambda());
    return *this;
  }

  AssignableLambdaImpl& operator=(AssignableLambdaImpl&& other) = default;

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
template <typename Lambda>
constexpr auto makeAssignableLambda(Lambda lambda) {
  return detail::AssignableLambdaImpl<Lambda,
                                      std::is_move_constructible_v<Lambda>,
                                      std::is_copy_constructible_v<Lambda>>{
      std::move(lambda)};
}
}  // namespace ad_utility

#endif  // QLEVER_LAMBDAHELPERS_H
