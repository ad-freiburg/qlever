//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author:
//   2022 -    Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TRANSPARENTFUNCTORS_H
#define QLEVER_TRANSPARENTFUNCTORS_H

#include <util/Forward.h>
#include <util/TypeTraits.h>

#include <utility>

/// Contains several function object types with templated operator() that wrap
/// overloaded functions from the standard library. This enables passing them as
/// function parameters.

/// Note that in theory all of them could be implemented shorter as captureless
/// lambda expressions. We have chosen not to do this because the STL also does
/// not choose this approach (see e.g. `std::less`, `std::plus`, etc.) and
/// because global inline lambdas in header files might in theory cause ODR (one
/// definition rule) problems, especially  when using different compilers.

namespace ad_utility {

namespace detail {

// Implementation of `first` (see below).
struct FirstImpl {
  template <typename T>
  constexpr decltype(auto) operator()(T&& pair) const noexcept {
    return std::get<0>(AD_FWD(pair));
  }
};

// Implementation of `second` (see below).
struct SecondImpl {
  template <typename T>
  constexpr decltype(auto) operator()(T&& pair) const noexcept {
    return std::get<1>(AD_FWD(pair));
  }
};

// Implementation of `holdsAlternative` (see below).
template <typename T>
struct HoldsAlternativeImpl {
  constexpr decltype(auto) operator()(auto&& variant) const {
    return std::holds_alternative<T>(AD_FWD(variant));
  }
};

// Implementation of `get` (see below).
template <typename T>
struct GetImpl {
  constexpr decltype(auto) operator()(auto&& variant) const {
    return std::get<T>(AD_FWD(variant));
  }
};

// Implementation of `getIf` (see below).
template <typename T>
struct GetIfImpl {
  template <typename Ptr>
  requires std::is_pointer_v<std::remove_cvref_t<Ptr>>
  constexpr decltype(auto) operator()(Ptr& variantPtr) const {
    return std::get_if<T>(variantPtr);
  }
  template <typename Ptr>
  requires(!std::is_pointer_v<std::remove_cvref_t<Ptr>>)
  constexpr decltype(auto) operator()(Ptr& variant) const {
    return std::get_if<T>(&variant);
  }
};

// Implementation of `toBool` (see below).
struct ToBoolImpl {
  constexpr decltype(auto) operator()(const auto& x) const {
    return static_cast<bool>(x);
  }
};

}  // namespace detail

/// Return the first element via perfect forwarding of any type for which
/// `std::get<0>(x)` is valid. This holds e.g. for `std::pair`, `std::tuple`,
/// and `std::array`.
static constexpr detail::FirstImpl first;

/// Return the second element via perfect forwarding of any type for which
/// `std::get<1>(x)` is valid. This holds e.g. for `std::pair`, `std::tuple`,
/// and `std::array`.
static constexpr detail::SecondImpl second;

/// Transparent functor for `std::holds_alternative`
template <typename T>
static constexpr detail::HoldsAlternativeImpl<T> holdsAlternative;

/// Transparent functor for `std::get`. Currently only works for `std::variant`
/// and not for `std::array` or `std::tuple`.
template <typename T>
static constexpr detail::GetImpl<T> get;

/// Transparent functor for `std::get_if`. As an extension to `std::get_if`,
/// `ad_utility::getIf` may also be called with a `variant` object or reference,
/// not only with a pointer.
template <typename T>
static constexpr detail::GetIfImpl<T> getIf;

static constexpr detail::ToBoolImpl toBool;

/// A functor that takes an arbitrary number of arguments by reference and does
/// nothing.
struct Noop {
  void operator()(const auto&...) const {
    // This function deliberately does nothing (static analysis expects a
    // comment here).
  }
};
[[maybe_unused]] static constexpr Noop noop{};

template <typename T>
struct StaticCast {
  constexpr decltype(auto) operator()(auto&& x) const {
    return static_cast<T>(AD_FWD(x));
  }
};

template <typename T>
static constexpr StaticCast<T> staticCast{};

}  // namespace ad_utility

#endif  // QLEVER_TRANSPARENTFUNCTORS_H
