//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <ranges>

// Implementation of std::ranges::owning_view for gcc 11.
#if __GNUC__ == 11
#include "util/TypeTraits.h"

template <std::ranges::range R>
requires std::movable<R> &&
         (!ad_utility::isInstantiation<std::remove_cv_t<R>,
                                       std::initializer_list>)class owning_view
    : public std::ranges::view_interface<owning_view<R>> {
 private:
  R range_ = R();

 public:
  owning_view() requires std::default_initializable<R> = default;

  constexpr owning_view(R&& r) noexcept(std::is_nothrow_move_constructible_v<R>)
      : range_(std::move(r)) {}

  owning_view(owning_view&&) = default;
  owning_view& operator=(owning_view&&) = default;

  constexpr R& base() & noexcept { return range_; }

  constexpr const R& base() const& noexcept { return range_; }

  constexpr R&& base() && noexcept { return std::move(range_); }

  constexpr const R&& base() const&& noexcept { return std::move(range_); }

  constexpr std::ranges::iterator_t<R> begin() {
    return std::ranges::begin(range_);
  }

  constexpr std::ranges::sentinel_t<R> end() {
    return std::ranges::end(range_);
  }

  constexpr auto begin() const requires std::ranges::range<const R> {
    return std::ranges::begin(range_);
  }

  constexpr auto end() const requires std::ranges::range<const R> {
    return std::ranges::end(range_);
  }

  constexpr bool empty() requires requires { std::ranges::empty(range_); } {
    return std::ranges::empty(range_);
  }

  constexpr bool empty() const requires requires { std::ranges::empty(range_); }
  {
    return std::ranges::empty(range_);
  }

  constexpr auto size() requires std::ranges::sized_range<R> {
    return std::ranges::size(range_);
  }

  constexpr auto size() const requires std::ranges::sized_range<const R> {
    return std::ranges::size(range_);
  }

  constexpr auto data() requires std::ranges::contiguous_range<R> {
    return std::ranges::data(range_);
  }

  constexpr auto data() const requires std::ranges::contiguous_range<const R> {
    return std::ranges::data(range_);
  }
};
#else
template <std::ranges::range R>
using owning_view = std::ranges::owning_view<R>;
#endif
