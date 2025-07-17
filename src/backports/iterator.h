// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_BACKPORTS_ITERATOR_H
#define QLEVER_SRC_BACKPORTS_ITERATOR_H

#include <iterator>
#include <type_traits>

#include "backports/concepts.h"
namespace ql {
// A backport of `std::move_sentinel` for C++17
CPP_template(typename Sent)(
    requires ql::concepts::semiregular<Sent>) class move_sentinel {
 public:
  constexpr move_sentinel() noexcept(
      std::is_nothrow_default_constructible_v<Sent>)
      : sent_() {}

  constexpr explicit move_sentinel(Sent s) noexcept(
      std::is_nothrow_move_constructible_v<Sent>)
      : sent_(std::move(s)) {}

  CPP_template_2(typename S2)(
      requires ql::concepts::convertible_to<
          const S2&,
          Sent>) constexpr move_sentinel(const move_sentinel<S2>&
                                             s) noexcept(std::
                                                             is_nothrow_constructible_v<
                                                                 Sent,
                                                                 const S2&>)
      : sent_(s.base()) {}

  CPP_template_2(typename S2)(requires ql::concepts::assignable_from<
                              Sent&, const S2&>) constexpr move_sentinel&
  operator=(const move_sentinel<S2>& s) noexcept(
      std::is_nothrow_assignable_v<Sent, const S2&>) {
    sent_ = s.base();
    return *this;
  }

  constexpr Sent base() const
      noexcept(std::is_nothrow_copy_constructible_v<Sent>) {
    return sent_;
  }

  CPP_template_2(typename It)(
      requires ql::concepts::sentinel_for<Sent, It>) friend bool
  operator==(const std::move_iterator<It> it, move_sentinel sent) {
    return it.base() == sent.base();
  }

  CPP_template_2(typename It)(
      requires ql::concepts::sentinel_for<Sent, It>) friend bool
  operator!=(const std::move_iterator<It> it, move_sentinel sent) {
    return it.base() != sent.base();
  }

 private:
  [[no_unique_address]] Sent sent_;
};
}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_ITERATOR_H
