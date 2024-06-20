//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_FORWARD_H
#define QLEVER_FORWARD_H
#include <utility>

/// A simple macro that allows writing `AD_FWD(x)` instead of
/// `std::forward<decltype(x)>(x)`.
#define AD_FWD(x) std::forward<decltype(x)>(x)

namespace ad_utility::detail {
template <typename T>
using MoveType =
    std::conditional_t<std::is_object_v<T>, T, std::remove_reference_t<T>&&>;
}

// TODO<joka921> Document and test.
#define AD_MOVE(x) static_cast<ad_utility::detail::MoveType<decltype((x))>>(x)

#endif  // QLEVER_FORWARD_H
