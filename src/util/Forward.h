//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_FORWARD_H
#define QLEVER_FORWARD_H
#include <utility>

/// A simple macro that allows writing `AD_FWD(x)` instead of
/// `std::forward<decltype(x)>(x)`.
#define AD_FWD(x) std::forward<decltype(x)>(x)

// Needed for the implementation of `AD_MOVE` below.
namespace ad_utility::detail {
template <typename T>
using MoveType =
    std::conditional_t<std::is_object_v<T>, T, std::remove_reference_t<T>&&>;
}

// `std::move(x)`, but only if `x` is not a temporary object. For example
// `std::string x; AD_MOVE(x)` will move the string, while
// `AD_MOVE(std::string{})` will not. This can be used in generic code where you
// always want to move, but the pattern `auto x = std::move(genericFunction())`
// should NOT move if the function returns an object, because that would prevent
// copy elision. Using `AD_MOVE` in this case only moves if the
// `genericFunction()` returns a reference.
// Note: The double parentheses in `decltype((x))` are necessary, because
// for a normal lvalue `std::string x;`, `decltype(x)` is `std::string` (which
// would not be moved by the corresponding `MoveType`), but `decltype((x))` is
// `std::string&`. For a temporary, even e.g. `decltype((std::string{"hi"}))` is
// `std::string`.
#define AD_MOVE(x) static_cast<ad_utility::detail::MoveType<decltype((x))>>(x)

#endif  // QLEVER_FORWARD_H
