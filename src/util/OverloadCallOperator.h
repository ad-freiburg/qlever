//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_OVERLOADCALLOPERATOR_H
#define QLEVER_OVERLOADCALLOPERATOR_H

namespace ad_utility {
/**
 * @brief A struct that inherits `operator()` from all its base classes `Ts...`
 * This is useful for example in connection with `std::visit` (see
 * https://en.cppreference.com/w/cpp/utility/variant/visit) or to create
 * symmetric lambdas for `std::equal_range` with heterogeneous types (see File
 * `global/ValueIdComparators.h`)
 *
 * @example:
 * auto overloadedCallable = OverloadCallOperator{[](double d) {return
 * "double";},
 *                              [](std::string s) {return "string";}};
 * overloadedCallable(2.4); // returns "double"
 * overloadedCallable("hello"); // returns "string"
 */
template <class... Ts>
struct OverloadCallOperator : Ts... {
  using Ts::operator()...;
};
// Explicit deduction guide (not needed as of C++20, but clang 13 still needs
// it).
template <class... Ts>
OverloadCallOperator(Ts...) -> OverloadCallOperator<Ts...>;
}  // namespace ad_utility

#endif  // QLEVER_OVERLOADCALLOPERATOR_H
