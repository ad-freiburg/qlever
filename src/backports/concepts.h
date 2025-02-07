// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <concepts/concepts.hpp>
#ifndef QLEVER_CPP_17
#include <concepts>
#endif

// Define the following macros:
//
// `QL_CONCEPT_OR_NOTHING(arg)`: expands to `arg` in C++20 mode, and to
// nothing in C++17 mode. It can be used to easily opt out of concepts that are
// only used for documentation and increased safety and not for overload
// resolution.
//
// `QL_CONCEPT_OR_TYPENAME(arg)`: expands to `arg` in C++20 mode, and to
// `typename` in C++17 mode. Example usage:
//
// Example usages:
//
// `QL_CONCEPT_OR_NOTHING(std::view) auto x = someFunction();`
//
// `QL_CONCEPT_OR_NOTHING(SameAsAny<int, float>)`
//
// `void f(QL_CONCEPT_OR_NOTHING(std::view) auto x) {...}`
//
// `template <QL_CONCEPT_OR_TYPENAME(ql::same_as<int>) T> void f(){...}`
//
// NOTE: The macros are variadic to allow for commas in the argument, like in
// the second example above.

#ifdef QLEVER_CPP_17
#define QL_CONCEPT_OR_NOTHING(...)
#define QL_CONCEPT_OR_TYPENAME(...) typename
#define CPP_and_def CPP_and_sfinae_def
#else
#define QL_CONCEPT_OR_NOTHING(...) __VA_ARGS__
#define QL_CONCEPT_OR_TYPENAME(...) __VA_ARGS__
#define CPP_and_def CPP_and
#endif

// The namespace `ql::concepts` includes concepts that are contained in the
// C++20 standard as well as in `range-v3`.
namespace ql {
namespace concepts {

#ifdef QLEVER_CPP_17
using namespace ::concepts;
#else
using namespace std;
#endif

}  // namespace concepts
}  // namespace ql

// A template with a requires clause
/// INTERNAL ONLY
#define QL_TEMPLATE_NO_DEFAULT_SFINAE_AUX_0(...)                              \
  , std::enable_if_t<                                                         \
        CPP_PP_CAT(QL_TEMPLATE_NO_DEFAULT_SFINAE_AUX_3_, __VA_ARGS__), int> = \
        0 > CPP_PP_IGNORE_CXX2A_COMPAT_END

#define QL_TEMPLATE_NO_DEFAULT_SFINAE_AUX_3_requires

#define QL_TEMPLATE_SFINAE_AUX_WHICH_(FIRST, ...) \
  CPP_PP_EVAL(CPP_PP_CHECK,                       \
              CPP_PP_CAT(CPP_TEMPLATE_SFINAE_PROBE_CONCEPT_, FIRST))

#define QL_TEMPLATE_NO_DEFAULT_SFINAE_AUX_(...)            \
  CPP_PP_CAT(QL_TEMPLATE_NO_DEFAULT_SFINAE_AUX_,           \
             QL_TEMPLATE_SFINAE_AUX_WHICH_(__VA_ARGS__, )) \
  (__VA_ARGS__)

#define QL_TEMPLATE_NO_DEFAULT(...) \
  CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN  \
  template <__VA_ARGS__ QL_TEMPLATE_NO_DEFAULT_SFINAE_AUX_
