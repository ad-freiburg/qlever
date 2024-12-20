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
#else
#define QL_CONCEPT_OR_NOTHING(...) __VA_ARGS__
#define QL_CONCEPT_OR_TYPENAME(...) __VA_ARGS__
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
