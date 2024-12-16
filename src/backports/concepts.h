//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <concepts/concepts.hpp>
#ifndef QLEVER_CPP_17
#include <concepts>
#endif

// Define the following macros:
// `QL_OPT_CONCEPT(arg)` which expands to `arg` in C++20 mode, and to nothing in
// C++17 mode. It can be used to easily opt out of concepts that are only used
// for documentation and increased safety and not for overload resolution.
// Example usage:
// `(QL_OPT_CONCEPT(std::view) auto x = someFunction();`
#ifdef QLEVER_CPP_17
#define QL_OPT_CONCEPT(arg)
#define QL_CONCEPT_OR_TYPENAME(arg) typename
#else
#define QL_OPT_CONCEPT(arg) arg
#define QL_CONCEPT_OR_TYPENAME(arg) arg
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
