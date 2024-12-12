//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

// Define the following macros:
// `QL_OPT_CONCEPT(arg)` which expands to `arg` in C++20 mode, and to nothing in
// C++17 mode. It can be used to easily opt out of concepts that are only used
// for documentation and increased safety and not for overload resolution.
// Example usage:
// `(QL_OPT_CONCEPT(std::view) auto x = someFunction();`
#ifdef QLEVER_CPP_17
#define QL_OPT_CONCEPT(arg)
#else
#define QL_OPT_CONCEPT(arg) arg
#endif
