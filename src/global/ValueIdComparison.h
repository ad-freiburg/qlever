// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_GLOBAL_VALUEIDCOMPARISON_H
#define QLEVER_SRC_GLOBAL_VALUEIDCOMPARISON_H

#include "util/EnumSize.h"

// The types that describe a comparison of `ValueId`s. They are in their own
// header, so that code that only names a comparison (for example a prefilter
// expression) doesn't have to include `global/ValueIdComparators.h`, which
// contains the (expensive to compile) comparison algorithms.
namespace valueIdComparators {
// This enum encodes the different numeric comparators LessThan, LessEqual,
// Equal, NotEqual, GreaterEqual, GreaterThan.
enum struct Comparison { LT, LE, EQ, NE, GE, GT };
QLEVER_ENUM_SIZE(Comparison, 6);

// The result of the comparisons is actually ternary because we sometimes
// distinguish between "false" and "type mismatch" (see the comment above
// `compareIds` in `global/ValueIdComparators.h` for details).
enum struct ComparisonResult { False, True, Undef };
}  // namespace valueIdComparators

#endif  // QLEVER_SRC_GLOBAL_VALUEIDCOMPARISON_H
