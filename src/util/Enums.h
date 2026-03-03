//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// This file contains several `enum`s that can be used for stronger typing
// and better readability instead of builtin types like `bool`.

#ifndef QLEVER_SRC_UTIL_ENUMS_H
#define QLEVER_SRC_UTIL_ENUMS_H

namespace ad_utility {

// Enum to distinguish between types that are conceptually const and types that
// are conceptually mutable. For example usages see the
// `IteratorForAccessOperator` and `IdTable` classes.
enum struct IsConst { True, False };
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ENUMS_H
