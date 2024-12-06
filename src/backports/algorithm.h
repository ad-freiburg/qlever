//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <algorithm>
#include <functional>
#include <range/v3/all.hpp>
#include <utility>

namespace ql {

// #if __cplusplus < 202002L || defined (_QLEVER_CPP_17)
#if true
namespace ranges {
using namespace ::ranges;
}
#else
using namespace std::ranges;
#endif
}  // namespace ql
