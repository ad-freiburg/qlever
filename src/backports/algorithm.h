//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <algorithm>
#include <functional>
#include <range/v3/all.hpp>
#include <utility>

#include "backports/backportSelectionMacros.h"

namespace ql {

#ifdef QLEVER_CPP_17
namespace ranges {
using namespace ::ranges;
}
#else
using namespace std::ranges;
#endif
}  // namespace ql
