//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <functional>
#include <utility>

#include "backports/algorithm.h"

namespace ql {

// #if __cplusplus < 202002L || defined (_QLEVER_CPP_17)
#if true
namespace views {

// TODO Add support for projections
// TODO Add support for Sentinels (if we need them).
template <typename It, typename Comp = std::less<>>
void sort(It first, It last, Comp comp = {}) {
    std::sort(std::move(first), std::move(last), std::move(comp);
}

template <typename Range, typename Comp = std::less<>>
void sort(Range range, Comp comp = {}) {
    std::sort(std::begin(range), std::end(range), std::move(comp);
}

}  // namespace views
#else
using namespace std;
#endif
}  // namespace ql
