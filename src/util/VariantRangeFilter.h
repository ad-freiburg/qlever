// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_VARIANTRANGEFILTER_H
#define QLEVER_SRC_UTIL_VARIANTRANGEFILTER_H

#include "backports/algorithm.h"
#include "util/TransparentFunctors.h"
#include "util/Views.h"

namespace ad_utility {

// Helper that filters a range, like `std::vector` which contains `std::variant`
// elements by a certain type `T` and returns a view of the contained values.
CPP_template(typename T, typename R)(
    requires ql::ranges::range<
        std::decay_t<R>>) auto filterRangeOfVariantsByType(R&& range) {
  return ad_utility::allView(AD_FWD(range)) |
         ql::views::filter(holdsAlternative<T>) | ql::views::transform(get<T>);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VARIANTRANGEFILTER_H
