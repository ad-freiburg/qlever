// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_ZIPMERGEUNIQUEVIEWTESTHELPERS_H
#define QLEVER_ZIPMERGEUNIQUEVIEWTESTHELPERS_H
#include "util/views/ZipMergeUniqueView.h"

// We need to explicitly tell GTest how to print a `ZipMergeUniqueView`,
// because the latter inherits an implicit `operator<<` from `range-v3`s
// `view_interface`. That operator leads to a hard compile error if the value
// type of the view is not printable. Note: `::testing::PrintToString`
// already falls back to a hex byte representation for non-streamable types, so
// no SFINAE machinery is needed here.
namespace ad_utility {
template <typename V1, typename V2, typename Compare, typename Projection>
void PrintTo(const ZipMergeUniqueView<V1, V2, Compare, Projection>& view,
             std::ostream* os) {
  *os << '[';
  lazyStrJoin(os,
              ql::views::transform(ql::ranges::ref_view{view},
                                   [](const auto& elem) {
                                     return ::testing::PrintToString(elem);
                                   }),
              ",");
  *os << ']';
}
}  // namespace ad_utility
#endif  // QLEVER_ZIPMERGEUNIQUEVIEWTESTHELPERS_H
