//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VIEWS_H
#define QLEVER_VIEWS_H

#include <future>

#include "./Generator.h"

namespace ad_utility {

/// Takes a input-iterable and yields the elements of that view (no visible
/// effect). The iteration over the input view is done on a separate thread with
/// a buffer size of `blockSize`. This might speed up the computation when the
/// values of the input view are expensive to compute.
template <typename View>
cppcoro::generator<typename View::value_type> bufferedAsyncView(
    View view, uint64_t blockSize) {
  using value_type = typename View::value_type;
  auto it = view.begin();
  auto end = view.end();
  auto getNextBlock = [&it, &end, blockSize] {
    std::vector<value_type> buffer;
    buffer.reserve(blockSize);
    size_t i = 0;
    while (i < blockSize && it != end) {
      buffer.push_back(*it);
      ++it;
      ++i;
    }
    return buffer;
  };

  auto block = getNextBlock();
  auto future = std::async(std::launch::async, getNextBlock);
  while (true) {
    for (auto& element : block) {
      co_yield element;
    }
    block = future.get();
    if (block.empty()) {
      co_return;
    }
    future = std::async(std::launch::async, getNextBlock);
  }
}

/// Takes a view and yields the elements of the same view, but skips over
/// consecutive duplicates.
template <typename SortedView>
cppcoro::generator<typename SortedView::value_type> uniqueView(
    SortedView view) {
  auto it = view.begin();
  if (it == view.end()) {
    co_return;
  }
  auto previousValue = std::move(*it);
  auto previousValueCopy = previousValue;
  co_yield previousValueCopy;
  ++it;

  for (; it != view.end(); ++it) {
    if (*it != previousValue) {
      previousValue = std::move(*it);
      previousValueCopy = previousValue;
      co_yield previousValueCopy;
    }
  }
}
}  // namespace ad_utility

#endif  // QLEVER_VIEWS_H
