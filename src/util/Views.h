//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VIEWS_H
#define QLEVER_VIEWS_H

#include <future>

#include "./Generator.h"
#include "util/Log.h"

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
template <typename SortedView, typename ValueType = SortedView::value_type>
cppcoro::generator<ValueType> uniqueView(SortedView view) {
  size_t numInputs = 0;
  size_t numUnique = 0;
  auto it = view.begin();
  if (it == view.end()) {
    co_return;
  }
  ValueType previousValue = std::move(*it);
  ValueType previousValueCopy = previousValue;
  co_yield previousValueCopy;
  numInputs = 1;
  numUnique = 1;
  ++it;

  for (; it != view.end(); ++it) {
    ++numInputs;
    if (*it != previousValue) {
      previousValue = std::move(*it);
      previousValueCopy = previousValue;
      ++numUnique;
      co_yield previousValueCopy;
    }
  }
  LOG(INFO) << "Number of inputs to `uniqueView`: " << numInputs << '\n';
  LOG(INFO) << "Number of unique outputs of `uniqueView`: " << numUnique
            << std::endl;
}

template <typename UnderlyingRange>
struct OwningView : public std::ranges::view_interface<OwningView<UnderlyingRange>> {
 private:
  UnderlyingRange value_;
 public:
  OwningView(UnderlyingRange range) : value_{std::move(range)} {
  }
  auto begin() {return value_.begin();}
  auto end() {return value_.end();}
};

}  // namespace ad_utility

#endif  // QLEVER_VIEWS_H
