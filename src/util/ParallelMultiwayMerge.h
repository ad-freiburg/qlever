//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PARALLELMULTIWAYMERGE_H
#define QLEVER_PARALLELMULTIWAYMERGE_H

#include "util/AsyncStream.h"
#include "util/Generator.h"

namespace ad_utility {

template <typename T>
cppcoro::generator<std::vector<T>> lazyBinaryMerge(size_t blocksize,
                                                   auto&& range1, auto&& range2,
                                                   auto comparison) {
  std::vector<T> buffer;
  buffer.reserve(blocksize);
  auto it1 = std::ranges::begin(range1);
  auto it2 = std::ranges::begin(range2);
  auto end1 = std::ranges::end(range1);
  auto end2 = std::ranges::end(range2);

  if (it1 != end1 && it2 != end2) {
    while (true) {
      if (comparison(*it1, *it2)) {
        buffer.push_back(std::move(*it1));
        ++it1;
        if (it1 == end1) {
          break;
        }
      } else {
        buffer.push_back(std::move(*it2));
        ++it2;
        if (it2 == end1) {
          break;
        }
      }
      if (buffer.size() >= blocksize) {
        co_yield buffer;
        buffer.clear();
        buffer.reserve(blocksize);
      }
    }
  }

  for (auto& el : std::ranges::subrange{it1, end1}) {
    buffer.push_back(std::move(el));
    if (buffer.size() >= blocksize) {
      co_yield buffer;
      buffer.clear();
      buffer.reserve(blocksize);
    }
  }
  for (auto& el : std::ranges::subrange{it2, end2}) {
    buffer.push_back(std::move(el));
    if (buffer.size() >= blocksize) {
      co_yield buffer;
      buffer.clear();
      buffer.reserve(blocksize);
    }
  }
  if (!buffer.empty()) {
    co_yield buffer;
  }
}

template <typename T>
cppcoro::generator<std::vector<T>> batchToVector(size_t blocksize,
                                                 auto&& range) {
  std::vector<T> buffer;
  buffer.reserve(blocksize);
  for (auto& el : range) {
    buffer.push_back(std::move(el));
    if (buffer.size() >= blocksize) {
      co_yield buffer;
      buffer.clear();
      buffer.reserve(blocksize);
    }
  }
  if (!buffer.empty()) {
    co_yield buffer;
  }
}

template <typename T>
cppcoro::generator<std::vector<T>> parallelMultiwayMerge(size_t blocksize,
                                                         auto&& rangeOfRanges,
                                                         auto comparison) {
  AD_CORRECTNESS_CHECK(!rangeOfRanges.empty());
  if (rangeOfRanges.size() == 1) {
    return batchToVector<T>(blocksize, rangeOfRanges.front());
  } else if (rangeOfRanges.size() == 2) {
    return lazyBinaryMerge<T>(blocksize, rangeOfRanges[0], rangeOfRanges[1],
                              comparison);
  } else {
    size_t size = std::ranges::size(rangeOfRanges);
    size_t split = size / 2;
    auto beg = rangeOfRanges.begin();
    auto splitIt = beg + split;
    auto end = rangeOfRanges.end();
    return ad_utility::streams::runStreamAsync(
        lazyBinaryMerge<T>(
            blocksize,
            std::views::join(parallelMultiwayMerge<T>(
                blocksize, std::ranges::subrange{beg, splitIt}, comparison)),
            std::views::join(parallelMultiwayMerge<T>(
                blocksize, std::ranges::subrange{splitIt, end}, comparison)),
            comparison),
        2);
  }
}
}  // namespace ad_utility

#endif  // QLEVER_PARALLELMULTIWAYMERGE_H
