// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "BooleanRangeExpressions.h"

namespace setOfIntervals {
// ___________________________________________________________________________
Set SortAndCheckInvariants(Set input) {
  auto cmp = [](const auto& a, const auto& b) { return a.first < b.first; };
  std::sort(input.begin(), input.end(), cmp);
  for (size_t i = 0; i < input.size(); ++i) {
    AD_CHECK(input[i].second > input[i].first);
  }

  for (size_t i = 1; i < input.size(); ++i) {
    AD_CHECK(input[i].first >= input[i - 1].second)
  }

  return input;
}

// ___________________________________________________________________________
Set Intersection::operator()(Set rangesA, Set rangesB) const {
  // First sort by the beginning of the interval
  rangesA = SortAndCheckInvariants(std::move(rangesA));
  rangesB = SortAndCheckInvariants(std::move(rangesB));

  Set result;
  auto iteratorA = rangesA.begin();
  auto iteratorB = rangesB.begin();

  // All values smaller than minIdxNotChecked are either already contained in
  // the result or will never become part of it. This variable will help us
  // enforce the invariant that the result intervals are disjoint.
  size_t minIdxNotChecked = 0;

  // Basically: List intersection extended to Intervals
  while (iteratorA < rangesA.end() && iteratorB < rangesB.end()) {
    // Invariant: All inputs before iteratorA/iteratorB have completely been
    // dealt with.
    auto& itSmaller =
        iteratorA->first < iteratorB->first ? iteratorA : iteratorB;
    auto& itGreaterEq =
        iteratorA->first < iteratorB->first ? iteratorB : iteratorA;
    if (itSmaller->second < itGreaterEq->first) {
      // The ranges do not overlap, due to the sorting and the disjoint property
      // within the inputs this means that the smaller (wrt starting index)
      // range is not part of the intersection.
      minIdxNotChecked = itSmaller->second;
      ++itSmaller;
    } else {
      // Calculate the relevant overlap.
      std::pair<size_t, size_t> nextOutput{
          std::max(minIdxNotChecked, itGreaterEq->first),
          std::min(itGreaterEq->second, itSmaller->second)};

      if (nextOutput.first < nextOutput.second) {
        // We actually contribute to the result.
        minIdxNotChecked = nextOutput.second;
        result.push_back(std::move(nextOutput));
      } else {
        // we can safely increase the smaller iterator which guarantees progress
        ++itSmaller;
        continue;
      }

      // Both of the following if conditions preserve our invariants, and at
      // least one of them always is true, which guarantess progress.
      if (minIdxNotChecked >= itSmaller->second) {
        ++itSmaller;
      }
      if (minIdxNotChecked >= itGreaterEq->second) {
        ++itGreaterEq;
      }
    }
  }

  return CheckSortedAndSimplify(SortAndCheckInvariants(std::move(result)));
}

// __________________________________________________________________________
Set Union::operator()(Set rangesA, Set rangesB) const {
  // First sort by the beginning of the interval
  rangesA = SortAndCheckInvariants(std::move(rangesA));
  rangesB = SortAndCheckInvariants(std::move(rangesB));
  Set result;
  auto iteratorA = rangesA.begin();
  auto iteratorB = rangesB.begin();

  // All values smaller than minIdxNotChecked are either already contained in
  // the result or will never become part of it. This variable will help us
  // enforce the invariant that the result intervals are disjoint.
  size_t minIdxNotChecked = 0;

  // Basically: Merging of sorted lists with intervals
  while (iteratorA < rangesA.end() && iteratorB < rangesB.end()) {
    auto& itSmaller =
        iteratorA->first < iteratorB->first ? iteratorA : iteratorB;
    auto& itGreaterEq =
        iteratorA->first < iteratorB->first ? iteratorB : iteratorA;

    if (itSmaller->second <= itGreaterEq->first) {
      // The ranges do not overlap, simply append the smaller one, if it was not
      // previously covered
      std::pair<size_t, size_t> nextOutput{
          std::max(minIdxNotChecked, itSmaller->first), itSmaller->second};
      // This check may fail, if the new interval is already covered
      if (nextOutput.first < nextOutput.second) {
        minIdxNotChecked = nextOutput.second;
        result.push_back(std::move(nextOutput));
      }
      ++itSmaller;
    } else {
      // The ranges overlap
      std::pair<size_t, size_t> nextOutput{
          std::max(minIdxNotChecked, itSmaller->first),
          std::max(itGreaterEq->second, itSmaller->second)};
      if (nextOutput.first < nextOutput.second) {
        minIdxNotChecked = nextOutput.second;
        result.push_back(std::move(nextOutput));
      }
      ++itSmaller;
      ++itGreaterEq;
    }
  }

  // Attach the remaining ranges, which do not have any overlap.
  auto attachRemainder = [&minIdxNotChecked, &result](auto iterator,
                                                      const auto& end) {
    for (; iterator < end; ++iterator) {
      std::pair<size_t, size_t> nextOutput{
          std::max(minIdxNotChecked, iterator->first), iterator->second};
      if (nextOutput.first < nextOutput.second) {
        minIdxNotChecked = nextOutput.second;
        result.push_back(std::move(nextOutput));
      }
    }
  };

  attachRemainder(iteratorA, rangesA.end());
  attachRemainder(iteratorB, rangesB.end());

  return CheckSortedAndSimplify(SortAndCheckInvariants(std::move(result)));
}

// __________________________________________________________________________
std::vector<bool> expandSet(const Set& a, size_t targetSize) {
  // Initialized to 0/false
  std::vector<bool> result(targetSize);
  for (const auto& [first, last] : a) {
    AD_CHECK(last <= targetSize);
    // TODO<joka921> Can we use a more efficient STL algorithm here?
    for (size_t i = first; i < last; ++i) {
      result[i] = true;
    }
  }
  return result;
}

// ___________________________________________________________________________
Set CheckSortedAndSimplify(const Set& input) {
  if (input.empty()) {
    return {};
  }
  auto current = input[0];
  Set result;
  for (size_t i = 1; i < input.size(); ++i) {
    AD_CHECK(input[i].first >= current.second);
    if (input[i].first == current.second) {
      current = {current.first, input[i].second};
    } else {
      result.push_back(current);
      current = input[i];
    }
  }
  result.push_back(current);
  return result;
}

}  // namespace setOfIntervals