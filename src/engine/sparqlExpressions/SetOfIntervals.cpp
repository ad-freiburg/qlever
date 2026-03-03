// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "engine/sparqlExpressions/SetOfIntervals.h"

#include "backports/algorithm.h"

namespace ad_utility {
// ___________________________________________________________________________
SetOfIntervals SetOfIntervals::SortAndCheckDisjointAndNonempty(
    SetOfIntervals input) {
  auto& vec = input._intervals;
  auto cmp = [](const auto& a, const auto& b) { return a.first < b.first; };
  std::sort(vec.begin(), vec.end(), cmp);
  for (size_t i = 0; i < vec.size(); ++i) {
    AD_CONTRACT_CHECK(vec[i].second > vec[i].first);
  }

  for (size_t i = 1; i < vec.size(); ++i) {
    AD_CONTRACT_CHECK(vec[i].first >= vec[i - 1].second);
  }

  return input;
}

// ___________________________________________________________________________
SetOfIntervals SetOfIntervals::Intersection::operator()(
    SetOfIntervals A, SetOfIntervals B) const {
  // First sort by the beginning of the interval
  A = SortAndCheckDisjointAndNonempty(std::move(A));
  B = SortAndCheckDisjointAndNonempty(std::move(B));

  SetOfIntervals result;
  auto itA = A._intervals.begin();
  auto itB = B._intervals.begin();

  // All values smaller than minIdxNotChecked are either already contained in
  // the result or will never become part of it. This variable helps us to
  // enforce the invariant that the result intervals are disjoint.
  size_t minIdxNotChecked = 0;

  // Compute the intersection using the "zipper" algorithm extended to
  // intervals.
  while (itA < A._intervals.end() && itB < B._intervals.end()) {
    // Invariant: All intervals before iteratorA and iteratorB have already been
    // completely dealt with.
    auto& itSmaller = itA->first < itB->first ? itA : itB;
    auto& itGreaterEq = itA->first < itB->first ? itB : itA;

    // Compute the intersection.
    std::pair<size_t, size_t> intersection{
        itGreaterEq->first, std::min(itGreaterEq->second, itSmaller->second)};

    // Truncate the intersection s.t. it lies completely after (including)
    // minIdxNotChecked. Also update minIdxNotChecked, which is then guaranteed
    // to be >= std::min(itGreaterEq->second, itSmaller->second)
    intersection.first = std::max(intersection.first, minIdxNotChecked);
    minIdxNotChecked = std::max(minIdxNotChecked, intersection.second);

    if (intersection.first < intersection.second) {
      result._intervals.push_back(std::move(intersection));
    }

    // At least one of the iterators is advanced, which guarantees progress.
    if (minIdxNotChecked >= itSmaller->second) {
      ++itSmaller;
    }
    if (minIdxNotChecked >= itGreaterEq->second) {
      ++itGreaterEq;
    }
  }

  return CheckSortedAndDisjointAndSimplify(
      SortAndCheckDisjointAndNonempty(std::move(result)));
}

// __________________________________________________________________________
SetOfIntervals SetOfIntervals::Union::operator()(SetOfIntervals A,
                                                 SetOfIntervals B) const {
  // First sort by the beginning of the interval
  A = SortAndCheckDisjointAndNonempty(std::move(A));
  B = SortAndCheckDisjointAndNonempty(std::move(B));
  SetOfIntervals result;
  auto itA = A._intervals.begin();
  auto itB = B._intervals.begin();

  // All values smaller than minIdxNotChecked are either already contained in
  // the result or will never become part of it. This variable helps us to
  // enforce the invariant that the result intervals are disjoint.
  size_t minIdxNotChecked = 0;

  // Truncate an interval such that it lies after (including) minIdxNotChecked.
  // Update minIdxNotChecked and append the interval to the result, if it did
  // not become empty by the truncation.
  auto truncateAndAppendInterval =
      [&minIdxNotChecked, &result](std::pair<size_t, size_t> interval) {
        interval.first = std::max(minIdxNotChecked, interval.first);
        minIdxNotChecked = std::max(minIdxNotChecked, interval.second);

        if (interval.first < interval.second) {
          result._intervals.push_back(interval);
        }
      };

  // Compute the union using the "zipper" algorithm extended to
  // intervals.
  while (itA < A._intervals.end() && itB < B._intervals.end()) {
    auto& itSmaller = itA->first < itB->first ? itA : itB;
    auto& itGreaterEq = itA->first < itB->first ? itB : itA;

    // If the intervals do not overlap, output the smaller one (unless
    // minIdxNotChecked >= the right end of the interval.
    if (itSmaller->second <= itGreaterEq->first) {
      truncateAndAppendInterval(*itSmaller);
      ++itSmaller;
      continue;
    }
    // The ranges overlap
    std::pair<size_t, size_t> nextUnion{
        itSmaller->first, std::max(itGreaterEq->second, itSmaller->second)};
    truncateAndAppendInterval(nextUnion);
    ;
    ++itSmaller;
    ++itGreaterEq;
  }

  // Attach the remaining intervals (which at this point either all come from A
  // or from B)
  std::for_each(itA, A._intervals.end(), truncateAndAppendInterval);
  std::for_each(itB, B._intervals.end(), truncateAndAppendInterval);

  return CheckSortedAndDisjointAndSimplify(
      SortAndCheckDisjointAndNonempty(std::move(result)));
}

// ___________________________________________________________________________
SetOfIntervals SetOfIntervals::CheckSortedAndDisjointAndSimplify(
    const SetOfIntervals& inputSet) {
  auto& inputVec = inputSet._intervals;
  if (inputVec.empty()) {
    return {};
  }
  auto current = inputVec[0];
  SetOfIntervals result;
  for (size_t i = 1; i < inputVec.size(); ++i) {
    AD_CONTRACT_CHECK(inputVec[i].first >= current.second);
    if (inputVec[i].first == current.second) {
      current = {current.first, inputVec[i].second};
    } else {
      result._intervals.push_back(current);
      current = inputVec[i];
    }
  }
  result._intervals.push_back(current);
  return result;
}

// ____________________________________________________________________________
SetOfIntervals SetOfIntervals::Complement::operator()(SetOfIntervals s) const {
  s = SortAndCheckDisjointAndNonempty(s);
  SetOfIntervals result;
  auto& intervals = result._intervals;

  size_t lastElement = 0;
  for (auto [begin, end] : s._intervals) {
    // The range that was previously false (not part of the set), now becomes
    // true.
    if (lastElement < begin) {
      intervals.emplace_back(lastElement, begin);
    }
    lastElement = end;
  }
  AD_CONTRACT_CHECK(lastElement <= s.upperBound);
  if (lastElement < SetOfIntervals::upperBound) {
    intervals.emplace_back(lastElement, SetOfIntervals::upperBound);
  }
  return result;
}

}  // namespace ad_utility
