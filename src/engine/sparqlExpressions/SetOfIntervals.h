// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SETOFINTERVALS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SETOFINTERVALS_H

#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "util/Exception.h"
#include "util/VectorWithMemoryLimit.h"

namespace ad_utility {

// A vector of pairs of <size_t, size_t> with the following semantics: It
// represents the union of the intervals [first, second) of the individual
// pairs. The intervals have to be pairwise disjoint and nonempty. They
// also have to be sorted in ascending order. The set additionally stores the
// `size` of the range [0, size) it refers to (typically the size of an
// `EvaluationContext`), all the intervals lie within this range.
struct SetOfIntervals {
  using Vec = std::vector<std::pair<size_t, size_t>>;
  Vec _intervals;

 private:
  size_t size_;

 public:
  // Construct from the `intervals` and the `size` of the range [0, size) the
  // set refers to. Throw if one of the `intervals` ends after `size`.
  SetOfIntervals(Vec intervals, size_t size);

  // Return the `size` of the range [0, size) this set refers to.
  size_t size() const { return size_; }

  // _________________________________________________________________________
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(SetOfIntervals, _intervals, size_)

  // Sort the intervals in ascending order and assert that they are indeed
  // disjoint and nonempty.
  static SetOfIntervals SortAndCheckDisjointAndNonempty(SetOfIntervals input);

  // Assert that the set is sorted, and simplify it by merging adjacent
  // intervals.
  static SetOfIntervals CheckSortedAndDisjointAndSimplify(
      const SetOfIntervals& input);

  // Compute the intersection of two sets of intervals, which must refer to
  // the same range.
  struct Intersection {
    SetOfIntervals operator()(SetOfIntervals A, SetOfIntervals B) const;
  };

  // Compute the union of two sets of intervals, which must refer to the same
  // range.
  struct Union {
    SetOfIntervals operator()(SetOfIntervals A, SetOfIntervals B) const;
  };

  // Compute the complement of a set of intervals within the range the set
  // refers to.
  struct Complement {
    SetOfIntervals operator()(SetOfIntervals s) const;
  };

  // Transform a SetOfIntervals to a std::vector<bool> of size `s.size()`
  // where the element at index i is true if and only if i is contained in the
  // set.
  // __________________________________________________________________________
  inline static std::vector<bool> toBitVector(const SetOfIntervals& s) {
    std::vector<bool> result(s.size(), false);
    for (const auto& [begin, end] : s._intervals) {
      std::fill(result.begin() + begin, result.begin() + end, true);
    }
    return result;
  }

  // Transform a `SetOfIntervals` into a vector of boolean `Id`s of size
  // `set.size()`. The i-th element is true iff i is contained in the set.
  inline static VectorWithMemoryLimit<Id> toIdVector(
      const SetOfIntervals& set,
      const VectorWithMemoryLimit<Id>::Allocator& allocator) {
    VectorWithMemoryLimit<Id> result{allocator};
    result.reserve(set.size());

    size_t previousEnd = 0;
    for (const auto& [begin, end] : set._intervals) {
      result.insert(result.end(), begin - previousEnd, Id::makeFromBool(false));
      result.insert(result.end(), end - begin, Id::makeFromBool(true));

      previousEnd = end;
    }

    result.insert(result.end(), set.size() - previousEnd,
                  Id::makeFromBool(false));

    return result;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SETOFINTERVALS_H
