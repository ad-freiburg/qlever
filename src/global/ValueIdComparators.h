//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_VALUEIDCOMPARATORS_H
#define QLEVER_VALUEIDCOMPARATORS_H

#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "global/Id.h"
#include "index/LocalVocabContext.h"
#include "util/Algorithm.h"
#include "util/ComparisonWithNan.h"
#include "util/OverloadCallOperator.h"

namespace valueIdComparators {
// This enum encodes the different numeric comparators LessThan, LessEqual,
// Equal, NotEqual, GreaterEqual, GreaterThan.
enum struct Comparison { LT, LE, EQ, NE, GE, GT };

// This enum can be used to configure the behavior of the `compareIds` method
// below in the case when two `Id`s have incompatible datatypes (e.g.
// `VocabIndex` and a numeric type, or `Undefined` and any other type).
// For `AlwaysUndef`, the comparison will always be UNDEF, so for example `"x"`
// is neither smaller than nor greater than nor equal to `42`. This behavior is
// similar to the behavior of floating point `NaN` values. It is used for
// example for filter expressions like `FILTER (?x < 42)` which will filter out
// all string values. For `CompareByType` such pairs of `Id`s will be compared
// by the numeric order of their datatypes, so for example all `Undefined` IDs
// will be smaller than all IDs with a type different from `Undefined`. This
// behavior is used e.g. in `ORDER BY` expressions where we need a consistent
// partial ordering on all possible IDs.
enum struct ComparisonForIncompatibleTypes { CompareByType, AlwaysUndef };

// The result of the comparisons is actually ternary because we sometimes
// distinguish between "false" and "type mismatch" (see the comment directly
// above for details).
enum struct ComparisonResult { False, True, Undef };

// Convert the comparison result to a boolean value, assuming that it is not
// `Undef`.
inline bool toBoolNotUndef(ComparisonResult comparisonResult) {
  using enum ComparisonResult;
  AD_EXPENSIVE_CHECK(comparisonResult != Undef);
  return comparisonResult == True;
}

// Convert a bool to a ternary `ComparisonResult`.
inline ComparisonResult fromBool(bool b) {
  using enum ComparisonResult;
  return b ? True : False;
}

// Convert a `ComparisonResult` to a `ValueId`.
inline ValueId toValueId(ComparisonResult comparisonResult) {
  using enum ComparisonResult;
  switch (comparisonResult) {
    case False:
      return ValueId::makeFromBool(false);
    case True:
      return ValueId::makeFromBool(true);
    case Undef:
      return ValueId::makeUndefined();
  }
  AD_FAIL();
}

// Compares two `ValueId`s directly on the underlying representation. Note
// that because the type bits are the most significant bits, all values of
// the same `Datatype` will be adjacent to each other. Unsigned index types
// are also ordered correctly. Signed integers are ordered as follows: first
// the positive integers (>= 0) in order and then the negative integers (< 0)
// in order. For doubles it is first the positive doubles in order, then the
// negative doubles in reversed order. In detail it is [0.0 ... infinity, NaN,
// -0.0, ... -infinity]. This is a direct consequence of comparing the bit
// representation of these values as unsigned integers.
inline bool compareByBits(ValueId a, ValueId b) { return a < b; }

// Return true iff `threeWayResult comparison 0` holds, where `threeWayResult`
// is the result of a three-way comparison (that is, a value less than, equal
// to, or greater than zero).
inline bool comparisonHolds(int threeWayResult, Comparison comparison) {
  using enum Comparison;
  switch (comparison) {
    case LT:
      return threeWayResult < 0;
    case LE:
      return threeWayResult <= 0;
    case EQ:
      return threeWayResult == 0;
    case NE:
      return threeWayResult != 0;
    case GE:
      return threeWayResult >= 0;
    case GT:
      return threeWayResult > 0;
  }
  AD_FAIL();
}

// Overload of `comparisonHolds` above for a three-way comparison that is
// expressed as an ordering.
inline bool comparisonHolds(ql::strong_ordering ordering,
                            Comparison comparison) {
  int threeWayResult = ordering < 0 ? -1 : (ordering > 0 ? 1 : 0);
  return comparisonHolds(threeWayResult, comparison);
}

// Return true iff the semantic (that is, by string value) comparison of `Id`s
// of the given `datatype` requires the explicit comparison of the words that
// the `context` provides, instead of the much cheaper comparison of the `Id`s
// themselves. This is the case if and only if the index has a secondary
// vocabulary (see `index/vocabulary/SecondaryVocabulary.h`) and the `datatype`
// is one of the string types: the words of such a vocabulary are all positioned
// after all words of the main vocabulary in the order of the `Id`s (the
// *internal* order, see `ValueId::compareThreeWay`), no matter what they are.
inline bool needsSemanticComparison(const LocalVocabContext* context,
                                    Datatype datatype) {
  return context != nullptr && context->hasSecondaryVocabulary() &&
         ad_utility::contains(ValueId::stringTypes_, datatype);
}

// Return the smallest `Id` of type `Datatype::SecondaryVocabIndex`. In a range
// of `Id`s that is sorted according to `compareByBits`, all `Id`s that are
// positioned in the secondary vocabulary are greater than or equal to it, and
// all `Id`s that are positioned in the main vocabulary are smaller. NOTE: This
// also holds for an `Id` of type `LocalVocabIndex`, which is ordered by the
// position of its word, see `ValueId::compareThreeWay`.
inline ValueId smallestSecondaryVocabId() {
  return ValueId::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(0));
}

// For a range of `Id`s that is sorted according to `compareByBits`, return the
// first `Id` that is positioned in the secondary vocabulary, or `end` if there
// is none. NOTE: In contrast to `detail::getBeginOfSecondaryVocab` below, this
// does not first check whether the index has a secondary vocabulary at all, so
// it is also usable by a caller that has no `LocalVocabContext` at hand.
template <typename RandomIt>
inline RandomIt lowerBoundOfSecondaryVocabIds(RandomIt begin, RandomIt end) {
  return std::lower_bound(begin, end, smallestSecondaryVocabId(),
                          &compareByBits);
}

// The result of `getRangesForId` and `getRangesForEqualIds` below.
//
// The `Id`s that those functions search are sorted in the *internal* order (see
// `ValueId::compareThreeWay`), so the matching ones can be found by binary
// search as long as that order agrees with the semantic order that the
// comparison implements. That is no longer the case as soon as the index has a
// secondary vocabulary, because the words of that vocabulary are all positioned
// after all words of the main vocabulary, no matter what they are. Those `Id`s
// therefore have to be filtered one by one, which is what the second member is
// for.
template <typename RandomIt>
struct IdRangesAndSecondaryMatches {
  // The matching `Id`s that were found by binary search, as non-overlapping
  // ranges in ascending order.
  std::vector<std::pair<RandomIt, RandomIt>> ranges_;
  // The individual matching `Id`s that are positioned in the secondary
  // vocabulary, in ascending order. These are deliberately NOT part of
  // `ranges_`: they are the result of a linear scan and not of a binary search,
  // and a caller that cannot handle them one by one (in particular the
  // prefilters in `PrefilterExpressionIndex.cpp`, which only ever see the first
  // and the last `Id` of a whole block) has to notice that.
  std::vector<RandomIt> secondaryVocabMatches_;
};

namespace detail {

// Returns a comparator predicate `pred` that can be called with two arguments:
// a `ValueId` and a templated `Value`. The predicate first calls the
// `valueIdProjection` on the `ValueId` and then calls the `comparator` with
// the `value` and the result of the projection. The predicate is symmetric, so
// both `pred(ValueId, Value)` and `pred(Value, ValueId)` work as expected.
// This function is useful for `std::equal_range` which expects both orders to
// work. Example: `makeSymmetricComparator(&ValueId::getDatatype,
// std::equal_to<>{})` returns a predicate that can be called with
// `pred(Datatype, ValueId)` and pred(ValueId, Datatype)` and returns true iff
// `Datatype` and the datatype of the Id are the same.
template <typename Projection, typename Comparator = std::less<>>
auto makeSymmetricComparator(Projection valueIdProjection,
                             Comparator comparator = Comparator{}) {
  auto pred1 = [=](ValueId id, auto value) {
    return comparator(std::invoke(valueIdProjection, id), value);
  };
  auto pred2 = [=](auto value, ValueId id) {
    return comparator(value, std::invoke(valueIdProjection, id));
  };
  return ad_utility::OverloadCallOperator{pred1, pred2};
}
}  // namespace detail

// For a range of `ValueId`s that is represented by `[begin, end)` and has to
// be sorted according to `compareByBits`, return the contiguous range of
// `ValueIds` (as a pair of iterators) where the Ids have the `datatype`.
template <typename RandomIt>
inline std::pair<RandomIt, RandomIt> getRangeForDatatype(RandomIt begin,
                                                         RandomIt end,
                                                         Datatype datatype) {
  // In a sorted input, the IDs of the string types (`VocabIndex`,
  // `LocalVocabIndex`, and `SecondaryVocabIndex`) might be interleaved because
  // they logically all store strings. We therefore need the range where any of
  // those datatypes match.
  auto comparatorForVocabTypes = [](Datatype d1, Datatype d2) {
    auto containsStringType = [](Datatype d) {
      return ad_utility::contains(ValueId::stringTypes_, d);
    };
    if (containsStringType(d1) && containsStringType(d2)) {
      return false;
    }
    return d1 < d2;
  };
  auto comparator = detail::makeSymmetricComparator(&ValueId::getDatatype,
                                                    comparatorForVocabTypes);

  return std::equal_range(begin, end, datatype, comparator);
}

namespace detail {

// A helper type that stores a vector of iterator pairs (ranges) and an
// `Comparison` and factors out common logic.
template <typename RandomIt>
class RangeFilter {
 private:
  using Vec = std::vector<std::pair<RandomIt, RandomIt>>;
  Comparison _comparison;
  Vec _result;

 public:
  explicit RangeFilter(Comparison comparison) : _comparison{comparison} {}
  Vec getResult() && { return std::move(_result); }

  // Let X be the set of numbers x for which x _comparison _value is true. The
  // given range for `addEqualRange` are numbers that are equal to `_value` (not
  // necessarily all of them). The function adds them if they are a subset of X.
  template <typename T>
  void addEqual(T begin, T end) {
    addImpl<Comparison::LE, Comparison::EQ, Comparison::GE>(begin, end);
  }

  // Analogous to `addEqual`.
  template <typename T>
  void addSmaller(T begin, T end) {
    addImpl<Comparison::LT, Comparison::LE, Comparison::NE>(begin, end);
  }

  // Analogous to `addEqual`.
  template <typename T>
  void addGreater(T begin, T end) {
    addImpl<Comparison::GE, Comparison::GT, Comparison::NE>(begin, end);
  }

  // Analogous to `addEqual`. Used for IDs or numbers that are not equal, but
  // also not smaller or greater. This applies for example for `not a number`
  // and IDs that represent different incompatible datatypes.
  void addNotEqual(RandomIt begin, RandomIt end) {
    addImpl<Comparison::NE>(begin, end);
  }

 private:
  // Only add the pair `[begin, end)` to `_result` of `_comparison` is any of
  // the `acceptedComparisons`
  template <Comparison... acceptedComparisons>
  void addImpl(RandomIt begin, RandomIt end) {
    // We use `equal_to` instead of `==` to silence a warning in clang13.
    if ((... || std::equal_to<>{}(_comparison, acceptedComparisons))) {
      _result.emplace_back(begin, end);
    }
  }
};

// This function is part of the implementation of `getRangesForId`. See the
// documentation there.
template <typename RandomIt, typename Value>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForDouble(
    RandomIt begin, RandomIt end, Value value, Comparison comparison) {
  std::tie(begin, end) = getRangeForDatatype(begin, end, Datatype::Double);
  if (std::is_floating_point_v<Value> && std::isnan(value)) {
    // NaN compares "not equal" to all values, even to NaN itself.
    if (comparison == Comparison::NE) {
      return {{begin, end}};
    } else {
      return {};
    }
  }
  // In `ids` the negative number stand AFTER the positive numbers because of
  // the bitOrdering. First rotate the negative numbers to the beginning.
  auto doubleIdIsNegative = [](ValueId id) -> bool {
    auto bits = absl::bit_cast<uint64_t>(id.getDouble());
    return bits & ad_utility::bitMaskForHigherBits(1);
  };

  auto beginOfNans = std::lower_bound(
      begin, end, true, [&doubleIdIsNegative](ValueId id, bool) {
        return !doubleIdIsNegative(id) && !std::isnan(id.getDouble());
      });
  auto beginOfNegatives = std::lower_bound(
      begin, end, true, [&doubleIdIsNegative](ValueId id, bool) {
        return !doubleIdIsNegative(id);
      });

  AD_CONTRACT_CHECK(beginOfNegatives >= beginOfNans);

  auto comparatorLess = makeSymmetricComparator(&ValueId::getDouble);
  auto comparatorGreater =
      makeSymmetricComparator(&ValueId::getDouble, std::greater<>{});

  RangeFilter<RandomIt> rangeFilter{comparison};

  rangeFilter.addNotEqual(beginOfNans, beginOfNegatives);
  if (value > 0) {
    // The order is [smaller positives, equal, greater positives, nan, all
    // negatives].
    auto [eqBegin, eqEnd] =
        std::equal_range(begin, beginOfNans, value, comparatorLess);
    rangeFilter.addSmaller(begin, eqBegin);
    rangeFilter.addEqual(eqBegin, eqEnd);
    rangeFilter.addGreater(eqEnd, beginOfNans);
    rangeFilter.addSmaller(beginOfNegatives, end);
  } else if (value < 0) {
    // The order is [all positives, nan,  greater negatives, equal, smaller
    // negatives].
    auto [eqBegin, eqEnd] =
        std::equal_range(beginOfNegatives, end, value, comparatorGreater);
    rangeFilter.addGreater(begin, beginOfNans);
    rangeFilter.addGreater(beginOfNegatives, eqBegin);
    rangeFilter.addEqual(eqBegin, eqEnd);
    rangeFilter.addSmaller(eqEnd, end);
  } else if (value == 0) {
    auto positiveEnd =
        std::upper_bound(begin, beginOfNegatives, 0.0, comparatorLess);
    auto negativeEnd =
        std::upper_bound(beginOfNegatives, end, 0.0, comparatorGreater);
    // The order is [0.0, > 0, nan,  -0.0, , < 0.0]
    rangeFilter.addEqual(begin, positiveEnd);
    rangeFilter.addGreater(positiveEnd, beginOfNans);
    rangeFilter.addEqual(beginOfNegatives, negativeEnd);
    rangeFilter.addSmaller(negativeEnd, end);
  } else {
    AD_FAIL();
  }
  return std::move(rangeFilter).getResult();
}

// This function is part of the implementation of `getRangesForId`. See the
// documentation there.
template <typename RandomIt, typename Value>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForInt(
    RandomIt begin, RandomIt end, Value value,
    Comparison comparison = Comparison::EQ) {
  std::tie(begin, end) = getRangeForDatatype(begin, end, Datatype::Int);

  if (std::is_floating_point_v<Value> && std::isnan(value)) {
    // NaN compares "not equal" to all values, even to NaN itself.
    if (comparison == Comparison::NE) {
      return {{begin, end}};
    } else {
      return {};
    }
  }

  // Find the first int < 0. It stands after all ints >= 0 because of the bit
  // representation of the 2s-complement. The constant `true` and the unnamed
  // `bool` argument are required because of the interface of `std::lower_bound`
  auto firstNegative = std::lower_bound(
      begin, end, true, [](ValueId id, bool) { return id.getInt() >= 0; });

  RangeFilter<RandomIt> rangeFilter{comparison};
  auto predicate = makeSymmetricComparator(&ValueId::getInt);
  if (value >= 0) {
    auto [eqBegin, eqEnd] =
        std::equal_range(begin, firstNegative, value, predicate);
    // The order is [smaller positives, equal, greater positives, all
    // negatives].
    rangeFilter.addSmaller(begin, eqBegin);
    rangeFilter.addEqual(eqBegin, eqEnd);
    rangeFilter.addGreater(eqEnd, firstNegative);
    rangeFilter.addSmaller(firstNegative, end);
  } else if (value < 0) {
    auto [eqBegin, eqEnd] =
        std::equal_range(firstNegative, end, value, predicate);
    // The order is [all positives, smaller negatives, equal, greater
    // negatives].
    rangeFilter.addGreater(begin, firstNegative);
    rangeFilter.addSmaller(firstNegative, eqBegin);
    rangeFilter.addEqual(eqBegin, eqEnd);
    rangeFilter.addGreater(eqEnd, end);
  } else {
    AD_FAIL();
  }
  return std::move(rangeFilter).getResult();
}

// This function is part of the implementation of `getRangesForId`. See the
// documentation there.
template <typename RandomIt, typename Value>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForIntsAndDoubles(
    RandomIt begin, RandomIt end, Value value, Comparison comparison) {
  auto result = getRangesForDouble(begin, end, value, comparison);
  auto resultInt = getRangesForInt(begin, end, value, comparison);
  result.insert(result.end(), resultInt.begin(), resultInt.end());
  return result;
}

// Split the range `[begin, end)` of `Id`s of a string type (which has to be
// sorted according to `compareByBits`) into the `Id`s that are positioned in
// the main vocabulary and the `Id`s that are positioned in the secondary
// vocabulary, and return the iterator that separates the two. If the semantic
// comparison does not require the words at all (see `needsSemanticComparison`),
// then return `end`, so that the whole range is treated as the main
// vocabulary, which is exactly the behavior of an index without a secondary
// vocabulary.
template <typename RandomIt>
inline RandomIt getBeginOfSecondaryVocab(RandomIt begin, RandomIt end,
                                         Datatype datatype,
                                         const LocalVocabContext* context) {
  if (!needsSemanticComparison(context, datatype)) {
    return end;
  }
  return lowerBoundOfSecondaryVocabIds(begin, end);
}

// Return those `Id`s in `[begin, end)` (all of which are positioned in the
// secondary vocabulary) for which `id comparison valueId` holds semantically.
// This is a linear scan, because those `Id`s are not sorted by their string
// value, see `IdRangesAndSecondaryMatches`.
template <typename RandomIt>
inline std::vector<RandomIt> getSecondaryVocabMatches(
    RandomIt begin, RandomIt end, ValueId valueId, Comparison comparison,
    const LocalVocabContext* context) {
  std::vector<RandomIt> result;
  for (auto it = begin; it != end; ++it) {
    if (comparisonHolds(context->compareIdsSemantically(*it, valueId),
                        comparison)) {
      result.push_back(it);
    }
  }
  return result;
}

// This function is part of the implementation of `getRangesForId`. See the
// documentation there.
template <typename RandomIt>
inline IdRangesAndSecondaryMatches<RandomIt> getRangesForIndexTypes(
    RandomIt begin, RandomIt end, ValueId valueId, Comparison comparison,
    const LocalVocabContext* context) {
  auto [beginType, endType] =
      getRangeForDatatype(begin, end, valueId.getDatatype());
  auto beginSecondary = getBeginOfSecondaryVocab(
      beginType, endType, valueId.getDatatype(), context);

  // NOTE: This has to be initialized by a single expression, because `RandomIt`
  // is not necessarily default-constructible (`IteratorForAccessOperator`, for
  // example, is not).
  auto equalRange = [&]() -> std::pair<RandomIt, RandomIt> {
    if (!needsSemanticComparison(context, valueId.getDatatype())) {
      // The common case: the internal order of the `Id`s is the semantic order,
      // so the equal range can be found by comparing the `Id`s directly.
      return std::equal_range(beginType, endType, valueId, &compareByBits);
    }
    // The `Id`s in `[beginType, beginSecondary)` are all positioned in the main
    // vocabulary, and for those the internal order IS the semantic order, so
    // the range is sorted with respect to the comparator below and we may
    // binary-search it.
    //
    // NOTE: We have to search with the semantic comparator and must NOT compare
    // the `Id`s directly, because `valueId` itself may be positioned in the
    // secondary vocabulary, in which case it is greater than every `Id` of the
    // range in the internal order. It is equally wrong to search for the
    // *position* of `valueId` in the main vocabulary instead: that position
    // does not distinguish a word that is stored at it from a word that is
    // merely sorted there, but the two compare differently (see
    // `ValueId::compareThreeWay`, which orders a `LocalVocabIndex` whose word
    // is contained in neither vocabulary strictly before the `VocabIndex` of
    // its position). Collapsing the position to a bare `VocabIndex` `Id` loses
    // exactly that distinction and misplaces such entries by one position.
    //
    // NOTE: Every step of this binary search looks up the words of two `Id`s
    // and runs the collation of the vocabulary on them, which is expensive. See
    // the `TODO<joka921>` at `LocalVocabContext::compareIdsSemantically`.
    auto semanticLess = [context](ValueId a, ValueId b) {
      return context->compareIdsSemantically(a, b) < 0;
    };
    return std::equal_range(beginType, beginSecondary, valueId, semanticLess);
  }();

  RangeFilter<RandomIt> rangeFilter{comparison};
  rangeFilter.addSmaller(beginType, equalRange.first);
  rangeFilter.addEqual(equalRange.first, equalRange.second);
  rangeFilter.addGreater(equalRange.second, beginSecondary);
  return {std::move(rangeFilter).getResult(),
          getSecondaryVocabMatches(beginSecondary, endType, valueId, comparison,
                                   context)};
}

// Analogous to `getSecondaryVocabMatches` above, but for a range
// `[boundBegin, boundEnd)` of positions in the main vocabulary that are
// considered equal, see `getRangesForEqualIds`. Both bounds have to be `Id`s of
// type `VocabIndex`.
//
// NOTE: Those two bounds only describe a *position* in the main vocabulary, and
// not the word behind it. If the word of an `Id` of the secondary vocabulary
// would be sorted at exactly that position, then the two cannot be
// distinguished here, and we report the `Id` of the secondary vocabulary as the
// greater one.
// TODO<joka921> Pass the word along, so that this corner case can be decided
// correctly. This is currently moot, because nothing constructs such a range
// for a word that is not in the main vocabulary anymore.
template <typename RandomIt>
inline std::vector<RandomIt> getSecondaryVocabMatches(
    RandomIt begin, RandomIt end, ValueId boundBegin, ValueId boundEnd,
    Comparison comparison, const LocalVocabContext* context) {
  std::vector<RandomIt> result;
  for (auto it = begin; it != end; ++it) {
    auto position = ValueId::makeFromVocabIndex(
        context->getSemanticPositionInMainVocab(*it).first);
    int threeWayResult = compareByBits(position, boundBegin) ? -1
                         : compareByBits(position, boundEnd) ? 0
                                                             : 1;
    if (comparisonHolds(threeWayResult, comparison)) {
      result.push_back(it);
    }
  }
  return result;
}

// This function is part of the implementation of `getRangesForEqualIds`. See
// the documentation there.
template <typename RandomIt>
inline IdRangesAndSecondaryMatches<RandomIt> getRangesForIndexTypes(
    RandomIt begin, RandomIt end, ValueId valueIdBegin, ValueId valueIdEnd,
    Comparison comparison, const LocalVocabContext* context) {
  auto [beginOfType, endOfType] =
      getRangeForDatatype(begin, end, valueIdBegin.getDatatype());
  auto beginSecondary = getBeginOfSecondaryVocab(
      beginOfType, endOfType, valueIdBegin.getDatatype(), context);

  // Just as in the overload above, the bounds have to be translated into
  // positions in the main vocabulary, because the `Id`s that we binary-search
  // are all positioned there. For an `Id` of type `VocabIndex` this translation
  // is the identity, so it only has an effect for the (currently
  // hypothetical) case of a bound that is positioned in the secondary
  // vocabulary.
  auto toPositionInMainVocab = [context](ValueId valueId) {
    if (!needsSemanticComparison(context, valueId.getDatatype())) {
      return valueId;
    }
    return ValueId::makeFromVocabIndex(
        context->getSemanticPositionInMainVocab(valueId).first);
  };
  auto boundBegin = toPositionInMainVocab(valueIdBegin);
  auto boundEnd = toPositionInMainVocab(valueIdEnd);

  RangeFilter<RandomIt> rangeFilter{comparison};
  auto eqBegin =
      std::lower_bound(beginOfType, beginSecondary, boundBegin, &compareByBits);
  auto eqEnd =
      std::lower_bound(beginOfType, beginSecondary, boundEnd, &compareByBits);
  rangeFilter.addSmaller(beginOfType, eqBegin);
  rangeFilter.addEqual(eqBegin, eqEnd);
  rangeFilter.addGreater(eqEnd, beginSecondary);
  return {std::move(rangeFilter).getResult(),
          getSecondaryVocabMatches(beginSecondary, endOfType, boundBegin,
                                   boundEnd, comparison, context)};
}

// Helper function: Sort the non-overlapping ranges in `input` by the first
// element, remove the empty ranges, and merge  directly adjacent ranges
template <typename RandomIt>
auto simplifyRanges(std::vector<std::pair<RandomIt, RandomIt>> input,
                    bool removeEmptyRanges = true) {
  if (removeEmptyRanges) {
    // Eliminate empty ranges
    ql::erase_if(input, [](const auto& p) { return p.first == p.second; });
  }
  std::sort(input.begin(), input.end());
  if (input.empty()) {
    return input;
  }
  // Merge directly adjacent ranges.
  // TODO<joka921, C++20> use `ql::ranges`
  decltype(input) result;
  result.push_back(input.front());
  for (auto it = input.begin() + 1; it != input.end(); ++it) {
    if (it->first == result.back().second) {
      result.back().second = it->second;
    } else {
      result.push_back(*it);
    }
  }
  return result;
}

}  // namespace detail

// Return all IDs x with the following properties:
// 1. x is contained in the given range `begin, end`.
// 2. The condition x `comparison` value is fulfilled, where value is the value
// of `valueId`.
// 3. The datatype of x and `valueId` are compatible.
//
// The result has two components, see `IdRangesAndSecondaryMatches`: the IDs
// that were found by binary search (as a sequence of non-overlapping ranges in
// ascending order), and the individual IDs of a secondary vocabulary that
// match. The latter are only ever non-empty if the `context` belongs to an
// index that has such a vocabulary.
//
// The `context` is the one of the index that the IDs belong to, and it is
// required for the semantic comparison of the words of a secondary vocabulary
// (see `LocalVocabContext::compareIdsSemantically`). It may be `nullptr`, in
// which case the caller guarantees that none of the IDs is positioned in such a
// vocabulary.
//
// When setting the flag argument `removeEmptyRanges` to false, empty ranges
// [`begin`, `end`] where `begin` is equal to `end` will not be discarded.
template <typename RandomIt>
inline IdRangesAndSecondaryMatches<RandomIt> getRangesForId(
    RandomIt begin, RandomIt end, ValueId valueId, Comparison comparison,
    const LocalVocabContext* context, bool removeEmptyRanges = true) {
  // A helper for the datatypes that a secondary vocabulary cannot contain, so
  // that they never have any matches of the second kind.
  auto onlyRanges = [](std::vector<std::pair<RandomIt, RandomIt>> ranges) {
    return IdRangesAndSecondaryMatches<RandomIt>{std::move(ranges), {}};
  };
  switch (valueId.getDatatype()) {
    case Datatype::Double:
      return onlyRanges(detail::simplifyRanges(
          detail::getRangesForIntsAndDoubles(begin, end, valueId.getDouble(),
                                             comparison),
          removeEmptyRanges));
    case Datatype::Int:
      return onlyRanges(
          detail::simplifyRanges(detail::getRangesForIntsAndDoubles(
                                     begin, end, valueId.getInt(), comparison),
                                 removeEmptyRanges));
    case Datatype::Undefined:
      // For the evaluation of FILTERs, comparisons that involve undefined
      // values are always false.
      return {};
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::SecondaryVocabIndex:
    case Datatype::WordVocabIndex:
    case Datatype::TextRecordIndex:
      // TODO<joka921> for the `EncodedVal` type, the behavior is only correct
      // for equality, because there also might be regular IRIs (of type
      // `[Local]VocabIndex` that are greater than or less than the encoded IRI.
      // (This also goes for the other way round).
    case Datatype::EncodedVal:
    case Datatype::Bool:
    case Datatype::Date:
    case Datatype::GeoPoint:
    case Datatype::BlankNodeIndex: {
      // For `Date` the trivial comparison via bits is also correct.
      auto result = detail::getRangesForIndexTypes(begin, end, valueId,
                                                   comparison, context);
      result.ranges_ =
          detail::simplifyRanges(std::move(result.ranges_), removeEmptyRanges);
      return result;
    }
  }
  AD_FAIL();
}

// Similar to `getRangesForId` above but takes a range [valueIdBegin,
// valueIdEnd) of Ids that are considered to be equal. `valueIdBegin` and
// `valueIdEnd` must have the same datatype which must be one of the index
// types `VocabIndex, LocalVocabIndex, ...`, otherwise an `AD_CONTRACT_CHECK`
// will fail at runtime. For the `context`, see `getRangesForId` above.
template <typename RandomIt>
inline IdRangesAndSecondaryMatches<RandomIt> getRangesForEqualIds(
    RandomIt begin, RandomIt end, ValueId valueIdBegin, ValueId valueIdEnd,
    Comparison comparison, const LocalVocabContext* context) {
  // For an explanation of the case `valueIdBegin == valueIdEnd`, see the
  // documentation of a similar check in `compareIds` below.
  AD_CONTRACT_CHECK(valueIdBegin <= valueIdEnd);
  // This lambda enforces the invariants `non-empty` and `sorted` and also
  // merges directly adjacent ranges.
  auto typeBegin = valueIdBegin.getDatatype();
  auto typeEnd = valueIdEnd.getDatatype();
  AD_CONTRACT_CHECK(typeBegin == typeEnd ||
                    (ad_utility::contains(Id::stringTypes_, typeBegin) &&
                     ad_utility::contains(Id::stringTypes_, typeEnd)));
  switch (valueIdBegin.getDatatype()) {
    case Datatype::Double:
    case Datatype::Int:
    case Datatype::Bool:
    case Datatype::Undefined:
    case Datatype::Date:
    case Datatype::GeoPoint:
    case Datatype::BlankNodeIndex:
      AD_FAIL();
    // TODO<joka921> check what the correct behavior is here.
    case Datatype::EncodedVal:
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::SecondaryVocabIndex:
    case Datatype::WordVocabIndex:
    case Datatype::TextRecordIndex: {
      auto result = detail::getRangesForIndexTypes(
          begin, end, valueIdBegin, valueIdEnd, comparison, context);
      result.ranges_ = detail::simplifyRanges(std::move(result.ranges_));
      return result;
    }
  }
  AD_FAIL();
}

namespace detail {

// Determine whether the two datatypes can be compared. If they cannot be
// compared, a comparison is always an `expression error` (term from the SPARQL
// standard) which we currently handle by all the comparisons returning `false`.
inline bool areTypesCompatible(Datatype typeA, Datatype typeB) {
  auto isNumeric = [](Datatype type) {
    return type == Datatype::Double || type == Datatype::Int;
  };
  // TODO<joka921> Make this work for the WordIndex also.
  auto isString = [](Datatype type) {
    return ad_utility::contains(ValueId::stringTypes_, type);
  };
  auto isUndefined = [](Datatype type) { return type == Datatype::Undefined; };
  // Note: Undefined values cannot be compared to other undefined values.
  return (!isUndefined(typeA)) &&
         ((typeA == typeB) || (isNumeric(typeA) && isNumeric(typeB)) ||
          (isString(typeA) && isString(typeB)));
}

// This function is part of the implementation of `compareIds` (see below).
//
// The order that this function implements is the *semantic* order, that is, two
// words are compared by their string value, which is what SPARQL requires. That
// order is deliberately NOT the order of the `Id`s themselves (call the latter
// the *internal* order, see `ValueId::compareThreeWay`), which is the order in
// which the index scans emit their `Id`s: in the internal order the words of a
// secondary vocabulary (see `index/vocabulary/SecondaryVocabulary.h`) are all
// greater than all words of the main vocabulary, no matter what they are.
//
// The two orders coincide for an index that has no secondary vocabulary, so in
// that case we take the much cheaper shortcut of comparing the `Id`s. For an
// index that has one, the words have to be compared explicitly, which is what
// the `context` is for. That case covers both an `Id` of type
// `SecondaryVocabIndex` and an `Id` of type `LocalVocabIndex` whose word
// happens to be stored in the secondary vocabulary, see the note in
// `index/LocalVocabEntry.h`.
//
// NOTE: A `context` of `nullptr` means "this index has no secondary
// vocabulary", so the caller has to pass one as soon as it might see `Id`s of
// an index that has one.
//
// NOTE: The following two deviations from the SPARQL semantics are pre-existing
// and unrelated to the secondary vocabulary; they are tracked separately:
// 1. Comparing an `Id` of type `EncodedVal` to a word of one of the
//    vocabularies is not correct, not even for equality, because an
//    encoded IRI is ordered by its encoding and not by its string value, see
//    issue #2448. The same holds for an `Id` of type `LocalVocabIndex` whose
//    word is an encoded IRI: it is not even consistently ordered with respect
//    to the *internal* order, see the note at `ValueId::compareThreeWay`.
// 2. Comparing literals with different datatypes blurs the distinction between
//    `sameTerm` and `=`, which is also why `!(A = B)` and `A != B` currently
//    behave differently, see issue #2405.
template <ComparisonForIncompatibleTypes comparisonForIncompatibleTypes =
              ComparisonForIncompatibleTypes::AlwaysUndef,
          typename Comparator>
ComparisonResult compareIdsImpl(ValueId a, ValueId b, Comparator comparator,
                                const LocalVocabContext* context) {
  Datatype typeA = a.getDatatype();
  Datatype typeB = b.getDatatype();
  using enum ComparisonResult;
  if (!areTypesCompatible(typeA, typeB)) {
    using enum ComparisonForIncompatibleTypes;
    if constexpr (comparisonForIncompatibleTypes == CompareByType) {
      return fromBool(comparator(a.getDatatype(), b.getDatatype()));
    } else {
      static_assert(comparisonForIncompatibleTypes == AlwaysUndef);
      return ComparisonResult::Undef;
    }
  }

  // If the index has a secondary vocabulary, then the internal order of the
  // `Id`s is not the semantic order, so the words have to be compared
  // explicitly, see the note above.
  if (needsSemanticComparison(context, typeA)) {
    // The types are compatible and `typeA` is a string type, so `typeB` is one
    // as well.
    AD_CORRECTNESS_CHECK(ad_utility::contains(ValueId::stringTypes_, typeB));
    return fromBool(
        std::invoke(comparator, context->compareIdsSemantically(a, b), 0));
  }

  // For the string types the ordinary comparison on `ValueId`s already does the
  // right thing here, because the index has no secondary vocabulary (see
  // above) and hence the internal order is the semantic order. NOTE: This is
  // not only a shortcut, but also the only correct way to compare an `Id` of
  // type `LocalVocabIndex`, whose bits are a pointer and not a value, and to
  // compare two `Id`s of different string types, which the visitor below
  // cannot do at all. The types are compatible, so if one of them is a string
  // type, then both are.
  if (ad_utility::contains(ValueId::stringTypes_, typeA)) {
    AD_CORRECTNESS_CHECK(ad_utility::contains(ValueId::stringTypes_, typeB));
    return fromBool(std::invoke(comparator, a, b));
  }

  // TODO<joka921> We currently don't perform correct comparisons (other than
  // equality) for the `EncodedVal` datatype. This will be added in a future
  // PR. This is okay for now, as 1. the maintainer of an inex has to explicitly
  // activate the encoding and 2. there are only few queries where the semantic
  // ordering of IRIs is actually important.

  // If both are geo points, compare the raw IDs.
  if (a.getDatatype() == Datatype::GeoPoint &&
      b.getDatatype() == Datatype::GeoPoint) {
    return fromBool(std::invoke(comparator, a.getBits(), b.getBits()));
  }

  auto visitor = [comparator, &a, &b](const auto& aValue,
                                      const auto& bValue) -> ComparisonResult {
    using A = std::decay_t<decltype(aValue)>;
    using B = std::decay_t<decltype(bValue)>;
    if constexpr (ranges::invocable<Comparator, A, B>) {
      return fromBool(std::invoke(comparator, aValue, bValue));
    } else {
      static_assert((!std::is_same_v<A, B>) ||
                    ad_utility::SameAsAny<A, LocalVocabIndex, GeoPoint,
                                          Id::UndefinedType>);
      AD_LOG_ERROR << "Comparison not implemented for types "
                   << toString(a.getDatatype()) << " and "
                   << toString(b.getDatatype()) << std::endl;
      AD_FAIL();
    }
  };
  return a.visit([&visitor, &b](const auto& aValue) {
    return b.visit([&visitor, aValue](const auto& bValue) -> ComparisonResult {
      return visitor(aValue, bValue);
    });
  });
}
}  // namespace detail

// Compare two `ValueId`s by their actual value.
// Returns true iff the following conditions are met:
// 1. The condition aValue `comparison` bValue is fulfilled, where aValue and
// bValue are the values contained in `a` and `b`.
// 2. The datatype of `a` and `b` are compatible, s.t. the comparison in
// condition one is well-defined.
// For the definition of the template parameter `comparisonForIncompatibleTypes`
// see the documentation of the enum `ComparisonForIncompatibleTypes` above. For
// the `context`, which is required for the semantic comparison of the words of
// a secondary vocabulary, see `detail::compareIdsImpl` above.
template <ComparisonForIncompatibleTypes comparisonForIncompatibleTypes =
              ComparisonForIncompatibleTypes::AlwaysUndef>
inline ComparisonResult compareIds(ValueId a, ValueId b, Comparison comparison,
                                   const LocalVocabContext* context) {
  // A helper lambda to factor out common code
  auto compare = [&](auto comparator) {
    // For the `compareByType` mode, which is used by ORDER BY, we also need a
    // proper order of NaN values to not run into undefined behavior.
    if constexpr (comparisonForIncompatibleTypes ==
                  ComparisonForIncompatibleTypes::CompareByType) {
      return detail::compareIdsImpl<comparisonForIncompatibleTypes>(
          a, b, ad_utility::makeComparatorForNans(comparator), context);
    } else {
      return detail::compareIdsImpl<comparisonForIncompatibleTypes>(
          a, b, comparator, context);
    }
  };

  using enum Comparison;
  switch (comparison) {
    case LT:
      return compare(std::less{});
    case LE:
      return compare(std::less_equal{});
    case EQ:
      return compare(std::equal_to{});
    case NE:
      return compare(std::not_equal_to{});
    case GE:
      return compare(std::greater_equal{});
    case GT:
      return compare(std::greater{});
    default:
      AD_FAIL();
  }
}

// Similar to `compareIds` above but takes a range [bBegin, bEnd) of Ids that
// are considered to be equal.
template <ComparisonForIncompatibleTypes comparisonForIncompatibleTypes =
              ComparisonForIncompatibleTypes::AlwaysUndef>
inline ComparisonResult compareWithEqualIds(ValueId a, ValueId bBegin,
                                            ValueId bEnd, Comparison comparison,
                                            const LocalVocabContext* context) {
  // The case `bBegin == bEnd` happens when IDs from QLever's vocabulary are
  // compared to "pseudo"-IDs that represent words that are not part of the
  // vocabulary. In this case the ID `bBegin` is the ID of the smallest
  // vocabulary entry that is larger than the non-existing word that it
  // represents.
  AD_CONTRACT_CHECK(bBegin <= bEnd);

  static constexpr auto mode = comparisonForIncompatibleTypes;

  // The comparison for `equal` is also used for the `not equal` case, so we
  // factor it out.
  auto compareEqual = [&]() {
    return toBoolNotUndef(detail::compareIdsImpl<mode>(
               a, bBegin, std::greater_equal<>(), context)) &&
           toBoolNotUndef(
               detail::compareIdsImpl<mode>(a, bEnd, std::less<>(), context));
  };
  using enum Comparison;
  switch (comparison) {
    case LT:
      return detail::compareIdsImpl<mode>(a, bBegin, std::less<>(), context);
    case LE:
      return detail::compareIdsImpl<mode>(a, bEnd, std::less<>(), context);
    case EQ: {
      if constexpr (mode == ComparisonForIncompatibleTypes::AlwaysUndef) {
        bool typesAreCompatible =
            detail::areTypesCompatible(a.getDatatype(), bBegin.getDatatype());
        return typesAreCompatible ? fromBool(compareEqual())
                                  : ComparisonResult::Undef;
      } else {
        return fromBool(compareEqual());
      }
    }
    case NE: {
      // If the datatypes are not compatible then we always yield `false`. This
      // is the correct behavior for SPARQL filters where this is called an
      // `expression error`.
      bool typesAreCompatible =
          detail::areTypesCompatible(a.getDatatype(), bBegin.getDatatype());
      if constexpr (mode == ComparisonForIncompatibleTypes::AlwaysUndef) {
        return typesAreCompatible ? fromBool(!compareEqual())
                                  : ComparisonResult::Undef;
      } else {
        return fromBool(!typesAreCompatible || !compareEqual());
      }
    }
    case GE:
      return detail::compareIdsImpl<mode>(a, bBegin, std::greater_equal<>(),
                                          context);
    case GT:
      return detail::compareIdsImpl<mode>(a, bEnd, std::greater_equal<>(),
                                          context);
    default:
      AD_FAIL();
  }
}

}  // namespace valueIdComparators

#endif  // QLEVER_VALUEIDCOMPARATORS_H
