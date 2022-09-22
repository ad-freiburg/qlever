//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VALUEIDCOMPARATORS_H
#define QLEVER_VALUEIDCOMPARATORS_H

#include <utility>

#include "../engine/ResultTable.h"
#include "../index/Vocabulary.h"
#include "../util/OverloadCallOperator.h"
#include "./ValueId.h"

namespace valueIdComparators {
/// This enum encodes the different numeric comparators LessThan, LessEqual,
/// EQual, NotEqual, GreaterEqual, GreaterThan.
enum struct Comparison { LT, LE, EQ, NE, GE, GT };

/// Compares two `ValueId`s directly on the underlying representation. Note
/// that because the type bits are the most significant bits, all values of
/// the same `Datatype` will be adjacent to each other. Unsigned index types
/// are also ordered correctly. Signed integers are ordered as follows: first
/// the positive integers (>= 0) in order and then the negative integers (< 0)
/// in order. For doubles it is first the positive doubles in order, then the
/// negative doubles in reversed order. In detail it is [0.0 ... infinity, NaN,
/// -0.0, ... -infinity]. This is a direct consequence of comparing the bit
/// representation of these values as unsigned integers.
inline bool compareByBits(ValueId a, ValueId b) {
  return a.getBits() < b.getBits();
}

namespace detail {

/// Returns a comparator predicate `pred` that can be called with two arguments:
/// a `ValueId` and a templated `Value`. The predicate first calls the
/// `valueIdProjection` on the `ValueId` and then calls the `comparator` with
/// the `value` and the result of the projection. The predicate is symmetric, so
/// both `pred(ValueId, Value)` and `pred(Value, ValueId)` work as expected.
/// This function is useful for `std::equal_range` which expects both orders to
/// work. Example: `makeSymmetricComparator(&ValueId::getDatatype,
/// std::equal_to<>{})` returns a predicate that can be called with
/// `pred(Datatype, ValueId)` and pred(ValueId, Datatype)` and returns true iff
/// `Datatype` and the datatype of the Id are the same.
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

/// For a range of `ValueId`s that is represented by `[begin, end)` and has to
/// be sorted according to `compareByBits`, return the contiguous range of
/// `ValueIds` (as a pair of iterators) where the Ids have the `datatype`.
template <typename RandomIt>
inline std::pair<RandomIt, RandomIt> getRangeForDatatype(RandomIt begin,
                                                         RandomIt end,
                                                         Datatype datatype) {
  return std::equal_range(
      begin, end, datatype,
      detail::makeSymmetricComparator(&ValueId::getDatatype));
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
  explicit RangeFilter(Comparison comparison) : _comparison{comparison} {};
  Vec getResult() && { return std::move(_result); }

  // Let X be the set of numbers x for which x _comparison _value is true. The
  // given range for `addEqualRange` are numbers that are equal to `_value` (not
  // necessarily all of them). The function adds them if they are a subset of X.
  void addEqual(auto begin, auto end) {
    addImpl<Comparison::LE, Comparison::EQ, Comparison::GE>(begin, end);
  };

  // Analogous to `addEqual`.
  void addSmaller(auto begin, auto end) {
    addImpl<Comparison::LT, Comparison::LE, Comparison::NE>(begin, end);
  }

  // Analogous to `addEqual`.
  void addGreater(auto begin, auto end) {
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
    auto bits = std::bit_cast<uint64_t>(id.getDouble());
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

  AD_CHECK(beginOfNegatives >= beginOfNans);

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

  // If the comparison is "not equal" we also have to add the ranges for
  // non-matching datatypes.
  if (comparison == Comparison::NE) {
    auto rangeOfDoubles = getRangeForDatatype(begin, end, Datatype::Double);
    auto rangeOfInts = getRangeForDatatype(begin, end, Datatype::Int);
    AD_CHECK(rangeOfInts.first <= rangeOfDoubles.first);
    result.push_back({begin, rangeOfInts.first});
    result.push_back({rangeOfInts.second, rangeOfDoubles.first});
    result.push_back({rangeOfDoubles.second, end});
  }

  return result;
}

// This function is part of the implementation of `getRangesForId`. See the
// documentation there.
template <typename RandomIt>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForIndexTypes(
    RandomIt begin, RandomIt end, ValueId valueId, Comparison comparison) {
  auto [beginType, endType] =
      getRangeForDatatype(begin, end, valueId.getDatatype());

  RangeFilter<RandomIt> rangeFilter{comparison};
  auto [eqBegin, eqEnd] =
      std::equal_range(beginType, endType, valueId, &compareByBits);
  rangeFilter.addNotEqual(begin, beginType);
  rangeFilter.addSmaller(beginType, eqBegin);
  rangeFilter.addEqual(eqBegin, eqEnd);
  rangeFilter.addGreater(eqEnd, endType);
  rangeFilter.addNotEqual(endType, end);
  return std::move(rangeFilter).getResult();
}

// This function is part of the implementation of `getRangesForEqualIds`. See
// the documentation there.
template <typename RandomIt>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForIndexTypes(
    RandomIt begin, RandomIt end, ValueId valueIdBegin, ValueId valueIdEnd,
    Comparison comparison) {
  auto [beginOfType, endOfType] =
      getRangeForDatatype(begin, end, valueIdBegin.getDatatype());

  RangeFilter<RandomIt> rangeFilter{comparison};
  auto eqBegin =
      std::lower_bound(beginOfType, endOfType, valueIdBegin, &compareByBits);
  auto eqEnd =
      std::lower_bound(beginOfType, endOfType, valueIdEnd, &compareByBits);
  rangeFilter.addNotEqual(begin, beginOfType);
  rangeFilter.addSmaller(beginOfType, eqBegin);
  rangeFilter.addEqual(eqBegin, eqEnd);
  rangeFilter.addGreater(eqEnd, endOfType);
  rangeFilter.addNotEqual(endOfType, end);
  return std::move(rangeFilter).getResult();
}

// Helper function: Sort the non-overlapping ranges in `input` by the first
// element, remove the empty ranges, and merge  directly adjacent ranges
inline auto simplifyRanges =
    []<typename RandomIt>(std::vector<std::pair<RandomIt, RandomIt>> input) {
      // Eliminate empty ranges
      std::erase_if(input, [](const auto& p) { return p.first == p.second; });
      std::sort(input.begin(), input.end());
      if (input.empty()) {
        return input;
      }
      // Merge directly adjacent ranges.
      // TODO<joka921, C++20> use `std::ranges`
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
    };

}  // namespace detail

// The function returns the sequence of all IDs x (as a sequence of
// non-overlapping ranges in ascending order) with the following properties:
// 1. x is contained in the given range `begin, end`.
// 2. The condition x `comparison` value is fulfilled, where value is the value
// of `valueId`.
// 3. The datatype of x and `valueId` are compatible.
template <typename RandomIt>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForId(
    RandomIt begin, RandomIt end, ValueId valueId, Comparison comparison) {
  // This lambda enforces the invariants `non-empty` and `sorted`.
  switch (valueId.getDatatype()) {
    case Datatype::Double:
      return detail::simplifyRanges(detail::getRangesForIntsAndDoubles(
          begin, end, valueId.getDouble(), comparison));
    case Datatype::Int:
      return detail::simplifyRanges(detail::getRangesForIntsAndDoubles(
          begin, end, valueId.getInt(), comparison));
    case Datatype::Undefined:
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextRecordIndex:
      return detail::simplifyRanges(
          detail::getRangesForIndexTypes(begin, end, valueId, comparison));
  }
  AD_FAIL();
}

/// Similar to `getRangesForId` above but takes a range [valueIdBegin,
/// valueIdEnd) of Ids that are considered to be equal. `valueIdBegin` and
/// `valueIdEnd` must have the same datatype which must be one of the index
/// types `VocabIndex, LocalVocabIndex, ...`, otherwise an `AD_CHECK` will fail
/// at runtime.
template <typename RandomIt>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForEqualIds(
    RandomIt begin, RandomIt end, ValueId valueIdBegin, ValueId valueIdEnd,
    Comparison comparison) {
  // For an explanation of the case `valueIdBegin == valueIdEnd`, see the
  // documentation of a similar check in `compareIds` below.
  AD_CHECK(valueIdBegin <= valueIdEnd);
  // This lambda enforces the invariants `non-empty` and `sorted` and also
  // merges directly adjacent ranges.
  AD_CHECK(valueIdBegin.getDatatype() == valueIdEnd.getDatatype());
  switch (valueIdBegin.getDatatype()) {
    case Datatype::Double:
    case Datatype::Int:
    case Datatype::Undefined:
      AD_FAIL();
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextRecordIndex:
      return detail::simplifyRanges(detail::getRangesForIndexTypes(
          begin, end, valueIdBegin, valueIdEnd, comparison));
  }
  AD_FAIL();
}

namespace detail {

// This function is part of the implementation of `compareIds` (see below).
bool compareIdsImpl(ValueId a, ValueId b, auto comparator) {
  auto isNumeric = [](Id id) {
    return id.getDatatype() == Datatype::Double ||
           id.getDatatype() == Datatype::Int;
  };
  bool compatible =
      (a.getDatatype() == b.getDatatype()) || (isNumeric(a) && isNumeric(b));
  if (!compatible) {
    return false;
  }

  auto visitor = [comparator](const auto& aValue, const auto& bValue) -> bool {
    if constexpr (requires() { std::invoke(comparator, aValue, bValue); }) {
      return std::invoke(comparator, aValue, bValue);
    } else {
      AD_FAIL();
    }
  };

  return ValueId::visitTwo(visitor, a, b);
}
}  // namespace detail

// Compare two `ValueId`s by their actual value.
// Returns true iff the following conditions are met:
// 1. The condition aValue `comparison` bValue is fulfilled, where aValue and
// bValue are the values contained in `a` and `b`.
// 2. The datatype of `a` and `b` are compatible, s.t. the comparison in
// condition one is well-defined.
inline bool compareIds(ValueId a, ValueId b, Comparison comparison) {
  switch (comparison) {
    case Comparison::LT:
      return detail::compareIdsImpl(a, b, std::less<>());
    case Comparison::LE:
      return detail::compareIdsImpl(a, b, std::less_equal<>());
    case Comparison::EQ:
      return detail::compareIdsImpl(a, b, std::equal_to<>());
    case Comparison::NE:
      // IDs with incompatible datatypes are also considered "not equal".
      return !compareIds(a, b, Comparison::EQ);
    case Comparison::GE:
      return detail::compareIdsImpl(a, b, std::greater_equal<>());
    case Comparison::GT:
      return detail::compareIdsImpl(a, b, std::greater<>());
    default:
      AD_FAIL();
  }
}

/// Similar to `compareIds` above but takes a range [bBegin, bEnd) of Ids that
/// are considered to be equal.
inline bool compareWithEqualIds(ValueId a, ValueId bBegin, ValueId bEnd,
                                Comparison comparison) {
  // The case `bBegin == bEnd` happens when IDs from QLever's vocabulary are
  // compared to "pseudo"-IDs that represent words that are not part of the
  // vocabulary. In this case the ID `bBegin` is the ID of the smallest
  // vocabulary entry that is larger than the non-existing word that it
  // represents.
  AD_CHECK(bBegin <= bEnd);
  switch (comparison) {
    case Comparison::LT:
      return detail::compareIdsImpl(a, bBegin, std::less<>());
    case Comparison::LE:
      return detail::compareIdsImpl(a, bEnd, std::less<>());
    case Comparison::EQ:
      return detail::compareIdsImpl(a, bBegin, std::greater_equal<>()) &&
             detail::compareIdsImpl(a, bEnd, std::less<>());
    case Comparison::NE:
      // IDs with incompatible datatypes are also considered "not equal".
      return !compareWithEqualIds(a, bBegin, bEnd, Comparison::EQ);
    case Comparison::GE:
      return detail::compareIdsImpl(a, bBegin, std::greater_equal<>());
    case Comparison::GT:
      return detail::compareIdsImpl(a, bEnd, std::greater_equal<>());
    default:
      AD_FAIL();
  }
}

// ____________________________________________________________________________
inline bool compareIds(ValueId a, double b, Comparison comparison) {
  return compareIds(a, ValueId::makeFromDouble(b), comparison);
}

// ____________________________________________________________________________
inline bool compareIds(double a, ValueId b, Comparison comparison) {
  return compareIds(ValueId::makeFromDouble(a), b, comparison);
}

// ____________________________________________________________________________
inline bool compareIds(ValueId a, int64_t b, Comparison comparison) {
  return compareIds(a, ValueId::makeFromInt(b), comparison);
}

// ____________________________________________________________________________
inline bool compareIds(int64_t a, ValueId b, Comparison comparison) {
  return compareIds(ValueId::makeFromInt(a), b, comparison);
}

}  // namespace valueIdComparators

#endif  // QLEVER_VALUEIDCOMPARATORS_H
