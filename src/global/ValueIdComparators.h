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
/// std::equal_to<>{})` returs a predicate that can be called with
/// `pred(Datatype, ValueId)` and pred(ValueId, Datatype)` and returns true iff
/// the `Datatype` and the datatype of the Id are the same.
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
  explicit RangeFilter(Comparison order) : _comparison{order} {};
  Vec getResult() && { return std::move(_result); }

  // Semantics: The elements in the range [begin, end) are all smaller than the
  // value we are comparing against, so this range is only to be included in the
  // result, if our ordering is LE, LT or NE (similar for the other `addXXX`
  // functions).
  void addSmaller(auto begin, auto end) {
    addImpl<Comparison::LT, Comparison::LE, Comparison::NE>(begin, end);
  }
  void addEqual(auto begin, auto end) {
    addImpl<Comparison::LE, Comparison::EQ, Comparison::GE>(begin, end);
  };
  void addGreater(auto begin, auto end) {
    addImpl<Comparison::GE, Comparison::GT, Comparison::NE>(begin, end);
  };

  void addNan(RandomIt begin, RandomIt end) {
    addImpl<Comparison::NE>(begin, end);
  }

 private:
  // Only add the pair `[begin, end)` to `_result` of `_comparison` is any of
  // the `acceptedComparisons`
  template <Comparison... acceptedComparisons>
  void addImpl(RandomIt begin, RandomIt end) {
    if ((... || (_comparison == acceptedComparisons))) {
      _result.emplace_back(begin, end);
    }
  }
};

// In a range of `ValueId`s, that is sorted according to `compareByBits` and
// denoted by `begin` and `end` return the contiguous ranges (as iterator pairs)
// of `ValueId`s where `id.getDatatype() == Double` and `id.getDouble() CMP
// value`, where `CMP` is the comparator associated with the `comparison`, so
// `<=` for `Comparison::LE` etc.
template <typename RandomIt, typename Value>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForDouble(
    RandomIt begin, RandomIt end, Value value,
    Comparison comparison = Comparison::EQ) {
  if (std::is_floating_point_v<Value> && std::isnan(value)) {
    // NaNs are non-comparable.
    return {};
  }
  std::tie(begin, end) = getRangeForDatatype(begin, end, Datatype::Double);
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

  RangeFilter<RandomIt> rangeManager{comparison};

  rangeManager.addNan(beginOfNans, beginOfNegatives);
  if (value > 0) {
    auto [eqBegin, eqEnd] =
        std::equal_range(begin, beginOfNans, value, comparatorLess);
    rangeManager.addEqual(eqBegin, eqEnd);
    rangeManager.addSmaller(begin, eqBegin);
    rangeManager.addSmaller(beginOfNegatives, end);
    rangeManager.addGreater(eqEnd, beginOfNans);
  } else if (value < 0) {
    // The negative range is inverted (greater values first)
    auto [eqBegin, eqEnd] =
        std::equal_range(beginOfNegatives, end, value, comparatorGreater);
    rangeManager.addSmaller(eqEnd, end);
    rangeManager.addGreater(begin, beginOfNans);
    rangeManager.addGreater(beginOfNegatives, eqBegin);
  } else if (value == 0) {
    auto positiveEnd =
        std::upper_bound(begin, beginOfNegatives, 0.0, comparatorLess);
    auto negativeEnd =
        std::upper_bound(beginOfNegatives, end, 0.0, comparatorGreater);
    rangeManager.addEqual(begin, positiveEnd);
    rangeManager.addEqual(beginOfNegatives, negativeEnd);
    rangeManager.addSmaller(negativeEnd, end);
    rangeManager.addGreater(positiveEnd, beginOfNans);
  } else {
    AD_CHECK(false);
  }
  return std::move(rangeManager).getResult();
}

// In a range of `ValueId`s, that is sorted according to `compareByBits` and
// denoted by `begin` and `end` return the contiguous ranges (as iterator pairs)
// of `ValueId`s where `id.getDatatype() == Int` and `id.getInt() CMP value`,
// where `CMP` is the comparator associated with the `comparison`, so `<=` for
// `Comparison::LE` etc.
template <typename RandomIt, typename Value>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForInt(
    RandomIt begin, RandomIt end, Value value,
    Comparison comparison = Comparison::EQ) {
  if (std::is_floating_point_v<Value> && std::isnan(value)) {
    // NaNs are non-comparable.
    return {};
  }

  std::tie(begin, end) = getRangeForDatatype(begin, end, Datatype::Int);

  // Find the first int < 0. It stands after all ints >= 0 because of the bit
  // representation of the 2s-complement. The constant `true` and the unnamed
  // `bool` argument are required because of the interface of `std::lower_bound`
  auto firstNegative = std::lower_bound(
      begin, end, true, [](ValueId id, bool) { return id.getInt() >= 0; });

  RangeFilter<RandomIt> manager{comparison};
  auto predicate = makeSymmetricComparator(&ValueId::getInt);
  if (value >= 0) {
    auto [eqBegin, eqEnd] =
        std::equal_range(begin, firstNegative, value, predicate);
    // The comparison is [smaller positives, equal, greater positives, all
    // negatives].
    manager.addSmaller(begin, eqBegin);
    manager.addEqual(eqBegin, eqEnd);
    manager.addGreater(eqEnd, firstNegative);
    manager.addSmaller(firstNegative, end);
  } else if (value < 0) {
    auto [eqBegin, eqEnd] =
        std::equal_range(firstNegative, end, value, predicate);
    // The comparison is [all positives, smaller negatives, equal, greater
    // negatives].
    manager.addGreater(begin, firstNegative);
    manager.addSmaller(firstNegative, eqBegin);
    manager.addEqual(eqBegin, eqEnd);
    manager.addGreater(eqEnd, end);
  } else {
    AD_CHECK(false);
  }
  return std::move(manager).getResult();
}

// In a range of `ValueId`s, that is sorted according to `compareByBits` and
// denoted by `begin` and `end` return the contiguous ranges (as iterator pairs)
// of `ValueId`s where `id.getDatatype()` returns `Double` or `Int` and
// `id.getXXX() CMP value`, where `CMP` is the comparator associated with the
// `comparison`, so `<=` for `Comparison::LE` etc and `XXX` is `Double` or
// `Int`, depending on the datatype.
template <typename RandomIt, typename Value>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForIntsAndDoubles(
    RandomIt begin, RandomIt end, Value value, Comparison comparison) {
  auto result = getRangesForDouble(begin, end, value, comparison);
  auto resultInt = getRangesForInt(begin, end, value, comparison);
  result.insert(result.end(), resultInt.begin(), resultInt.end());
  return result;
}

// For a range from `begin` to `end` of `ValueId`s, which is sorted by
// `compareByBits` and return the contiguous ranges (as iterator pairs) of
// `ValueId`s where `idInRange.getDatatype() == valueId.getDatatype()`
// `someId.getXXX() CMP valueId.getXXX()`, where `CMP` is the comparator
// associated with the `comparison`, so `<=` for `Comparison::LE` etc and `XXX`
// is the Datatype of `valueId`. This function only returns correct results if
// `valueId.getDatatype()` is an unsigned index type (`Vocab, LocalVocab,
// TextVocab`).
template <typename RandomIt>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForIndexTypes(
    RandomIt begin, RandomIt end, ValueId valueId,
    Comparison comparison = Comparison::EQ) {
  std::tie(begin, end) = getRangeForDatatype(begin, end, valueId.getDatatype());

  RangeFilter<RandomIt> manager{comparison};
  auto [eqBegin, eqEnd] = std::equal_range(begin, end, valueId, &compareByBits);
  manager.addSmaller(begin, eqBegin);
  manager.addEqual(eqBegin, eqEnd);
  manager.addGreater(eqEnd, end);
  return std::move(manager).getResult();
}

}  // namespace detail

/// In a range of `ValueId`s, that is sorted according to `compareByBits` and
/// denoted by `begin` and `end` return the contiguous ranges (as iterator
/// pairs) of `ValueId`s where `idInRange.getDatatype()` and
/// `valueId.getDatatype()` are compatible and `idInRange.getXXX() CMP
/// valueId.getYYY()`, where `CMP` is the comparator associated with the
/// `order`, so `<=` for `Comparison::LE` etc and `XXX` is the Datatype of
/// `idInRange()` and `YYY` is the Datatype of `valueId`. Two datatypes are
/// compatible, if they are the same, or if they are both numeric. The ranges in
/// the result are non-overlapping, non-empy, and sorted (ranges that contain
/// smaller iterators come first).
// TODO<joka921> A version of this function that takes `double`, `int64_t`,
// `VocabIndex` etc. directly would probably also come in handy, but it requires
// the index types to be strong for a nice implementation.
template <typename RandomIt>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForId(
    RandomIt begin, RandomIt end, ValueId id, Comparison order) {
  // This lambda enforces the invariants `non-empty` and `sorted`.
  auto simplify = [](std::vector<std::pair<RandomIt, RandomIt>>&& result) {
    std::sort(result.begin(), result.end());
    // Eliminate empty ranges
    std::erase_if(result, [](const auto& p) { return p.first == p.second; });
    return std::move(result);
  };
  switch (id.getDatatype()) {
    case Datatype::Double:
      return simplify(detail::getRangesForIntsAndDoubles(
          begin, end, id.getDouble(), order));
    case Datatype::Int:
      return simplify(
          detail::getRangesForIntsAndDoubles(begin, end, id.getInt(), order));
    case Datatype::Undefined:
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextIndex:
      return simplify(detail::getRangesForIndexTypes(begin, end, id, order));
  }
  AD_CHECK(false);
}

}  // namespace valueIdComparators

#endif  // QLEVER_VALUEIDCOMPARATORS_H
