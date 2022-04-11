//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VALUEIDCOMPARATORS_H
#define QLEVER_VALUEIDCOMPARATORS_H

#include <utility>

#include "../engine/ResultTable.h"
#include "../index/Vocabulary.h"
#include "../util/Overloaded.h"
#include "./ValueId.h"


enum struct Ordering { LT, LE, EQ, NE, GE, GT };


namespace valueIdComparators {
inline bool compareByBits(ValueId a, ValueId b) {
  return a.getBits() < b.getBits();
}

template <typename RandomIt>
inline std::pair<RandomIt, RandomIt> getRangeForDatatype(RandomIt begin,
                                                         RandomIt end,
                                                         Datatype datatype) {
  auto pred1 = [](ValueId id, Datatype type) {
    return id.getDatatype() < type;
  };
  auto pred2 = [](Datatype type, ValueId id) {
    return type < id.getDatatype();
  };
  return std::equal_range(begin, end, datatype,
                          ad_utility::Overloaded{pred1, pred2});
}

namespace detail {

template<typename RandomIt>
class RangeManager {
 private:
  using Vec = std::vector<std::pair<RandomIt, RandomIt>>;
  Ordering _order;
  Vec _result;
 public:
  explicit RangeManager(Ordering order): _order{order}{};

  Vec getResult() && {
    return std::move(_result);
  }

  void addSmallerRange (auto begin, auto end) {
    addImpl<Ordering::LT, Ordering::LE, Ordering::NE>(begin, end);
  }
  void addEqualRange (auto begin, auto end) {
    addImpl<Ordering::LE, Ordering::EQ, Ordering::GE>(begin, end);
  };
  void addGreaterRange (auto begin, auto end) {
    addImpl<Ordering::GE, Ordering::GT, Ordering::NE>(begin, end);
  };

  void addNanRange(RandomIt begin, RandomIt end) {
    addImpl<Ordering::NE>(begin, end);
  }

 private:
  template <Ordering... acceptedOrders>
  void addImpl(RandomIt begin, RandomIt end) {
    if ((... ||(_order == acceptedOrders))) {
      _result.emplace_back(begin, end);
    }
  }

};
template <typename RandomIt, typename Value>
// TODO<joka921> `Value` must be double or int64_t
inline std::vector<std::pair<RandomIt, RandomIt>> getEqualRangeForDouble(
    RandomIt begin, RandomIt end, Value value, Ordering order = Ordering::EQ) {
  if (std::is_floating_point_v<Value> && std::isnan(value)) {
    // NaNs are non-comparable.
    return {};
  }
  auto [doubleBegin, doubleEnd] =
      getRangeForDatatype(begin, end, Datatype::Double);
  // In `ids` the negative number stand AFTER the positive numbers because of
  // the bitOrdering. First rotate the negative numbers to the beginning.
  auto doubleIdIsNegative = [](ValueId id) {
    auto bits = std::bit_cast<uint64_t>(id.getDouble());
    return bits & ad_utility::bitMaskForHigherBits(1);
  };

  auto firstNan = std::lower_bound(
      doubleBegin, doubleEnd, true, [&doubleIdIsNegative](ValueId id, bool) {
        return !doubleIdIsNegative(id) && !std::isnan(id.getDouble());
      });
  auto firstNegative = std::lower_bound(
      doubleBegin, doubleEnd, true, [&doubleIdIsNegative](ValueId id, bool) {
        return !doubleIdIsNegative(id);
      });

  AD_CHECK(firstNegative >= firstNan);

  // TODO<joka921> filter Nan range.
  auto pred1 = [](ValueId id, Value val) { return id.getDouble() < val; };
  auto pred2 = [](Value val, ValueId id) { return val < id.getDouble(); };
  auto predicate = ad_utility::Overloaded{pred1, pred2};
  RangeManager<RandomIt> manager{order};

  manager.addNanRange(firstNan, firstNegative);
  if (value > 0) {
    auto [eqBegin, eqEnd] =
        std::equal_range(doubleBegin, firstNan, value, predicate);
    manager.addEqualRange(eqBegin, eqEnd);
    manager.addSmallerRange(doubleBegin, eqBegin);
    manager.addSmallerRange(firstNegative, doubleEnd);
    manager.addGreaterRange(eqEnd, firstNan);
  } else if (value < 0) {
    // The negative range is inverted.
    auto [reverseEnd, reverseBegin] = std::equal_range(
        std::make_reverse_iterator(doubleEnd),
        std::make_reverse_iterator(firstNegative), value, predicate);
    auto eqBegin = (reverseBegin + 1).base();
    auto eqEnd = (reverseEnd + 1).base();

    manager.addSmallerRange(eqEnd, doubleEnd);
    manager.addGreaterRange(doubleBegin, firstNan);
    manager.addGreaterRange(firstNegative, eqBegin);
  } else if (value == 0) {
    auto positiveEnd =
        std::upper_bound(doubleBegin, firstNegative, 0.0, predicate);
    auto reverseUpper = std::lower_bound(
        std::make_reverse_iterator(doubleEnd),
        std::make_reverse_iterator(firstNegative), 0.0, predicate);
    auto negativeEnd = reverseUpper.base();
    manager.addEqualRange(doubleBegin, positiveEnd);
    manager.addEqualRange(firstNegative, negativeEnd);
    manager.addSmallerRange(negativeEnd, doubleEnd);
    manager.addGreaterRange(positiveEnd, firstNan);
  } else {
    AD_CHECK(false);
  }
  return std::move(manager).getResult();
}

template <typename RandomIt, typename Value>
// TODO<joka921> `Value` must be double or int64_t
inline std::vector<std::pair<RandomIt, RandomIt>> getEqualRangeForInt(
    RandomIt begin, RandomIt end, Value value, Ordering order = Ordering::EQ) {
  auto [intBegin, intEnd] = getRangeForDatatype(begin, end, Datatype::Int);

  // The negative numbers stand after the positive numbers because of the bit
  // representation of the 2s-complement.
  auto intIdIsNegative = [](ValueId id) {
    auto bits = std::bit_cast<uint64_t>(id.getInt());
    return bits & ad_utility::bitMaskForHigherBits(1);
  };

  auto firstNegative = std::lower_bound(
      intBegin, intEnd, true,
      [&intIdIsNegative](ValueId id, bool) { return !intIdIsNegative(id); });

  auto pred1 = [](ValueId id, Value val) { return id.getInt() < val; };
  auto pred2 = [](Value val, ValueId id) { return val < id.getInt(); };
  auto predicate = ad_utility::Overloaded{pred1, pred2};

  if (std::is_floating_point_v<Value> && std::isnan(value)) {
    // NaNs are non-comparable.
    return {};
  }

  RangeManager<RandomIt> manager{order};
  if (value >= 0) {
    auto [eqBegin, eqEnd] =
        std::equal_range(intBegin, firstNegative, value, predicate);
    manager.addEqualRange(eqBegin, eqEnd);
    manager.addSmallerRange(intBegin, eqBegin);
    manager.addSmallerRange(firstNegative, intEnd);
    manager.addGreaterRange(eqEnd, firstNegative);
  } else if (value < 0) {
    auto [eqBegin, eqEnd] =
        std::equal_range(firstNegative, intEnd, value, predicate);
    manager.addEqualRange(eqBegin, eqEnd);
    manager.addSmallerRange(firstNegative, eqBegin);
    manager.addGreaterRange(intBegin, firstNegative);
    manager.addGreaterRange(eqEnd, intEnd);
  } else {
    AD_CHECK(false);
  }
  return std::move(manager).getResult();
}
template <typename RandomIt, typename Value>
// TODO<joka921> `Value` must be double or int64_t
inline std::vector<std::pair<RandomIt, RandomIt>>
getEqualRangeForIntsAndDoubles(RandomIt begin, RandomIt end, Value value,
                               Ordering order) {
  auto result = getEqualRangeForDouble(begin, end, value);
  auto resultInt = getEqualRangeForInt(begin, end, value, order);
  result.insert(result.end(), resultInt.begin(), resultInt.end());
  return result;
}

template <typename RandomIt>
// TODO<joka921> `Value` must be double or int64_t
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForIndexTypes(
    RandomIt begin, RandomIt end, ValueId id, Ordering order = Ordering::EQ) {
  std::tie(begin, end) = getRangeForDatatype(begin, end, id.getDatatype());

  RangeManager<RandomIt> manager {order};
  auto [eqBegin, eqEnd] = std::equal_range(begin, end, id, &compareByBits);
  manager.addSmallerRange(begin, eqBegin);
  manager.addEqualRange(eqBegin, eqEnd);
  manager.addGreaterRange(eqEnd, end);
  return std::move(manager).getResult();
}

}  // namespace detail

// TODO<joka921> Should this function work on `int` `double`, etc. directly
// instead of `ValueId`? Or probably we need both versions for the expressions,
// but we probably first need strong index types.
template <typename RandomIt>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForId(
    RandomIt begin, RandomIt end, ValueId id, Ordering order) {
  auto simplify = [](std::vector<std::pair<RandomIt, RandomIt>>&& result) {
    // Sort the ranges.
    std::sort(result.begin(), result.end());
    // Eliminate empty ranges
    std::erase_if(result, [](const auto& p) { return p.first == p.second; });
    return std::move(result);
  };
  switch (id.getDatatype()) {
    case Datatype::Double:
      return simplify(detail::getEqualRangeForIntsAndDoubles(
          begin, end, id.getDouble(), order));
    case Datatype::Int:
      return simplify(detail::getEqualRangeForIntsAndDoubles(
          begin, end, id.getInt(), order));
    case Datatype::Undefined:
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextIndex:
      // TODO<joka921> Ordering TextRecords is probably not useful, but where
      // should we warn about this?
      return simplify(detail::getRangesForIndexTypes(begin, end, id, order));
  }
  AD_CHECK(false);
}

}  // namespace valueIdComparators

#endif  // QLEVER_VALUEIDCOMPARATORS_H
