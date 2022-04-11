//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VALUEIDCOMPARATORS_H
#define QLEVER_VALUEIDCOMPARATORS_H

#include "./ValueId.h"
#include "../index/Vocabulary.h"
#include "../engine/ResultTable.h"
#include <utility>

namespace ad_utility {
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };

}

enum struct Ordering {
  LT, LE, EQ, NE, GE, GT
};

namespace valueIdComparators {
inline auto compareByBits(ValueId a, ValueId b) {
  return a.getBits() <=> b.getBits();
}

template <typename RandomIt>
inline std::pair<RandomIt, RandomIt> getRangeForDatatype(RandomIt begin, RandomIt end, Datatype datatype) {
  auto pred1 = [](ValueId id, Datatype type ) {return id.getDatatype() < type;};
  auto pred2 = [](Datatype type, ValueId id ) {return type < id.getDatatype();};
  return std::equal_range(begin, end, datatype, ad_utility::overloaded{pred1, pred2});
}

namespace detail {
template <typename RandomIt, typename Value>
// TODO<joka921> `Value` must be double or int64_t
inline std::vector<std::pair<RandomIt, RandomIt>> getEqualRangeForDouble(RandomIt begin, RandomIt end, Value value, Ordering order = Ordering::EQ) {
  if (std::is_floating_point_v<Value> && std::isnan(value)) {
    // NaNs are non-comparable.
    return {};
  }
  auto [doubleBegin, doubleEnd] = getRangeForDatatype(begin, end, Datatype::Double);
  // In `ids` the negative number stand AFTER the positive numbers because of
  // the bitOrdering. First rotate the negative numbers to the beginning.
  auto doubleIdIsNegative = [](ValueId id) {
    auto bits = std::bit_cast<uint64_t>(id.getDouble());
    return bits & ad_utility::bitMaskForHigherBits(1);
  };

  auto firstNan = std::lower_bound(doubleBegin, doubleEnd, true, [&doubleIdIsNegative](ValueId id, bool){return !doubleIdIsNegative(id) && !std::isnan(id.getDouble());});
  auto firstNegative = std::lower_bound(doubleBegin, doubleEnd, true, [&doubleIdIsNegative](ValueId id, bool){return !doubleIdIsNegative(id);});

  AD_CHECK(firstNegative >= firstNan);

  // TODO<joka921> filter Nan range.
  auto pred1 = [](ValueId id, Value val){return id.getDouble() < val;};
  auto pred2 = [](Value val, ValueId id){return val < id.getDouble();};
  auto predicate = ad_utility::overloaded{pred1, pred2};
  std::vector<std::pair<RandomIt, RandomIt>> result;
  auto addSmallerRange = [&](auto begin, auto end) {
    if (order == Ordering::LT || order == Ordering::LE || order == Ordering::NE) {
      result.emplace_back(begin, end);
    }
  };
  auto addEqualRange = [&](auto begin, auto end) {
    if (order == Ordering::LE || order == Ordering::EQ || order == Ordering::GE) {
      result.emplace_back(begin, end);
    }
  };
  auto addGreaterRange = [&](auto begin, auto end) {
    if (order == Ordering::GE || order == Ordering::GT || order == Ordering::NE) {
      result.emplace_back(begin, end);
    }
  };

  if (order == Ordering::NE) {
    result.emplace_back(firstNan, firstNegative);
  }

  if (value > 0) {
    auto [eqBegin, eqEnd] = std::equal_range(doubleBegin, firstNan, value, predicate);
    addEqualRange(eqBegin, eqEnd);
    addSmallerRange(doubleBegin, eqBegin);
    addSmallerRange(firstNegative, doubleEnd);
    addGreaterRange(eqEnd, firstNan);
  } else
  if (value < 0) {
    // The negative range is inverted.
    auto [reverseEnd, reverseBegin] = std::equal_range(std::make_reverse_iterator(doubleEnd), std::make_reverse_iterator(firstNegative), value, predicate);
    auto eqBegin = (reverseBegin+1).base();
    auto eqEnd = (reverseEnd+1).base();

    addSmallerRange(eqEnd, doubleEnd);
    addGreaterRange(doubleBegin, firstNan);
    addGreaterRange(firstNegative, eqBegin);
  } else
  if (value == 0) {
    auto positiveEnd = std::upper_bound(doubleBegin, firstNegative, 0.0, predicate);
    auto reverseUpper = std::lower_bound(std::make_reverse_iterator(doubleEnd), std::make_reverse_iterator(firstNegative), 0.0, predicate);
    auto negativeEnd = reverseUpper.base();
    addEqualRange(doubleBegin, positiveEnd);
    addEqualRange(firstNegative, negativeEnd);
    addSmallerRange(negativeEnd, doubleEnd);
    addGreaterRange(positiveEnd, firstNan);
  } else {
    AD_CHECK(false);
  }
  return result;
}

template <typename RandomIt, typename Value>
// TODO<joka921> `Value` must be double or int64_t
inline std::vector<std::pair<RandomIt, RandomIt>> getEqualRangeForInt(RandomIt begin, RandomIt end, Value value, Ordering order = Ordering::EQ) {
  auto [intBegin, intEnd] = getRangeForDatatype(begin, end, Datatype::Int);

  // The negative numbers stand after the positive numbers because of the bit representation of the 2s-complement.
  auto intIdIsNegative = [](ValueId id) {
    auto bits = std::bit_cast<uint64_t>(id.getInt());
    return bits & ad_utility::bitMaskForHigherBits(1);
  };

  auto firstNegative = std::lower_bound(
      intBegin, intEnd, true, [&intIdIsNegative](ValueId id, bool){return !intIdIsNegative(id);});

  auto pred1 = [](ValueId id, Value val){return id.getInt() < val;};
  auto pred2 = [](Value val, ValueId id){return val < id.getInt();};
  auto predicate = ad_utility::overloaded{pred1, pred2};

  if (std::is_floating_point_v<Value> && std::isnan(value)) {
    // NaNs are non-comparable.
    return {};
  }

  std::vector<std::pair<RandomIt, RandomIt>> result;
  auto addSmallerRange = [&](auto begin, auto end) {
    if (order == Ordering::LT || order == Ordering::LE || order == Ordering::NE) {
      result.emplace_back(begin, end);
    }
  };
  auto addEqualRange = [&](auto begin, auto end) {
    if (order == Ordering::LE || order == Ordering::EQ || order == Ordering::GE) {
      result.emplace_back(begin, end);
    }
  };
  auto addGreaterRange = [&](auto begin, auto end) {
    if (order == Ordering::GE || order == Ordering::GT || order == Ordering::NE) {
      result.emplace_back(begin, end);
    }
  };
  if (value >= 0) {
    auto [eqBegin, eqEnd] =  std::equal_range(intBegin, firstNegative, value, predicate);
    addEqualRange(eqBegin, eqEnd);
    addSmallerRange(intBegin, eqBegin);
    addSmallerRange(firstNegative, intEnd);
    addGreaterRange(eqEnd, firstNegative);
  } else
  if (value < 0) {
    auto [eqBegin, eqEnd] =  std::equal_range(firstNegative, intEnd, value, predicate);
    addEqualRange(eqBegin, eqEnd);
    addSmallerRange(firstNegative, eqBegin);
    addGreaterRange(intBegin, firstNegative);
    addGreaterRange(eqEnd, intEnd);
  } else {
    AD_CHECK(false);
  }
  return result;
}
template <typename RandomIt, typename Value>
// TODO<joka921> `Value` must be double or int64_t
inline std::vector<std::pair<RandomIt, RandomIt>> getEqualRangeForIntsAndDoubles(RandomIt begin, RandomIt end, Value value, Ordering order) {
  return { getEqualRangeForDouble(begin, end, value), getEqualRangeForInt(begin, end, value, order)};
}

template <typename RandomIt, typename Value>
// TODO<joka921> `Value` must be double or int64_t
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForIndexTypes(RandomIt begin, RandomIt end, ValueId id, Ordering order = Ordering::EQ) {
  std::tie(begin, end) = getRangeForDatatype(begin, end, id.getDatatype());
  std::vector<std::pair<RandomIt, RandomIt>> result;
  auto addSmallerRange = [&](auto begin, auto end) {
    if (order == Ordering::LT || order == Ordering::LE || order == Ordering::NE) {
      result.emplace_back(begin, end);
    }
  };
  auto addEqualRange = [&](auto begin, auto end) {
    if (order == Ordering::LE || order == Ordering::EQ || order == Ordering::GE) {
      result.emplace_back(begin, end);
    }
  };
  auto addGreaterRange = [&](auto begin, auto end) {
    if (order == Ordering::GE || order == Ordering::GT || order == Ordering::NE) {
      result.emplace_back(begin, end);
    }
  };

  auto [eqBegin, eqEnd] = std::equal_range(begin, end, id, &compareByBits);
  addSmallerRange(begin, eqBegin);
  addEqualRange(eqBegin, eqEnd);
  addGreaterRange(eqEnd, end);
}


}

// TODO<joka921> Should this function work on `int` `double`, etc. directly instead of `ValueId`? Or probably we need both versions for the expressions, but we probably first need strong index types.
template<typename RandomIt>
inline std::vector<std::pair<RandomIt, RandomIt>> getRangesForId(RandomIt begin, RandomIt end, ValueId id, Ordering order) {
  switch (id.getDatatype()) {
    case Datatype::Double:
      return detail::getEqualRangeForIntsAndDoubles(begin, end, id.getDouble());
    case Datatype::Int:
      return detail::getEqualRangeForIntsAndDoubles(begin, end, id.getInt());
    case Datatype::Undefined:
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextIndex:
      // TODO<joka921> Ordering TextRecords is probably not useful, but where should we warn about this?
      return detail::getRangesForIndexTypes(begin, end, id, order);
  }
}

}

#endif  // QLEVER_VALUEIDCOMPARATORS_H
