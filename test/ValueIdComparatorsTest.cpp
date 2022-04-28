//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/global/ValueIdComparators.h"
#include "../src/util/Random.h"

using namespace valueIdComparators;

std::vector<ValueId> makeAndSortIds() {
  std::vector<ValueId> ids;
  ids.push_back(ValueId::makeFromVocabIndex(0));
  ids.push_back(ValueId::makeFromVocabIndex(3));
  ids.push_back(ValueId::makeFromVocabIndex(ValueId::maxIndex));

  ids.push_back(ValueId::makeFromLocalVocabIndex(0));
  ids.push_back(ValueId::makeFromLocalVocabIndex(3));
  ids.push_back(ValueId::makeFromLocalVocabIndex(ValueId::maxIndex));

  ids.push_back(ValueId::makeFromTextIndex(0));
  ids.push_back(ValueId::makeFromTextIndex(3));
  ids.push_back(ValueId::makeFromTextIndex(ValueId::maxIndex));

  auto i = [&](int64_t value) { ids.push_back(ValueId::makeFromInt(value)); };
  auto d = [&](double value) { ids.push_back(ValueId::makeFromDouble(value)); };

  i(0);
  i(42);
  i(12395);
  i(-1235);
  i(-21);
  i(ValueId::IntegerType::min());
  i(ValueId::IntegerType::max());

  d(0.0);
  d(-0.0);
  d(42.0);
  d(1230e12);
  d(-16.2);
  d(-1239.3e5);
  d(std::numeric_limits<double>::quiet_NaN());
  d(std::numeric_limits<double>::signaling_NaN());
  d(std::numeric_limits<double>::max());
  d(-std::numeric_limits<double>::max());
  d(std::numeric_limits<double>::infinity());
  d(-std::numeric_limits<double>::infinity());

  randomShuffle(ids.begin(), ids.end());
  std::sort(ids.begin(), ids.end(), &valueIdComparators::compareByBits);
  return ids;
}

TEST(ValueIdComparators, Int) {
  auto ids = makeAndSortIds();
  auto ranges = getRangesForId(ids.begin(), ids.end(), ValueId::makeFromInt(42),
                               Comparison::EQ);
  ASSERT_EQ(ranges.size(), 2ul);
  ASSERT_EQ(ranges[0].second - ranges[0].first, 1u);
  ASSERT_EQ(ranges[0].first->getDatatype(), Datatype::Int);
  ASSERT_EQ(ranges[0].first->getInt(), 42);
  ASSERT_EQ(ranges[1].second - ranges[1].first, 1u);
  ASSERT_EQ(ranges[1].first->getDatatype(), Datatype::Double);
  ASSERT_DOUBLE_EQ(ranges[1].first->getDouble(), 42.0);
}

TEST(ValueIdComparators, GetRangeForDatatype) {
  std::vector<Datatype> datatypes {Datatype::Int, Datatype::Double, Datatype::VocabIndex, Datatype::Undefined, Datatype::LocalVocabIndex, Datatype::TextIndex};
  auto ids = makeAndSortIds();
  for (auto datatype : datatypes) {
    auto [begin, end] = getRangeForDatatype(ids.begin(), ids.end(), datatype);
    for (auto it = ids.begin(); it < begin; ++it) {
      ASSERT_NE(it->getDatatype(), datatype);
    }
    for (auto it = begin; it < end; ++it) {
      ASSERT_EQ(it->getDatatype(), datatype);
    }
    for (auto it = end; it < ids.end(); ++it) {
      ASSERT_NE(it->getDatatype(), datatype);
    }
  }
}

template<Comparison comparison>
auto toFunctor() {
#define RETURN(comp, func)                         \
  if constexpr (comparison == comp) {                    \
    return func<>();                               \
  }

  RETURN(Comparison::LT, std::less)
  RETURN(Comparison::LE, std::less_equal)
  RETURN(Comparison::EQ, std::equal_to)
  RETURN(Comparison::NE, std::not_equal_to)
  RETURN(Comparison::GE, std::greater_equal)
  RETURN(Comparison::GT, std::greater)
#undef RETURN
}

auto testGetRangesForId(auto begin, auto end, ValueId id, auto getFromId) {
  auto valueFromId = std::invoke(getFromId, id);

  auto impl = [&]<Comparison comparison>() {
    auto ranges = getRangesForId(begin, end, id, comparison);
    auto comparator = toFunctor<comparison>;
    for (auto [singleBegin, singleEnd] : ranges) {
      for (auto it = singleBegin; it != singleEnd; ++it) {
        auto valueFromRange = std::invoke(getFromId, *it);
        ASSERT_TRUE(comparator(valueFromRange, valueFromId));
      }
    }
  };

  impl.template operator()<Comparison::LT>();
  impl.template operator()<Comparison::LE>();
  impl.template operator()<Comparison::EQ>();
  impl.template operator()<Comparison::NE>();
  impl.template operator()<Comparison::GE>();
  impl.template operator()<Comparison::GT>();
}








