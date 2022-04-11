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
                               Ordering::EQ);
  ASSERT_EQ(ranges.size(), 2ul);
  ASSERT_EQ(ranges[0].second - ranges[0].first, 1u);
  ASSERT_EQ(ranges[0].first->getDatatype(), Datatype::Int);
  ASSERT_EQ(ranges[0].first->getInt(), 42);
  ASSERT_EQ(ranges[1].second - ranges[1].first, 1u);
  ASSERT_EQ(ranges[1].first->getDatatype(), Datatype::Double);
  ASSERT_DOUBLE_EQ(ranges[1].first->getDouble(), 42.0);
}
