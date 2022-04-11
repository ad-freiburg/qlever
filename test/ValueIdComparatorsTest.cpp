//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "../src/global/ValueIdComparators.h"

#include <gtest/gtest.h>

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

  auto i = [&](int64_t value) {
    ids.push_back(ValueId::makeFromInt(value));
  };
  auto d = [&](double value) {
    ids.push_back(ValueId::makeFromDouble(value));
  };

  i(0);
  i(42);
  i(12395);
  i(-1235);
  i(-21);
  i(ValueId::IntegerType::min());
  i(ValueId::IntegerType::max());

  d(0.0);
  d(-0.0);
  d(12.2);
  d()
}

TEST(ValueIdComparators, Int) {

}

