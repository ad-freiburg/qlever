//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/global/ValueIdComparators.h"
#include "../src/util/Random.h"
#include "./ValueIdTestHelpers.h"

using namespace valueIdComparators;

TEST(ValueIdComparators, GetRangeForDatatype) {
  std::vector<Datatype> datatypes{Datatype::Int,
                                  Datatype::Double,
                                  Datatype::VocabIndex,
                                  Datatype::Undefined,
                                  Datatype::LocalVocabIndex,
                                  Datatype::TextRecordIndex};
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end(), compareByBits);
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

template <Comparison comparison>
auto getComparisonFunctor() {
#define RETURN(comp, func)            \
  if constexpr (comparison == comp) { \
    return func<>();                  \
  }

  RETURN(Comparison::LT, std::less)
  RETURN(Comparison::LE, std::less_equal)
  RETURN(Comparison::EQ, std::equal_to)
  RETURN(Comparison::NE, std::not_equal_to)
  RETURN(Comparison::GE, std::greater_equal)
  RETURN(Comparison::GT, std::greater)
}

// Test whether `getRangesForID` behaves as expected for all of the
// `Comparison`s `isMatchingDatatype(ValueId cmp)` must return true iff the
// `Datatype` of `id` and of `cmp` are compatible. `applyComparator(comparator,
// ValueId a, ValueId b) must apply the comparator (like `std::less` on the
// values contained in `a` and `b` (`isMatchingDatatype(a) and
// `isMatchingDatatype(b)` both are true when `applyComparator` is called.
auto testGetRangesForId(auto begin, auto end, ValueId id,
                        auto isMatchingDatatype, auto applyComparator) {
  // Perform the testing for a single `Comparison`
  auto testImpl = [&]<Comparison comparison>() {
    auto ranges = getRangesForId(begin, end, id, comparison);
    auto comparator = getComparisonFunctor<comparison>();
    auto it = begin;
    for (auto [rangeBegin, rangeEnd] : ranges) {
      while (it != rangeBegin) {
        ASSERT_FALSE(isMatchingDatatype(*it) &&
                     applyComparator(comparator, *it, id));
        ++it;
      }
      while (it != rangeEnd) {
        ASSERT_TRUE(isMatchingDatatype(*it) &&
                    applyComparator(comparator, *it, id));
        ++it;
      }
    }
    while (it != end) {
      ASSERT_FALSE(isMatchingDatatype(*it) &&
                   applyComparator(comparator, *it, id));
      ++it;
    }
  };

  testImpl.template operator()<Comparison::LT>();
  testImpl.template operator()<Comparison::LE>();
  testImpl.template operator()<Comparison::EQ>();
  testImpl.template operator()<Comparison::NE>();
  testImpl.template operator()<Comparison::GE>();
  testImpl.template operator()<Comparison::GT>();
}

// Test that `getRangesFromId` works correctly for `ValueId`s of the unsigned
// index types (`VocabIndex`, `TextRecordIndex`, `LocalVocabIndex`)
TEST(ValueIdComparators, IndexTypes) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end(), compareByBits);

  // Perform the testing for a single `Datatype`
  auto testImpl = [&]<Datatype datatype>(auto getFromId) {
    auto [beginOfDatatype, endOfDatatype] =
        getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto numEntries = endOfDatatype - beginOfDatatype;
    AD_CHECK(numEntries > 0);
    auto getRandomIndex = SlowRandomIntGenerator<uint64_t>(0, numEntries - 1);

    auto isTypeMatching = [&](ValueId id) {
      return id.getDatatype() == datatype;
    };

    auto applyComparator = [&](auto comparator, ValueId a, ValueId b) {
      return comparator(std::invoke(getFromId, a), std::invoke(getFromId, b));
    };

    for (size_t i = 0; i < 200; ++i) {
      testGetRangesForId(ids.begin(), ids.end(),
                         *(beginOfDatatype + getRandomIndex()), isTypeMatching,
                         applyComparator);
    }
  };

  testImpl.operator()<Datatype::VocabIndex>(&getVocabIndex);
  testImpl.operator()<Datatype::TextRecordIndex>(&getTextRecordIndex);
  testImpl.operator()<Datatype::LocalVocabIndex>(&getLocalVocabIndex);
}

// Test that `getRangesFromId` works correctly for `ValueId`s of the numeric
// types (`Int` and `Double`)
TEST(ValueIdComparators, NumericTypes) {
  auto impl = [](Datatype datatype, auto isTypeMatching, auto applyComparator) {
    auto ids = makeRandomIds();
    std::sort(ids.begin(), ids.end(), compareByBits);
    auto [beginOfDatatype, endOfDatatype] =
        getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto numEntries = endOfDatatype - beginOfDatatype;
    AD_CHECK(numEntries > 0);
    auto getRandomIndex = SlowRandomIntGenerator<uint64_t>(0, numEntries - 1);

    for (size_t i = 0; i < 200; ++i) {
      testGetRangesForId(ids.begin(), ids.end(),
                         *(beginOfDatatype + getRandomIndex()), isTypeMatching,
                         applyComparator);
    }
  };
  auto isTypeMatching = [&](ValueId id) {
    auto type = id.getDatatype();
    return type == Datatype::Double || type == Datatype::Int;
  };

  auto applyComparator = [&](auto comparator, ValueId aId, ValueId bId) {
    std::variant<int64_t, double> aValue, bValue;
    if (aId.getDatatype() == Datatype::Double) {
      aValue = aId.getDouble();
    } else {
      aValue = aId.getInt();
    }
    if (bId.getDatatype() == Datatype::Double) {
      bValue = bId.getDouble();
    } else {
      bValue = bId.getInt();
    }

    return std::visit([&](auto a, auto b) { return comparator(a, b); }, aValue,
                      bValue);
  };

  impl(Datatype::Double, isTypeMatching, applyComparator);
  impl(Datatype::Int, isTypeMatching, applyComparator);
}

// Test that `getRangesFromId` works correctly for the undefined Id.
TEST(ValueIdComparators, Undefined) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end(), compareByBits);
  auto undefined = ValueId::makeUndefined();

  auto undefinedRange =
      getRangeForDatatype(ids.begin(), ids.end(), Datatype::Undefined);

  for (auto comparison : {Comparison::EQ, Comparison::LE, Comparison::GE}) {
    auto equalRange =
        getRangesForId(ids.begin(), ids.end(), undefined, comparison);
    ASSERT_EQ(equalRange.size(), 1);
    ASSERT_EQ(equalRange[0], undefinedRange);
  }
  for (auto comparison : {Comparison::NE, Comparison::GT, Comparison::LT}) {
    auto equalRange =
        getRangesForId(ids.begin(), ids.end(), undefined, comparison);
    ASSERT_TRUE(equalRange.empty());
  }
}
