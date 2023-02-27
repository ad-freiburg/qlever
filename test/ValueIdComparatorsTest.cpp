//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./ValueIdTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "global/ValueIdComparators.h"
#include "util/Random.h"

using namespace valueIdComparators;
using ad_utility::source_location;

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
// `Comparison`s.
// `isMatchingDatatype(ValueId cmp)` must return true iff the
// `Datatype` of `id` and of `cmp` are compatible.
// `applyComparator(comparator, ValueId a, ValueId b) must apply the comparator
// (like `std::less` on the values contained in `a` and `b`
// (`isMatchingDatatype(a) and `isMatchingDatatype(b)` both are true when
// `applyComparator` is called.
auto testGetRangesForId(auto begin, auto end, ValueId id,
                        auto isMatchingDatatype, auto applyComparator,
                        source_location l = source_location::current()) {
  auto trage = generateLocationTrace(l);
  // Perform the testing for a single `Comparison`
  auto testImpl = [&]<Comparison comparison>() {
    auto ranges = getRangesForId(begin, end, id, comparison);
    auto comparator = getComparisonFunctor<comparison>();
    auto it = begin;

    auto isMatching = [&](ValueId a, ValueId b) {
      return isMatchingDatatype(a) && applyComparator(comparator, a, b);
    };
    for (auto [rangeBegin, rangeEnd] : ranges) {
      while (it != rangeBegin) {
        ASSERT_FALSE(isMatching(*it, id)) << *it << ' ' << id;
        ASSERT_FALSE(compareIds(*it, id, comparison)) << *it << ' ' << id;
        ++it;
      }
      while (it != rangeEnd) {
        ASSERT_TRUE(isMatching(*it, id)) << *it << ' ' << id;
        ASSERT_TRUE(compareIds(*it, id, comparison)) << *it << ' ' << id;
        ++it;
      }
    }
    while (it != end) {
      ASSERT_FALSE(isMatching(*it, id));
      ASSERT_FALSE(compareIds(*it, id, comparison)) << *it << ' ' << id;
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

// Test that `getRangesFromId` works correctly for `ValueId`s of the numeric
// types (`Int` and `Double`)
TEST(ValueIdComparators, NumericTypes) {
  auto impl = [](Datatype datatype, auto isTypeMatching, auto applyComparator) {
    auto ids = makeRandomIds();
    std::sort(ids.begin(), ids.end(), compareByBits);
    auto [beginOfDatatype, endOfDatatype] =
        getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto numEntries = endOfDatatype - beginOfDatatype;
    AD_CONTRACT_CHECK(numEntries > 0);
    auto getRandomIndex = SlowRandomIntGenerator<uint64_t>(0, numEntries - 1);

    for (size_t i = 0; i < 200; ++i) {
      auto randomId = *(beginOfDatatype + getRandomIndex());
      testGetRangesForId(ids.begin(), ids.end(), randomId, isTypeMatching,
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

// Test that `getRangesFromId` works correctly for the undefined ID.
TEST(ValueIdComparators, Undefined) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end(), compareByBits);
  auto undefined = ValueId::makeUndefined();

  for (auto comparison : {Comparison::EQ, Comparison::LE, Comparison::GE,
                          Comparison::GT, Comparison::LT, Comparison::NE}) {
    auto equalRange =
        getRangesForId(ids.begin(), ids.end(), undefined, comparison);
    ASSERT_EQ(equalRange.size(), 0);
  }
}

// Similar to `testGetRanges` (see above) but tests the comparison to a range of
// `ValueId`s that are considered equal.
auto testGetRangesForEqualIds(auto begin, auto end, ValueId idBegin,
                              ValueId idEnd, auto isMatchingDatatype) {
  // Perform the testing for a single `Comparison`
  auto testImpl = [&]<Comparison comparison>() {
    if (comparison == Comparison::NE &&
        idBegin.getDatatype() == Datatype::VocabIndex) {
      EXPECT_TRUE(true);
    }
    auto ranges = getRangesForEqualIds(begin, end, idBegin, idEnd, comparison);
    auto it = begin;
    for (auto [rangeBegin, rangeEnd] : ranges) {
      while (it != rangeBegin) {
        ASSERT_FALSE(compareWithEqualIds(*it, idBegin, idEnd, comparison))
            << *it << " " << idBegin << ' ' << idEnd << ' '
            << static_cast<int>(comparison);
        ++it;
      }
      while (it != rangeEnd) {
        // The "not equal" relation also yields true for different datatypes.
        ASSERT_TRUE(isMatchingDatatype(*it) || comparison == Comparison::NE);
        ASSERT_TRUE(compareWithEqualIds(*it, idBegin, idEnd, comparison));
        ++it;
      }
    }
    while (it != end) {
      ASSERT_FALSE(compareWithEqualIds(*it, idBegin, idEnd, comparison));
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
// index types (`VocabIndex`, `TextRecordIndex`, `LocalVocabIndex`).
TEST(ValueIdComparators, IndexTypes) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end(), compareByBits);

  // Perform the test for a single `Datatype`.
  auto testImpl = [&]<Datatype datatype>(auto getFromId) {
    auto [beginOfDatatype, endOfDatatype] =
        getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto numEntries = endOfDatatype - beginOfDatatype;
    AD_CONTRACT_CHECK(numEntries > 0);
    auto getRandomIndex = SlowRandomIntGenerator<uint64_t>(0, numEntries - 1);

    auto isTypeMatching = [&](ValueId id) {
      return id.getDatatype() == datatype;
    };

    auto applyComparator = [&](auto comparator, ValueId a, ValueId b) {
      return comparator(std::invoke(getFromId, a), std::invoke(getFromId, b));
    };

    for (size_t i = 0; i < 200; ++i) {
      auto begin = beginOfDatatype + getRandomIndex();
      auto end = beginOfDatatype + getRandomIndex();
      if (*begin > *end) {
        std::swap(begin, end);
      }
      testGetRangesForId(ids.begin(), ids.end(), *begin, isTypeMatching,
                         applyComparator);
      if (*begin == *end) {
        continue;
      }
      testGetRangesForEqualIds(ids.begin(), ids.end(), *begin, *end,
                               isTypeMatching);
    }
  };

  testImpl.operator()<Datatype::VocabIndex>(&getVocabIndex);
  testImpl.operator()<Datatype::TextRecordIndex>(&getTextRecordIndex);
  testImpl.operator()<Datatype::LocalVocabIndex>(&getLocalVocabIndex);
}

// _______________________________________________________________________
TEST(ValueIdComparators, undefinedWithItself) {
  auto u = ValueId::makeUndefined();
  ASSERT_FALSE(valueIdComparators::compareIds(u, u, Comparison::LT));
  ASSERT_FALSE(valueIdComparators::compareIds(u, u, Comparison::LE));
  ASSERT_FALSE(valueIdComparators::compareIds(u, u, Comparison::EQ));
  ASSERT_FALSE(valueIdComparators::compareIds(u, u, Comparison::NE));
  ASSERT_FALSE(valueIdComparators::compareIds(u, u, Comparison::GT));
  ASSERT_FALSE(valueIdComparators::compareIds(u, u, Comparison::GE));
}

// _______________________________________________________________________
TEST(ValueIdComparators, contractViolations) {
  auto u = ValueId::makeUndefined();
  auto I = ad_utility::testing::IntId;
  // Invalid value for the `Comparison` enum.
  ASSERT_ANY_THROW((compareIds(u, u, static_cast<Comparison>(542))));
  ASSERT_ANY_THROW(
      (compareWithEqualIds(u, u, u, static_cast<Comparison>(542))));

  // The third argument must be >= the second.
  ASSERT_ANY_THROW((compareWithEqualIds(I(3), I(25), I(12), Comparison::LE)));
}
