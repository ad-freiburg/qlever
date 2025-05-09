//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./ValueIdTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "global/ValueIdComparators.h"
#include "util/Random.h"

using namespace valueIdComparators;
namespace valueIdComparators {
inline std::ostream& operator<<(std::ostream& str, Comparison c) {
  switch (c) {
    using enum Comparison;
    case LT:
      str << "LT";
      break;
    case LE:
      str << "LE";
      break;
    case EQ:
      str << "EQ";
      break;
    case NE:
      str << "NE";
      break;
    case GE:
      str << "GE";
      break;
    case GT:
      str << "GT";
      break;
  }
  return str;
}
}  // namespace valueIdComparators
using ad_utility::source_location;

struct ValueIdComparators : public ::testing::Test {
  ValueIdComparators() {
    // We need to initialize a (static). index, otherwise we can't compare
    // VocabIndex to LocalVocabIndex entries
    ad_utility::testing::getQec();
  }
};

TEST_F(ValueIdComparators, GetRangeForDatatype) {
  std::vector<Datatype> datatypes{Datatype::Int,
                                  Datatype::Double,
                                  Datatype::VocabIndex,
                                  Datatype::Undefined,
                                  Datatype::LocalVocabIndex,
                                  Datatype::TextRecordIndex,
                                  Datatype::WordVocabIndex};
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end(), compareByBits);
  for (auto datatype : datatypes) {
    auto [begin, end] = getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto hasMatchingDatatype = [&datatype](ValueId id) {
      std::array vocabTypes{Datatype::VocabIndex, Datatype::LocalVocabIndex};
      if (ad_utility::contains(vocabTypes, datatype)) {
        return ad_utility::contains(vocabTypes, id.getDatatype());
      } else {
        return id.getDatatype() == datatype;
      }
    };
    for (auto it = ids.begin(); it < begin; ++it) {
      ASSERT_FALSE(hasMatchingDatatype(*it));
    }
    for (auto it = begin; it < end; ++it) {
      ASSERT_TRUE(hasMatchingDatatype(*it));
    }
    for (auto it = end; it < ids.end(); ++it) {
      ASSERT_FALSE(hasMatchingDatatype(*it));
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
template <typename It, typename IsMatchingDatatype, typename ApplyComparator>
auto testGetRangesForId(It begin, It end, ValueId id,
                        IsMatchingDatatype isMatchingDatatype,
                        ApplyComparator applyComparator,
                        source_location l = source_location::current()) {
  auto trage = generateLocationTrace(l);
  // Perform the testing for a single `Comparison`
  auto testImpl = [&](auto comparison) {
    auto ranges = getRangesForId(begin, end, id, comparison);
    auto comparator = getComparisonFunctor<comparison>();
    auto it = begin;

    auto isMatching = [&](ValueId a, ValueId b) {
      bool m = isMatchingDatatype(a);
      if (!m) {
        return m;
      }
      auto y = applyComparator(comparator, a, b);
      return y;
    };
    using enum ComparisonResult;
    for (auto [rangeBegin, rangeEnd] : ranges) {
      while (it != rangeBegin) {
        ASSERT_FALSE(isMatching(*it, id))
            << *it << ' ' << id << comparison.value;
        auto expected = isMatchingDatatype(*it) ? False : Undef;
        ASSERT_EQ(compareIds(*it, id, comparison), expected)
            << *it << ' ' << id;
        ++it;
      }
      while (it != rangeEnd) {
        ASSERT_TRUE(isMatching(*it, id))
            << *it << ' ' << id << comparison.value;
        ASSERT_EQ(compareIds(*it, id, comparison), True) << *it << ' ' << id;
        ++it;
      }
    }
    while (it != end) {
      ASSERT_FALSE(isMatching(*it, id))
          << *it << ", " << id << comparison.value;
      auto expected = isMatchingDatatype(*it) ? False : Undef;
      ASSERT_EQ(compareIds(*it, id, comparison), expected) << *it << ' ' << id;
      ++it;
    }
  };

  using ad_utility::use_value_identity::vi;
  testImpl(vi<Comparison::LT>);
  testImpl(vi<Comparison::LE>);
  testImpl(vi<Comparison::EQ>);
  testImpl(vi<Comparison::NE>);
  testImpl(vi<Comparison::GE>);
  testImpl(vi<Comparison::GT>);
}

// Test that `getRangesFromId` works correctly for `ValueId`s of the numeric
// types (`Int` and `Double`)
TEST_F(ValueIdComparators, NumericTypes) {
  auto impl = [](Datatype datatype, auto isTypeMatching, auto applyComparator) {
    auto ids = makeRandomIds();
    std::sort(ids.begin(), ids.end(), compareByBits);
    auto [beginOfDatatype, endOfDatatype] =
        getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto numEntries = endOfDatatype - beginOfDatatype;
    AD_CONTRACT_CHECK(numEntries > 0);
    auto getRandomIndex =
        ad_utility::SlowRandomIntGenerator<uint64_t>(0, numEntries - 1);

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
TEST_F(ValueIdComparators, Undefined) {
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
template <typename It, typename IsMatchingDatatype>
auto testGetRangesForEqualIds(It begin, It end, ValueId idBegin, ValueId idEnd,
                              IsMatchingDatatype isMatchingDatatype) {
  // Perform the testing for a single `Comparison`
  auto testImpl = [&](auto comparison) {
    if (comparison == Comparison::NE &&
        idBegin.getDatatype() == Datatype::VocabIndex) {
      EXPECT_TRUE(true);
    }
    using enum ComparisonResult;
    auto ranges = getRangesForEqualIds(begin, end, idBegin, idEnd, comparison);
    auto it = begin;
    for (auto [rangeBegin, rangeEnd] : ranges) {
      while (it != rangeBegin) {
        // TODO<joka921> Correctly determine, which of these cases we want.
        ASSERT_THAT(compareWithEqualIds(*it, idBegin, idEnd, comparison),
                    ::testing::AnyOf(False, Undef))
            << *it << " " << idBegin << ' ' << idEnd << ' '
            << static_cast<int>(comparison.value);
        ++it;
      }
      while (it != rangeEnd) {
        // The "not equal" relation also yields true for different datatypes.
        ASSERT_TRUE(isMatchingDatatype(*it) || comparison == Comparison::NE);
        ASSERT_EQ(compareWithEqualIds(*it, idBegin, idEnd, comparison), True)
            << *it << ' ' << idBegin << ' ' << idEnd;
        ++it;
      }
    }
    while (it != end) {
      // TODO<joka921> Correctly determine, which of these cases we want.
      ASSERT_THAT(compareWithEqualIds(*it, idBegin, idEnd, comparison),
                  ::testing::AnyOf(False, Undef));
      ++it;
    }
  };

  using ad_utility::use_value_identity::vi;
  testImpl(vi<Comparison::LT>);
  testImpl(vi<Comparison::LE>);
  testImpl(vi<Comparison::EQ>);
  testImpl(vi<Comparison::NE>);
  testImpl(vi<Comparison::GE>);
  testImpl(vi<Comparison::GT>);
}

// Test that `getRangesFromId` works correctly for `ValueId`s of the unsigned
// index types (`VocabIndex`, `TextRecordIndex`, `LocalVocabIndex`,
// `WordVocabIndex`).
TEST_F(ValueIdComparators, IndexTypes) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end(), compareByBits);

  // Perform the test for a single `Datatype`.
  auto testImpl = [&](auto datatype, auto getFromId) {
    auto [beginOfDatatype, endOfDatatype] =
        getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto numEntries = endOfDatatype - beginOfDatatype;
    AD_CONTRACT_CHECK(numEntries > 0);
    auto getRandomIndex =
        ad_utility::SlowRandomIntGenerator<uint64_t>(0, numEntries - 1);

    auto isTypeMatching = [&](ValueId id) {
      auto vocabTypes =
          std::array{Datatype::LocalVocabIndex, Datatype::VocabIndex};
      if (ad_utility::contains(vocabTypes, datatype)) {
        return ad_utility::contains(vocabTypes, id.getDatatype());
      }
      return id.getDatatype() == datatype;
    };

    auto applyComparator = [&](auto comparator, ValueId a, ValueId b) {
      if (a.getDatatype() == Datatype::LocalVocabIndex ||
          a.getDatatype() == Datatype::VocabIndex) {
        return comparator(a, b);
      }
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

  // TODO<joka921> The tests for local vocab and VocabIndex now have to be more
  // complex....
  using ad_utility::use_value_identity::vi;
  testImpl(vi<Datatype::VocabIndex>, &getVocabIndex);
  testImpl(vi<Datatype::TextRecordIndex>, &getTextRecordIndex);
  testImpl(vi<Datatype::LocalVocabIndex>, &getLocalVocabIndex);
  testImpl(vi<Datatype::WordVocabIndex>, &getWordVocabIndex);
}

// _______________________________________________________________________
TEST_F(ValueIdComparators, undefinedWithItself) {
  auto u = ValueId::makeUndefined();
  using enum ComparisonResult;
  using enum ComparisonForIncompatibleTypes;
  ASSERT_EQ(compareIds(u, u, Comparison::LT), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::LE), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::EQ), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::NE), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::GT), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::GE), Undef);

  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::LT), False);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::LE), True);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::EQ), True);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::NE), False);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::GT), False);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::GE), True);
}

// _______________________________________________________________________
TEST_F(ValueIdComparators, contractViolations) {
  auto u = ValueId::makeUndefined();
  auto I = ad_utility::testing::IntId;
  // Invalid value for the `Comparison` enum.
  ASSERT_ANY_THROW((compareIds(u, u, static_cast<Comparison>(542))));
  ASSERT_ANY_THROW(
      (compareWithEqualIds(u, u, u, static_cast<Comparison>(542))));

  // The third argument must be >= the second.
  ASSERT_ANY_THROW((compareWithEqualIds(I(3), I(25), I(12), Comparison::LE)));
}
