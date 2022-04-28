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
auto toFunctor() {
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
#undef RETURN
}

auto testGetRangesForId(auto begin, auto end, ValueId id,
                        auto isMatchingDatatype, auto applyComparator) {
  auto impl = [&]<Comparison comparison>() {
    auto ranges = getRangesForId(begin, end, id, comparison);
    auto comparator = toFunctor<comparison>();
    auto it = begin;
    for (auto [singleBegin, singleEnd] : ranges) {
      while (it != singleBegin) {
        if (isMatchingDatatype(*it) && applyComparator(comparator, *it, id)) {
          std::cout << *it << ' ' << id << std::endl;
        }
        ASSERT_TRUE(!isMatchingDatatype(*it) ||
                    !applyComparator(comparator, *it, id));
        ++it;
      }
      while (it != singleEnd) {
        ASSERT_TRUE(isMatchingDatatype(*it));
        ASSERT_TRUE(applyComparator(comparator, *it, id));
        ++it;
      }
    }
    while (it != end) {
      if (isMatchingDatatype(*it) && applyComparator(comparator, *it, id)) {
        std::cout << *it << ' ' << id << std::endl;
      }
      ASSERT_TRUE(!isMatchingDatatype(*it) ||
                  !applyComparator(comparator, *it, id));
      ++it;
    }
  };

  impl.template operator()<Comparison::LT>();
  impl.template operator()<Comparison::LE>();
  impl.template operator()<Comparison::EQ>();
  impl.template operator()<Comparison::NE>();
  impl.template operator()<Comparison::GE>();
  impl.template operator()<Comparison::GT>();
}

TEST(ValueIdComparators, IndexTypes) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end(), compareByBits);

  auto impl = [&]<Datatype datatype>(auto getFromId) {
    auto [beginType, endType] =
        getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto numEntries = endType - beginType;
    AD_CHECK(numEntries > 0);
    auto generator = SlowRandomIntGenerator<uint64_t>(0, numEntries - 1);

    auto isTypeMatching = [&](ValueId id) {
      return id.getDatatype() == datatype;
    };

    auto applyComparator = [&](auto comparator, ValueId a, ValueId b) {
      return comparator(std::invoke(getFromId, a), std::invoke(getFromId, b));
    };

    for (size_t i = 0; i < 200; ++i) {
      testGetRangesForId(ids.begin(), ids.end(), *(beginType + generator()),
                         isTypeMatching, applyComparator);
    }
  };

  impl.operator()<Datatype::VocabIndex>(&getVocabIndex);
  impl.operator()<Datatype::TextRecordIndex>(&getTextRecordIndex);
  impl.operator()<Datatype::LocalVocabIndex>(&getLocalVocabIndex);
}

TEST(ValueIdComparators, NumericTypes) {
  auto impl = [](Datatype datatype, auto isTypeMatching, auto applyComparator) {
    auto ids = makeRandomIds();
    std::sort(ids.begin(), ids.end(), compareByBits);
    auto [beginType, endType] =
        getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto numEntries = endType - beginType;
    AD_CHECK(numEntries > 0);
    auto generator = SlowRandomIntGenerator<uint64_t>(0, numEntries - 1);

    for (size_t i = 0; i < 200; ++i) {
      testGetRangesForId(ids.begin(), ids.end(), *(beginType + generator()),
                         isTypeMatching, applyComparator);
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
