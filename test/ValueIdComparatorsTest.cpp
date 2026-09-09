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
#include "index/IndexImpl.h"
#include "index/LocalVocabContext.h"
#include "index/LocalVocabEntry.h"
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

// Return true iff the datatype of `id` is compatible with `datatype` in the
// sense of `getRangeForDatatype`: all the string types form a single merged
// range, and every other datatype forms a range of its own. NOTE:
// `Datatype::SecondaryVocabIndex` is one of the string types, so it is merged
// with the others, which is exactly what the semantic comparison of those
// `Id`s requires.
bool hasCompatibleDatatype(ValueId id, Datatype datatype) {
  if (ad_utility::contains(ValueId::stringTypes_, datatype)) {
    return ad_utility::contains(ValueId::stringTypes_, id.getDatatype());
  }
  return id.getDatatype() == datatype;
}

TEST_F(ValueIdComparators, GetRangeForDatatype) {
  std::vector<Datatype> datatypes{Datatype::Int,
                                  Datatype::Double,
                                  Datatype::VocabIndex,
                                  Datatype::Undefined,
                                  Datatype::LocalVocabIndex,
                                  Datatype::TextRecordIndex,
                                  Datatype::WordVocabIndex,
                                  Datatype::SecondaryVocabIndex};
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end(), compareByBits);
  for (auto datatype : datatypes) {
    auto [begin, end] = getRangeForDatatype(ids.begin(), ids.end(), datatype);
    auto hasMatchingDatatype = [&datatype](ValueId id) {
      return hasCompatibleDatatype(id, datatype);
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
                        source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trage = generateLocationTrace(l);
  // Perform the testing for a single `Comparison`
  auto testImpl = [&](auto comparison) {
    // NOTE: All the `Id`s here belong to an index without a secondary
    // vocabulary, so we pass a `nullptr` context, and the linear scan over the
    // secondary vocabulary never finds anything.
    auto rangesAndMatches = getRangesForId(begin, end, id, comparison, nullptr);
    ASSERT_TRUE(rangesAndMatches.secondaryVocabMatches_.empty());
    const auto& ranges = rangesAndMatches.ranges_;
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
        ASSERT_EQ(compareIds(*it, id, comparison, nullptr), expected)
            << *it << ' ' << id;
        ++it;
      }
      while (it != rangeEnd) {
        ASSERT_TRUE(isMatching(*it, id))
            << *it << ' ' << id << comparison.value;
        ASSERT_EQ(compareIds(*it, id, comparison, nullptr), True)
            << *it << ' ' << id;
        ++it;
      }
    }
    while (it != end) {
      ASSERT_FALSE(isMatching(*it, id))
          << *it << ", " << id << comparison.value;
      auto expected = isMatchingDatatype(*it) ? False : Undef;
      ASSERT_EQ(compareIds(*it, id, comparison, nullptr), expected)
          << *it << ' ' << id;
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
    auto rangesAndMatches =
        getRangesForId(ids.begin(), ids.end(), undefined, comparison, nullptr);
    ASSERT_EQ(rangesAndMatches.ranges_.size(), 0);
    ASSERT_EQ(rangesAndMatches.secondaryVocabMatches_.size(), 0);
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
    auto rangesAndMatches =
        getRangesForEqualIds(begin, end, idBegin, idEnd, comparison, nullptr);
    ASSERT_TRUE(rangesAndMatches.secondaryVocabMatches_.empty());
    const auto& ranges = rangesAndMatches.ranges_;
    auto it = begin;
    for (auto [rangeBegin, rangeEnd] : ranges) {
      while (it != rangeBegin) {
        // TODO<joka921> Correctly determine, which of these cases we want.
        ASSERT_THAT(
            compareWithEqualIds(*it, idBegin, idEnd, comparison, nullptr),
            ::testing::AnyOf(False, Undef))
            << *it << " " << idBegin << ' ' << idEnd << ' '
            << static_cast<int>(comparison.value);
        ++it;
      }
      while (it != rangeEnd) {
        // The "not equal" relation also yields true for different datatypes.
        ASSERT_TRUE(isMatchingDatatype(*it) || comparison == Comparison::NE);
        ASSERT_EQ(compareWithEqualIds(*it, idBegin, idEnd, comparison, nullptr),
                  True)
            << *it << ' ' << idBegin << ' ' << idEnd;
        ++it;
      }
    }
    while (it != end) {
      // TODO<joka921> Correctly determine, which of these cases we want.
      ASSERT_THAT(compareWithEqualIds(*it, idBegin, idEnd, comparison, nullptr),
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

// Test that `getRangesForId` works correctly for `ValueId`s of the unsigned
// index types (`VocabIndex`, `TextRecordIndex`, `LocalVocabIndex`,
// `WordVocabIndex`, `SecondaryVocabIndex`).
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

    // All the string types are compatible with each other. NOTE: This now also
    // includes `Datatype::SecondaryVocabIndex`, which previously was
    // incompatible with the other two.
    auto isTypeMatching = [&](ValueId id) {
      return hasCompatibleDatatype(id, datatype);
    };

    // The `Id`s here belong to an index without a secondary vocabulary, and the
    // comparison is performed with a `nullptr` context (see
    // `testGetRangesForId`). For the string types it hence falls back to the
    // comparison of the `Id`s themselves, that is, to the *internal* order,
    // which is exactly what `compareByBits` implements.
    auto applyComparator = [&](auto comparator, ValueId a, ValueId b) {
      if (ad_utility::contains(ValueId::stringTypes_, a.getDatatype())) {
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
  testImpl(vi<Datatype::SecondaryVocabIndex>, &getSecondaryVocabIndex);
}

// _______________________________________________________________________
TEST_F(ValueIdComparators, undefinedWithItself) {
  auto u = ValueId::makeUndefined();
  using enum ComparisonResult;
  using enum ComparisonForIncompatibleTypes;
  ASSERT_EQ(compareIds(u, u, Comparison::LT, nullptr), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::LE, nullptr), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::EQ, nullptr), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::NE, nullptr), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::GT, nullptr), Undef);
  ASSERT_EQ(compareIds(u, u, Comparison::GE, nullptr), Undef);

  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::LT, nullptr), False);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::LE, nullptr), True);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::EQ, nullptr), True);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::NE, nullptr), False);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::GT, nullptr), False);
  ASSERT_EQ(compareIds<CompareByType>(u, u, Comparison::GE, nullptr), True);
}

// _______________________________________________________________________
TEST_F(ValueIdComparators, contractViolations) {
  auto u = ValueId::makeUndefined();
  auto I = ad_utility::testing::IntId;
  // Invalid value for the `Comparison` enum.
  ASSERT_ANY_THROW((compareIds(u, u, static_cast<Comparison>(542), nullptr)));
  ASSERT_ANY_THROW(
      (compareWithEqualIds(u, u, u, static_cast<Comparison>(542), nullptr)));

  // The third argument must be >= the second.
  ASSERT_ANY_THROW(
      (compareWithEqualIds(I(3), I(25), I(12), Comparison::LE, nullptr)));
}

// _____________________________________________________________________________
// `comparisonHolds` turns the result of a three-way comparison into the result
// of one of the `Comparison`s.
TEST(ValueIdComparatorsFreeFunctions, comparisonHolds) {
  using enum Comparison;
  // For each `Comparison`, the expected results for a three-way comparison that
  // is smaller than zero, equal to zero, and greater than zero.
  std::vector<std::pair<Comparison, std::array<bool, 3>>> expected{
      {LT, {{true, false, false}}}, {LE, {{true, true, false}}},
      {EQ, {{false, true, false}}}, {NE, {{true, false, true}}},
      {GE, {{false, true, true}}},  {GT, {{false, false, true}}}};
  for (const auto& [comparison, results] : expected) {
    SCOPED_TRACE(comparison);
    EXPECT_EQ(comparisonHolds(-1, comparison), results[0]);
    EXPECT_EQ(comparisonHolds(0, comparison), results[1]);
    EXPECT_EQ(comparisonHolds(1, comparison), results[2]);
    // Any negative resp. positive value has to behave like `-1` resp. `1`,
    // because a comparator may return an arbitrary difference.
    EXPECT_EQ(comparisonHolds(-42, comparison), results[0]);
    EXPECT_EQ(comparisonHolds(42, comparison), results[2]);
    // The overload for an ordering has to agree with the one for an `int`.
    EXPECT_EQ(comparisonHolds(ql::strong_ordering::less, comparison),
              results[0]);
    EXPECT_EQ(comparisonHolds(ql::strong_ordering::equal, comparison),
              results[1]);
    EXPECT_EQ(comparisonHolds(ql::strong_ordering::greater, comparison),
              results[2]);
  }
  // An invalid value for the `Comparison` enum.
  EXPECT_ANY_THROW(comparisonHolds(0, static_cast<Comparison>(542)));
}

// _____________________________________________________________________________
// `getRangesForId` for an index that has a secondary vocabulary (see
// `index/vocabulary/SecondaryVocabulary.h`). The `Id`s of such a vocabulary are
// sorted after all `Id`s of the main vocabulary, no matter what their words
// are, so they cannot be found by the binary search and are reported
// separately, see `IdRangesAndSecondaryMatches`.
TEST(ValueIdComparatorsFreeFunctions, getRangesForIdWithSecondaryVocabulary) {
  ad_utility::testing::TestIndexConfig config{"<s> <p> <a> . <s> <p> <c> ."};
  config.secondaryVocabWords = std::vector<std::string>{"\"a\"", "<b>", "<d>"};
  Index index =
      ad_utility::testing::makeTestIndex(gtestCurrentTestName(), config);
  const LocalVocabContext& context = index.getImpl().getLocalVocabContext();
  ASSERT_TRUE(context.hasSecondaryVocabulary());
  auto getId = ad_utility::testing::makeGetId(index);
  auto secondaryId = [](uint64_t i) {
    return Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(i));
  };

  // A word that is contained in neither vocabulary. It is positioned between
  // `<c>` and `<p>` in the main vocabulary.
  LocalVocabEntry entryE = LocalVocabEntry::fromIriref("<e>", context);
  Id idE = Id::makeFromLocalVocabIndex(&entryE);

  // The `Id`s in the *internal* order, that is, the order in which the index
  // scans emit them: first the words that are positioned in the main vocabulary
  // (which includes the local vocab entry), then the words of the secondary
  // vocabulary in the order of their indices. Their semantic order is a
  // different one, namely `"a" < <a> < <b> < <c> < <d> < <e> < <p> < <s>`.
  std::vector<ValueId> ids{getId("<a>"),   getId("<c>"),  idE,
                           getId("<p>"),   getId("<s>"),  secondaryId(0),
                           secondaryId(1), secondaryId(2)};
  ASSERT_TRUE(ql::ranges::is_sorted(ids, compareByBits));

  // Check `getRangesForId` for a single `Comparison`. The `expectedRanges` are
  // pairs of indices into `ids`, and the `expectedSecondaryMatches` are
  // individual indices into `ids`.
  auto testComparison =
      [&](ValueId referenceId, Comparison comparison,
          std::vector<std::pair<size_t, size_t>> expectedRanges,
          std::vector<size_t> expectedSecondaryMatches,
          source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        SCOPED_TRACE(comparison);
        auto result = getRangesForId(ids.begin(), ids.end(), referenceId,
                                     comparison, &context);
        auto toIndex = [&ids](auto it) {
          return static_cast<size_t>(it - ids.begin());
        };
        std::vector<std::pair<size_t, size_t>> actualRanges;
        std::vector<size_t> actualMatches;
        for (const auto& [rangeBegin, rangeEnd] : result.ranges_) {
          actualRanges.emplace_back(toIndex(rangeBegin), toIndex(rangeEnd));
          for (auto it = rangeBegin; it != rangeEnd; ++it) {
            actualMatches.push_back(toIndex(it));
          }
        }
        std::vector<size_t> actualSecondaryMatches;
        for (auto it : result.secondaryVocabMatches_) {
          actualSecondaryMatches.push_back(toIndex(it));
          actualMatches.push_back(toIndex(it));
        }
        EXPECT_THAT(actualRanges, ::testing::ElementsAreArray(expectedRanges));
        EXPECT_THAT(actualSecondaryMatches,
                    ::testing::ElementsAreArray(expectedSecondaryMatches));

        // The two components together have to be exactly those `Id`s for which
        // the semantic comparison holds, which is the actual invariant of
        // `getRangesForId`.
        std::vector<size_t> semanticMatches;
        for (size_t i = 0; i < ids.size(); ++i) {
          if (comparisonHolds(
                  context.compareIdsSemantically(ids[i], referenceId),
                  comparison)) {
            semanticMatches.push_back(i);
          }
        }
        EXPECT_THAT(actualMatches,
                    ::testing::ElementsAreArray(semanticMatches));
      };

  using enum Comparison;
  // Comparisons against `<c>`, a word of the main vocabulary. Smaller than it
  // are `<a>` (index 0), `"a"` (5), and `<b>` (6); greater than it are `<e>`
  // (2), `<p>` (3), `<s>` (4), and `<d>` (7).
  Id idC = getId("<c>");
  testComparison(idC, LT, {{0, 1}}, {5, 6});
  testComparison(idC, LE, {{0, 2}}, {5, 6});
  testComparison(idC, EQ, {{1, 2}}, {});
  testComparison(idC, NE, {{0, 1}, {2, 5}}, {5, 6, 7});
  testComparison(idC, GE, {{1, 5}}, {7});
  testComparison(idC, GT, {{2, 5}}, {7});

  // Comparisons against `<b>`, a word of the secondary vocabulary. Smaller than
  // it are `<a>` (0) and `"a"` (5); greater than it are `<c>` (1), `<e>` (2),
  // `<p>` (3), `<s>` (4), and `<d>` (7).
  Id idB = secondaryId(1);
  testComparison(idB, LT, {{0, 1}}, {5});
  testComparison(idB, LE, {{0, 1}}, {5, 6});
  testComparison(idB, EQ, {}, {6});
  testComparison(idB, NE, {{0, 5}}, {5, 7});
  testComparison(idB, GE, {{1, 5}}, {6, 7});
  testComparison(idB, GT, {{1, 5}}, {7});

  // Comparisons against `<e>`, a word that is in neither vocabulary. Smaller
  // than it are `<a>` (0), `<c>` (1), `"a"` (5), `<b>` (6), and `<d>` (7);
  // greater than it are `<p>` (3) and `<s>` (4).
  testComparison(idE, LT, {{0, 2}}, {5, 6, 7});
  testComparison(idE, LE, {{0, 3}}, {5, 6, 7});
  testComparison(idE, EQ, {{2, 3}}, {});
  testComparison(idE, NE, {{0, 2}, {3, 5}}, {5, 6, 7});
  testComparison(idE, GE, {{2, 5}}, {});
  testComparison(idE, GT, {{3, 5}}, {});

  // Comparisons against `"a"`, the literal of the secondary vocabulary, which
  // is smaller than all the IRIs.
  Id idLiteralA = secondaryId(0);
  testComparison(idLiteralA, LT, {}, {});
  testComparison(idLiteralA, LE, {}, {5});
  testComparison(idLiteralA, EQ, {}, {5});
  testComparison(idLiteralA, NE, {{0, 5}}, {6, 7});
  testComparison(idLiteralA, GE, {{0, 5}}, {5, 6, 7});
  testComparison(idLiteralA, GT, {{0, 5}}, {6, 7});
}
