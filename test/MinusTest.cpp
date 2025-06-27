// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "./util/IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "engine/Minus.h"
#include "engine/ValuesForTesting.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

TEST(Minus, computeMinus) {
  IdTable a = makeIdTableFromVector(
      {{1, 2, 1}, {2, 1, 4}, {5, 4, 1}, {8, 1, 2}, {8, 2, 3}});

  IdTable b = makeIdTableFromVector({{1, 2, 7, 5}, {3, 3, 1, 5}, {1, 8, 1, 5}});

  std::vector<std::array<ColumnIndex, 2>> jcls;
  jcls.push_back(std::array<ColumnIndex, 2>{{0, 1}});
  jcls.push_back(std::array<ColumnIndex, 2>{{1, 0}});

  // Subtract b from a on the column pairs 1,2 and 2,1 (entries from columns 1
  // of a have to equal those of column 2 of b and vice versa).
  auto* qec = ad_utility::testing::getQec();
  Minus m{qec,
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, a.clone(),
              std::vector<std::optional<Variable>>{
                  Variable{"?a"}, Variable{"?b"}, std::nullopt}),
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, b.clone(),
              std::vector<std::optional<Variable>>{
                  Variable{"?b"}, Variable{"?a"}, std::nullopt, std::nullopt})};
  IdTable res = m.computeMinus(a, b, jcls);

  EXPECT_EQ(res, makeIdTableFromVector({{1, 2, 1}, {5, 4, 1}, {8, 2, 3}}));

  // Test subtracting without matching columns
  res.clear();
  jcls.clear();
  res = m.computeMinus(a, b, jcls);
  EXPECT_EQ(res, a);

  // Test minus with variable sized data.
  IdTable va = makeIdTableFromVector(
      {{1, 2, 3, 4, 5, 6}, {1, 2, 3, 7, 5, 6}, {7, 6, 5, 4, 3, 2}});

  IdTable vb = makeIdTableFromVector({{2, 3, 4}, {2, 3, 5}, {6, 7, 4}});

  jcls.clear();
  jcls.push_back({1, 0});
  jcls.push_back({2, 1});

  Minus vm{qec,
           ad_utility::makeExecutionTree<ValuesForTesting>(
               qec, va.clone(),
               std::vector<std::optional<Variable>>{
                   std::nullopt, Variable{"?a"}, Variable{"?b"}, std::nullopt,
                   std::nullopt, std::nullopt}),
           ad_utility::makeExecutionTree<ValuesForTesting>(
               qec, vb.clone(),
               std::vector<std::optional<Variable>>{
                   Variable{"?a"}, Variable{"?b"}, std::nullopt})};

  IdTable vres = vm.computeMinus(va, vb, jcls);

  EXPECT_EQ(vres, makeIdTableFromVector({{7, 6, 5, 4, 3, 2}}));
}

// _____________________________________________________________________________
TEST(Minus, computeMinusWithEmptyTables) {
  IdTable nonEmpty = makeIdTableFromVector({{1, 2}, {3, 3}, {1, 8}});
  IdTable empty = IdTable{2, nonEmpty.getAllocator()};

  std::vector<std::array<ColumnIndex, 2>> jcls;
  jcls.push_back(std::array<ColumnIndex, 2>{{0, 0}});

  auto* qec = ad_utility::testing::getQec();
  Minus m{
      qec,
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, empty.clone(),
          std::vector<std::optional<Variable>>{Variable{"?a"}, std::nullopt}),
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, nonEmpty.clone(),
          std::vector<std::optional<Variable>>{Variable{"?a"}, std::nullopt})};

  {
    IdTable res = m.computeMinus(empty, nonEmpty, jcls);

    EXPECT_EQ(res, empty);
  }
  {
    IdTable res = m.computeMinus(nonEmpty, empty, jcls);

    EXPECT_EQ(res, nonEmpty);
  }
}

// _____________________________________________________________________________
TEST(Minus, computeMinusWithUndefined) {
  auto U = Id::makeUndefined();
  IdTable a =
      makeIdTableFromVector({{U, U, 10}, {U, 1, 11}, {1, U, 12}, {5, 4, 13}});
  IdTable b = makeIdTableFromVector({{U, U, 20}, {3, U, 21}, {1, 2, 22}});

  std::vector<std::array<ColumnIndex, 2>> jcls;
  jcls.push_back(std::array<ColumnIndex, 2>{{0, 1}});
  jcls.push_back(std::array<ColumnIndex, 2>{{1, 0}});

  auto* qec = ad_utility::testing::getQec();
  Minus m{qec,
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, a.clone(),
              std::vector<std::optional<Variable>>{
                  Variable{"?a"}, Variable{"?b"}, std::nullopt}),
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, b.clone(),
              std::vector<std::optional<Variable>>{
                  Variable{"?b"}, Variable{"?a"}, std::nullopt})};

  IdTable res = m.computeMinus(a, b, jcls);
  EXPECT_EQ(res, makeIdTableFromVector({{U, U, 10}, {1, U, 12}, {5, 4, 13}}));
}

// _____________________________________________________________________________
TEST(Minus, clone) {
  auto* qec = ad_utility::testing::getQec();
  Minus minus{
      qec,
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, makeIdTableFromVector({{0, 1}}),
          std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}}),
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, makeIdTableFromVector({{0, 1}}),
          std::vector<std::optional<Variable>>{Variable{"?x"},
                                               Variable{"?z"}})};

  auto clone = minus.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(minus, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), minus.getDescriptor());
}

// _____________________________________________________________________________
TEST(Minus, columnOriginatesFromGraphOrUndef) {
  using ad_utility::triple_component::Iri;
  auto* qec = ad_utility::testing::getQec();
  auto values1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  auto values2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto index = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::POS,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Iri::fromIriref("<c>")});

  Minus minus1{qec, values1, values1};
  EXPECT_FALSE(minus1.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(minus1.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_THROW(
      minus1.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  Minus minus2{qec, values1, values2};
  EXPECT_FALSE(minus2.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(minus2.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_THROW(minus2.columnOriginatesFromGraphOrUndef(Variable{"?c"}),
               ad_utility::Exception);
  EXPECT_THROW(
      minus2.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  Minus minus3{qec, index, values1};
  EXPECT_TRUE(minus3.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_THROW(minus3.columnOriginatesFromGraphOrUndef(Variable{"?b"}),
               ad_utility::Exception);
  EXPECT_THROW(
      minus3.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  Minus minus4{qec, values1, index};
  EXPECT_FALSE(minus4.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(minus4.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_THROW(
      minus4.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);
}
