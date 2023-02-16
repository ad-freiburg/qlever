//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <vector>

#include "./IndexTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/ResultTable.h"
#include "engine/Values.h"
#include "engine/idTable/IdTable.h"

using TC = TripleComponent;
using ValuesComponents = std::vector<std::vector<TripleComponent>>;

// ____________________________________________________________________________
TEST(Values, sanitizeValues) {
  // Query execution context (with small test index), see `IndexTestHelpers.h`.
  using ad_utility::AllocatorWithLimit;
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  AllocatorWithLimit<Id> testAllocator = ad_utility::testing::makeAllocator();

  // Make sure that the `UNDEF` value gets assigned `LocalVocabIndex:0`.
  Id undefId = Id::makeFromLocalVocabIndex(LocalVocabIndex::make(0));

  // Lambda for checking that the given `ResultTable` contains the given
  // `IdTable`.
  auto checkResult = [&](const ResultTable& result,
                         const ValuesComponents values) -> void {
    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
      ASSERT_EQ(result.width(), values[i].size()) << "row " << i;
      for (size_t j = 0; j < result.width(); ++j) {
        std::optional<Id> idOrNullIfUndef = result._idTable[i][j];
        if (idOrNullIfUndef.value() == undefId) {
          idOrNullIfUndef = std::nullopt;
        }
        ASSERT_EQ(idOrNullIfUndef,
                  values[i][j].toValueId(testQec->getIndex().getVocab()))
            << "row " << i << ", column " << j;
      }
    }
  };

  // No UNDEF values, nothing filtered out.
  {
    ValuesComponents values{{TC{"<x>"}, TC{"<y>"}}};
    Values valuesOperation(testQec, {{Variable{"?x"}, Variable{"?y"}}, values});
    checkResult(*valuesOperation.getResult(), values);
  }

  // Some UNDEF values, but no row or columns with only UNDEF values, so nothing
  // filtered out (but the "UNDEF" are added to the local vocabulary, which is
  // weird, but that's how it currently is).
  {
    ValuesComponents values{{TC{"UNDEF"}, TC{"<y>"}}, {TC{"<x>"}, TC{"UNDEF"}}};
    Values valuesOperation(testQec, {{Variable{"?x"}, Variable{"?y"}}, values});
    checkResult(*valuesOperation.getResult(), values);
  }

  // A row full of UNDEF values (which then gets removed).
  {
    ValuesComponents values{{TC{"<x>"}, TC{"<y>"}}, {TC{"UNDEF"}, TC{"UNDEF"}}};
    Values valuesOperation(testQec, {{Variable{"?x"}, Variable{"?y"}}, values});
    checkResult(*valuesOperation.getResult(), {{TC{"<x>"}, TC{"<y>"}}});
  }

  // A column full of UNDEF values (which then gets removed).
  {
    ValuesComponents values{{TC{"UNDEF"}, TC{"<x>"}}, {TC{"UNDEF"}, TC{"<y>"}}};
    Values valuesOperation(testQec, {{Variable{"?x"}, Variable{"?y"}}, values});
    checkResult(*valuesOperation.getResult(), {{TC{"<x>"}}, {{TC{"<y>"}}}});
  }

  // A row and a column full of UNDEF values (which both get removed).
  {
    ValuesComponents values{{TC{"UNDEF"}, TC{"UNDEF"}},
                            {TC{"UNDEF"}, TC{"<y>"}}};
    Values valuesOperation(testQec, {{Variable{"?x"}, Variable{"?y"}}, values});
    checkResult(*valuesOperation.getResult(), {{{TC{"<y>"}}}});
  }
}

// ________________________________________________________________
TEST(Values, BasicMethods) {
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  ValuesComponents values{{TC{1}, TC{2}, TC{3}},
                          {TC{5}, TC{2}, TC{3}},
                          {TC{7}, TC{42}, TC{3}},
                          {TC{7}, TC{42}, TC{3}}};
  Values valuesOp(testQec,
                  {{Variable{"?x"}, Variable{"?y"}, Variable{"?z"}}, values});
  EXPECT_FALSE(valuesOp.knownEmptyResult());
  EXPECT_EQ(valuesOp.getSizeEstimate(), 4u);
  EXPECT_EQ(valuesOp.getCostEstimate(), 4u);
  EXPECT_EQ(valuesOp.getDescriptor(), "Values with variables ?x\t?y\t?z");
  EXPECT_TRUE(valuesOp.resultSortedOn().empty());
  EXPECT_EQ(valuesOp.getResultWidth(), 3u);
  // Col 0: 1, 5, 42, 42
  EXPECT_FLOAT_EQ(valuesOp.getMultiplicity(0), 4.0 / 3.0);
  // Col 2: 2, 2, 3, 3
  EXPECT_FLOAT_EQ(valuesOp.getMultiplicity(1), 2.0f);
  // Col 3: always `3`.
  EXPECT_FLOAT_EQ(valuesOp.getMultiplicity(2), 4.0f);
  // Out-of-bounds columns always return 1.
  EXPECT_FLOAT_EQ(valuesOp.getMultiplicity(4), 1.0f);

  auto varToCol = valuesOp.getExternallyVisibleVariableColumns();
  ASSERT_EQ(varToCol.size(), 3u);
  ASSERT_EQ(varToCol.at(Variable{"?x"}), 0);
  ASSERT_EQ(varToCol.at(Variable{"?y"}), 1);
  ASSERT_EQ(varToCol.at(Variable{"?z"}), 2);

  {
    // Test some corner cases for an empty VALUES clause
    // TODO<joka921> This case is currently disallowed by the `sanitizeValues`
    // function which will be removed soon anyway. Then we can reinstate this
    // code. But maybe we want to disallow empty VALUES clauses altogether and
    // remove that code.
    Values emptyValuesOp(testQec, {});
    EXPECT_TRUE(emptyValuesOp.knownEmptyResult());
    // The current implementation always returns `1.0` for nonexisting columns.
    EXPECT_FLOAT_EQ(emptyValuesOp.getMultiplicity(32), 1.0);
  }
}

TEST(Values, computeResult) {
  auto qec = ad_utility::testing::getQec("<x> <x> <x> .");
  ValuesComponents values{{TC{12}, TC{"<x>"}}, {TC::UNDEF{}, TC{"<y>"}}};
  Values valuesOperation(qec, {{Variable{"?x"}, Variable{"?y"}}, values});
  auto result = valuesOperation.getResult();
  const auto& table = result->_idTable;
  Id x;
  bool success = qec->getIndex().getId("<x>", &x);
  AD_CORRECTNESS_CHECK(success);
  auto I = ad_utility::testing::IntId;
  auto L = ad_utility::testing::LocalVocabId;
  auto U = Id::makeUndefined();
  ASSERT_EQ(table, makeIdTableFromIdVector({{I(12), x}, {U, L(0)}}));
}

TEST(Values, illegalInput) {
  auto qec = ad_utility::testing::getQec();
  ValuesComponents values{{TC{12}, TC{"<x>"}}, {TC::UNDEF{}}};
  ASSERT_ANY_THROW(Values(qec, {{Variable{"?x"}, Variable{"?y"}}, values}));
}
