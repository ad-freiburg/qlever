//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <vector>

#include "./IndexTestHelpers.h"
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
