// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Anna Kaiser <anna.kaiser@uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/ColumnStrippingHelpers.h"
#include "engine/Distinct.h"
#include "engine/StripColumns.h"

using namespace columnStrippingHelpers;
using ::testing::ElementsAre;
using ::testing::Key;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;
using V = Variable;

// Test that `VarsRequiredFromSubtree` returns the original set as long as only
// contained variables are added, and a copy with the added variables otherwise.
TEST(ColumnStrippingHelpers, VarsRequiredFromSubtree) {
  // An empty set and a set with two variables.
  std::set<V> empty;
  std::set<V> vars{V{"?a"}, V{"?b"}};

  // Without any `add`, `get` returns the original set itself.
  VarsRequiredFromSubtree emptyHelper{&empty};
  EXPECT_EQ(&emptyHelper.get(), &empty);
  VarsRequiredFromSubtree helper{&vars};
  EXPECT_EQ(&helper.get(), &vars);

  // Adding a contained variable still returns the original set.
  helper.add(V{"?a"});
  EXPECT_EQ(&helper.get(), &vars);

  // Adding a new variable returns a copy with that variable, the original set
  // is unchanged.
  helper.add(V{"?c"});
  EXPECT_NE(&helper.get(), &vars);
  EXPECT_THAT(helper.get(), ElementsAre(V{"?a"}, V{"?b"}, V{"?c"}));
  EXPECT_THAT(vars, ElementsAre(V{"?a"}, V{"?b"}));

  // Further additions work on the copy.
  helper.add(V{"?b"});
  helper.add(V{"?d"});
  EXPECT_THAT(helper.get(), ElementsAre(V{"?a"}, V{"?b"}, V{"?c"}, V{"?d"}));
}

// Test that `allVariablesAreRequired` is true iff the requested variables are a
// superset of the variables of the tree.
TEST(ColumnStrippingHelpers, allVariablesAreRequired) {
  auto qec = ad_utility::testing::getQec();
  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2}}),
      std::vector<std::optional<V>>{V{"?a"}, V{"?b"}});
  EXPECT_TRUE(allVariablesAreRequired(*values, {V{"?a"}, V{"?b"}}));
  EXPECT_TRUE(allVariablesAreRequired(*values, {V{"?a"}, V{"?b"}, V{"?c"}}));
  EXPECT_FALSE(allVariablesAreRequired(*values, {V{"?a"}}));
  EXPECT_FALSE(allVariablesAreRequired(*values, {}));
}

// Test that `makeTreeWithOptionalStripOperation` adds a `StripColumns` on top
// of the new operation exactly if the operation exports variables that the
// parent did not request.
TEST(ColumnStrippingHelpers, makeTreeWithOptionalStripOperation) {
  // A subtree with the variables `?a`, `?b`, `?c`. The new root is a `Distinct`
  // on `?b`, which exports all three variables.
  auto qec = ad_utility::testing::getQec();
  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2, 3}}),
      std::vector<std::optional<V>>{V{"?a"}, V{"?b"}, V{"?c"}});

  // All exported variables are requested, the `Distinct` is the root.
  {
    auto tree = makeTreeWithOptionalStripOperation<Distinct>(
        qec, std::set<V>{V{"?a"}, V{"?b"}, V{"?c"}}, values,
        std::vector<ColumnIndex>{1});
    ASSERT_TRUE(tree.has_value());
    EXPECT_TRUE(
        std::dynamic_pointer_cast<Distinct>((*tree)->getRootOperation()));
    EXPECT_THAT((*tree)->getVariableColumns(), SizeIs(3));
  }

  // Only `?a` is requested, a `StripColumns` to `?a` is added on top of the
  // `Distinct`.
  {
    auto tree = makeTreeWithOptionalStripOperation<Distinct>(
        qec, std::set<V>{V{"?a"}}, values, std::vector<ColumnIndex>{1});
    ASSERT_TRUE(tree.has_value());
    auto strip =
        std::dynamic_pointer_cast<StripColumns>((*tree)->getRootOperation());
    ASSERT_TRUE(strip);
    EXPECT_THAT((*tree)->getVariableColumns(),
                UnorderedElementsAre(Key(V{"?a"})));
    const auto* child = strip->getChildren().at(0);
    EXPECT_TRUE(std::dynamic_pointer_cast<Distinct>(child->getRootOperation()));
    EXPECT_THAT(child->getVariableColumns(), SizeIs(3));
  }
}
