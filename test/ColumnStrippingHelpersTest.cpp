// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Anna Kaiser <anna.kaiser@uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/ColumnStrippingHelpers.h"

TEST(VarsRequiredFromSubtree, add) {
    Variable nameVar("name");
    Variable ageVar("age");
    Variable streetVar("street");
    std::set<Variable> varSet = {nameVar, ageVar, streetVar};
    VarsRequiredFromSubtree helper(&varSet);

    // Add another Variable that has already been added to the helper via constructor
    helper.add(nameVar);
    EXPECT_TRUE(ad_utility::contains(helper->varsRequiredFromSubtree_, nameVar));
    EXPECT_EQ((helper->varsRequiredFromSubtree).size(), 3);

    // Add another Variable that is not yet part of the helper
    Variable cityVar("city");
    EXPECT_FALSE(ad_utility::contains(helper->varsRequiredFromSubtree_, cityVar));
    helper.add(cityVar);
    EXPECT_TRUE(ad_utility::contains(helper->varsRequiredFromSubtree_, cityVar));
    EXPECT_EQ((helper->varsRequiredFromSubtree).size(), 4);
}

TEST(VarsRequiredFromSubtree, get) {
    // check return value if constructor received empty set
    std::set<Variable> varSet = {};
    VarsRequiredFromSubtree helper(&varSet);
    EXPECT_EQ(varSet, helper.get());

    // check return value if add() is never called
    std::set<Variable> varSet_1 = {Variable("city")};
    VarsRequiredFromSubtree helper_1(&varSet_1);
    EXPECT_EQ(varSet_1, helper_1.get());

    // check return value after add() has been called
    helper_1.add(Variable("age"));
    auto returnSet = helper_1.get();
    EXPECT_EQ(returnSet.size(), 2);
    EXPECT_TRUE(ad_utility::contains(returnSet, Variable("age")));
    EXPECT_TRUE(ad_utility::contains(returnSet, Variable("city")));
}