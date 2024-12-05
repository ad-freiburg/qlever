// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../printers/PayloadVariablePrinters.h"
#include "../util/GTestHelpers.h"
#include "gmock/gmock.h"
#include "parser/PayloadVariables.h"
#include "parser/data/Variable.h"

namespace {  // anonymous namespace to avoid linker problems

TEST(PayloadVariablesTest, PayloadVariables) {
  PayloadVariables pv1;
  ASSERT_TRUE(pv1.empty());
  ASSERT_FALSE(pv1.isAll());
  ASSERT_EQ(pv1.getVariables(), std::vector<Variable>{});

  PayloadVariables pv2{std::vector<Variable>{}};
  ASSERT_EQ(pv1, pv2);
  ASSERT_TRUE(pv2.empty());
  ASSERT_FALSE(pv2.isAll());

  pv2.setToAll();
  ASSERT_TRUE(pv2.isAll());
  ASSERT_FALSE(pv2.empty());
  ASSERT_NE(pv1, pv2);

  pv2.addVariable(Variable{"?a"});
  ASSERT_TRUE(pv2.isAll());
  ASSERT_FALSE(pv2.empty());
  AD_EXPECT_THROW_WITH_MESSAGE(
      pv2.getVariables(),
      ::testing::HasSubstr(
          "getVariables may only be called on a non-all PayloadVariables "
          "object"));

  auto pv3 = PayloadVariables::all();
  ASSERT_EQ(pv2, pv3);
  ASSERT_TRUE(pv3.isAll());
  ASSERT_FALSE(pv3.empty());

  PayloadVariables pv4{std::vector<Variable>{Variable{"?a"}, Variable{"?b"}}};
  ASSERT_FALSE(pv4.isAll());
  ASSERT_FALSE(pv4.empty());
  ASSERT_NE(pv3, pv4);
  ASSERT_NE(pv1, pv4);
  std::vector<Variable> expect4{Variable{"?a"}, Variable{"?b"}};
  ASSERT_EQ(pv4.getVariables(), expect4);

  PayloadVariables pv5;
  ASSERT_EQ(pv5.getVariables(), std::vector<Variable>{});
  pv5.addVariable(Variable{"?var"});
  std::vector<Variable> expect5_step1{Variable{"?var"}};
  ASSERT_EQ(pv5.getVariables(), expect5_step1);
  pv5.addVariable(Variable{"?var2"});
  std::vector<Variable> expect5_step2{Variable{"?var"}, Variable{"?var2"}};
  ASSERT_EQ(pv5.getVariables(), expect5_step2);
  ASSERT_FALSE(pv5.isAll());
  ASSERT_FALSE(pv5.empty());
};

}  // namespace
