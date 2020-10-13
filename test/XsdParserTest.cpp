//
// Created by johannes on 13.10.20.
//

#include <gtest/gtest.h>
#include "../src/parser/XsdParser.h"


TEST(XsdParser, ParseFloat) {
  auto x = XsdParser::parseFloat("23.0");
  ASSERT_TRUE(x);
  ASSERT_FLOAT_EQ(x.value(), 23.0f);
  x= XsdParser::parseFloat("NaN");
  ASSERT_TRUE(x);
  ASSERT_NE(x.value(), x.value());

  x= XsdParser::parseFloat("-3.4e-12");
  ASSERT_TRUE(x);
  ASSERT_FLOAT_EQ(x.value(), -3.4e-12f);
  x= XsdParser::parseFloat("-INF");
  ASSERT_TRUE(x);
  ASSERT_TRUE(std::isinf(x.value()) && x.value() < .0f);
}