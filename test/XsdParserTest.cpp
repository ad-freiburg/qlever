//
// Created by johannes on 13.10.20.
//

#include <gtest/gtest.h>
#include "../src/parser/XsdParser.h"


TEST(XsdParser, ParseFloat) {
  auto x = XsdParser::parseFloat("23.0");
  ASSERT_TRUE(x);
  ASSERT_FLOAT_EQ(x.value(), 23.0f);
  XsdParser::parseFloat("NaN");

  XsdParser::parseFloat("-3.4e-12");
}