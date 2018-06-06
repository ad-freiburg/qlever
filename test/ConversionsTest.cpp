// Copyright 2016, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>
#include "../src/util/Conversions.h"

using std::string;
using std::vector;

using namespace ad_utility;

TEST(ConversionsTest, getBase10ComplementOfIntegerString) {
  ASSERT_EQ("1234", getBase10ComplementOfIntegerString("8765"));
  ASSERT_EQ("0", getBase10ComplementOfIntegerString("9"));
  ASSERT_EQ("0001", getBase10ComplementOfIntegerString("9998"));
  ASSERT_EQ("898989", getBase10ComplementOfIntegerString("101010"));
}

TEST(ConversionsTest, convertFloatToIndexWord) {
  string zero = "0.0";
  string pos = "0.339";
  string pos2 = "1.7";
  string pos3 = "2.0";
  string pos4 = "2.0000009999";
  string pos5 = "2.9999";
  string pos6 = "111000.05";
  string neg = "-0.0005002";
  string neg2 = "-0.005002";
  string neg3 = "-2023.414";
  string neg4 = "-3023.414";
  string extra = "0.001";
  string extra2 = "-0.001";
  string extra3 = "-0.10001";
  string extra4 = "-0.100001";

  vector<string> indexWords;
  indexWords.push_back(convertFloatToIndexWord(zero, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos2, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos3, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(neg, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(neg2, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(neg3, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(neg4, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos4, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos5, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos6, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(extra, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(extra2, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(extra3, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(extra4, 10, 20));

  std::sort(indexWords.begin(), indexWords.end());

  ASSERT_EQ(neg4, convertIndexWordToFloat(indexWords[0]));
  ASSERT_EQ(neg3, convertIndexWordToFloat(indexWords[1]));
  ASSERT_EQ(extra3, convertIndexWordToFloat(indexWords[2]));
  ASSERT_EQ(extra4, convertIndexWordToFloat(indexWords[3]));
  ASSERT_EQ(neg2, convertIndexWordToFloat(indexWords[4]));
  ASSERT_EQ(extra2, convertIndexWordToFloat(indexWords[5]));
  ASSERT_EQ(neg, convertIndexWordToFloat(indexWords[6]));
  ASSERT_EQ(zero, convertIndexWordToFloat(indexWords[7]));
  ASSERT_EQ(extra, convertIndexWordToFloat(indexWords[8]));
  ASSERT_EQ(pos, convertIndexWordToFloat(indexWords[9]));
  ASSERT_EQ(pos2, convertIndexWordToFloat(indexWords[10]));
  ASSERT_EQ(pos3, convertIndexWordToFloat(indexWords[11]));
  ASSERT_EQ(pos4, convertIndexWordToFloat(indexWords[12]));
  ASSERT_EQ(pos5, convertIndexWordToFloat(indexWords[13]));
  ASSERT_EQ(pos6, convertIndexWordToFloat(indexWords[14]));

  ASSERT_EQ("0.0", convertIndexWordToFloat(convertFloatToIndexWord("0", 5, 5)));
  ASSERT_EQ("1.0", convertIndexWordToFloat(convertFloatToIndexWord("1", 5, 5)));
  ASSERT_EQ("-1.0",
            convertIndexWordToFloat(convertFloatToIndexWord("-1", 5, 5)));
}

TEST(ConversionsTest, convertDateToIndexWord) {
  string ymdhmsmzIn = "1990-01-01T13:10:09.123456Z";
  string ymdhmsmzOut = ":v:date:0000000000000001990-01-01T13:10:09";
  ASSERT_EQ(ymdhmsmzOut, convertDateToIndexWord(ymdhmsmzIn));

  string negymdhmszIn = "-1990-01-01T13:10:09-03:00";
  string negymdhmszOut = ":v:date:-999999999999998009-01-01T13:10:09";
  ASSERT_EQ(negymdhmszOut, convertDateToIndexWord(negymdhmszIn));

  string ymdhmzIn = "1990-01-01T13:10+03:00";
  string ymdhmzOut = ":v:date:0000000000000001990-01-01T13:10:00";
  ASSERT_EQ(ymdhmzOut, convertDateToIndexWord(ymdhmzIn));

  string ymdhzIn = "1990-01-01T13+03:00";
  string ymdhzOut = ":v:date:0000000000000001990-01-01T13:00:00";
  ASSERT_EQ(ymdhzOut, convertDateToIndexWord(ymdhzIn));

  string ymdhmsmIn = "1990-01-01T13:10:09.000";
  string ymdhmsmOut = ":v:date:0000000000000001990-01-01T13:10:09";
  ASSERT_EQ(ymdhmsmOut, convertDateToIndexWord(ymdhmsmIn));

  string ymdhmsIn = "1990-01-01T13:10:09";
  string ymdhmsOut = ":v:date:0000000000000001990-01-01T13:10:09";
  ASSERT_EQ(ymdhmsOut, convertDateToIndexWord(ymdhmsIn));

  string yyyyIn = "1990";
  string yyyyOut = ":v:date:0000000000000001990-00-00T00:00:00";
  ASSERT_EQ(yyyyOut, convertDateToIndexWord(yyyyIn));

  string yIn = "2";
  string yOut = ":v:date:0000000000000000002-00-00T00:00:00";
  ASSERT_EQ(yOut, convertDateToIndexWord(yIn));

  string negyIn = "-2";
  string negyOut = ":v:date:-999999999999999997-00-00T00:00:00";
  ASSERT_EQ(negyOut, convertDateToIndexWord(negyIn));

  string hmIn = "T04:20";
  string hmOut = ":v:date:0000000000000000000-00-00T04:20:00";
  ASSERT_EQ(hmOut, convertDateToIndexWord(hmIn));

  string negyyIn = "-0900";
  string negyyOut = ":v:date:-999999999999999099-00-00T00:00:00";
  ASSERT_EQ(negyyOut, convertDateToIndexWord(negyyIn));
}

TEST(ConversionsTest, endToEndDate) {
  string in =
      "\"1990-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>";
  string in2 = "\"1990-01-01\"^^<http://www.w3.org/2001/XMLSchema#date>";
  string in3 = "\"1990-01-01\"^^xsd:date";
  string in4 = "\"1990-01-01T00:00:00\"^^xsd:dateTime";
  string indexW = ":v:date:0000000000000001990-01-01T00:00:00";
  ASSERT_EQ(indexW, convertValueLiteralToIndexWord(in));
  ASSERT_EQ(indexW, convertValueLiteralToIndexWord(in2));
  ASSERT_EQ(indexW, convertValueLiteralToIndexWord(in3));
  ASSERT_EQ(indexW, convertValueLiteralToIndexWord(in4));
  ASSERT_EQ(in, convertIndexWordToValueLiteral(indexW));
}

TEST(ConversionsTest, shortFormEquivalence) {
  string in = "\"1230.7\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string in_short = "\"1230.7\"^^xsd:float";
  string nin = "\"-1230.7\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string nin_short = "\"-1230.7\"^^xsd:float";
  string in2 = "\"1000\"^^<http://www.w3.org/2001/XMLSchema#int>";
  string in2_short = "\"1000\"^^xsd:int";
  string nin2 = "\"-1000\"^^<http://www.w3.org/2001/XMLSchema#int>";
  string nin2_short = "\"-1000\"^^xsd:int";
  ASSERT_EQ(convertValueLiteralToIndexWord(in),
            convertValueLiteralToIndexWord(in_short));
  ASSERT_EQ(convertValueLiteralToIndexWord(nin),
            convertValueLiteralToIndexWord(nin_short));
  ASSERT_EQ(convertValueLiteralToIndexWord(in2),
            convertValueLiteralToIndexWord(in2_short));
  ASSERT_EQ(convertValueLiteralToIndexWord(nin2),
            convertValueLiteralToIndexWord(nin2_short));
}

TEST(ConversionsTest, endToEndNumbers) {
  string in = "\"1000\"^^<http://www.w3.org/2001/XMLSchema#int>";
  string nin = "\"-1000\"^^<http://www.w3.org/2001/XMLSchema#int>";
  string in2 = "\"500\"^^<http://www.w3.org/2001/XMLSchema#int>";
  string nin2 = "\"-500\"^^<http://www.w3.org/2001/XMLSchema#int>";
  string in3 = "\"80.7\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string nin3 = "\"-80.7\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string in4 = "\"1230.7\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string nin4 = "\"-1230.7\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string in5 = "\"1230.9\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string in6 = "\"1230.9902\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string in7 = "\"1230.998\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string in8 = "\"1230.999\"^^<http://www.w3.org/2001/XMLSchema#float>";
  string in9 = "\"1230.99901\"^^<http://www.w3.org/2001/XMLSchema#float>";

  string nin5 = "\"-42.42421\"^^<http://www.w3.org/2001/XMLSchema#decimal>";
  string nout5 = "\"-42.42421\"^^<http://www.w3.org/2001/XMLSchema#float>";

  vector<string> indexWords;
  indexWords.push_back(convertValueLiteralToIndexWord(in));
  indexWords.push_back(convertValueLiteralToIndexWord(in2));
  indexWords.push_back(convertValueLiteralToIndexWord(in3));
  indexWords.push_back(convertValueLiteralToIndexWord(in4));
  indexWords.push_back(convertValueLiteralToIndexWord(in5));
  indexWords.push_back(convertValueLiteralToIndexWord(in6));
  indexWords.push_back(convertValueLiteralToIndexWord(in7));
  indexWords.push_back(convertValueLiteralToIndexWord(in8));
  indexWords.push_back(convertValueLiteralToIndexWord(in9));
  indexWords.push_back(convertValueLiteralToIndexWord(nin));
  indexWords.push_back(convertValueLiteralToIndexWord(nin2));
  indexWords.push_back(convertValueLiteralToIndexWord(nin3));
  indexWords.push_back(convertValueLiteralToIndexWord(nin4));
  indexWords.push_back(convertValueLiteralToIndexWord(nin5));

  std::sort(indexWords.begin(), indexWords.end());

  ASSERT_EQ(nin4, convertIndexWordToValueLiteral(indexWords[0]));
  ASSERT_EQ(nin, convertIndexWordToValueLiteral(indexWords[1]));
  ASSERT_EQ(nin2, convertIndexWordToValueLiteral(indexWords[2]));
  ASSERT_EQ(nin3, convertIndexWordToValueLiteral(indexWords[3]));
  ASSERT_EQ(nout5, convertIndexWordToValueLiteral(indexWords[4]));
  ASSERT_EQ(in3, convertIndexWordToValueLiteral(indexWords[5]));
  ASSERT_EQ(in2, convertIndexWordToValueLiteral(indexWords[6]));
  ASSERT_EQ(in, convertIndexWordToValueLiteral(indexWords[7]));
  ASSERT_EQ(in4, convertIndexWordToValueLiteral(indexWords[8]));
  ASSERT_EQ(in5, convertIndexWordToValueLiteral(indexWords[9]));
  ASSERT_EQ(in6, convertIndexWordToValueLiteral(indexWords[10]));
  ASSERT_EQ(in7, convertIndexWordToValueLiteral(indexWords[11]));
  ASSERT_EQ(in8, convertIndexWordToValueLiteral(indexWords[12]));
  ASSERT_EQ(in9, convertIndexWordToValueLiteral(indexWords[13]));
}


TEST(ConversionsTest, convertIndexWordToFloatValue) {
  string zero = "0.0";
  string pos = "0.339";
  string pos2 = "1.7";
  string pos3 = "2.0";
  string pos4 = "2.0000009999";
  string pos5 = "2.9999";
  string pos6 = "111000.05";
  string neg = "-0.0005002";
  string neg2 = "-0.005002";
  string neg3 = "-2023.414";
  string neg4 = "-3023.414";
  string extra = "0.001";
  string extra2 = "-0.001";
  string extra3 = "-0.10001";
  string extra4 = "-0.100001";

  vector<string> indexWords;
  indexWords.push_back(convertFloatToIndexWord(zero, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos2, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos3, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos4, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos5, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(pos6, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(neg, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(neg2, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(neg3, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(neg4, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(extra, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(extra2, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(extra3, 10, 20));
  indexWords.push_back(convertFloatToIndexWord(extra4, 10, 20));


  ASSERT_FLOAT_EQ(0, convertIndexWordToFloatValue(indexWords[0]));
  ASSERT_FLOAT_EQ(0.339, convertIndexWordToFloatValue(indexWords[1]));
  ASSERT_FLOAT_EQ(1.7, convertIndexWordToFloatValue(indexWords[2]));
  ASSERT_FLOAT_EQ(2.0, convertIndexWordToFloatValue(indexWords[3]));
  ASSERT_FLOAT_EQ(2.0000009999, convertIndexWordToFloatValue(indexWords[4]));
  ASSERT_FLOAT_EQ(2.9999, convertIndexWordToFloatValue(indexWords[5]));
  ASSERT_FLOAT_EQ(111000.05, convertIndexWordToFloatValue(indexWords[6]));
  ASSERT_FLOAT_EQ(-0.0005002, convertIndexWordToFloatValue(indexWords[7]));
  ASSERT_FLOAT_EQ(-0.005002, convertIndexWordToFloatValue(indexWords[8]));
  ASSERT_FLOAT_EQ(-2023.414, convertIndexWordToFloatValue(indexWords[9]));
  ASSERT_FLOAT_EQ(-3023.414, convertIndexWordToFloatValue(indexWords[10]));
  ASSERT_FLOAT_EQ(0.001, convertIndexWordToFloatValue(indexWords[11]));
  ASSERT_FLOAT_EQ(-0.001, convertIndexWordToFloatValue(indexWords[12]));
  ASSERT_FLOAT_EQ(-0.10001, convertIndexWordToFloatValue(indexWords[13]));
  ASSERT_FLOAT_EQ(-0.100001, convertIndexWordToFloatValue(indexWords[14]));
  ASSERT_FLOAT_EQ(0, convertIndexWordToFloatValue(
      convertFloatToIndexWord("0", 5, 5)));
  ASSERT_FLOAT_EQ(1, convertIndexWordToFloatValue(
      convertFloatToIndexWord("1", 5, 5)));
  ASSERT_FLOAT_EQ(-1, convertIndexWordToFloatValue(
      convertFloatToIndexWord("-1", 5, 5)));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
