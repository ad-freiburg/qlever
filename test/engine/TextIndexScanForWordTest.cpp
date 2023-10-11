#include <gtest/gtest.h>

#include "../IndexTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "engine/IndexScan.h"
#include "engine/TextIndexScanForWord.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;
using ad_utility::source_location;

TEST(TextIndexScanForWord, WordScanBasic) {
  // build qec
  auto qec = getQec(
      "<a> <p> \"he failed the test\". <a> <p> \"testing can help\". <a> <p> "
      "\"some other sentence\". <b> <p> \"test but with wrong subject\". <b> "
      "<x2> <x>. <b> <x2> <xb2> .");
  // create operation
  TextIndexScanForWord s1{qec, Variable{"?a"}, "test*"};
  // compute test results
  auto result = s1.computeResultOnlyForTesting();
  auto idConverter = makeGetId(qec->getIndex());
  // IdTable expectedResult =
  //    makeIdTableFromVector({{idConverter("\"he failed the test\""),
  //                            idConverter("\"test\""), idConverter("1")}});//
  //                            make id from int 1
  // stattdessen idToString in Type
  // id datentyp checken
  IdTable expectedResult = makeIdTableFromVector({});
  // compare results
  ASSERT_EQ(expectedResult, result.idTable());
}
