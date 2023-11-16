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
      "<a> <p> \"he failed the test\" . <a> <p> \"testing can help\" . <a> <p> "
      "\"some other sentence\" . <b> <p> \"test but with wrong subject\" . <b> "
      "<x2> <x> . <b> <x2> <xb2> .", true, true, true, 32UL, true);
  // create operations
  TextIndexScanForWord s1{qec, Variable{"?a"}, "test*"};
  // compute test results
  auto result = s1.computeResultOnlyForTesting();
  // compare results
  for (int i=0; i<result.idTable().getColumn(0).size(); i++) {
      TextRecordIndex tridx = result.idTable().getColumn(0)[i].getTextRecordIndex();
      std::string s = qec->getIndex().getTextExcerpt(tridx);
      LOG(INFO) << " Text: " << s << " Word: " << qec->getIndex().idToOptionalString(result.idTable().getColumn(1)[i].getWordVocabIndex()).value() << endl;
  }

  ASSERT_EQ(result.idTable(), makeIdTableFromVector({}));
  ASSERT_EQ(qec->getIndex().getTextExcerpt(result.idTable().getColumn(0)[0].getTextRecordIndex()), "he failed the test");

  // TODO: add entity scan
  // TODO: fixed entity case
}
