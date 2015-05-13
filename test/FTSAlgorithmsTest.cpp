// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>
#include "../src/index/FTSAlgorithms.h"

TEST(FTSAlgorithmsTest, filterByRangeTest) {
  IdRange idRange;
  idRange._first = 5;
  idRange._last = 7;

  vector<Id> blockCids;
  vector<Id> blockWids;
  vector<Score> blockScores;
  vector<Id> resultCids;
  vector<Score> resultScores;

  // Empty
  FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                               resultCids, resultScores);
  ASSERT_EQ(0, resultCids.size());

  // None
  blockCids.push_back(0);
  blockWids.push_back(2);
  blockScores.push_back(1);

  FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                               resultCids, resultScores);
  ASSERT_EQ(0, resultCids.size());

  // Match
  blockCids.push_back(0);
  blockCids.push_back(1);
  blockCids.push_back(2);
  blockCids.push_back(3);

  blockWids.push_back(5);
  blockWids.push_back(7);
  blockWids.push_back(5);
  blockWids.push_back(6);

  blockScores.push_back(1);
  blockScores.push_back(1);
  blockScores.push_back(1);
  blockScores.push_back(1);

  FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                               resultCids, resultScores);
  ASSERT_EQ(4, resultCids.size());
  ASSERT_EQ(4, resultScores.size());

  resultCids.clear();
  resultScores.clear();

  blockCids.push_back(4);
  blockWids.push_back(8);
  blockScores.push_back(1);

  // Partial
  FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                               resultCids, resultScores);
  ASSERT_EQ(4, resultCids.size());
  ASSERT_EQ(4, resultScores.size());

};

TEST(FTSAlgorithmsTest, intersectTest) {
  vector<Id> matchingContexts;
  vector<Score> matchingContextScores;
  vector<Id> eBlockCids;
  vector<Id> eBlockWids;
  vector<Score> eBlockScores;
  vector<Id> resultCids;
  vector<Id> resultEids;
  vector<Score> resultScores;
  FTSAlgorithms::intersect(matchingContexts, matchingContextScores, eBlockCids,
                           eBlockWids, eBlockScores, resultCids, resultEids,
                           resultScores);
  ASSERT_EQ(0, resultCids.size());
  ASSERT_EQ(0, resultScores.size());

  matchingContexts.push_back(0);
  matchingContexts.push_back(2);
  matchingContexts.push_back(3);
  matchingContextScores.push_back(1);
  matchingContextScores.push_back(1);
  matchingContextScores.push_back(1);

  FTSAlgorithms::intersect(matchingContexts, matchingContextScores, eBlockCids,
                           eBlockWids, eBlockScores, resultCids, resultEids,
                           resultScores);
  ASSERT_EQ(0, resultCids.size());
  ASSERT_EQ(0, resultScores.size());

  eBlockCids.push_back(1);
  eBlockCids.push_back(2);
  eBlockCids.push_back(2);
  eBlockCids.push_back(4);
  eBlockWids.push_back(10);
  eBlockWids.push_back(1);
  eBlockWids.push_back(1);
  eBlockWids.push_back(2);
  eBlockScores.push_back(1);
  eBlockScores.push_back(1);
  eBlockScores.push_back(1);
  eBlockScores.push_back(1);

  FTSAlgorithms::intersect(matchingContexts, matchingContextScores, eBlockCids,
                           eBlockWids, eBlockScores, resultCids, resultEids,
                           resultScores);
  ASSERT_EQ(2, resultCids.size());
  ASSERT_EQ(2, resultEids.size());
  ASSERT_EQ(2, resultScores.size());
};

TEST(FTSAlgorithmsTest, intersectTwoPostingListsTest) {
  vector<Id> cids1;
  vector<Score> scores1;
  vector<Id> cids2;
  vector<Score> scores2;
  vector<Id> resCids;
  vector<Score> resScores;
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(0, resCids.size());
  ASSERT_EQ(0, resScores.size());

  cids1.push_back(0);
  cids1.push_back(2);
  scores1.push_back(1);
  scores1.push_back(1);
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(0, resCids.size());
  ASSERT_EQ(0, resScores.size());

  cids2.push_back(2);
  scores2.push_back(1);
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(1, resCids.size());
  ASSERT_EQ(1, resScores.size());

  cids2.push_back(4);
  scores2.push_back(1);
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(1, resCids.size());
  ASSERT_EQ(1, resScores.size());
};

TEST(FTSAlgorithmsTest, intersectKWayTest) {
  vector<vector<Id>> cidVecs;
  vector<vector<Score>> scoreVecs;
  vector<Id> eids;
  vector<Id> resCids;
  vector<Id> resEids;
  vector<Score> resScores;


  vector<Score> fourScores;
  fourScores.push_back(1);
  fourScores.push_back(1);
  fourScores.push_back(1);
  fourScores.push_back(1);

  vector<Id> cids1;
  cids1.push_back(0);
  cids1.push_back(1);
  cids1.push_back(2);
  cids1.push_back(10);

  cidVecs.push_back(cids1);
  scoreVecs.push_back(fourScores);

  cids1[2] = 8;
  cidVecs.push_back(cids1);
  scoreVecs.push_back(fourScores);

  cids1[1] = 6;
  fourScores[3] = 3;
  cidVecs.push_back(cids1);
  scoreVecs.push_back(fourScores);

  // No eids / no special case
  FTSAlgorithms::intersectKWay(cidVecs, scoreVecs, nullptr, resCids, resEids,
                               resScores);
  ASSERT_EQ(2, resCids.size());
  ASSERT_EQ(0, resEids.size());
  ASSERT_EQ(2, resScores.size());
  ASSERT_EQ(3, resScores[0]);
  ASSERT_EQ(5, resScores[1]);

  // With eids
  eids.push_back(1);
  eids.push_back(4);
  eids.push_back(1);
  eids.push_back(4);
  eids.push_back(1);
  eids.push_back(2);

  vector<Id> cids2;
  vector<Score> scores2;

  cids2.push_back(1);
  cids2.push_back(1);
  cids2.push_back(3);
  cids2.push_back(4);
  cids2.push_back(10);
  cids2.push_back(10);

  scores2.push_back(1);
  scores2.push_back(4);
  scores2.push_back(1);
  scores2.push_back(4);
  scores2.push_back(1);
  scores2.push_back(4);

  cidVecs.push_back(cids2);
  scoreVecs.push_back(scores2);

  FTSAlgorithms::intersectKWay(cidVecs, scoreVecs, &eids, resCids, resEids,
                               resScores);
  ASSERT_EQ(2, resCids.size());
  ASSERT_EQ(2, resEids.size());
  ASSERT_EQ(2, resScores.size());
  ASSERT_EQ(10, resCids[0]);
  ASSERT_EQ(10, resCids[1]);
  ASSERT_EQ(1, resEids[0]);
  ASSERT_EQ(2, resEids[1]);
  ASSERT_EQ(6, resScores[0]);
  ASSERT_EQ(9, resScores[1]);
};

TEST(FTSAlgorithmsTest, aggScoresAndTakeTopKContextsTest) {
  FAIL() << "Not yet implemented";
};

TEST(FTSAlgorithmsTest, aggScoresAndTakeTopContextTest) {
  FTSAlgorithms::WidthThreeList result;
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  FTSAlgorithms::aggScoresAndTakeTopContext(cids, eids, scores, &result);
  ASSERT_EQ(0, result.size());

  cids.push_back(0);
  cids.push_back(1);
  cids.push_back(2);

  eids.push_back(0);
  eids.push_back(0);
  eids.push_back(0);

  scores.push_back(0);
  scores.push_back(1);
  scores.push_back(2);

  FTSAlgorithms::aggScoresAndTakeTopContext(cids, eids, scores, &result);
  ASSERT_EQ(1, result.size());
  ASSERT_EQ(0, result[0][0]);
  ASSERT_EQ(3, result[0][1]);
  ASSERT_EQ(2, result[0][2]);

  cids.push_back(3);
  eids.push_back(1);
  scores.push_back(1);

  FTSAlgorithms::aggScoresAndTakeTopContext(cids, eids, scores, &result);
  ASSERT_EQ(2, result.size());
  std::sort(result.begin(), result.end(),
            [](const array<Id, 3>& a, const array<Id, 3>& b) {
              return a[0] < b[0];
            });
  ASSERT_EQ(0, result[0][0]);
  ASSERT_EQ(3, result[0][1]);
  ASSERT_EQ(2, result[0][2]);
  ASSERT_EQ(1, result[1][0]);
  ASSERT_EQ(1, result[1][1]);
  ASSERT_EQ(3, result[1][2]);

  cids.push_back(4);
  eids.push_back(0);
  scores.push_back(10);

  FTSAlgorithms::aggScoresAndTakeTopContext(cids, eids, scores, &result);
  ASSERT_EQ(2, result.size());
  std::sort(result.begin(), result.end(),
            [](const array<Id, 3>& a, const array<Id, 3>& b) {
              return a[0] < b[0];
            });
  ASSERT_EQ(0, result[0][0]);
  ASSERT_EQ(13, result[0][1]);
  ASSERT_EQ(10, result[0][2]);
  ASSERT_EQ(1, result[1][0]);
  ASSERT_EQ(1, result[1][1]);
  ASSERT_EQ(3, result[1][2]);
};

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

