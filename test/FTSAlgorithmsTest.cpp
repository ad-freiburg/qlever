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
  ASSERT_EQ(0u, resultCids.size());

  // None
  blockCids.push_back(0);
  blockWids.push_back(2);
  blockScores.push_back(1);

  FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                               resultCids, resultScores);
  ASSERT_EQ(0u, resultCids.size());

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
  ASSERT_EQ(4u, resultCids.size());
  ASSERT_EQ(4u, resultScores.size());

  resultCids.clear();
  resultScores.clear();

  blockCids.push_back(4);
  blockWids.push_back(8);
  blockScores.push_back(1);

  // Partial
  FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                               resultCids, resultScores);
  ASSERT_EQ(4u, resultCids.size());
  ASSERT_EQ(4u, resultScores.size());
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
  FTSAlgorithms::intersect(matchingContexts, eBlockCids, eBlockWids,
                           eBlockScores, resultCids, resultEids, resultScores);
  ASSERT_EQ(0u, resultCids.size());
  ASSERT_EQ(0u, resultScores.size());

  matchingContexts.push_back(0);
  matchingContexts.push_back(2);
  matchingContexts.push_back(3);
  matchingContextScores.push_back(1);
  matchingContextScores.push_back(1);
  matchingContextScores.push_back(1);

  FTSAlgorithms::intersect(matchingContexts, eBlockCids, eBlockWids,
                           eBlockScores, resultCids, resultEids, resultScores);
  ASSERT_EQ(0u, resultCids.size());
  ASSERT_EQ(0u, resultScores.size());

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

  FTSAlgorithms::intersect(matchingContexts, eBlockCids, eBlockWids,
                           eBlockScores, resultCids, resultEids, resultScores);
  ASSERT_EQ(2u, resultCids.size());
  ASSERT_EQ(2u, resultEids.size());
  ASSERT_EQ(2u, resultScores.size());
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
  ASSERT_EQ(0u, resCids.size());
  ASSERT_EQ(0u, resScores.size());

  cids1.push_back(0);
  cids1.push_back(2);
  scores1.push_back(1);
  scores1.push_back(1);
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(0u, resCids.size());
  ASSERT_EQ(0u, resScores.size());

  cids2.push_back(2);
  scores2.push_back(1);
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(1u, resCids.size());
  ASSERT_EQ(1u, resScores.size());

  cids2.push_back(4);
  scores2.push_back(1);
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(1u, resCids.size());
  ASSERT_EQ(1u, resScores.size());
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
  ASSERT_EQ(2u, resCids.size());
  ASSERT_EQ(0u, resEids.size());
  ASSERT_EQ(2u, resScores.size());
  ASSERT_EQ(3u, resScores[0]);
  ASSERT_EQ(5u, resScores[1]);

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
  ASSERT_EQ(2u, resCids.size());
  ASSERT_EQ(2u, resEids.size());
  ASSERT_EQ(2u, resScores.size());
  ASSERT_EQ(10u, resCids[0]);
  ASSERT_EQ(10u, resCids[1]);
  ASSERT_EQ(1u, resEids[0]);
  ASSERT_EQ(2u, resEids[1]);
  ASSERT_EQ(6u, resScores[0]);
  ASSERT_EQ(9u, resScores[1]);
};

TEST(FTSAlgorithmsTest, aggScoresAndTakeTopKContextsTest) {
  try {
    IdTable result;
    result.setCols(3);
    vector<Id> cids;
    vector<Id> eids;
    vector<Score> scores;

    FTSAlgorithms::aggScoresAndTakeTopKContexts(cids, eids, scores, 2, &result);
    ASSERT_EQ(0u, result.size());

    cids.push_back(0);
    cids.push_back(1);
    cids.push_back(2);

    eids.push_back(0);
    eids.push_back(0);
    eids.push_back(0);

    scores.push_back(0);
    scores.push_back(1);
    scores.push_back(2);

    FTSAlgorithms::aggScoresAndTakeTopKContexts(cids, eids, scores, 2, &result);
    ASSERT_EQ(2u, result.size());
    ASSERT_EQ(2u, result(0, 0));
    ASSERT_EQ(0u, result(0, 2));
    ASSERT_EQ(3u, result(0, 1));
    ASSERT_EQ(1u, result(1, 0));
    ASSERT_EQ(0u, result(1, 2));
    ASSERT_EQ(3u, result(1, 1));

    cids.push_back(4);
    eids.push_back(1);
    scores.push_back(1);

    result.clear();
    FTSAlgorithms::aggScoresAndTakeTopKContexts(cids, eids, scores, 2, &result);
    ASSERT_EQ(3u, result.size());
    std::sort(result.begin(), result.end(),
              [](const auto& a, const auto& b) { return a[0] < b[0]; });
    ASSERT_EQ(4u, result(2, 0));
    ASSERT_EQ(1u, result(2, 1));
    ASSERT_EQ(1u, result(2, 2));
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
};

TEST(FTSAlgorithmsTest, aggScoresAndTakeTopContextTest) {
  IdTable result;
  result.setCols(3);
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  FTSAlgorithms::aggScoresAndTakeTopContext(cids, eids, scores, &result);
  ASSERT_EQ(0u, result.size());

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
  ASSERT_EQ(1u, result.size());
  ASSERT_EQ(2u, result(0, 0));
  ASSERT_EQ(3u, result(0, 1));
  ASSERT_EQ(0u, result(0, 2));

  cids.push_back(3);
  eids.push_back(1);
  scores.push_back(1);

  FTSAlgorithms::aggScoresAndTakeTopContext(cids, eids, scores, &result);
  ASSERT_EQ(2u, result.size());
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a[2] < b[2]; });
  ASSERT_EQ(2u, result(0, 0));
  ASSERT_EQ(3u, result(0, 1));
  ASSERT_EQ(0u, result(0, 2));
  ASSERT_EQ(3u, result(1, 0));
  ASSERT_EQ(1u, result(1, 1));
  ASSERT_EQ(1u, result(1, 2));

  cids.push_back(4);
  eids.push_back(0);
  scores.push_back(10);

  FTSAlgorithms::aggScoresAndTakeTopContext(cids, eids, scores, &result);
  ASSERT_EQ(2u, result.size());
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a[2] < b[2]; });

  ASSERT_EQ(4u, result(0, 0));
  ASSERT_EQ(4u, result(0, 1));
  ASSERT_EQ(0u, result(0, 2));
  ASSERT_EQ(3u, result(1, 0));
  ASSERT_EQ(1u, result(1, 1));
  ASSERT_EQ(1u, result(1, 2));
};

TEST(FTSAlgorithmsTest, appendCrossProductWithSingleOtherTest) {
  ad_utility::HashMap<Id, vector<array<Id, 1>>> subRes;
  subRes[1] = vector<array<Id, 1>>{{{1}}};

  vector<array<Id, 4>> res;

  vector<Id> cids;
  cids.push_back(1);
  cids.push_back(1);

  vector<Id> eids;
  eids.push_back(0);
  eids.push_back(1);

  vector<Score> scores;
  scores.push_back(2);
  scores.push_back(2);

  FTSAlgorithms::appendCrossProduct(cids, eids, scores, 0, 2, subRes, res);

  ASSERT_EQ(2u, res.size());
  ASSERT_EQ(0u, res[0][0]);
  ASSERT_EQ(2u, res[0][1]);
  ASSERT_EQ(1u, res[0][2]);
  ASSERT_EQ(1u, res[0][3]);
  ASSERT_EQ(1u, res[1][0]);
  ASSERT_EQ(2u, res[1][1]);
  ASSERT_EQ(1u, res[1][2]);
  ASSERT_EQ(1u, res[1][3]);

  subRes[0] = vector<array<Id, 1>>{{{0}}};
  res.clear();
  FTSAlgorithms::appendCrossProduct(cids, eids, scores, 0, 2, subRes, res);

  ASSERT_EQ(4u, res.size());
  ASSERT_EQ(0u, res[0][0]);
  ASSERT_EQ(2u, res[0][1]);
  ASSERT_EQ(1u, res[0][2]);
  ASSERT_EQ(0u, res[0][3]);
  ASSERT_EQ(0u, res[1][0]);
  ASSERT_EQ(2u, res[1][1]);
  ASSERT_EQ(1u, res[1][2]);
  ASSERT_EQ(1u, res[1][3]);
  ASSERT_EQ(1u, res[2][0]);
  ASSERT_EQ(2u, res[2][1]);
  ASSERT_EQ(1u, res[2][2]);
  ASSERT_EQ(0u, res[2][3]);
  ASSERT_EQ(1u, res[3][0]);
  ASSERT_EQ(2u, res[3][1]);
  ASSERT_EQ(1u, res[3][2]);
  ASSERT_EQ(1u, res[3][3]);
}

TEST(FTSAlgorithmsTest, appendCrossProductWithTwoW1Test) {
  ad_utility::HashSet<Id> subRes1;
  subRes1.insert(1);
  subRes1.insert(2);

  ad_utility::HashSet<Id> subRes2;
  subRes2.insert(0);
  subRes2.insert(5);

  vector<array<Id, 5>> res;

  vector<Id> cids;
  cids.push_back(1);
  cids.push_back(1);

  vector<Id> eids;
  eids.push_back(0);
  eids.push_back(1);

  vector<Score> scores;
  scores.push_back(2);
  scores.push_back(2);

  FTSAlgorithms::appendCrossProduct(cids, eids, scores, 0, 2, subRes1, subRes2,
                                    res);

  ASSERT_EQ(2u, res.size());
  ASSERT_EQ(0u, res[0][0]);
  ASSERT_EQ(2u, res[0][1]);
  ASSERT_EQ(1u, res[0][2]);
  ASSERT_EQ(1u, res[0][3]);
  ASSERT_EQ(0u, res[0][4]);
  ASSERT_EQ(1u, res[1][0]);
  ASSERT_EQ(2u, res[1][1]);
  ASSERT_EQ(1u, res[1][2]);
  ASSERT_EQ(1u, res[1][3]);
  ASSERT_EQ(0u, res[0][4]);
}

TEST(FTSAlgorithmsTest, multVarsAggScoresAndTakeTopKContexts) {
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  size_t nofVars = 2;
  size_t k = 1;
  IdTable resW4(4);
  FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                      nofVars, k, &resW4);
  ASSERT_EQ(0u, resW4.size());
  nofVars = 5;
  k = 10;
  IdTable resWV(13);
  FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                      nofVars, k, &resWV);
  ASSERT_EQ(0u, resWV.size());

  cids.push_back(0);
  cids.push_back(1);
  cids.push_back(1);
  cids.push_back(2);
  cids.push_back(2);
  cids.push_back(2);

  eids.push_back(0);
  eids.push_back(0);
  eids.push_back(1);
  eids.push_back(0);
  eids.push_back(1);
  eids.push_back(2);

  scores.push_back(10);
  scores.push_back(1);
  scores.push_back(3);
  scores.push_back(1);
  scores.push_back(1);
  scores.push_back(1);

  nofVars = 2;
  k = 1;
  FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                      nofVars, k, &resW4);

  // Res 0-0-0 (3) | 0-1 1-0 1-1 (2) | 0-2 1-2 2-0 2-1 2-2 (1)
  ASSERT_EQ(9u, resW4.size());
  std::sort(std::begin(resW4), std::end(resW4),
            [](const auto& a, const auto& b) { return a[1] > b[1]; });
  ASSERT_EQ(3u, resW4(0, 1));
  ASSERT_EQ(0u, resW4(0, 0));
  ASSERT_EQ(0u, resW4(0, 2));
  ASSERT_EQ(0u, resW4(0, 3));
  ASSERT_EQ(2u, resW4(1, 1));
  ASSERT_EQ(2u, resW4(2, 1));
  ASSERT_EQ(2u, resW4(3, 1));
  ASSERT_EQ(1u, resW4(4, 1));
  ASSERT_EQ(1u, resW4(5, 1));
  k = 2;
  resW4.clear();
  FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                      nofVars, k, &resW4);
  ASSERT_EQ(13u, resW4.size());
  std::sort(std::begin(resW4), std::end(resW4),
            [](const auto& a, const auto& b) { return a[1] > b[1]; });
  ASSERT_EQ(3u, resW4(0, 1));
  ASSERT_EQ(0u, resW4(0, 0));
  ASSERT_EQ(0u, resW4(0, 2));
  ASSERT_EQ(0u, resW4(0, 3));
  ASSERT_EQ(1u, resW4(1, 0));
  ASSERT_EQ(3u, resW4(1, 1));
  ASSERT_EQ(0u, resW4(1, 2));
  ASSERT_EQ(0u, resW4(1, 3));

  nofVars = 3;
  k = 1;
  IdTable resW5(5);
  FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                      nofVars, k, &resW5);
  ASSERT_EQ(27u, resW5.size());  // Res size 3^3

  nofVars = 10;
  FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                      nofVars, k, &resWV);
  ASSERT_EQ(59049u, resWV.size());  // Res size: 3^10 = 59049
}

TEST(FTSAlgorithmsTest, oneVarFilterAggScoresAndTakeTopKContexts) {
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  size_t k = 1;
  IdTable resW3(3);
  ad_utility::HashMap<Id, IdTable> fMap1;

  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                          fMap1, k, &resW3);
  ASSERT_EQ(0u, resW3.size());

  cids.push_back(0);
  cids.push_back(1);
  cids.push_back(1);
  cids.push_back(2);
  cids.push_back(2);
  cids.push_back(2);

  eids.push_back(0);
  eids.push_back(0);
  eids.push_back(1);
  eids.push_back(0);
  eids.push_back(1);
  eids.push_back(2);

  scores.push_back(10);
  scores.push_back(1);
  scores.push_back(3);
  scores.push_back(1);
  scores.push_back(1);
  scores.push_back(1);

  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                          fMap1, k, &resW3);
  ASSERT_EQ(0u, resW3.size());

  fMap1[1] = IdTable(1);
  fMap1[1].push_back({1});

  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                          fMap1, k, &resW3);
  ASSERT_EQ(1u, resW3.size());
  resW3.clear();
  k = 10;
  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                          fMap1, k, &resW3);
  ASSERT_EQ(2u, resW3.size());

  fMap1[0] = IdTable(1);
  fMap1[0].push_back({0});
  resW3.clear();
  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                          fMap1, k, &resW3);
  ASSERT_EQ(5u, resW3.size());

  ad_utility::HashMap<Id, IdTable> fMap4;
  fMap4[0] = IdTable(4);
  fMap4[0].push_back({0, 0, 0, 0});
  fMap4[0].push_back({0, 1, 0, 0});
  fMap4[0].push_back({0, 2, 0, 0});
  IdTable resVar(7);
  k = 1;
  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                          fMap4, k, &resVar);
  ASSERT_EQ(3u, resVar.size());

  fMap4[2] = IdTable(4);
  fMap4[2].push_back({2, 2, 2, 2});
  resVar.clear();
  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(cids, eids, scores,
                                                          fMap4, k, &resVar);
  ASSERT_EQ(4u, resVar.size());
}

TEST(FTSAlgorithmsTest, multVarsFilterAggScoresAndTakeTopKContexts) {
  try {
    vector<Id> cids;
    vector<Id> eids;
    vector<Score> scores;
    cids.push_back(0);
    cids.push_back(1);
    cids.push_back(1);
    cids.push_back(2);
    cids.push_back(2);
    cids.push_back(2);

    eids.push_back(0);
    eids.push_back(0);
    eids.push_back(1);
    eids.push_back(0);
    eids.push_back(1);
    eids.push_back(2);

    scores.push_back(10);
    scores.push_back(3);
    scores.push_back(3);
    scores.push_back(1);
    scores.push_back(1);
    scores.push_back(1);

    size_t k = 1;
    IdTable resW4(4);
    ad_utility::HashMap<Id, IdTable> fMap1;

    size_t nofVars = 2;
    FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts(
        cids, eids, scores, fMap1, nofVars, k, &resW4);
    ASSERT_EQ(0u, resW4.size());

    fMap1[1] = IdTable(1);
    fMap1[1].push_back({1});

    FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts(
        cids, eids, scores, fMap1, nofVars, k, &resW4);

    ASSERT_EQ(3u, resW4.size());  // 1-1 1-0 1-2

    std::sort(resW4.begin(), resW4.end(), [](const auto& a, const auto& b) {
      if (a[1] == b[1]) {
        if (a[2] == b[2]) {
          return a[0] < b[0];
        }
        return a[2] < b[2];
      }
      return a[1] > b[1];
    });

    ASSERT_EQ(1u, resW4(0, 0));
    ASSERT_EQ(2u, resW4(0, 1));
    ASSERT_EQ(0u, resW4(0, 2));
    ASSERT_EQ(1u, resW4(0, 3));
    ASSERT_EQ(1u, resW4(1, 0));
    ASSERT_EQ(2u, resW4(1, 1));
    ASSERT_EQ(1u, resW4(1, 2));
    ASSERT_EQ(1u, resW4(1, 3));
    ASSERT_EQ(2u, resW4(2, 0));
    ASSERT_EQ(1u, resW4(2, 1));
    ASSERT_EQ(2u, resW4(2, 2));
    ASSERT_EQ(1u, resW4(2, 3));

    resW4.clear();
    FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts(
        cids, eids, scores, fMap1, nofVars, 2, &resW4);
    ASSERT_EQ(5u, resW4.size());  // 2x 1-1  2x 1-0   1x 1-2

    std::sort(std::begin(resW4), std::end(resW4),
              [](const auto& a, const auto& b) {
                if (a[1] == b[1]) {
                  if (a[2] == b[2]) {
                    return a[0] < b[0];
                  }
                  return a[2] < b[2];
                }
                return a[1] > b[1];
              });

    ASSERT_EQ(1u, resW4(0, 0));
    ASSERT_EQ(2u, resW4(0, 1));
    ASSERT_EQ(0u, resW4(0, 2));
    ASSERT_EQ(1u, resW4(0, 3));
    ASSERT_EQ(2u, resW4(1, 0));
    ASSERT_EQ(2u, resW4(1, 1));
    ASSERT_EQ(0u, resW4(1, 2));
    ASSERT_EQ(1u, resW4(1, 3));

    ASSERT_EQ(1u, resW4(2, 0));
    ASSERT_EQ(2u, resW4(2, 1));
    ASSERT_EQ(1u, resW4(2, 2));
    ASSERT_EQ(1u, resW4(2, 3));
    ASSERT_EQ(2u, resW4(3, 0));
    ASSERT_EQ(2u, resW4(3, 1));
    ASSERT_EQ(1u, resW4(3, 2));
    ASSERT_EQ(1u, resW4(3, 3));

    ASSERT_EQ(2u, resW4(4, 0));
    ASSERT_EQ(1u, resW4(4, 1));
    ASSERT_EQ(2u, resW4(4, 2));
    ASSERT_EQ(1u, resW4(4, 3));

  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
