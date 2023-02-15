// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/engine/CallFixedSize.h"
#include "../src/index/FTSAlgorithms.h"
#include "util/DisableWarningsClang13.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}
auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};
auto T = [](const auto& id) { return TextRecordIndex::make(id); };
auto TextId = [](const auto& id) { return Id::makeFromTextRecordIndex(T(id)); };
auto IntId = [](const auto& id) { return Id::makeFromInt(id); };

TEST(FTSAlgorithmsTest, filterByRangeTest) {
  IdRange idRange;
  idRange._first = VocabIndex::make(5);
  idRange._last = VocabIndex::make(7);

  vector<TextRecordIndex> blockCids;
  vector<WordIndex> blockWids;
  vector<Score> blockScores;
  vector<TextRecordIndex> resultCids;
  vector<Score> resultScores;

  // Empty
  FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                               resultCids, resultScores);
  ASSERT_EQ(0u, resultCids.size());

  // None
  blockCids.push_back(T(0));
  blockWids.push_back(2);
  blockScores.push_back(1);

  FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                               resultCids, resultScores);
  ASSERT_EQ(0u, resultCids.size());

  // Match
  blockCids.push_back(T(0));
  blockCids.push_back(T(1));
  blockCids.push_back(T(2));
  blockCids.push_back(T(3));

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

  blockCids.push_back(T(4));
  blockWids.push_back(8);
  blockScores.push_back(1);

  // Partial
  FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                               resultCids, resultScores);
  ASSERT_EQ(4u, resultCids.size());
  ASSERT_EQ(4u, resultScores.size());
};

TEST(FTSAlgorithmsTest, intersectTest) {
  vector<TextRecordIndex> matchingContexts;
  vector<Score> matchingContextScores;
  vector<TextRecordIndex> eBlockCids;
  vector<Id> eBlockWids;
  vector<Score> eBlockScores;
  vector<TextRecordIndex> resultCids;
  vector<Id> resultEids;
  vector<Score> resultScores;
  FTSAlgorithms::intersect(matchingContexts, eBlockCids, eBlockWids,
                           eBlockScores, resultCids, resultEids, resultScores);
  ASSERT_EQ(0u, resultCids.size());
  ASSERT_EQ(0u, resultScores.size());

  matchingContexts.push_back(T(0));
  matchingContexts.push_back(T(2));
  matchingContexts.push_back(T(3));
  matchingContextScores.push_back(1);
  matchingContextScores.push_back(1);
  matchingContextScores.push_back(1);

  FTSAlgorithms::intersect(matchingContexts, eBlockCids, eBlockWids,
                           eBlockScores, resultCids, resultEids, resultScores);
  ASSERT_EQ(0u, resultCids.size());
  ASSERT_EQ(0u, resultScores.size());

  eBlockCids.push_back(T(1));
  eBlockCids.push_back(T(2));
  eBlockCids.push_back(T(2));
  eBlockCids.push_back(T(4));
  eBlockWids.push_back(I(10));
  eBlockWids.push_back(I(1));
  eBlockWids.push_back(I(1));
  eBlockWids.push_back(I(2));
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
  vector<TextRecordIndex> cids1;
  vector<Score> scores1;
  vector<TextRecordIndex> cids2;
  vector<Score> scores2;
  vector<TextRecordIndex> resCids;
  vector<Score> resScores;
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(0u, resCids.size());
  ASSERT_EQ(0u, resScores.size());

  cids1.push_back(T(0));
  cids1.push_back(T(2));
  scores1.push_back(1);
  scores1.push_back(1);
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(0u, resCids.size());
  ASSERT_EQ(0u, resScores.size());

  cids2.push_back(T(2));
  scores2.push_back(1);
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(1u, resCids.size());
  ASSERT_EQ(1u, resScores.size());

  cids2.push_back(T(4));
  scores2.push_back(1);
  FTSAlgorithms::intersectTwoPostingLists(cids1, scores1, cids2, scores2,
                                          resCids, resScores);
  ASSERT_EQ(1u, resCids.size());
  ASSERT_EQ(1u, resScores.size());
};

TEST(FTSAlgorithmsTest, intersectKWayTest) {
  vector<vector<TextRecordIndex>> cidVecs;
  vector<vector<Score>> scoreVecs;
  vector<Id> eids;
  vector<TextRecordIndex> resCids;
  vector<Id> resEids;
  vector<Score> resScores;

  vector<Score> fourScores;
  fourScores.push_back(1);
  fourScores.push_back(1);
  fourScores.push_back(1);
  fourScores.push_back(1);

  vector<TextRecordIndex> cids1;
  cids1.push_back(T(0));
  cids1.push_back(T(1));
  cids1.push_back(T(2));
  cids1.push_back(T(10));

  cidVecs.push_back(cids1);
  scoreVecs.push_back(fourScores);

  cids1[2] = T(8);
  cidVecs.push_back(cids1);
  scoreVecs.push_back(fourScores);

  cids1[1] = T(6);
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
  eids.push_back(I(1));
  eids.push_back(I(4));
  eids.push_back(I(1));
  eids.push_back(I(4));
  eids.push_back(I(1));
  eids.push_back(I(2));

  vector<TextRecordIndex> cids2;
  vector<Score> scores2;

  cids2.push_back(T(1));
  cids2.push_back(T(1));
  cids2.push_back(T(3));
  cids2.push_back(T(4));
  cids2.push_back(T(10));
  cids2.push_back(T(10));

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
  ASSERT_EQ(10, resCids[0].get());
  ASSERT_EQ(10, resCids[1].get());
  ASSERT_EQ(I(1), resEids[0]);
  ASSERT_EQ(I(2), resEids[1]);
  ASSERT_EQ(6u, resScores[0]);
  ASSERT_EQ(9u, resScores[1]);
};

TEST(FTSAlgorithmsTest, aggScoresAndTakeTopKContextsTest) {
  IdTable result{allocator()};
  result.setNumColumns(3);
  vector<TextRecordIndex> cids;
  vector<Id> eids;
  vector<Score> scores;

  FTSAlgorithms::aggScoresAndTakeTopKContexts(cids, eids, scores, 2, &result);
  ASSERT_EQ(0u, result.size());

  cids.push_back(T(0));
  cids.push_back(T(1));
  cids.push_back(T(2));

  eids.push_back(I(0));
  eids.push_back(I(0));
  eids.push_back(I(0));

  scores.push_back(0);
  scores.push_back(1);
  scores.push_back(2);

  FTSAlgorithms::aggScoresAndTakeTopKContexts(cids, eids, scores, 2, &result);
  ASSERT_EQ(2u, result.size());
  ASSERT_EQ(TextId(2u), result(0, 0));
  ASSERT_EQ(IntId(3u), result(0, 1));
  ASSERT_EQ(I(0u), result(0, 2));
  ASSERT_EQ(TextId(1u), result(1, 0));
  ASSERT_EQ(IntId(3u), result(1, 1));
  ASSERT_EQ(I(0u), result(1, 2));

  cids.push_back(T(4));
  eids.push_back(I(1));
  scores.push_back(1);

  result.clear();
  FTSAlgorithms::aggScoresAndTakeTopKContexts(cids, eids, scores, 2, &result);
  ASSERT_EQ(3u, result.size());
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a[0] < b[0]; });
  ASSERT_EQ(TextId(4u), result(2, 0));
  ASSERT_EQ(IntId(1u), result(2, 1));
  ASSERT_EQ(I(1u), result(2, 2));
};

TEST(FTSAlgorithmsTest, aggScoresAndTakeTopContextTest) {
  IdTable result{allocator()};
  result.setNumColumns(3);
  vector<TextRecordIndex> cids;
  vector<Id> eids;
  vector<Score> scores;
  int width = result.numColumns();

  // In the following there are many similar calls to `callFixedSize`.
  // We use a lambda to reduce code duplication.
  DISABLE_WARNINGS_CLANG_13
  auto callFixed = [](int width, auto&&... args) {
    ad_utility::callFixedSize(width, [&]<int WIDTH>() {
      FTSAlgorithms::aggScoresAndTakeTopContext<WIDTH>(AD_FWD(args)...);
    });
  };
  ENABLE_WARNINGS_CLANG_13

  callFixed(width, cids, eids, scores, &result);

  ASSERT_EQ(0u, result.size());

  cids.push_back(T(0));
  cids.push_back(T(1));
  cids.push_back(T(2));

  eids.push_back(I(0));
  eids.push_back(I(0));
  eids.push_back(I(0));

  scores.push_back(0);
  scores.push_back(1);
  scores.push_back(2);

  callFixed(width, cids, eids, scores, &result);
  ASSERT_EQ(1u, result.size());
  ASSERT_EQ(TextId(2u), result(0, 0));
  ASSERT_EQ(IntId(3u), result(0, 1));
  ASSERT_EQ(I(0u), result(0, 2));

  cids.push_back(T(3));
  eids.push_back(I(1));
  scores.push_back(1);

  callFixed(width, cids, eids, scores, &result);
  ASSERT_EQ(2u, result.size());
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a[2] < b[2]; });
  ASSERT_EQ(TextId(2u), result(0, 0));
  ASSERT_EQ(IntId(3u), result(0, 1));
  ASSERT_EQ(I(0u), result(0, 2));
  ASSERT_EQ(TextId(3u), result(1, 0));
  ASSERT_EQ(IntId(1u), result(1, 1));
  ASSERT_EQ(I(1u), result(1, 2));

  cids.push_back(T(4));
  eids.push_back(I(0));
  scores.push_back(10);

  DISABLE_WARNINGS_CLANG_13
  ad_utility::callFixedSize(
      width, [&cids, &eids, &scores, &result]<int WIDTH>() mutable {
        ENABLE_WARNINGS_CLANG_13
        FTSAlgorithms::aggScoresAndTakeTopContext<WIDTH>(cids, eids, scores,
                                                         &result);
      });
  ASSERT_EQ(2u, result.size());
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a[2] < b[2]; });

  ASSERT_EQ(TextId(4u), result(0, 0));
  ASSERT_EQ(IntId(4u), result(0, 1));
  ASSERT_EQ(I(0u), result(0, 2));
  ASSERT_EQ(TextId(3u), result(1, 0));
  ASSERT_EQ(IntId(1u), result(1, 1));
  ASSERT_EQ(I(1u), result(1, 2));
};

TEST(FTSAlgorithmsTest, appendCrossProductWithSingleOtherTest) {
  ad_utility::HashMap<Id, vector<array<Id, 1>>> subRes;
  subRes[I(1)] = vector<array<Id, 1>>{{{I(1)}}};

  vector<array<Id, 4>> res;

  vector<TextRecordIndex> cids;
  cids.push_back(T(1));
  cids.push_back(T(1));

  vector<Id> eids;
  eids.push_back(I(0));
  eids.push_back(I(1));

  vector<Score> scores;
  scores.push_back(2);
  scores.push_back(2);

  FTSAlgorithms::appendCrossProduct(cids, eids, scores, 0, 2, subRes, res);

  ASSERT_EQ(2, res.size());
  ASSERT_EQ(I(0), res[0][0]);
  ASSERT_EQ(IntId(2), res[0][1]);
  ASSERT_EQ(TextId(1), res[0][2]);
  ASSERT_EQ(I(1), res[0][3]);
  ASSERT_EQ(I(1), res[1][0]);
  ASSERT_EQ(IntId(2), res[1][1]);
  ASSERT_EQ(TextId(1), res[1][2]);
  ASSERT_EQ(I(1), res[1][3]);

  subRes[I(0)] = vector<array<Id, 1>>{{{I(0)}}};
  res.clear();
  FTSAlgorithms::appendCrossProduct(cids, eids, scores, 0, 2, subRes, res);

  ASSERT_EQ(4u, res.size());
  ASSERT_EQ(I(0), res[0][0]);
  ASSERT_EQ(IntId(2), res[0][1]);
  ASSERT_EQ(TextId(1), res[0][2]);
  ASSERT_EQ(I(0), res[0][3]);
  ASSERT_EQ(I(0), res[1][0]);
  ASSERT_EQ(IntId(2), res[1][1]);
  ASSERT_EQ(TextId(1), res[1][2]);
  ASSERT_EQ(I(1), res[1][3]);
  ASSERT_EQ(I(1), res[2][0]);
  ASSERT_EQ(IntId(2), res[2][1]);
  ASSERT_EQ(TextId(1), res[2][2]);
  ASSERT_EQ(I(0), res[2][3]);
  ASSERT_EQ(I(1), res[3][0]);
  ASSERT_EQ(IntId(2), res[3][1]);
  ASSERT_EQ(TextId(1), res[3][2]);
  ASSERT_EQ(I(1), res[3][3]);
}

TEST(FTSAlgorithmsTest, appendCrossProductWithTwoW1Test) {
  ad_utility::HashSet<Id> subRes1;
  subRes1.insert(I(1));
  subRes1.insert(I(2));

  ad_utility::HashSet<Id> subRes2;
  subRes2.insert(I(0));
  subRes2.insert(I(5));

  vector<array<Id, 5>> res;

  vector<TextRecordIndex> cids;
  cids.push_back(T(1));
  cids.push_back(T(1));

  vector<Id> eids;
  eids.push_back(I(0));
  eids.push_back(I(1));

  vector<Score> scores;
  scores.push_back(2);
  scores.push_back(2);

  FTSAlgorithms::appendCrossProduct(cids, eids, scores, 0, 2, subRes1, subRes2,
                                    res);

  ASSERT_EQ(2u, res.size());
  ASSERT_EQ(I(0), res[0][0]);
  ASSERT_EQ(IntId(2), res[0][1]);
  ASSERT_EQ(TextId(1), res[0][2]);
  ASSERT_EQ(I(1), res[0][3]);
  ASSERT_EQ(I(0), res[0][4]);
  ASSERT_EQ(I(1), res[1][0]);
  ASSERT_EQ(IntId(2), res[1][1]);
  ASSERT_EQ(TextId(1), res[1][2]);
  ASSERT_EQ(I(1), res[1][3]);
  ASSERT_EQ(I(0), res[0][4]);
}

TEST(FTSAlgorithmsTest, multVarsAggScoresAndTakeTopKContexts) {
  auto callFixed = [](int width, auto&&... args) {
    ad_utility::callFixedSize(
        DISABLE_WARNINGS_CLANG_13 width, [&]<int WIDTH>() mutable {
          ENABLE_WARNINGS_CLANG_13
          FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<WIDTH>(
              AD_FWD(args)...);
        });
  };

  vector<TextRecordIndex> cids;
  vector<Id> eids;
  vector<Score> scores;
  size_t nofVars = 2;
  size_t k = 1;
  IdTable resW4{4, allocator()};
  int width = resW4.numColumns();
  callFixed(width, cids, eids, scores, nofVars, k, &resW4);
  ASSERT_EQ(0u, resW4.size());
  nofVars = 5;
  k = 10;
  IdTable resWV{13, allocator()};
  width = resWV.numColumns();
  callFixed(width, cids, eids, scores, nofVars, k, &resWV);
  ASSERT_EQ(0u, resWV.size());

  cids.push_back(T(0));
  cids.push_back(T(1));
  cids.push_back(T(1));
  cids.push_back(T(2));
  cids.push_back(T(2));
  cids.push_back(T(2));

  eids.push_back(I(0));
  eids.push_back(I(0));
  eids.push_back(I(1));
  eids.push_back(I(0));
  eids.push_back(I(1));
  eids.push_back(I(2));

  scores.push_back(10);
  scores.push_back(1);
  scores.push_back(3);
  scores.push_back(1);
  scores.push_back(1);
  scores.push_back(1);

  nofVars = 2;
  k = 1;
  width = resW4.numColumns();
  callFixed(width, cids, eids, scores, nofVars, k, &resW4);

  // Res 0-0-0 (3) | 0-1 1-0 1-1 (2) | 0-2 1-2 2-0 2-1 2-2 (1)
  ASSERT_EQ(9u, resW4.size());
  std::sort(std::begin(resW4), std::end(resW4),
            [](const auto& a, const auto& b) { return a[1] > b[1]; });
  ASSERT_EQ(TextId(0), resW4(0, 0));
  ASSERT_EQ(IntId(3), resW4(0, 1));
  ASSERT_EQ(I(0), resW4(0, 2));
  ASSERT_EQ(I(0), resW4(0, 3));
  ASSERT_EQ(IntId(2), resW4(1, 1));
  ASSERT_EQ(IntId(2), resW4(2, 1));
  ASSERT_EQ(IntId(2), resW4(3, 1));
  ASSERT_EQ(IntId(1), resW4(4, 1));
  ASSERT_EQ(IntId(1), resW4(5, 1));
  k = 2;
  resW4.clear();
  callFixed(width, cids, eids, scores, nofVars, k, &resW4);
  ASSERT_EQ(13u, resW4.size());
  std::sort(std::begin(resW4), std::end(resW4),
            [](const auto& a, const auto& b) {
              return a[1] != b[1] ? a[1] > b[1] : a[0] < b[0];
            });
  ASSERT_EQ(TextId(0), resW4(0, 0));
  ASSERT_EQ(IntId(3), resW4(0, 1));
  ASSERT_EQ(I(0), resW4(0, 2));
  ASSERT_EQ(I(0), resW4(0, 3));
  ASSERT_EQ(TextId(1), resW4(1, 0));
  ASSERT_EQ(IntId(3), resW4(1, 1));
  ASSERT_EQ(I(0), resW4(1, 2));
  ASSERT_EQ(I(0), resW4(1, 3));

  nofVars = 3;
  k = 1;
  IdTable resW5{5, allocator()};
  width = resW5.numColumns();
  callFixed(width, cids, eids, scores, nofVars, k, &resW5);
  ASSERT_EQ(27u, resW5.size());  // Res size 3^3

  nofVars = 10;
  width = resWV.numColumns();
  callFixed(width, cids, eids, scores, nofVars, k, &resWV);
  ASSERT_EQ(59049u, resWV.size());  // Res size: 3^10 = 59049
}

TEST(FTSAlgorithmsTest, oneVarFilterAggScoresAndTakeTopKContexts) {
  vector<TextRecordIndex> cids;
  vector<Id> eids;
  vector<Score> scores;
  size_t k = 1;
  IdTable resW3{3, allocator()};
  ad_utility::HashMap<Id, IdTable> fMap1;

  int width = resW3.numColumns();
  CALL_FIXED_SIZE(width,
                  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts, cids,
                  eids, scores, fMap1, k, &resW3);
  ASSERT_EQ(0u, resW3.size());

  cids.push_back(T(0));
  cids.push_back(T(1));
  cids.push_back(T(1));
  cids.push_back(T(2));
  cids.push_back(T(2));
  cids.push_back(T(2));

  eids.push_back(I(0));
  eids.push_back(I(0));
  eids.push_back(I(1));
  eids.push_back(I(0));
  eids.push_back(I(1));
  eids.push_back(I(2));

  scores.push_back(10);
  scores.push_back(1);
  scores.push_back(3);
  scores.push_back(1);
  scores.push_back(1);
  scores.push_back(1);

  CALL_FIXED_SIZE(width,
                  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts, cids,
                  eids, scores, fMap1, k, &resW3);
  ASSERT_EQ(0u, resW3.size());

  auto [it, success] = fMap1.emplace(I(1), IdTable{1, allocator()});
  ASSERT_TRUE(success);
  it->second.push_back({I(1)});

  CALL_FIXED_SIZE(width,
                  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts, cids,
                  eids, scores, fMap1, k, &resW3);
  ASSERT_EQ(1u, resW3.size());
  resW3.clear();
  k = 10;
  CALL_FIXED_SIZE(width,
                  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts, cids,
                  eids, scores, fMap1, k, &resW3);
  ASSERT_EQ(2u, resW3.size());

  {
    auto [it, suc] = fMap1.emplace(I(0), IdTable{1, allocator()});
    ASSERT_TRUE(suc);
    it->second.push_back({I(0)});
  }
  resW3.clear();
  CALL_FIXED_SIZE(width,
                  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts, cids,
                  eids, scores, fMap1, k, &resW3);
  ASSERT_EQ(5u, resW3.size());

  ad_utility::HashMap<Id, IdTable> fMap4;
  {
    auto [it, suc] = fMap4.emplace(I(0), IdTable{4, allocator()});
    ASSERT_TRUE(suc);
    auto& el = it->second;
    el.push_back({I(0), I(0), I(0), I(0)});
    el.push_back({I(0), I(1), I(0), I(0)});
    el.push_back({I(0), I(2), I(0), I(0)});
  }
  IdTable resVar{7, allocator()};
  k = 1;
  width = 7;
  CALL_FIXED_SIZE(width,
                  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts, cids,
                  eids, scores, fMap4, k, &resVar);
  ASSERT_EQ(3u, resVar.size());

  {
    auto [it, suc] = fMap4.emplace(I(2), IdTable(4, allocator()));
    ASSERT_TRUE(suc);
    it->second.push_back({I(2), I(2), I(2), I(2)});
  }
  resVar.clear();
  CALL_FIXED_SIZE(width,
                  FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts, cids,
                  eids, scores, fMap4, k, &resVar);
  ASSERT_EQ(4u, resVar.size());
}

TEST(FTSAlgorithmsTest, multVarsFilterAggScoresAndTakeTopKContexts) {
  vector<TextRecordIndex> cids;
  vector<Id> eids;
  vector<Score> scores;
  cids.push_back(T(0));
  cids.push_back(T(1));
  cids.push_back(T(1));
  cids.push_back(T(2));
  cids.push_back(T(2));
  cids.push_back(T(2));

  eids.push_back(I(0));
  eids.push_back(I(0));
  eids.push_back(I(1));
  eids.push_back(I(0));
  eids.push_back(I(1));
  eids.push_back(I(2));

  scores.push_back(10);
  scores.push_back(3);
  scores.push_back(3);
  scores.push_back(1);
  scores.push_back(1);
  scores.push_back(1);

  size_t k = 1;
  IdTable resW4{4, allocator()};
  ad_utility::HashMap<Id, IdTable> fMap1;

  size_t nofVars = 2;
  int width = resW4.numColumns();

  // The `multVarsFilterAggScoresAndTakeTopKContexts` function is overloaded,
  // so it doesn't work with the `CALL_FIXED_SIZE` macro. We thus need
  // to use an explicit lambda to pass to `callFixedSize`.

  auto test = [&](int width, auto&&... args) {
    ad_utility::callFixedSize(
        DISABLE_WARNINGS_CLANG_13 width, [&]<int I>() mutable {
          ENABLE_WARNINGS_CLANG_13
          FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<I>(
              AD_FWD(args)...);
        });
  };
  test(width, cids, eids, scores, fMap1, nofVars, k, &resW4);
  ASSERT_EQ(0u, resW4.size());

  auto [it, suc] = fMap1.emplace(I(1), IdTable{1, allocator()});
  it->second.push_back({I(1)});

  test(width,

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

  ASSERT_EQ(TextId(1), resW4(0, 0));
  ASSERT_EQ(IntId(2), resW4(0, 1));
  ASSERT_EQ(I(0), resW4(0, 2));
  ASSERT_EQ(I(1), resW4(0, 3));
  ASSERT_EQ(TextId(1), resW4(1, 0));
  ASSERT_EQ(IntId(2), resW4(1, 1));
  ASSERT_EQ(I(1), resW4(1, 2));
  ASSERT_EQ(I(1), resW4(1, 3));
  ASSERT_EQ(TextId(2), resW4(2, 0));
  ASSERT_EQ(IntId(1), resW4(2, 1));
  ASSERT_EQ(I(2), resW4(2, 2));
  ASSERT_EQ(I(1), resW4(2, 3));

  resW4.clear();
  test(width,

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

  ASSERT_EQ(TextId(1u), resW4(0, 0));
  ASSERT_EQ(IntId(2u), resW4(0, 1));
  ASSERT_EQ(I(0u), resW4(0, 2));
  ASSERT_EQ(I(1u), resW4(0, 3));
  ASSERT_EQ(TextId(2u), resW4(1, 0));
  ASSERT_EQ(IntId(2u), resW4(1, 1));
  ASSERT_EQ(I(0u), resW4(1, 2));
  ASSERT_EQ(I(1u), resW4(1, 3));

  ASSERT_EQ(TextId(1u), resW4(2, 0));
  ASSERT_EQ(IntId(2u), resW4(2, 1));
  ASSERT_EQ(I(1u), resW4(2, 2));
  ASSERT_EQ(I(1u), resW4(2, 3));
  ASSERT_EQ(TextId(2u), resW4(3, 0));
  ASSERT_EQ(IntId(2u), resW4(3, 1));
  ASSERT_EQ(I(1u), resW4(3, 2));
  ASSERT_EQ(I(1u), resW4(3, 3));

  ASSERT_EQ(TextId(2u), resW4(4, 0));
  ASSERT_EQ(IntId(1u), resW4(4, 1));
  ASSERT_EQ(I(2u), resW4(4, 2));
  ASSERT_EQ(I(1u), resW4(4, 3));
}
