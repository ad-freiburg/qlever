// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "index/FTSAlgorithms.h"

using namespace ad_utility::testing;

namespace {
auto T = [](const auto& id) { return TextRecordIndex::make(id); };
auto V = VocabId;
}  // namespace

TEST(FTSAlgorithmsTest, filterByRangeTest) {
  IdRange<WordVocabIndex> idRange;
  idRange._first = WordVocabIndex::make(5);
  idRange._last = WordVocabIndex::make(7);

  Index::WordEntityPostings wep;
  Index::WordEntityPostings resultWep;

  // Empty
  resultWep = FTSAlgorithms::filterByRange(idRange, wep);
  ASSERT_EQ(0u, resultWep.cids_.size());

  // None
  wep.cids_ = {T(0)};
  wep.wids_ = {{2}};
  wep.scores_ = {1};

  resultWep = FTSAlgorithms::filterByRange(idRange, wep);
  ASSERT_EQ(0u, resultWep.cids_.size());

  // Match
  wep.cids_ = {T(0), T(0), T(1), T(2), T(3)};
  wep.wids_ = {{2, 5, 7, 5, 6}};
  wep.scores_ = {1, 1, 1, 1, 1};

  resultWep = FTSAlgorithms::filterByRange(idRange, wep);
  ASSERT_EQ(4u, resultWep.cids_.size());
  ASSERT_EQ(4u, resultWep.wids_[0].size());
  ASSERT_EQ(4u, resultWep.scores_.size());

  wep.cids_ = {T(0), T(0), T(1), T(2), T(3), T(4)};
  wep.wids_ = {{2, 5, 7, 5, 6, 8}};
  wep.scores_ = {1, 1, 1, 1, 1, 1};

  // Partial
  resultWep = FTSAlgorithms::filterByRange(idRange, wep);
  ASSERT_EQ(4u, resultWep.cids_.size());
  ASSERT_EQ(4u, resultWep.wids_[0].size());
  ASSERT_EQ(4u, resultWep.scores_.size());
};

TEST(FTSAlgorithmsTest, crossIntersectTest) {
  Index::WordEntityPostings matchingContextsWep;
  Index::WordEntityPostings eBlockWep;
  Index::WordEntityPostings resultWep;
  resultWep = FTSAlgorithms::crossIntersect(matchingContextsWep, eBlockWep);
  ASSERT_EQ(0u, resultWep.wids_[0].size());
  ASSERT_EQ(0u, resultWep.cids_.size());
  ASSERT_EQ(0u, resultWep.eids_.size());
  ASSERT_EQ(0u, resultWep.scores_.size());

  matchingContextsWep.cids_ = {T(0), T(2)};
  matchingContextsWep.wids_ = {{1, 4}};
  matchingContextsWep.scores_ = {1, 1};

  resultWep = FTSAlgorithms::crossIntersect(matchingContextsWep, eBlockWep);
  ASSERT_EQ(0u, resultWep.wids_[0].size());
  ASSERT_EQ(0u, resultWep.cids_.size());
  ASSERT_EQ(0u, resultWep.eids_.size());
  ASSERT_EQ(0u, resultWep.scores_.size());

  eBlockWep.cids_ = {T(1), T(2), T(2), T(4)};
  eBlockWep.eids_ = {V(10), V(1), V(1), V(2)};
  eBlockWep.scores_ = {1, 1, 1, 1};

  resultWep = FTSAlgorithms::crossIntersect(matchingContextsWep, eBlockWep);
  ASSERT_EQ(2u, resultWep.wids_[0].size());
  ASSERT_EQ(2u, resultWep.cids_.size());
  ASSERT_EQ(2u, resultWep.eids_.size());
  ASSERT_EQ(2u, resultWep.scores_.size());

  matchingContextsWep.cids_ = {T(0), T(2), T(2)};
  matchingContextsWep.wids_ = {{1, 4, 8}};
  matchingContextsWep.scores_ = {1, 1, 1};

  resultWep = FTSAlgorithms::crossIntersect(matchingContextsWep, eBlockWep);
  ASSERT_EQ(4u, resultWep.wids_[0].size());
  ASSERT_EQ(4u, resultWep.cids_.size());
  ASSERT_EQ(4u, resultWep.eids_.size());
  ASSERT_EQ(4u, resultWep.scores_.size());
  ASSERT_EQ(8u, resultWep.wids_[0][1]);

  eBlockWep.cids_ = {T(0), T(2)};
  eBlockWep.eids_ = {V(10), V(1)};
  eBlockWep.scores_ = {1, 1};

  resultWep = FTSAlgorithms::crossIntersect(matchingContextsWep, eBlockWep);
  ASSERT_EQ(3u, resultWep.wids_[0].size());
  ASSERT_EQ(3u, resultWep.cids_.size());
  ASSERT_EQ(3u, resultWep.eids_.size());
  ASSERT_EQ(3u, resultWep.scores_.size());
  ASSERT_EQ(4u, resultWep.wids_[0][1]);
};

TEST(FTSAlgorithmsTest, crossIntersectKWayTest) {
  vector<Index::WordEntityPostings> wepVecs;
  Index::WordEntityPostings resultWep;

  Index::WordEntityPostings wep1;
  wep1.scores_ = {1, 1, 1, 1};
  wep1.cids_ = {T(0), T(1), T(2), T(10)};
  wep1.wids_ = {{3, 2, 5, 3}};

  Index::WordEntityPostings wep2;
  wep2.scores_ = {1, 1, 1, 1};
  wep2.cids_ = {T(0), T(0), T(0), T(10)};
  wep2.wids_ = {{8, 7, 6, 9}};

  Index::WordEntityPostings wep3;
  wep3.scores_ = {1, 1, 1, 3};
  wep3.cids_ = {T(0), T(6), T(8), T(10)};
  wep3.wids_ = {{23, 22, 25, 23}};

  wepVecs.push_back(wep1);
  wepVecs.push_back(wep2);
  wepVecs.push_back(wep3);

  // No eids / no special case
  resultWep = FTSAlgorithms::crossIntersectKWay(wepVecs, nullptr);
  ASSERT_EQ(4u, resultWep.cids_.size());
  ASSERT_EQ(0u, resultWep.eids_.size());
  ASSERT_EQ(4u, resultWep.scores_.size());
  ASSERT_EQ(3u, resultWep.wids_.size());
  ASSERT_EQ(4u, resultWep.wids_[0].size());
  ASSERT_EQ(3u, resultWep.scores_[0]);
  ASSERT_EQ(3u, resultWep.scores_[1]);
  ASSERT_EQ(5u, resultWep.scores_[3]);
  ASSERT_EQ(23u, resultWep.wids_[2][1]);
  ASSERT_EQ(7u, resultWep.wids_[1][1]);
  ASSERT_EQ(T(0), resultWep.cids_[2]);

  // With eids
  vector<Id> eids = {V(1), V(4), V(1), V(4), V(1), V(2)};

  Index::WordEntityPostings wep4;
  wep4.cids_ = {T(0), T(0), T(3), T(4), T(10), T(10), T(11)};
  wep4.scores_ = {1, 4, 1, 4, 1, 4};
  wep4.wids_ = {{33, 29, 45, 76, 42, 31}};

  wepVecs.push_back(wep4);

  resultWep = FTSAlgorithms::crossIntersectKWay(wepVecs, &eids);
  ASSERT_EQ(8u, resultWep.cids_.size());
  ASSERT_EQ(8u, resultWep.eids_.size());
  ASSERT_EQ(8u, resultWep.scores_.size());
  ASSERT_EQ(4u, resultWep.wids_.size());
  ASSERT_EQ(8u, resultWep.wids_[0].size());
  ASSERT_EQ(T(0), resultWep.cids_[0]);
  ASSERT_EQ(T(0), resultWep.cids_[5]);
  ASSERT_EQ(T(10), resultWep.cids_[6]);
  ASSERT_EQ(T(10), resultWep.cids_[7]);
  ASSERT_EQ(V(1), resultWep.eids_[0]);
  ASSERT_EQ(V(4), resultWep.eids_[3]);
  ASSERT_EQ(V(1), resultWep.eids_[6]);
  ASSERT_EQ(V(2), resultWep.eids_[7]);
  ASSERT_EQ(vector<vector<WordIndex>>(
                {vector<WordIndex>({3, 3, 3, 3, 3, 3, 3, 3}),
                 vector<WordIndex>({8, 8, 7, 7, 6, 6, 9, 9}),
                 vector<WordIndex>({23, 23, 23, 23, 23, 23, 23, 23}),
                 vector<WordIndex>({33, 29, 33, 29, 33, 29, 42, 31})}),
            resultWep.wids_);
  ASSERT_EQ(4u, resultWep.scores_[0]);
  ASSERT_EQ(7u, resultWep.scores_[1]);
  ASSERT_EQ(6u, resultWep.scores_[6]);
  ASSERT_EQ(9u, resultWep.scores_[7]);

  // special cases
  vector<Index::WordEntityPostings> wepVecs2;
  Index::WordEntityPostings wep5;

  wepVecs2.push_back(wep5);
  resultWep = FTSAlgorithms::crossIntersectKWay(wepVecs2, &eids);
  ASSERT_EQ(0u, resultWep.cids_.size());
  ASSERT_EQ(0u, resultWep.eids_.size());
  ASSERT_EQ(0u, resultWep.scores_.size());
  ASSERT_EQ(1u, resultWep.wids_.size());
  ASSERT_EQ(0u, resultWep.wids_[0].size());

  wepVecs2.push_back(wep1);
  resultWep = FTSAlgorithms::crossIntersectKWay(wepVecs2, &eids);
  ASSERT_EQ(0u, resultWep.cids_.size());
  ASSERT_EQ(0u, resultWep.eids_.size());
  ASSERT_EQ(0u, resultWep.scores_.size());
  ASSERT_EQ(0u, resultWep.wids_[0].size());
};

TEST(FTSAlgorithmsTest, aggScoresAndTakeTopKContextsTest) {
  IdTable result{makeAllocator()};
  result.setNumColumns(4);
  Index::WordEntityPostings wep;

  FTSAlgorithms::aggScoresAndTakeTopKContexts<4>(wep, 2, &result);
  ASSERT_EQ(0u, result.size());

  wep.cids_ = {T(0), T(1), T(2), T(2)};
  wep.eids_ = {V(0), V(0), V(0), V(0)};
  wep.scores_ = {0, 1, 2, 2};
  wep.wids_ = {{1, 1, 2, 3}};

  FTSAlgorithms::aggScoresAndTakeTopKContexts<4>(wep, 2, &result);
  ASSERT_EQ(3u, result.size());
  ASSERT_EQ(TextRecordId(2u), result(0, 0));
  ASSERT_EQ(IntId(3u), result(0, 1));
  ASSERT_EQ(V(0u), result(0, 2));
  ASSERT_EQ(WordVocabId(2u), result(0, 3));
  ASSERT_EQ(TextRecordId(1u), result(2, 0));
  ASSERT_EQ(IntId(3u), result(2, 1));
  ASSERT_EQ(V(0u), result(2, 2));
  ASSERT_EQ(WordVocabId(1u), result(2, 3));
  ASSERT_EQ(WordVocabId(3u), result(1, 3));

  wep.cids_ = {T(0), T(1), T(2), T(4)};
  wep.eids_ = {V(0), V(0), V(0), V(1)};
  wep.scores_ = {0, 1, 2, 1};
  wep.wids_ = {{1, 1, 2, 4}, {5, 7, 8, 9}};

  result.clear();
  result.setNumColumns(5);

  FTSAlgorithms::aggScoresAndTakeTopKContexts<5>(wep, 2, &result);
  ASSERT_EQ(3u, result.size());
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a[0] < b[0]; });
  ASSERT_EQ(TextRecordId(4u), result(2, 0));
  ASSERT_EQ(IntId(1u), result(2, 1));
  ASSERT_EQ(V(1u), result(2, 2));
  ASSERT_EQ(WordVocabId(4u), result(2, 3));
  ASSERT_EQ(WordVocabId(9u), result(2, 4));

  result.clear();
  FTSAlgorithms::aggScoresAndTakeTopKContexts<5>(wep, 1, &result);
  ASSERT_EQ(2u, result.size());
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a[0] < b[0]; });
  ASSERT_EQ(TextRecordId(2u), result(0, 0));
  ASSERT_EQ(IntId(3u), result(0, 1));
  ASSERT_EQ(V(0u), result(0, 2));
  ASSERT_EQ(WordVocabId(2u), result(0, 3));
  ASSERT_EQ(WordVocabId(8u), result(0, 4));
};

TEST(FTSAlgorithmsTest, aggScoresAndTakeTopContextTest) {
  IdTable result{makeAllocator()};
  result.setNumColumns(5);
  Index::WordEntityPostings wep;
  int width = result.numColumns();

  // In the following there are many similar calls to `callFixedSize`.
  // We use a lambda to reduce code duplication.
  auto callFixed = [](int width, auto&&... args) {
    ad_utility::callFixedSize(width, [&]<int WIDTH>() {
      FTSAlgorithms::aggScoresAndTakeTopContext<WIDTH>(AD_FWD(args)...);
    });
  };

  callFixed(width, wep, &result);

  ASSERT_EQ(0u, result.size());

  wep.cids_ = {T(0), T(1), T(2), T(2)};
  wep.eids_ = {V(0), V(0), V(0), V(0)};
  wep.scores_ = {0, 1, 2, 2};
  wep.wids_ = {{1, 1, 2, 3}, {5, 7, 8, 9}};

  callFixed(width, wep, &result);
  ASSERT_EQ(2u, result.size());
  ASSERT_EQ(TextRecordId(2u), result(0, 0));
  ASSERT_EQ(IntId(3u), result(0, 1));
  ASSERT_EQ(V(0u), result(0, 2));
  ASSERT_EQ(WordVocabId(2u), result(0, 3));
  ASSERT_EQ(WordVocabId(8u), result(0, 4));
  ASSERT_EQ(WordVocabId(3u), result(1, 3));
  ASSERT_EQ(WordVocabId(9u), result(1, 4));

  result.clear();

  result.setNumColumns(4);
  width = result.numColumns();

  wep.cids_ = {T(0), T(1), T(2), T(3)};
  wep.eids_ = {V(0), V(0), V(0), V(1)};
  wep.scores_ = {0, 1, 2, 1};
  wep.wids_ = {{1, 1, 2, 4}};

  callFixed(width, wep, &result);
  ASSERT_EQ(2u, result.size());
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a[2] < b[2]; });
  ASSERT_EQ(TextRecordId(2u), result(0, 0));
  ASSERT_EQ(IntId(3u), result(0, 1));
  ASSERT_EQ(V(0u), result(0, 2));
  ASSERT_EQ(WordVocabId(2u), result(0, 3));
  ASSERT_EQ(TextRecordId(3u), result(1, 0));
  ASSERT_EQ(IntId(1u), result(1, 1));
  ASSERT_EQ(V(1u), result(1, 2));
  ASSERT_EQ(WordVocabId(4u), result(1, 3));

  result.clear();

  wep.cids_ = {T(0), T(1), T(2), T(3), T(4)};
  wep.eids_ = {V(0), V(0), V(0), V(1), V(0)};
  wep.scores_ = {0, 1, 2, 1, 10};
  wep.wids_ = {{1, 1, 2, 4, 4}};

  ad_utility::callFixedSize(width, [&wep, &result]<int WIDTH>() mutable {
    FTSAlgorithms::aggScoresAndTakeTopContext<WIDTH>(wep, &result);
  });
  ASSERT_EQ(2u, result.size());
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a[2] < b[2]; });

  ASSERT_EQ(TextRecordId(4u), result(0, 0));
  ASSERT_EQ(IntId(4u), result(0, 1));
  ASSERT_EQ(V(0u), result(0, 2));
  ASSERT_EQ(WordVocabId(4u), result(0, 3));
  ASSERT_EQ(TextRecordId(3u), result(1, 0));
  ASSERT_EQ(IntId(1u), result(1, 1));
  ASSERT_EQ(V(1u), result(1, 2));
  ASSERT_EQ(WordVocabId(4u), result(1, 3));
};

TEST(FTSAlgorithmsTest, multVarsAggScoresAndTakeTopKContexts) {
  auto callFixed = [](int width, auto&&... args) {
    ad_utility::callFixedSize(width, [&]<int WIDTH>() mutable {
      FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<WIDTH>(
          AD_FWD(args)...);
    });
  };

  Index::WordEntityPostings wep;
  size_t nofVars = 2;
  size_t k = 1;
  IdTable resW5{5, makeAllocator()};
  int width = resW5.numColumns();
  callFixed(width, wep, nofVars, k, &resW5);
  ASSERT_EQ(0u, resW5.size());
  nofVars = 5;
  k = 10;
  IdTable resWV{13, makeAllocator()};
  width = resWV.numColumns();
  callFixed(width, wep, nofVars, k, &resWV);
  ASSERT_EQ(0u, resWV.size());

  wep.cids_ = {T(0), T(1), T(1), T(2), T(2), T(2)};
  wep.eids_ = {V(0), V(0), V(1), V(0), V(2), V(2)};
  wep.scores_ = {1, 10, 3, 1, 1, 1};
  wep.wids_ = {{1, 1, 2, 1, 3, 5}, {6, 9, 8, 8, 7, 9}};

  IdTable resW6{6, makeAllocator()};
  width = resW6.numColumns();

  nofVars = 2;
  k = 1;
  width = resW6.numColumns();
  callFixed(width, wep, nofVars, k, &resW6);

  // Result (Note that in the IdTable the rows and columns are swapped):
  // cid:    1 1 1 1 1 1 2 2 2 2 2 2 2 2
  // scores: 3 1 1 1 1 1 1 1 1 1 1 1 1 1
  // eid1:   0 0 0 1 1 1 0 0 0 2 2 2 2 2
  // eid2:   0 1 1 0 0 1 2 2 2 0 0 0 2 2
  // wids:   1 1 2 1 2 2 1 3 5 1 3 5 3 5
  // wids2:  9 9 8 9 8 8 8 7 9 8 7 9 7 9

  ASSERT_EQ(14u, resW6.size());
  // sort it in the same way as shown above
  std::sort(std::begin(resW6), std::end(resW6),
            [](const auto& a, const auto& b) {
              return a[0] < b[0] ||
                     (a[0] == b[0] &&
                      (a[2] < b[2] ||
                       (a[2] == b[2] &&
                        (a[3] < b[3] || (a[3] == b[3] && a[4] < b[4])))));
            });
  ASSERT_EQ(TextRecordId(1u), resW6(0, 0));
  ASSERT_EQ(TextRecordId(1u), resW6(4, 0));
  ASSERT_EQ(TextRecordId(2u), resW6(9, 0));
  ASSERT_EQ(TextRecordId(2u), resW6(13, 0));
  ASSERT_EQ(IntId(3u), resW6(0, 1));
  ASSERT_EQ(IntId(1u), resW6(4, 1));
  ASSERT_EQ(IntId(1u), resW6(9, 1));
  ASSERT_EQ(IntId(1u), resW6(13, 1));
  ASSERT_EQ(V(0u), resW6(0, 2));
  ASSERT_EQ(V(1u), resW6(4, 2));
  ASSERT_EQ(V(2u), resW6(9, 2));
  ASSERT_EQ(V(2u), resW6(13, 2));
  ASSERT_EQ(V(0u), resW6(0, 3));
  ASSERT_EQ(V(0u), resW6(4, 3));
  ASSERT_EQ(V(0u), resW6(9, 3));
  ASSERT_EQ(V(2u), resW6(13, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(0, 4));
  ASSERT_EQ(WordVocabId(2u), resW6(4, 4));
  ASSERT_EQ(WordVocabId(1u), resW6(9, 4));
  ASSERT_EQ(WordVocabId(5u), resW6(13, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(0, 5));
  ASSERT_EQ(WordVocabId(8u), resW6(4, 5));
  ASSERT_EQ(WordVocabId(8u), resW6(9, 5));
  ASSERT_EQ(WordVocabId(9u), resW6(13, 5));

  k = 2;
  resW6.clear();
  callFixed(width, wep, nofVars, k, &resW6);
  ASSERT_EQ(15u, resW6.size());
  std::sort(std::begin(resW6), std::end(resW6),
            [](const auto& a, const auto& b) {
              return a[0] < b[0] ||
                     (a[0] == b[0] &&
                      (a[2] < b[2] ||
                       (a[2] == b[2] &&
                        (a[3] < b[3] || (a[3] == b[3] && a[4] < b[4])))));
            });
  ASSERT_EQ(TextRecordId(0u), resW6(0, 0));
  ASSERT_EQ(TextRecordId(1u), resW6(1, 0));
  ASSERT_EQ(TextRecordId(1u), resW6(5, 0));
  ASSERT_EQ(TextRecordId(2u), resW6(10, 0));
  ASSERT_EQ(TextRecordId(2u), resW6(14, 0));
  ASSERT_EQ(IntId(3u), resW6(0, 1));
  ASSERT_EQ(IntId(3u), resW6(1, 1));
  ASSERT_EQ(IntId(1u), resW6(5, 1));
  ASSERT_EQ(IntId(1u), resW6(10, 1));
  ASSERT_EQ(IntId(1u), resW6(14, 1));
  ASSERT_EQ(V(0u), resW6(0, 2));
  ASSERT_EQ(V(0u), resW6(1, 2));
  ASSERT_EQ(V(1u), resW6(5, 2));
  ASSERT_EQ(V(2u), resW6(10, 2));
  ASSERT_EQ(V(2u), resW6(14, 2));
  ASSERT_EQ(V(0u), resW6(0, 3));
  ASSERT_EQ(V(0u), resW6(1, 3));
  ASSERT_EQ(V(0u), resW6(5, 3));
  ASSERT_EQ(V(0u), resW6(10, 3));
  ASSERT_EQ(V(2u), resW6(14, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(0, 4));
  ASSERT_EQ(WordVocabId(1u), resW6(1, 4));
  ASSERT_EQ(WordVocabId(2u), resW6(5, 4));
  ASSERT_EQ(WordVocabId(1u), resW6(10, 4));
  ASSERT_EQ(WordVocabId(5u), resW6(14, 4));
  ASSERT_EQ(WordVocabId(6u), resW6(0, 5));
  ASSERT_EQ(WordVocabId(9u), resW6(1, 5));
  ASSERT_EQ(WordVocabId(8u), resW6(5, 5));
  ASSERT_EQ(WordVocabId(8u), resW6(10, 5));
  ASSERT_EQ(WordVocabId(9u), resW6(14, 5));

  wep.cids_ = {T(0), T(0), T(0)};
  wep.eids_ = {V(0), V(1), V(2)};
  wep.scores_ = {1, 10, 3};
  wep.wids_ = {{1, 1, 1}};

  resW5.clear();

  nofVars = 3;
  k = 1;
  callFixed(width, wep, nofVars, k, &resW5);
  ASSERT_EQ(27u, resW5.size());  // Res size 3^3

  nofVars = 10;
  width = resWV.numColumns();
  callFixed(width, wep, nofVars, k, &resWV);
  ASSERT_EQ(59049u, resWV.size());  // Res size: 3^10 = 59049
}

TEST(FTSAlgorithmsTest, oneVarFilterAggScoresAndTakeTopKContexts) {
  Index::WordEntityPostings wep;
  size_t k = 1;
  IdTable resW3{3, makeAllocator()};
  ad_utility::HashMap<Id, IdTable> fMap1;
  HashSet<Id> fSet1;

  int width = resW3.numColumns();

  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fMap1, k, &resW3);
  ASSERT_EQ(0u, resW3.size());

  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fSet1, k, &resW3);
  ASSERT_EQ(0u, resW3.size());

  wep.cids_ = {T(0), T(1), T(1), T(2), T(2), T(2)};
  wep.eids_ = {V(0), V(0), V(1), V(0), V(1), V(2)};
  wep.scores_ = {10, 1, 1, 1, 3, 1};
  wep.wids_ = {{1, 1, 2, 1, 3, 5}, {11, 13, 12, 14, 15, 10}};

  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fMap1, k, &resW3);
  ASSERT_EQ(0u, resW3.size());

  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fSet1, k, &resW3);
  ASSERT_EQ(0u, resW3.size());

  IdTable resW5{5, makeAllocator()};
  width = resW5.numColumns();

  auto [it, success] = fMap1.emplace(V(1), IdTable{1, makeAllocator()});
  ASSERT_TRUE(success);
  it->second.push_back({V(1)});

  fSet1.insert(V(1));

  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fMap1, k, &resW5);
  ASSERT_EQ(1u, resW5.size());
  ASSERT_EQ(TextRecordId(2u), resW5(0, 0));
  ASSERT_EQ(IntId(2u), resW5(0, 1));
  ASSERT_EQ(V(1u), resW5(0, 2));
  ASSERT_EQ(WordVocabId(3u), resW5(0, 3));
  ASSERT_EQ(WordVocabId(15u), resW5(0, 4));
  resW5.clear();

  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fSet1, k, &resW5);
  ASSERT_EQ(1u, resW5.size());
  ASSERT_EQ(TextRecordId(2u), resW5(0, 0));
  ASSERT_EQ(IntId(2u), resW5(0, 1));
  ASSERT_EQ(V(1u), resW5(0, 2));
  ASSERT_EQ(WordVocabId(3u), resW5(0, 3));
  ASSERT_EQ(WordVocabId(15u), resW5(0, 4));
  resW5.clear();

  wep.cids_ = {T(0), T(1), T(1), T(2), T(2), T(2)};
  wep.eids_ = {V(0), V(0), V(1), V(0), V(1), V(1)};
  wep.scores_ = {10, 1, 1, 1, 3, 3};
  wep.wids_ = {{1, 1, 2, 1, 3, 5}, {11, 13, 12, 14, 15, 10}};

  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fMap1, k, &resW5);
  ASSERT_EQ(2u, resW5.size());
  ASSERT_EQ(TextRecordId(2u), resW5(0, 0));
  ASSERT_EQ(TextRecordId(2u), resW5(1, 0));
  ASSERT_EQ(IntId(2u), resW5(0, 1));
  ASSERT_EQ(IntId(2u), resW5(1, 1));
  ASSERT_EQ(V(1u), resW5(0, 2));
  ASSERT_EQ(V(1u), resW5(1, 2));
  ASSERT_EQ(WordVocabId(3u), resW5(0, 3));
  ASSERT_EQ(WordVocabId(5u), resW5(1, 3));
  ASSERT_EQ(WordVocabId(15u), resW5(0, 4));
  ASSERT_EQ(WordVocabId(10u), resW5(1, 4));
  resW5.clear();

  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fSet1, k, &resW5);
  ASSERT_EQ(2u, resW5.size());
  ASSERT_EQ(TextRecordId(2u), resW5(0, 0));
  ASSERT_EQ(TextRecordId(2u), resW5(1, 0));
  ASSERT_EQ(IntId(2u), resW5(0, 1));
  ASSERT_EQ(IntId(2u), resW5(1, 1));
  ASSERT_EQ(V(1u), resW5(0, 2));
  ASSERT_EQ(V(1u), resW5(1, 2));
  ASSERT_EQ(WordVocabId(3u), resW5(0, 3));
  ASSERT_EQ(WordVocabId(5u), resW5(1, 3));
  ASSERT_EQ(WordVocabId(15u), resW5(0, 4));
  ASSERT_EQ(WordVocabId(10u), resW5(1, 4));
  resW5.clear();

  wep.cids_ = {T(0), T(1), T(1), T(2), T(2), T(2)};
  wep.eids_ = {V(0), V(0), V(1), V(0), V(1), V(2)};
  wep.scores_ = {10, 1, 1, 1, 3, 1};
  wep.wids_ = {{1, 1, 2, 1, 3, 5}, {11, 13, 12, 14, 15, 10}};

  k = 10;
  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fMap1, k, &resW5);
  ASSERT_EQ(2u, resW5.size());

  {
    auto [it, suc] = fMap1.emplace(V(0), IdTable{1, makeAllocator()});
    ASSERT_TRUE(suc);
    it->second.push_back({V(0)});
  }
  resW5.clear();
  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fMap1, k, &resW5);
  ASSERT_EQ(5u, resW5.size());

  ad_utility::HashMap<Id, IdTable> fMap4;
  {
    auto [it, suc] = fMap4.emplace(V(0), IdTable{4, makeAllocator()});
    ASSERT_TRUE(suc);
    auto& el = it->second;
    el.push_back({V(0), V(0), V(0), V(0)});
    el.push_back({V(0), V(1), V(0), V(0)});
    el.push_back({V(0), V(2), V(0), V(0)});
  }
  IdTable resVar{8, makeAllocator()};
  k = 1;
  width = 7;
  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fMap4, k, &resVar);
  ASSERT_EQ(3u, resVar.size());

  {
    auto [it, suc] = fMap4.emplace(V(2), IdTable(4, makeAllocator()));
    ASSERT_TRUE(suc);
    it->second.push_back({V(2), V(2), V(2), V(2)});
  }
  resVar.clear();
  ad_utility::callFixedSize(
      width,
      []<int I>(auto&&... args) {
        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<I>(
            AD_FWD(args)...);
      },
      wep, fMap4, k, &resVar);
  ASSERT_EQ(4u, resVar.size());
}

TEST(FTSAlgorithmsTest, multVarsFilterAggScoresAndTakeTopKContexts) {
  Index::WordEntityPostings wep;

  wep.cids_ = {T(0), T(1), T(1), T(2), T(2), T(2)};
  wep.eids_ = {V(0), V(0), V(1), V(0), V(2), V(2)};
  wep.scores_ = {1, 10, 3, 1, 1, 1};
  wep.wids_ = {{1, 1, 2, 1, 3, 5}, {6, 9, 8, 8, 7, 9}};

  size_t k = 1;
  IdTable resW6{6, makeAllocator()};

  ad_utility::HashMap<Id, IdTable> fMap1;
  HashSet<Id> fSet1;

  size_t nofVars = 2;
  int width = resW6.numColumns();

  // The `multVarsFilterAggScoresAndTakeTopKContexts` function is overloaded,
  // so it doesn't work with the `CALL_FIXED_SIZE` macro. We thus need
  // to use an explicit lambda to pass to `callFixedSize`.

  auto test = [&](int width, auto&&... args) {
    ad_utility::callFixedSize(width, [&]<int V>() mutable {
      FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<V>(
          AD_FWD(args)...);
    });
  };
  test(width, wep, fMap1, nofVars, k, &resW6);
  ASSERT_EQ(0u, resW6.size());

  test(width, wep, fSet1, nofVars, k, &resW6);
  ASSERT_EQ(0u, resW6.size());

  auto [it, suc] = fMap1.emplace(V(0), IdTable{1, makeAllocator()});
  it->second.push_back({V(0)});

  fSet1.insert(V(0));

  // Result (Note that in the IdTable the rows and columns are swapped):
  // cid:    1 1 1 2 2 2
  // scores: 3 1 1 1 1 1
  // eid1:   0 1 1 2 2 2
  // eid2:   0 0 0 0 0 0
  // wids:   1 1 2 1 3 5
  // wids2:  9 9 8 8 7 9

  test(width, wep, fMap1, nofVars, k, &resW6);
  ASSERT_EQ(6u, resW6.size());

  std::sort(std::begin(resW6), std::end(resW6),
            [](const auto& a, const auto& b) {
              return a[0] < b[0] ||
                     (a[0] == b[0] &&
                      (a[2] < b[2] ||
                       (a[2] == b[2] &&
                        (a[3] < b[3] || (a[3] == b[3] && a[4] < b[4])))));
            });

  ASSERT_EQ(TextRecordId(1u), resW6(0, 0));
  ASSERT_EQ(IntId(3u), resW6(0, 1));
  ASSERT_EQ(V(0u), resW6(0, 2));
  ASSERT_EQ(V(0u), resW6(0, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(0, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(0, 5));
  ASSERT_EQ(TextRecordId(1u), resW6(1, 0));
  ASSERT_EQ(IntId(1u), resW6(1, 1));
  ASSERT_EQ(V(1u), resW6(1, 2));
  ASSERT_EQ(V(0u), resW6(1, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(1, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(1, 5));
  ASSERT_EQ(TextRecordId(2u), resW6(5, 0));
  ASSERT_EQ(IntId(1u), resW6(5, 1));
  ASSERT_EQ(V(2u), resW6(5, 2));
  ASSERT_EQ(V(0u), resW6(5, 3));
  ASSERT_EQ(WordVocabId(5u), resW6(5, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(5, 5));

  resW6.clear();

  test(width, wep, fSet1, nofVars, k, &resW6);
  ASSERT_EQ(6u, resW6.size());

  std::sort(std::begin(resW6), std::end(resW6),
            [](const auto& a, const auto& b) {
              return a[0] < b[0] ||
                     (a[0] == b[0] &&
                      (a[2] < b[2] ||
                       (a[2] == b[2] &&
                        (a[3] < b[3] || (a[3] == b[3] && a[4] < b[4])))));
            });

  ASSERT_EQ(TextRecordId(1u), resW6(0, 0));
  ASSERT_EQ(IntId(3u), resW6(0, 1));
  ASSERT_EQ(V(0u), resW6(0, 2));
  ASSERT_EQ(V(0u), resW6(0, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(0, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(0, 5));
  ASSERT_EQ(TextRecordId(1u), resW6(1, 0));
  ASSERT_EQ(IntId(1u), resW6(1, 1));
  ASSERT_EQ(V(1u), resW6(1, 2));
  ASSERT_EQ(V(0u), resW6(1, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(1, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(1, 5));
  ASSERT_EQ(TextRecordId(2u), resW6(5, 0));
  ASSERT_EQ(IntId(1u), resW6(5, 1));
  ASSERT_EQ(V(2u), resW6(5, 2));
  ASSERT_EQ(V(0u), resW6(5, 3));
  ASSERT_EQ(WordVocabId(5u), resW6(5, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(5, 5));

  resW6.clear();

  k = 3;

  test(width, wep, fMap1, nofVars, k, &resW6);
  ASSERT_EQ(8u, resW6.size());
  std::sort(std::begin(resW6), std::end(resW6),
            [](const auto& a, const auto& b) {
              return a[0] < b[0] ||
                     (a[0] == b[0] &&
                      (a[2] < b[2] ||
                       (a[2] == b[2] &&
                        (a[3] < b[3] || (a[3] == b[3] && a[4] < b[4])))));
            });
  ASSERT_EQ(TextRecordId(0u), resW6(0, 0));
  ASSERT_EQ(IntId(3u), resW6(0, 1));
  ASSERT_EQ(V(0u), resW6(0, 2));
  ASSERT_EQ(V(0u), resW6(0, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(0, 4));
  ASSERT_EQ(WordVocabId(6u), resW6(0, 5));
  ASSERT_EQ(TextRecordId(1u), resW6(1, 0));
  ASSERT_EQ(IntId(3u), resW6(1, 1));
  ASSERT_EQ(V(0u), resW6(1, 2));
  ASSERT_EQ(V(0u), resW6(1, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(1, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(1, 5));
  ASSERT_EQ(TextRecordId(2u), resW6(4, 0));
  ASSERT_EQ(IntId(3u), resW6(4, 1));
  ASSERT_EQ(V(0u), resW6(4, 2));
  ASSERT_EQ(V(0u), resW6(4, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(4, 4));
  ASSERT_EQ(WordVocabId(8u), resW6(4, 5));
  ASSERT_EQ(TextRecordId(2u), resW6(7, 0));
  ASSERT_EQ(IntId(1u), resW6(7, 1));
  ASSERT_EQ(V(2u), resW6(7, 2));
  ASSERT_EQ(V(0u), resW6(7, 3));
  ASSERT_EQ(WordVocabId(5u), resW6(7, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(7, 5));

  resW6.clear();

  test(width, wep, fSet1, nofVars, k, &resW6);
  ASSERT_EQ(8u, resW6.size());
  std::sort(std::begin(resW6), std::end(resW6),
            [](const auto& a, const auto& b) {
              return a[0] < b[0] ||
                     (a[0] == b[0] &&
                      (a[2] < b[2] ||
                       (a[2] == b[2] &&
                        (a[3] < b[3] || (a[3] == b[3] && a[4] < b[4])))));
            });
  ASSERT_EQ(TextRecordId(0u), resW6(0, 0));
  ASSERT_EQ(IntId(3u), resW6(0, 1));
  ASSERT_EQ(V(0u), resW6(0, 2));
  ASSERT_EQ(V(0u), resW6(0, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(0, 4));
  ASSERT_EQ(WordVocabId(6u), resW6(0, 5));
  ASSERT_EQ(TextRecordId(1u), resW6(1, 0));
  ASSERT_EQ(IntId(3u), resW6(1, 1));
  ASSERT_EQ(V(0u), resW6(1, 2));
  ASSERT_EQ(V(0u), resW6(1, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(1, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(1, 5));
  ASSERT_EQ(TextRecordId(2u), resW6(4, 0));
  ASSERT_EQ(IntId(3u), resW6(4, 1));
  ASSERT_EQ(V(0u), resW6(4, 2));
  ASSERT_EQ(V(0u), resW6(4, 3));
  ASSERT_EQ(WordVocabId(1u), resW6(4, 4));
  ASSERT_EQ(WordVocabId(8u), resW6(4, 5));
  ASSERT_EQ(TextRecordId(2u), resW6(7, 0));
  ASSERT_EQ(IntId(1u), resW6(7, 1));
  ASSERT_EQ(V(2u), resW6(7, 2));
  ASSERT_EQ(V(0u), resW6(7, 3));
  ASSERT_EQ(WordVocabId(5u), resW6(7, 4));
  ASSERT_EQ(WordVocabId(9u), resW6(7, 5));
}
