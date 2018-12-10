// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <array>
#include <vector>

#include <gtest/gtest.h>

#include "../src/engine/Union.h"
#include "../src/global/Id.h"

TEST(UnionTest, computeUnion) {
  // the left and right data vectors will be deleted by the result tables
  vector<array<Id, 1>>* leftData = new vector<array<Id, 1>>();
  leftData->push_back({1});
  leftData->push_back({2});
  leftData->push_back({3});

  vector<array<Id, 2>>* rightData = new vector<array<Id, 2>>();
  rightData->push_back({4, 5});
  rightData->push_back({6, 7});

  using std::array;
  using std::vector;
  ResultTable res;
  res._nofColumns = 2;
  std::shared_ptr<ResultTable> left = std::make_shared<ResultTable>();
  left->_nofColumns = 1;
  left->_fixedSizeData = leftData;
  std::shared_ptr<ResultTable> right = std::make_shared<ResultTable>();
  right->_nofColumns = 2;
  right->_fixedSizeData = rightData;

  const std::vector<std::array<size_t, 2>> columnOrigins = {
      {0, 1}, {Union::NO_COLUMN, 0}};
  Union::computeUnion(&res, left, right, columnOrigins);

  const vector<array<Id, 2>>* resData =
      static_cast<vector<array<Id, 2>>*>(res._fixedSizeData);
  ASSERT_EQ(5u, resData->size());
  for (size_t i = 0; i < leftData->size(); i++) {
    ASSERT_EQ((*leftData)[i][0], resData->at(i)[0]);
    ASSERT_EQ(ID_NO_VALUE, resData->at(i)[1]);
  }
  for (size_t i = 0; i < rightData->size(); i++) {
    ASSERT_EQ((*rightData)[i][0], resData->at(i + leftData->size())[1]);
    ASSERT_EQ((*rightData)[i][1], resData->at(i + leftData->size())[0]);
  }
}

TEST(UnionTest, computeUnionOptimized) {
  // the left and right data vectors will be deleted by the result tables
  vector<array<Id, 2>>* leftData = new vector<array<Id, 2>>();
  leftData->push_back({1, 2});
  leftData->push_back({2, 3});
  leftData->push_back({3, 4});

  vector<array<Id, 2>>* rightData = new vector<array<Id, 2>>();
  rightData->push_back({4, 5});
  rightData->push_back({6, 7});

  using std::array;
  using std::vector;
  ResultTable res;
  res._nofColumns = 2;
  std::shared_ptr<ResultTable> left = std::make_shared<ResultTable>();
  left->_nofColumns = 2;
  left->_fixedSizeData = leftData;
  std::shared_ptr<ResultTable> right = std::make_shared<ResultTable>();
  right->_nofColumns = 2;
  right->_fixedSizeData = rightData;

  const std::vector<std::array<size_t, 2>> columnOrigins = {{0, 0}, {1, 1}};
  Union::computeUnion(&res, left, right, columnOrigins);

  const vector<array<Id, 2>>* resData =
      static_cast<vector<array<Id, 2>>*>(res._fixedSizeData);
  ASSERT_EQ(5u, resData->size());
  for (size_t i = 0; i < leftData->size(); i++) {
    ASSERT_EQ((*leftData)[i][0], resData->at(i)[0]);
    ASSERT_EQ((*leftData)[i][1], resData->at(i)[1]);
  }
  for (size_t i = 0; i < rightData->size(); i++) {
    ASSERT_EQ((*rightData)[i][0], resData->at(i + leftData->size())[0]);
    ASSERT_EQ((*rightData)[i][1], resData->at(i + leftData->size())[1]);
  }
}
