// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdio>
#include "../src/engine/CallFixedSize.h"
#include "../src/engine/EntityCountPredicates.h"
#include "../src/engine/HasPredicateScan.h"

// used to test HasRelationScan with a subtree
class DummyOperation : public Operation {
 public:
  DummyOperation(QueryExecutionContext* ctx) : Operation(ctx) {}
  virtual void computeResult(ResultTable* result) override {
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_data.setCols(2);
    for (size_t i = 0; i < 10; i++) {
      result->_data.push_back({10 - i, 2 * i});
    }
    result->finish();
  }

  string asString(size_t indent = 0) const override {
    (void)indent;
    return "dummy";
  }

  string getDescriptor() const override { return "dummy"; }

  virtual size_t getResultWidth() const override { return 2; }

  virtual vector<size_t> resultSortedOn() const override { return {1}; }

  virtual void setTextLimit(size_t limit) override { (void)limit; }

  virtual size_t getCostEstimate() override { return 10; }

  virtual size_t getSizeEstimate() override { return 10; }

  virtual float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  virtual bool knownEmptyResult() override { return false; }

  virtual ad_utility::HashMap<string, size_t> getVariableColumns()
      const override {
    ad_utility::HashMap<string, size_t> m;
    m["?a"] = 0;
    m["?b"] = 1;
    return m;
  }
};

TEST(EntityCountPredicates, compute) {
  // The input table containing entity ids
  IdTable input(1);
  for (Id i = 0; i < 8; i++) {
    input.push_back({i});
  }
  // Used to store the result.
  IdTable result(2);
  // Maps entities to their patterns. If an entity id is higher than the lists
  // length the hasRelation relation is used instead.
  vector<PatternID> hasPattern = {0, NO_PATTERN, NO_PATTERN, 1, 0};
  // The has relation relation, which is used when an entity does not have a
  // pattern
  vector<vector<Id>> hasRelationSrc = {{},     {0, 3}, {0},    {}, {},
                                       {0, 3}, {3, 4}, {2, 4}, {3}};
  // Maps pattern ids to patterns
  vector<vector<Id>> patternsSrc = {{0, 2, 3}, {1, 3, 4, 2, 0}};

  std::vector<Id> predicateGlobalIds = {0, 1, 2, 3, 4};

  // These are used to store the relations and patterns in contiguous blocks
  // of memory.
  CompactStringVector<Id, Id> hasRelation(hasRelationSrc);
  CompactStringVector<size_t, Id> patterns(patternsSrc);

  try {
    CALL_FIXED_SIZE_1(input.cols(), EntityCountPredicates::compute, input,
                      &result, hasPattern, hasRelation, patterns, 0);
  } catch (const std::runtime_error& e) {
    // More verbose output in the case of an exception occuring.
    std::cout << e.what() << std::endl;
    ASSERT_TRUE(false);
  }

  ASSERT_EQ(8u, result.size());

  ASSERT_EQ(0u, result(0, 0));
  ASSERT_EQ(3u, result(0, 1));

  ASSERT_EQ(1u, result(1, 0));
  ASSERT_EQ(2u, result(1, 1));

  ASSERT_EQ(2u, result(2, 0));
  ASSERT_EQ(1u, result(2, 1));

  ASSERT_EQ(3u, result(3, 0));
  ASSERT_EQ(5u, result(3, 1));

  ASSERT_EQ(4u, result(4, 0));
  ASSERT_EQ(3u, result(4, 1));

  ASSERT_EQ(5u, result(5, 0));
  ASSERT_EQ(2u, result(5, 1));

  ASSERT_EQ(6u, result(6, 0));
  ASSERT_EQ(2u, result(6, 1));

  ASSERT_EQ(7u, result(7, 0));
  ASSERT_EQ(2u, result(7, 1));

  // Test the pattern trick for all entities
  result.clear();
  try {
    EntityCountPredicates::computeAllEntities(&result, hasPattern, hasRelation,
                                              patterns);
  } catch (const std::runtime_error& e) {
    // More verbose output in the case of an exception occuring.
    std::cout << e.what() << std::endl;
    ASSERT_TRUE(false);
  }
  std::sort(
      result.begin(), result.end(),
      [](const auto& i1, const auto& i2) -> bool { return i1[0] < i2[0]; });

  ASSERT_EQ(9u, result.size());

  ASSERT_EQ(0u, result(0, 0));
  ASSERT_EQ(3u, result(0, 1));

  ASSERT_EQ(1u, result(1, 0));
  ASSERT_EQ(2u, result(1, 1));

  ASSERT_EQ(2u, result(2, 0));
  ASSERT_EQ(1u, result(2, 1));

  ASSERT_EQ(3u, result(3, 0));
  ASSERT_EQ(5u, result(3, 1));

  ASSERT_EQ(4u, result(4, 0));
  ASSERT_EQ(3u, result(4, 1));

  ASSERT_EQ(5u, result(5, 0));
  ASSERT_EQ(2u, result(5, 1));

  ASSERT_EQ(6u, result(6, 0));
  ASSERT_EQ(2u, result(6, 1));

  ASSERT_EQ(7u, result(7, 0));
  ASSERT_EQ(2u, result(7, 1));

  ASSERT_EQ(8u, result(8, 0));
  ASSERT_EQ(1u, result(8, 1));
}
