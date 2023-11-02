//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/RuntimeInformation.h"

using std::chrono::milliseconds;

// ________________________________________________________________
TEST(RuntimeInformation, addLimitOffsetRow) {
  RuntimeInformation rti;
  rti.descriptor_ = "BaseOperation";
  rti.totalTime_ = milliseconds{4};
  rti.sizeEstimate_ = 34;

  rti.addLimitOffsetRow(LimitOffsetClause{23, 1, 4}, milliseconds{20}, true);
  EXPECT_EQ(rti.descriptor_, "LIMIT 23 OFFSET 4");
  EXPECT_EQ(rti.totalTime_, milliseconds{24});
  EXPECT_EQ(rti.getOperationTime(), milliseconds{20});

  ASSERT_EQ(rti.children_.size(), 1u);
  auto& child = *rti.children_.at(0);
  EXPECT_EQ(child.descriptor_, "BaseOperation");
  EXPECT_EQ(child.totalTime_, milliseconds{4});
  EXPECT_EQ(child.getOperationTime(), milliseconds{4});
  EXPECT_TRUE(child.details_.at("not-written-to-cache-because-child-of-limit"));

  rti.addLimitOffsetRow(LimitOffsetClause{std::nullopt, 1, 17},
                        milliseconds{15}, false);
  EXPECT_FALSE(rti.children_.at(0)->details_.at(
      "not-written-to-cache-because-child-of-limit"));
  EXPECT_EQ(rti.descriptor_, "OFFSET 17");

  rti.addLimitOffsetRow(LimitOffsetClause{42, 1, 0}, milliseconds{15}, true);
  EXPECT_EQ(rti.descriptor_, "LIMIT 42");
}

// ________________________________________________________________
TEST(RuntimeInformation, getOperationTimeAndCostEstimate) {
  RuntimeInformation child1;
  child1.totalTime_ = milliseconds{3};
  child1.costEstimate_ = 12;
  RuntimeInformation child2;
  child2.totalTime_ = milliseconds{4};
  child2.costEstimate_ = 43;

  RuntimeInformation parent;
  parent.totalTime_ = milliseconds{10};
  parent.costEstimate_ = 100;

  parent.children_.push_back(std::make_shared<RuntimeInformation>(child1));
  parent.children_.push_back(std::make_shared<RuntimeInformation>(child2));

  // 3 == 10 - 4 - 3
  ASSERT_EQ(parent.getOperationTime(), milliseconds{3});

  // 45 == 100 - 43 - 12
  ASSERT_EQ(parent.getOperationCostEstimate(), 45);
}

// ________________________________________________________________
TEST(RuntimeInformation, setColumnNames) {
  RuntimeInformation rti;
  rti.columnNames_.push_back("?blimbim");
  rti.setColumnNames({});
  EXPECT_TRUE(rti.columnNames_.empty());

  auto col = makeAlwaysDefinedColumn;
  using V = Variable;
  VariableToColumnMap m{{V{"?x"}, col(1)}, {V{"?y"}, col(0)}};
  rti.setColumnNames(m);
  EXPECT_THAT(rti.columnNames_, ::testing::ElementsAre("?y", "?x"));
}

// ________________________________________________________________
TEST(RuntimeInformation, statusToString) {
  using enum RuntimeInformation::Status;
  using R = RuntimeInformation;
  EXPECT_EQ(R::toString(fullyMaterialized), "fully materialized");
  EXPECT_EQ(R::toString(lazilyMaterialized), "lazily materialized");
  EXPECT_EQ(R::toString(notStarted), "not started");
  EXPECT_EQ(R::toString(optimizedOut), "optimized out");
  EXPECT_EQ(R::toString(failed), "failed");
  EXPECT_EQ(R::toString(failedBecauseChildFailed),
            "failed because child failed");
  EXPECT_ANY_THROW(R::toString(static_cast<RuntimeInformation::Status>(72)));
}

// ________________________________________________________________
TEST(RuntimeInformation, formatDetailValue) {
  std::ostringstream s;
  // Imbue with the same locale as std::cout which uses for example
  // thousands separators.
  s.imbue(ad_utility::commaLocale);
  // So floats use fixed precision
  s << std::fixed << std::setprecision(2);
  using R = RuntimeInformation;
  R::formatDetailValue(s, "", 421234u);
  EXPECT_EQ(s.str(), "421,234");
  s.str("");

  R::formatDetailValue(s, "", -421234);
  EXPECT_EQ(s.str(), "-421,234");
  s.str("");

  R::formatDetailValue(s, "", -421.234);
  EXPECT_EQ(s.str(), "-421.23");
  s.str("");

  R::formatDetailValue(s, "", true);
  EXPECT_EQ(s.str(), "true");
  s.str("");

  R::formatDetailValue(s, "someTime", 48);
  EXPECT_EQ(s.str(), "48 ms");
  s.str("");
}

// ________________________________________________________________
TEST(RuntimeInformation, toStringAndJson) {
  RuntimeInformation child;
  child.descriptor_ = "child";
  child.numCols_ = 2;
  child.numRows_ = 7;
  child.columnNames_.emplace_back("?x");
  child.columnNames_.emplace_back("?y");
  child.totalTime_ = milliseconds{3};
  child.cacheStatus_ = ad_utility::CacheStatus::cachedPinned;
  child.status_ = RuntimeInformation::Status::optimizedOut;
  child.addDetail("minor detail", 42);

  RuntimeInformation parent;
  parent.descriptor_ = "parent";
  parent.numCols_ = 6;
  parent.numRows_ = 4;
  parent.columnNames_.push_back("?alpha");
  parent.totalTime_ = milliseconds{6};
  parent.cacheStatus_ = ad_utility::CacheStatus::computed;
  parent.status_ = RuntimeInformation::Status::fullyMaterialized;

  parent.children_.push_back(std::make_shared<RuntimeInformation>(child));

  auto str = parent.toString();
  ASSERT_EQ(str,
            "│\n"
            R"(├─ parent
│  result_size: 4 x 6
│  columns: ?alpha
│  total_time: 6 ms
│  operation_time: 3 ms
│  status: fully materialized
│  cache_status: computed
│  ┬
│  │
│  ├─ child
│  │  result_size: 7 x 2
│  │  columns: ?x, ?y
│  │  total_time: 3 ms
│  │  operation_time: 3 ms
│  │  status: optimized out
│  │  cache_status: cached_pinned
│  │  original_total_time: 0 ms
│  │  original_operation_time: 0 ms
│  │    minor detail: 42
)");
  nlohmann::ordered_json j = parent;
  std::string expectedJson = R"(
{
"description": "parent",
"result_rows": 4,
"result_cols": 6,
"column_names": [
    "?alpha"
],
"total_time": 6,
"operation_time": 3,
"original_total_time": 0,
"original_operation_time": 0,
"cache_status": "computed",
"details": null,
"estimated_total_cost": 0,
"estimated_operation_cost": 0,
"estimated_column_multiplicities": [],
"estimated_size": 0,
"status": "fully materialized",
"children": [
    {
        "description": "child",
        "result_rows": 7,
        "result_cols": 2,
        "column_names": [
            "?x",
            "?y"
        ],
        "total_time": 3,
        "operation_time": 3,
        "original_total_time": 0,
        "original_operation_time": 0,
        "cache_status": "cached_pinned",
        "details": {
            "minor detail": 42
        },
        "estimated_total_cost": 0,
        "estimated_operation_cost": 0,
        "estimated_column_multiplicities": [],
        "estimated_size": 0,
        "status": "optimized out",
        "children": []
    }
]
}
)";
  ASSERT_EQ(j, nlohmann::ordered_json::parse(expectedJson));
}
