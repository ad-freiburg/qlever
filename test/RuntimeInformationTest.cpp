//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/RuntimeInformation.h"
#include "gtest/gtest.h"

TEST(RuntimeInformation, addLimitOffsetRow) {
  RuntimeInformation rti;
  rti.descriptor_ = "BaseOperation";
  rti.totalTime_ = 4.0;
  rti.sizeEstimate_ = 34;

  rti.addLimitOffsetRow(LimitOffsetClause{23, 1, 4}, 20, true);
  EXPECT_EQ(rti.descriptor_, "LIMIT 23 OFFSET 4");
  EXPECT_EQ(rti.totalTime_, 24.0);
  EXPECT_EQ(rti.getOperationTime(), 20.0);

  ASSERT_EQ(rti.children_.size(), 1u);
  const auto& child = rti.children_.at(0);
  EXPECT_EQ(child.descriptor_, "BaseOperation");
  EXPECT_EQ(child.totalTime_, 4.0);
  EXPECT_EQ(child.getOperationTime(), 4.0);
  EXPECT_TRUE(child.details_.at("not-written-to-cache-because-child-of-limit"));

  rti.addLimitOffsetRow(LimitOffsetClause{std::nullopt, 1, 17}, 15, false);
  EXPECT_FALSE(rti.children_.at(0).details_.at(
      "not-written-to-cache-because-child-of-limit"));
  EXPECT_EQ(rti.descriptor_, "OFFSET 17");

  rti.addLimitOffsetRow(LimitOffsetClause{42, 1, 0}, 15, true);
  EXPECT_EQ(rti.descriptor_, "LIMIT 42");
}
