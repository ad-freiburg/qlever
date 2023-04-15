// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <type_traits>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "util/Timer.h"
#include "util/json.h"

namespace ad_benchmark {
/*
@brief Creates a lambda, that waits the given amount of milliseconds,
before finishing. Note: 1000 milliseconds are 1 second.
*/
static auto createWaitLambda(const size_t& waitDuration) {
  return [&waitDuration]() {
    // This will measure, if at least the given amount of time has passed.
    ad_utility::TimeoutTimer timer(std::chrono::milliseconds{waitDuration},
                                   ad_utility::Timer::InitialStatus::Started);
    // Just do nothing, while waiting.
    while (!timer.hasTimedOut())
      ;
  };
}

TEST(BenchmarkMeasurementContainerTest, ResultEntry) {
  // There's really no special cases.
  const std::string entryDescriptor{"entry"};
  // The function should just wait 0.1 seconds.
  constexpr size_t waitTimeinMillieseconds = 100;

  ResultEntry entry(entryDescriptor, createWaitLambda(waitTimeinMillieseconds));

  ASSERT_EQ(entry.descriptor_, entryDescriptor);

  // The time measurements can't be 100% accurat, so we give it a 'window'.
  ASSERT_NEAR(waitTimeinMillieseconds / 1000.0, entry.measuredTime_, 0.01);
}

TEST(BenchmarkMeasurementContainerTest, ResultGroup) {
  // There's really no special cases.
  ResultGroup group("group");

  ASSERT_EQ(group.descriptor_, "group");
  ASSERT_EQ(group.entries_.size(), 0);

  // Adding a measurement and checking, if it was added correctly.
  ResultEntry& entry = group.addMeasurement("new entry", createWaitLambda(100));

  ASSERT_EQ(group.entries_.size(), 1);
  ASSERT_EQ(entry.descriptor_, "new entry");

  // The time measurements can't be 100% accurat, so we give it a 'window'.
  ASSERT_NEAR(0.1, entry.measuredTime_, 0.01);
}

TEST(BenchmarkMeasurementContainerTest, ResultTable) {
  // Special case: A table with no rows, or columns. Should just throw
  // exceptions, if you try to do anything with it.
  {
    ResultTable table("0 by 0 table", {}, {});

    ASSERT_EQ(table.descriptor_, "0 by 0 table");
    ASSERT_TRUE(table.rowNames_.empty());
    ASSERT_TRUE(table.columnNames_.empty());
    ASSERT_TRUE(table.entries_.empty());

    // Those should just throw exceptions.
    ASSERT_ANY_THROW(table.addMeasurement(0, 0, []() {}));
    ASSERT_ANY_THROW(table.setEntry(0, 0, (float)0.1));
    ASSERT_ANY_THROW(table.getEntry<float>(0, 0));
  }

  // Normal case.
  const std::vector<std::string> rowNames{"row1", "row2"};
  const std::vector<std::string> columnNames{"column1", "column2"};
  ResultTable table("My table", rowNames, columnNames);

  // Was it created correctly?
  ASSERT_EQ("My table", table.descriptor_);
  ASSERT_EQ(rowNames, table.rowNames_);
  ASSERT_EQ(columnNames, table.columnNames_);
  ASSERT_EQ(2, table.entries_.size());
  ASSERT_EQ(2, table.entries_.at(0).size());
  ASSERT_EQ(2, table.entries_.at(1).size());

  // Add measured function to it.
  table.addMeasurement(0, 0, createWaitLambda(100));
  ASSERT_NEAR(0.1, table.getEntry<float>(0, 0), 0.01);

  // Set custom entries.
  table.setEntry(0, 1, (float)4.9);
  table.setEntry(1, 1, "Custom entry");

  ASSERT_FLOAT_EQ(table.getEntry<float>(0, 1), 4.9);
  ASSERT_EQ(table.getEntry<std::string>(1, 1), "Custom entry");

  // Trying to get entries with the wrong type, should cause an error.
  ASSERT_ANY_THROW(table.getEntry<std::string>(0, 1));
  ASSERT_ANY_THROW(table.getEntry<float>(1, 1));

  // Same for trying to get the value of an entry, who has never been set,
  // or added.
  ASSERT_ANY_THROW(table.getEntry<std::string>(1, 0));
  ASSERT_ANY_THROW(table.getEntry<float>(1, 0));
}
}  // namespace ad_benchmark
