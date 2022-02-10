// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <limits>

#include "../util/Exception.h"

typedef uint64_t Id;
typedef uint16_t Score;

// A value to use when the result should be empty (e.g. due to an optional join)
// The highest two values are used as sentinels.
static const Id ID_NO_VALUE = std::numeric_limits<Id>::max() - 2;

namespace ad_utility {

// An exception that is thrown when an integer overflow occurs in the
// `MilestoneIdManager`
class MilestoneOverflowException : public std::exception {
 private:
  std::string _message;

 public:
  explicit MilestoneOverflowException(std::string message)
      : _message{std::move(message)} {}
  [[nodiscard]] const char* what() const noexcept override {
    return _message.c_str();
  }
};

// Manages two kinds of IDs: plain IDs (unsigned 64-bit integers, just called
// "Ids" in the following), and milestone IDs (unsigned 64-bit integers that are
// multiples of `distanceBetweenMilestones`. This class has the functionality to
// emit ascending Ids, and to detect and convert milestone IDs.
template <size_t distanceBetweenMilestones>
class MilestoneIdManager {
 private:
  // The next free ID;
  uint64_t nextId{0};
  // The last ID that was assigned. Used for overflow detection.
  uint64_t lastId{0};

 public:
  MilestoneIdManager() = default;

  // The maximum number of milestone Ids.
  constexpr static uint64_t numMilestoneIds =
      std::numeric_limits<uint64_t>::max() / distanceBetweenMilestones;

  // Get the smallest milestone ID that is larger than all (milestone and
  // non-milestone) previously obtained IDs.
  uint64_t getNextMilestoneId() {
    if (!isMilestoneId(nextId)) {
      nextId = (MilestoneIdFromLocal(MilestoneIdToLocal(nextId) + 1));
    }
    return getNextId();
  }

  // Get the smallest ID that is larger than all previously obtained IDs.
  uint64_t getNextId() {
    if (nextId < lastId) {
      throw MilestoneOverflowException{absl::StrCat(
          "Overflow while assigning Ids from a MilestoneIdManager. The last "
          "milestone Id was ",
          lastId, " the next id would be ", nextId,
          ". The maximum number of milestones is ", numMilestoneIds, ".")};
    }
    lastId = nextId;
    nextId++;
    return lastId;
  }

  // Is this ID a milestone id, equivalently: Is this ID a multiple of
  // `distanceBetweenMilestones`?
  constexpr static bool isMilestoneId(uint64_t id) {
    return id % distanceBetweenMilestones == 0;
  }

  // Convert a milestone ID to its "actual value" by dividing it by
  // `distanceBetweenMilestones` (The i-th milestone ID will become `i`).
  constexpr static uint64_t MilestoneIdToLocal(uint64_t id) {
    return id / distanceBetweenMilestones;
  }

  // Convert `i` to the `i-th` Milestone ID by multiplying it with
  // `distanceBetweenMilestones`.
  constexpr static uint64_t MilestoneIdFromLocal(uint64_t id) {
    return id * distanceBetweenMilestones;
  }
};

}  // namespace ad_utility
