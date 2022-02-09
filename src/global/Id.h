// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <cstdint>
#include <limits>

#include "../util/Exception.h"

typedef uint64_t Id;
typedef uint16_t Score;

// A value to use when the result should be empty (e.g. due to an optional join)
// The highest two values are used as sentinels.
static const Id ID_NO_VALUE = std::numeric_limits<Id>::max() - 2;

namespace ad_utility {

// Manages two kinds of IDs: unrounded IDs (unsigned 64-bit integers), and
// rounded IDs (unsigned 64-bit integers, where the least significant
// `numBytesRounded` bytes are all `rounded IDs` (unsigned 64-bit integers,
// where the least significant `numBytesRounded` bytes are all 0. Has
// functionality to give out ascending Ids, and to detect and convert rounded
// IDs
template <size_t numBytesRounded>
requires(numBytesRounded < 8) class RoundedIdManager {
 private:
  constexpr static size_t numBitsRounded = numBytesRounded * 8;
  // The next free unrounded ID;
  uint64_t nextId{0};
  // The last ID that was assigned. Used for overflow detection.
  uint64_t lastId{0};

 public:
  RoundedIdManager() = default;

  // Get the smallest ID that is larger than all previously obtained IDs and
  // has the lower `numBytesRounded` bytes set to zero.
  uint64_t getNextRoundedId() {
    if (!isRoundedId(nextId)) {
      nextId = (fromRoundedId(toRoundedId(nextId) + 1));
    }
    return getNextUnroundedId();
  }

  // Get the smallest ID that is larger than all previously obtained IDs.
  uint64_t getNextUnroundedId() {
    if (nextId < lastId) {
      // TODO<joka921> Proper exception type.
      throw std::runtime_error{"Overflow while rounding Ids"};
    }
    lastId = nextId;
    nextId++;
    return lastId;
  }

  // Is this ID a "rounded ID", equivalently: Are the lower `numBytesRounded`
  // bytes all zero.
  constexpr static bool isRoundedId(uint64_t id) {
    // Bits set to one in the range where the external part is stored.
    constexpr uint64_t mask = (~uint64_t(0)) >> (64 - numBitsRounded);
    return !(mask & id);
  }

  // Convert a rounded ID to its "actual value" by shifting it to the right by
  // `numBytesRounded` (The i-th rounded ID will become `i`).
  // TODO:: Naming!!!
  constexpr static uint64_t toRoundedId(uint64_t id) {
    return id >> numBitsRounded;
  }

  // TODO::Naming!
  // Convert `i` to the `i-th` rounded ID by shifting it to the left by
  // `numBytesRounded`.
  constexpr static uint64_t fromRoundedId(uint64_t id) {
    return id << numBitsRounded;
  }
};

}  // namespace ad_utility
