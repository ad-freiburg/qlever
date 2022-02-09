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
// `numBytesRounded` bytes are all 0rounded IDs (unsigned 64-bit integers, where
// the least significant `numBytesRounded` bytes are all 0. Has functionality to
// give out ascending Ids, and to detect and convert rounded IDs
template <size_t numBytesRounded>
requires(numBytesRounded < 8) class RoundedIdManager {
 private:
  using T = uint64_t;
  T nextId{0};

 public:
  RoundedIdManager() = default;
  constexpr static uint64_t maxRoundedId =
      uint64_t{(1ull << ((8 - numBytesRounded) * 8)) - 1};
  constexpr static uint64_t maxExternalIdPerBlock{
      (1ull << (numBytesRounded * 8)) - 1};
  constexpr static uint64_t maxId = std::numeric_limits<uint64_t>::max();

  uint64_t getNextRoundedId() {
    if (!isRoundedId(nextId)) {
      nextId = (fromRoundedId(toRoundedId(nextId) + 1));
    }
    return getNextUnroundedId();
  }

  uint64_t getNextUnroundedId() {
    AD_CHECK(nextId <= maxId);
    return nextId++;
  }

  constexpr static uint64_t toRoundedId(uint64_t id) {
    return id >> (numBytesRounded * 8);
  }

  constexpr static uint64_t fromRoundedId(uint64_t id) {
    return id << (numBytesRounded * 8);
  }
  constexpr static bool isRoundedId(uint64_t id) {
    // Bits set to one in the range where the external part is stored.
    constexpr uint64_t mask = (~uint64_t(0)) >> (64 - numBytesRounded * 8);
    return !(mask & id);
  }
};

using DefaultIdManager = RoundedIdManager<3>;
}  // namespace ad_utility
