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
template<size_t numBytesInternal, size_t numBytesPerBlockExternal> requires (numBytesInternal + numBytesPerBlockExternal <= 8 && numBytesPerBlockExternal > 0)
class InternalExternalIdManager {
 public:
  using T = uint64_t;
 private:

  T nextId = 0;

 public:
  InternalExternalIdManager() = default;
  constexpr static T maxInternalId = (T{1} << (numBytesInternal * 8)) - 1;
  constexpr static T maxExternalIdPerBlock = (T{1} << (numBytesPerBlockExternal * 8)) - 1;
  constexpr static T maxId = (T{1} << ((numBytesPerBlockExternal + numBytesInternal) * 8)) - 1;

  T getNextInternalId() {
    if (!isInternalId(nextId)) {
      nextId = (fromInternalId(toInternalId(nextId) + 1));
    }
    return getNextExternalId();
  }

  T getNextExternalId() {
    AD_CHECK(nextId <= maxId);
    return nextId++;
  }

  constexpr static T toInternalId(T id) {
    return id >> (numBytesPerBlockExternal * 8);
  }

  constexpr static T fromInternalId(T id) {
    return id << (numBytesPerBlockExternal * 8);
  }
  constexpr static bool isInternalId(T id) {
    // Bits set to one in the range where the external part is stored.
    constexpr uint64_t mask = (~uint64_t(0)) >> (64 - numBytesPerBlockExternal * 8);
    return !(mask & id);
  }
};
}
