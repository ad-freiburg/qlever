// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <cstdint>
#include <limits>

#include "../util/Exception.h"
#include <NamedType/named_type.hpp>

typedef uint64_t Id;
typedef uint16_t Score;

// A value to use when the result should be empty (e.g. due to an optional join)
// The highest two values are used as sentinels.
static const Id ID_NO_VALUE = std::numeric_limits<Id>::max() - 2;

namespace ad_utility {
namespace detail {
template <typename T>
concept trivial8Bytes = std::is_trivially_copyable_v<T> && sizeof(T)  == 8;
}
template <size_t numBytesInternal, size_t numBytesPerBlockExternal, typename CompleteId = uint64_t, typename InternalId = uint64_t>
requires(numBytesInternal + numBytesPerBlockExternal <= 8 &&
         numBytesPerBlockExternal > 0 && detail::trivial8Bytes<CompleteId> && detail::trivial8Bytes<InternalId>) class InternalExternalIdManager {
 private:
  CompleteId nextId = CompleteId{0};

 public:
  InternalExternalIdManager() = default;
  constexpr static CompleteId maxInternalId = CompleteId{(1ull << (numBytesInternal * 8)) - 1};
  constexpr static CompleteId maxExternalIdPerBlock{
      (1ull << (numBytesPerBlockExternal * 8)) - 1};
  constexpr static CompleteId maxId  {
    (1ull << (numBytesPerBlockExternal + numBytesInternal) * 8) - 1};

  CompleteId getNextInternalId() {
    if (!isInternalId(nextId)) {
      nextId = (fromInternalId(toInternalId(nextId) + 1));
    }
    return getNextExternalId();
  }

  CompleteId getNextExternalId() {
    AD_CHECK(nextId <= maxId);
    auto result = nextId;
    nextId += CompleteId{1};
    return result;
  }

  // TODO<joka921> Is this even needed?
  [[nodiscard]] CompleteId getNextExternalIdWithoutIncrement() const {
    AD_CHECK(nextId <= maxId);
    return nextId;
  }


  constexpr static InternalId  toInternalId(CompleteId id) {
    return id >> (numBytesPerBlockExternal * 8);
  }

  constexpr static CompleteId fromInternalId(InternalId id) {
    return id << (numBytesPerBlockExternal * 8);
  }
  constexpr static bool isInternalId(CompleteId id) {
    // Bits set to one in the range where the external part is stored.
    constexpr uint64_t mask =
        (~uint64_t(0)) >> (64 - numBytesPerBlockExternal * 8);
    return !(mask & id);
  }
};

using InternalId = fluent::NamedType<uint64_t, struct InternalIdTag, fluent::Arithmetic>;
using InternalUnsignedId = fluent::NamedType<int64_t, struct InternalUnsignedIdTag, fluent::Arithmetic>;
using CompleteId = fluent::NamedType<uint64_t, struct CompleteIdTag, fluent::Arithmetic>;
using DefaultIdManager = InternalExternalIdManager<4, 3, CompleteId, InternalId>;
}  // namespace ad_utility
