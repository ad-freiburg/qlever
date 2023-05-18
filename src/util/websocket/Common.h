//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <type_traits>

#include "engine/RuntimeInformation.h"

// Provides types required by all the other *.cpp files in this directory
// and a select few other places
namespace ad_utility::websocket::common {

class QueryId {
  std::string id_;
  explicit QueryId(std::string id) : id_{std::move(id)} {}

 public:
  static QueryId idFromString(std::string id) { return QueryId{std::move(id)}; }
  bool empty() const noexcept { return id_.empty(); }

  template <typename H>
  friend H AbslHashValue(H h, const QueryId& c) {
    return H::combine(std::move(h), c.id_);
  }

  // Starting with gcc 12 and clang 15 these can be constexpr
  bool operator==(const QueryId&) const noexcept = default;
  // required for multimap
  auto operator<=>(const QueryId&) const noexcept = default;
};

class OwningQueryId {
  QueryId id_;
  std::function<void(const QueryId&)> unregister_;

  friend class QueryRegistry;

  OwningQueryId(QueryId id, std::function<void(const QueryId&)> unregister)
      : id_{std::move(id)}, unregister_{std::move(unregister)} {
    AD_CORRECTNESS_CHECK(!id_.empty());
  }

 public:
  // Prevent copies to prevent preemptive cleanup
  OwningQueryId(const OwningQueryId&) = delete;
  OwningQueryId& operator=(const OwningQueryId&) = delete;

  OwningQueryId(OwningQueryId&&) = default;
  OwningQueryId& operator=(OwningQueryId&&) = default;

  [[nodiscard]] const QueryId& toQueryId() const noexcept { return id_; }

  ~OwningQueryId() {
    if (unregister_) {
      unregister_(id_);
    }
  }
};

static_assert(!std::is_copy_constructible_v<OwningQueryId>);
static_assert(!std::is_copy_assignable_v<OwningQueryId>);

class QueryRegistry {
  std::mutex registryMutex_{};
  std::unordered_set<QueryId, absl::Hash<QueryId>> registry_{};

 public:
  QueryRegistry() = default;

  std::optional<OwningQueryId> uniqueIdFromString(std::string id) {
    auto queryId = QueryId::idFromString(std::move(id));
    std::lock_guard lock{registryMutex_};
    if (registry_.count(queryId)) {
      return std::nullopt;
    }
    registry_.insert(queryId);
    return OwningQueryId{std::move(queryId), [this](const QueryId& id) {
                           AD_CORRECTNESS_CHECK(!id.empty());
                           std::lock_guard lock{registryMutex_};
                           registry_.erase(id);
                         }};
  }

  OwningQueryId uniqueId() {
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distrib{};
    std::optional<OwningQueryId> result = std::nullopt;
    while (!result.has_value()) {
      result = uniqueIdFromString(std::to_string(distrib(generator)));
    }
    return std::move(result.value());
  }
};

using Timestamp = std::chrono::time_point<std::chrono::steady_clock>;

struct PayloadAndTimestamp {
  std::string payload;
  Timestamp updateMoment;
};

using SharedPayloadAndTimestamp = std::shared_ptr<const PayloadAndTimestamp>;
}  // namespace ad_utility::websocket::common
