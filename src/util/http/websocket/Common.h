//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <cstdint>
#include <random>
#include <type_traits>

#include "util/CleanupDeleter.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/Synchronized.h"

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

  // Starting with gcc 12 and clang 15 this can be constexpr
  bool operator==(const QueryId&) const noexcept = default;
};

class OwningQueryId {
  using deleter = cleanup_deleter::CleanupDeleter<QueryId>;
  std::unique_ptr<QueryId, deleter> id_;

  friend class QueryRegistry;

  OwningQueryId(QueryId id, std::function<void(const QueryId&)> unregister)
      : id_{deleter::cleanUpAfterUse(std::move(id), std::move(unregister))} {
    AD_CORRECTNESS_CHECK(!id_->empty());
  }

 public:
  [[nodiscard]] const QueryId& toQueryId() const noexcept { return *id_; }
};

static_assert(!std::is_copy_constructible_v<OwningQueryId>);
static_assert(!std::is_copy_assignable_v<OwningQueryId>);

class QueryRegistry {
  ad_utility::Synchronized<ad_utility::HashSet<QueryId>> registry_{};

 public:
  QueryRegistry() = default;

  std::optional<OwningQueryId> uniqueIdFromString(std::string id) {
    auto queryId = QueryId::idFromString(std::move(id));
    bool success = registry_.withWriteLock([&queryId](auto& registry) {
      if (registry.count(queryId)) {
        return false;
      }
      registry.insert(queryId);
      return true;
    });
    if (success) {
      return OwningQueryId{std::move(queryId), [this](const QueryId& id) {
                             AD_CORRECTNESS_CHECK(!id.empty());
                             registry_.wlock()->erase(id);
                           }};
    }
    return std::nullopt;
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
}  // namespace ad_utility::websocket::common
