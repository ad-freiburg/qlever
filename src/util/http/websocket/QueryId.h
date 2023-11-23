//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <cstdint>
#include <random>
#include <type_traits>

#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Synchronized.h"
#include "util/UniqueCleanup.h"
#include "util/json.h"

namespace ad_utility::websocket {

/// Typed wrapper class for a query id represented as a string
class QueryId {
  std::string id_;
  explicit QueryId(std::string id) : id_{std::move(id)} {
    AD_CONTRACT_CHECK(!id_.empty());
  }

  friend void to_json(nlohmann::json& json, const QueryId& queryId) {
    json = queryId.id_;
  }

 public:
  /// Construct this object with the passed string
  /// Note that this does *not* ensure uniqueness.
  static QueryId idFromString(std::string id) { return QueryId{std::move(id)}; }

  /// Checks if the id is empty. Because empty ids are not allowed,
  /// this is usually a good indicator if the object has been moved out of
  [[nodiscard]] bool empty() const noexcept { return id_.empty(); }

  template <typename H>
  friend H AbslHashValue(H h, const QueryId& c) {
    return H::combine(std::move(h), c.id_);
  }

  // Starting with gcc 12 and clang 15 this can be constexpr
  bool operator==(const QueryId&) const noexcept = default;
};

/// This class is similar to QueryId, but it's instances are all unique within
/// the registry it was created with. (It can not be created without a registry)
/// Therefore it is not copyable and removes itself from said registry
/// on destruction.
class OwningQueryId {
  unique_cleanup::UniqueCleanup<QueryId> id_;

  friend class QueryRegistry;

  OwningQueryId(QueryId id, std::function<void(const QueryId&)> unregister)
      : id_{std::move(id), std::move(unregister)} {
    AD_CORRECTNESS_CHECK(!id_->empty());
  }

 public:
  [[nodiscard]] const QueryId& toQueryId() const& noexcept { return *id_; }
};

// Ensure promised copy semantics
static_assert(!std::is_copy_constructible_v<OwningQueryId>);
static_assert(!std::is_copy_assignable_v<OwningQueryId>);

/// A factory class to create unique query ids within each individual instance.
class QueryRegistry {
  using SharedCancellationHandle = std::shared_ptr<CancellationHandle>;
  using SynchronizedType = ad_utility::Synchronized<
      ad_utility::HashMap<QueryId, SharedCancellationHandle>>;
  // Technically no shared pointer is required because the registry lives
  // for the entire lifetime of the application, but since the instances of
  // `OwningQueryId` need to deregister themselves again they need some
  // sort of reference, which is safe when a shared ptr is used.
  std::shared_ptr<SynchronizedType> registry_{
      std::make_shared<SynchronizedType>()};

 public:
  QueryRegistry() = default;

  /// Tries to create a new unique OwningQueryId object from the given string.
  /// \param id The id representation of the potential candidate.
  /// \return A std::optional<OwningQueryId> object wrapping the passed string
  ///         if it was not present in the registry before. An empty
  ///         std::optional if the id already existed before.
  std::optional<OwningQueryId> uniqueIdFromString(std::string id) {
    auto queryId = QueryId::idFromString(std::move(id));
    bool success =
        registry_->wlock()
            ->emplace(queryId, std::make_shared<CancellationHandle>())
            .second;
    if (success) {
      // Avoid undefined behavior when the registry is no longer alive at the
      // time the `OwningQueryId` is destroyed.
      std::weak_ptr<SynchronizedType> weakRegistry = registry_;
      return OwningQueryId{
          std::move(queryId),
          [weakRegistry = std::move(weakRegistry)](const QueryId& qid) {
            AD_CORRECTNESS_CHECK(!qid.empty());
            // Registry might be destroyed already, do nothing in this case.
            if (auto registry = weakRegistry.lock()) {
              registry->wlock()->erase(qid);
            }
          }};
    }
    return std::nullopt;
  }

  /// Generates a unique pseudo-random OwningQueryId object for this registry
  OwningQueryId uniqueId() {
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distrib{};
    std::optional<OwningQueryId> result;
    do {
      result = uniqueIdFromString(std::to_string(distrib(generator)));
    } while (!result.has_value());
    return std::move(result.value());
  }

  /// Member function that acquires a read lock and returns a vector
  /// of all currently registered queries.
  std::vector<QueryId> getActiveQueries() const {
    return registry_->withReadLock([](const auto& map) {
      // TODO<C++23> Use `ranges::to` to transform map keys into vector
      std::vector<QueryId> result;
      result.reserve(map.size());
      for (const auto& entry : map) {
        result.emplace_back(entry.first);
      }
      return result;
    });
  }

  /// Returns the cancellation handle from the registry if it exists, nullptr
  /// otherwise.
  SharedCancellationHandle getCancellationHandle(const QueryId& queryId) const {
    return registry_->withReadLock([&queryId](const auto& map) {
      auto it = map.find(queryId);
      return it != map.end() ? it->second : nullptr;
    });
  }
};
}  // namespace ad_utility::websocket
