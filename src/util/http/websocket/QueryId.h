//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HTTP_WEBSOCKET_QUERYID_H
#define QLEVER_SRC_UTIL_HTTP_WEBSOCKET_QUERYID_H

#include <chrono>
#include <cstdint>
#include <ostream>
#include <random>

#include "backports/three_way_comparison.h"
#include "backports/type_traits.h"
#include "util/CancellationHandle.h"
#include "util/CopyableSynchronization.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Log.h"
#include "util/Synchronized.h"
#include "util/UniqueCleanup.h"
#include "util/json.h"

namespace ad_utility::websocket {

// Typed wrapper class for a query id represented as a string
class QueryId {
  std::string id_;

  explicit QueryId(std::string id) : id_{std::move(id)} {
    AD_CONTRACT_CHECK(!id_.empty());
  }

  friend void to_json(nlohmann::json& json, const QueryId& queryId) {
    json = queryId.id_;
  }

  friend std::ostream& operator<<(std::ostream& os, const QueryId& queryId) {
    return os << queryId.id_;
  }

 public:
  // Construct this object with the passed string
  // Note that this does *not* ensure uniqueness.
  static QueryId idFromString(std::string id) { return QueryId{std::move(id)}; }

  // Checks if the id is empty. Because empty ids are not allowed,
  // this is usually a good indicator if the object has been moved out of
  [[nodiscard]] bool empty() const noexcept { return id_.empty(); }

  template <typename H>
  friend H AbslHashValue(H h, const QueryId& c) {
    return H::combine(std::move(h), c.id_);
  }

  // Starting with gcc 12 and clang 15 this can be constexpr
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(QueryId, id_)
};

// Terminal status of a query, as observed at the moment its
// `OwningQueryId` is destroyed. `Unknown` is the default and indicates
// that no caller invoked `OwningQueryId::setStatus` before destruction;
// see `OwningQueryId::status_` for the rationale.
enum class QueryStatus { Unknown, Ok, Failed, Cancelled, Timeout };

// This class is similar to QueryId, but it's instances are all unique within
// the registry it was created with. (It can not be created without a registry)
// Therefore it is not copyable and removes itself from said registry
// on destruction.
class OwningQueryId {
  unique_cleanup::UniqueCleanup<QueryId> id_;
  // Terminal status of this query. Written by call sites (success path
  // and catch blocks) via `setStatus`, read by the unregister lambda
  // when it builds the end event. Atomic because the writer and the
  // destructor read may execute on different threads;
  ad_utility::CopyableAtomic<QueryStatus> status_{QueryStatus::Unknown};

  friend class QueryRegistry;

  OwningQueryId(QueryId id, std::function<void(const QueryId&)> unregister)
      : id_{std::move(id), std::move(unregister)} {
    AD_CORRECTNESS_CHECK(!id_->empty());
  }

 public:
  [[nodiscard]] const QueryId& toQueryId() const& noexcept { return *id_; }

  // Record the terminal status of this query. Intended to be called
  // once on the success path or in a catch block before this object is
  // destroyed; the unregister lambda reads the final value
  // during `~OwningQueryId`. Safe to call from any thread.
  void setStatus(QueryStatus s) noexcept { status_.store(s); }

  // Current value of the terminal-status field. Returns
  // `QueryStatus::Unknown` until `setStatus` is invoked.
  [[nodiscard]] QueryStatus status() const noexcept { return status_.load(); }
};

// Ensure promised copy semantics
static_assert(!std::is_copy_constructible_v<OwningQueryId>);
static_assert(!std::is_copy_assignable_v<OwningQueryId>);

// A factory class to create unique query ids within each individual instance.
class QueryRegistry {
  struct CancellationHandleWithQuery {
    SharedCancellationHandle cancellationHandle_ =
        std::make_shared<CancellationHandle<>>();
    std::string query_;
    // Wall-clock instant at which this entry was registered.
    std::chrono::system_clock::time_point startedAt_ =
        std::chrono::system_clock::now();
    explicit CancellationHandleWithQuery(std::string_view query)
        : query_{query} {}
  };
  using SynchronizedType = ad_utility::Synchronized<
      ad_utility::HashMap<QueryId, CancellationHandleWithQuery>>;
  // Technically no shared pointer is required because the registry lives
  // for the entire lifetime of the application, but since the instances of
  // `OwningQueryId` need to deregister themselves again they need some
  // sort of reference, which is safe when a shared ptr is used.
  std::shared_ptr<SynchronizedType> registry_{
      std::make_shared<SynchronizedType>()};

 public:
  // Snapshot of a single active query. Returned from `getActiveQueries`.
  struct ActiveQueryInfo {
    std::string query_;
    // Wall-clock instant when the query was registered. Serialized to
    // clients as a Unix-epoch timestamp in milliseconds.
    std::chrono::system_clock::time_point startedAt_;

    friend void to_json(nlohmann::json& json, const ActiveQueryInfo& info) {
      json = {
          {"query", info.query_},
          {"started-at", std::chrono::duration_cast<std::chrono::milliseconds>(
                             info.startedAt_.time_since_epoch())
                             .count()},
      };
    }
  };

  QueryRegistry() = default;

  // Tries to create a new unique OwningQueryId object from the given string.
  // \param id The id representation of the potential candidate.
  // \param query The string representation of the associated SPARQL query.
  // \return A std::optional<OwningQueryId> object wrapping the passed string
  //         if it was not present in the registry before. An empty
  //         std::optional if the id already existed before.
  std::optional<OwningQueryId> uniqueIdFromString(std::string id,
                                                  std::string_view query) {
    auto queryId = QueryId::idFromString(std::move(id));
    // Capture the entry's `startedAt_` while the lock is held so the value
    // logged here matches what `getActiveQueries()` reports for this query.
    std::chrono::system_clock::time_point startedAt;
    {
      auto lockedMap = registry_->wlock();
      auto [it, success] = lockedMap->try_emplace(queryId, query);
      if (!success) {
        return std::nullopt;
      }
      startedAt = it->second.startedAt_;
    }
    auto startedAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                           startedAt.time_since_epoch())
                           .count();
    AD_LOG_METRIC << nlohmann::json{{"event", "start"},
                                    {"query-id", queryId},
                                    {"started-at", startedAtMs},
                                    {"query", query}}
                         .dump()
                  << std::endl;
    // Avoid undefined behavior when the registry is no longer alive at the
    // time the `OwningQueryId` is destroyed.
    std::weak_ptr<SynchronizedType> weakRegistry = registry_;
    return OwningQueryId{
        std::move(queryId),
        [weakRegistry = std::move(weakRegistry)](const QueryId& qid) {
          AD_CORRECTNESS_CHECK(!qid.empty());
          // Registry might be destroyed already, do nothing in this case.
          if (auto registry = weakRegistry.lock()) {
            // Wall-clock instant at which the query exited the registry.
            auto endedAtMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
            AD_LOG_METRIC << nlohmann::json{{"event", "end"},
                                            {"query-id", qid},
                                            {"ended-at", endedAtMs}}
                                 .dump()
                          << std::endl;
            registry->wlock()->erase(qid);
          }
        }};
  }

  // Generates a unique pseudo-random OwningQueryId object for this registry
  // and associates it with the given query.
  OwningQueryId uniqueId(std::string_view query) {
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distrib{};
    std::optional<OwningQueryId> result;
    do {
      result = uniqueIdFromString(std::to_string(distrib(generator)), query);
    } while (!result.has_value());
    return std::move(result.value());
  }

  // Member function that acquires a read lock and returns a snapshot of all
  //  currently registered queries. See `ActiveQueryInfo` for the value shape.
  ad_utility::HashMap<QueryId, ActiveQueryInfo> getActiveQueries() const {
    return registry_->withReadLock([](const auto& map) {
      ad_utility::HashMap<QueryId, ActiveQueryInfo> result;
      result.reserve(map.size());
      for (const auto& entry : map) {
        result.emplace(entry.first, ActiveQueryInfo{entry.second.query_,
                                                    entry.second.startedAt_});
      }
      return result;
    });
  }

  // Returns the cancellation handle from the registry if it exists, nullptr
  // otherwise.
  SharedCancellationHandle getCancellationHandle(const QueryId& queryId) const {
    return registry_->withReadLock([&queryId](const auto& map) {
      auto it = map.find(queryId);
      return it != map.end() ? it->second.cancellationHandle_ : nullptr;
    });
  }
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_SRC_UTIL_HTTP_WEBSOCKET_QUERYID_H
