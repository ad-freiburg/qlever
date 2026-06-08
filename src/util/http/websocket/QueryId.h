//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HTTP_WEBSOCKET_QUERYID_H
#define QLEVER_SRC_UTIL_HTTP_WEBSOCKET_QUERYID_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <string_view>

#include "backports/three_way_comparison.h"
#include "backports/type_traits.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/QueryEventLog.h"
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

// Terminal status of a query, observed at `OwningQueryId` destruction.
enum class QueryStatus { Unknown, Ok, Failed, Cancelled, Timeout };

inline std::string_view toString(QueryStatus s) noexcept {
  switch (s) {
    case QueryStatus::Ok:
      return "ok";
    case QueryStatus::Failed:
      return "failed";
    case QueryStatus::Cancelled:
      return "cancelled";
    case QueryStatus::Timeout:
      return "timeout";
    case QueryStatus::Unknown:
      return "unknown";
  }
  return "unknown";
}

// This class is similar to QueryId, but it's instances are all unique within
// the registry it was created with. (It can not be created without a registry)
// Therefore it is not copyable and removes itself from said registry
// on destruction.
class OwningQueryId {
  // Shared with the unregister lambda in `id_`. `shared_ptr` because
  // `OwningQueryId` is movable; a captured `&status_` would dangle.
  std::shared_ptr<std::atomic<QueryStatus>> status_;
  unique_cleanup::UniqueCleanup<QueryId> id_;

  friend class QueryRegistry;

  OwningQueryId(QueryId id, std::shared_ptr<std::atomic<QueryStatus>> status,
                std::function<void(const QueryId&)> unregister)
      : status_{std::move(status)}, id_{std::move(id), std::move(unregister)} {
    AD_CORRECTNESS_CHECK(!id_->empty());
    AD_CORRECTNESS_CHECK(status_ != nullptr);
  }

 public:
  [[nodiscard]] const QueryId& toQueryId() const& noexcept { return *id_; }

  // Record the terminal status. Read by the unregister lambda during
  // `~OwningQueryId`; safe to call from any thread.
  void setStatus(QueryStatus s) noexcept { status_->store(s); }

  // Returns `QueryStatus::Unknown` until `setStatus` is invoked.
  [[nodiscard]] QueryStatus status() const noexcept { return status_->load(); }

  // Shared handle to the atomic; lets a caller publish the status
  // without holding this object.
  [[nodiscard]] std::shared_ptr<std::atomic<QueryStatus>> sharedStatus()
      const noexcept {
    return status_;
  }
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
    // Wall-clock instant at which this entry was registered. Supplied by the
    // caller so the same value also feeds the start-event log line.
    std::chrono::system_clock::time_point startedAt_;
    CancellationHandleWithQuery(std::string_view query,
                                std::chrono::system_clock::time_point startedAt)
        : query_{query}, startedAt_{startedAt} {}
  };
  using SynchronizedType = ad_utility::Synchronized<
      ad_utility::HashMap<QueryId, CancellationHandleWithQuery>>;
  // Technically no shared pointer is required because the registry lives
  // for the entire lifetime of the application, but since the instances of
  // `OwningQueryId` need to deregister themselves again they need some
  // sort of reference, which is safe when a shared ptr is used.
  std::shared_ptr<SynchronizedType> registry_{
      std::make_shared<SynchronizedType>()};
  // Where start/end events are pushed. Non-owning; must outlive the
  // registry. Tests inject a local instance.
  QueryEventLog* eventLog_ = &QueryEventLog::instance();

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

  // Test-only constructor that redirects start/end events to a
  // caller-owned `QueryEventLog`. The pointer is non-owning and must outlive
  // the registry; it is dereferenced unconditionally when emitting events.
  explicit QueryRegistry(QueryEventLog* eventLog) : eventLog_{eventLog} {
    AD_CONTRACT_CHECK(eventLog_ != nullptr);
  }

  // Tries to create a new unique OwningQueryId object from the given string.
  // \param id The id representation of the potential candidate.
  // \param query The string representation of the associated SPARQL query.
  // \return A std::optional<OwningQueryId> object wrapping the passed string
  //         if it was not present in the registry before. An empty
  //         std::optional if the id already existed before.
  std::optional<OwningQueryId> uniqueIdFromString(
      std::string id, std::string_view query, std::string_view clientIp = {}) {
    auto queryId = QueryId::idFromString(std::move(id));

    // Do all work that might throw (JSON, allocations) before inserting into
    // the registry, so a failure here leaves no leftover entry. `startedAt` is
    // computed once and used for both the entry and the start line, so both
    // report the same time.
    auto startedAt = std::chrono::system_clock::now();
    auto startedAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                           startedAt.time_since_epoch())
                           .count();
    // `ordered_json` so field order matches the on-disk contract
    // (default `nlohmann::json` sorts keys).
    nlohmann::ordered_json startLine = {
        {"ts-ms", startedAtMs},
        {"event", "start"},
        {"qid", nlohmann::json(queryId)},
        {"client-ip", clientIp},
        {"query", query},
    };
    auto startPayload = startLine.dump();
    startPayload.push_back('\n');

    // Created before the lambda so the lambda can capture it.
    auto status =
        std::make_shared<std::atomic<QueryStatus>>(QueryStatus::Unknown);
    // `weak_ptr` so the lambda doesn't keep the registry alive. The
    // end-event push runs unconditionally (one `end` per `start`); the
    // erase is best-effort.
    std::weak_ptr<SynchronizedType> weakRegistry = registry_;
    auto unregister = [weakRegistry = std::move(weakRegistry), status,
                       eventLog = eventLog_](const QueryId& qid) {
      AD_CORRECTNESS_CHECK(!qid.empty());
      auto endedAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
      nlohmann::ordered_json endLine = {
          {"ts-ms", endedAtMs},
          {"event", "end"},
          {"qid", nlohmann::json(qid)},
          {"status", toString(status->load())},
      };
      auto endPayload = endLine.dump();
      endPayload.push_back('\n');
      eventLog->push(std::move(endPayload));
      if (auto registry = weakRegistry.lock()) {
        registry->wlock()->erase(qid);
      }
    };

    // Insert into the registry. If the id is already in use, nothing was
    // added, so we just return.
    {
      auto lockedMap = registry_->wlock();
      if (!lockedMap->try_emplace(queryId, query, startedAt).second) {
        return std::nullopt;
      }
    }

    // Build the OwningQueryId first (only moves, never throws), then write the
    // start event. If the write throws, the OwningQueryId is destroyed and
    // removes the entry again.
    OwningQueryId owningQueryId{std::move(queryId), std::move(status),
                                std::move(unregister)};
    eventLog_->push(std::move(startPayload));
    return owningQueryId;
  }

  // Generates a unique pseudo-random OwningQueryId object for this registry
  // and associates it with the given query.
  OwningQueryId uniqueId(std::string_view query,
                         std::string_view clientIp = {}) {
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distrib{};
    std::optional<OwningQueryId> result;
    do {
      result = uniqueIdFromString(std::to_string(distrib(generator)), query,
                                  clientIp);
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
