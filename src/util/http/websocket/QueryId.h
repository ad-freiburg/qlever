//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HTTP_WEBSOCKET_QUERYID_H
#define QLEVER_SRC_UTIL_HTTP_WEBSOCKET_QUERYID_H

#include <absl/functional/any_invocable.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <string_view>
#include <vector>

#include "backports/three_way_comparison.h"
#include "backports/type_traits.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Synchronized.h"
#include "util/Timer.h"
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
enum class QueryStatus { OK, FAILED, CANCELLED, TIMEOUT };

inline std::string_view toString(QueryStatus s) noexcept {
  using enum QueryStatus;
  switch (s) {
    case OK:
      return "ok";
    case FAILED:
      return "failed";
    case CANCELLED:
      return "cancelled";
    case TIMEOUT:
      return "timeout";
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

  // Returns `QueryStatus::FAILED` until `setStatus` is invoked.
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
    // Wall-clock instant at which this entry was registered. Stamped here so
    // the registry owns the start time; the start-event log line reads it back.
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
          {"started-at", epochMillis(info.startedAt_)},
      };
    }
  };

  // What an `onStart` callback receives. The registry fills this in at
  // registration; the callback frames it as a log line.
  struct StartInfo {
    const QueryId& queryId_;
    std::string_view query_;
    std::string_view clientIp_;
    std::chrono::system_clock::time_point startedAt_;

    // Field-by-field so `ordered_json` keeps insertion order (the on-disk
    // contract); a brace-init can't mix the foreign `qid` json with the rest.
    friend void to_json(nlohmann::ordered_json& json, const StartInfo& info) {
      json["ts-ms"] = epochMillis(info.startedAt_);
      json["event"] = "start";
      json["qid"] = nlohmann::json(info.queryId_);
      json["client-ip"] = info.clientIp_;
      json["query"] = info.query_;
    }
  };

  // What an `onEnd` callback receives, filled in when the query finishes.
  struct EndInfo {
    const QueryId& queryId_;
    QueryStatus status_;
    std::chrono::system_clock::time_point endedAt_;

    // Field-by-field so `ordered_json` keeps insertion order (the on-disk
    // contract); a brace-init can't mix the foreign `qid` json with the rest.
    friend void to_json(nlohmann::ordered_json& json, const EndInfo& info) {
      json["ts-ms"] = epochMillis(info.endedAt_);
      json["event"] = "end";
      json["qid"] = nlohmann::json(info.queryId_);
      json["status"] = toString(info.status_);
    }
  };

  // `const`-invocable so the same callback can run concurrently from many
  // query threads without a lock.
  using StartCallback = absl::AnyInvocable<void(const StartInfo&) const>;
  using EndCallback = absl::AnyInvocable<void(const EndInfo&) const>;

  QueryRegistry() = default;

  // Register a callback fired when a query is registered / unregistered.
  // Call at setup (single-threaded); callbacks then run on the hot path.
  void addOnStart(StartCallback cb) { onStart_.push_back(std::move(cb)); }
  void addOnEnd(EndCallback cb) { onEnd_->push_back(std::move(cb)); }

  // Tries to create a new unique OwningQueryId object from the given string.
  // \param id The id representation of the potential candidate.
  // \param query The string representation of the associated SPARQL query.
  // \return A std::optional<OwningQueryId> object wrapping the passed string
  //         if it was not present in the registry before. An empty
  //         std::optional if the id already existed before.
  std::optional<OwningQueryId> uniqueIdFromString(
      std::string id, std::string_view query, std::string_view clientIp = {}) {
    auto queryId = QueryId::idFromString(std::move(id));

    // The `end` event reports a failure unless the query thread
    // overwrites this with an explicit outcome (`OK`/`CANCELLED`/`TIMEOUT`).
    auto status =
        std::make_shared<std::atomic<QueryStatus>>(QueryStatus::FAILED);
    // `weak_ptr` so the lambda doesn't keep the registry alive. The
    // end-event push runs unconditionally (one `end` per `start`); the
    // erase is best-effort.
    std::weak_ptr<SynchronizedType> weakRegistry = registry_;
    auto unregister = [weakRegistry = std::move(weakRegistry), status,
                       onEnd = onEnd_](const QueryId& qid) {
      AD_CORRECTNESS_CHECK(!qid.empty());
      EndInfo info{qid, status->load(), std::chrono::system_clock::now()};
      for (const auto& cb : *onEnd) {
        cb(info);
      }
      if (auto registry = weakRegistry.lock()) {
        registry->wlock()->erase(qid);
      }
    };

    // Insert into the registry. If the id is already in use, nothing was
    // added, so we just return.
    std::chrono::system_clock::time_point startedAt;
    {
      auto lockedMap = registry_->wlock();
      auto [it, inserted] = lockedMap->try_emplace(queryId, query);
      if (!inserted) {
        return std::nullopt;
      }
      startedAt = it->second.startedAt_;
    }

    // Build the OwningQueryId first (only moves, never throws), then fire the
    // start callbacks. If one throws, the OwningQueryId is destroyed and
    // removes the entry again.
    OwningQueryId owningQueryId{std::move(queryId), std::move(status),
                                std::move(unregister)};
    StartInfo info{owningQueryId.toQueryId(), query, clientIp, startedAt};
    for (const auto& cb : onStart_) {
      cb(info);
    }
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

 private:
  std::vector<StartCallback> onStart_;
  // `shared_ptr` so the unregister lambda keeps these alive even if the
  // registry is destroyed first (e.g. on server shutdown).
  std::shared_ptr<std::vector<EndCallback>> onEnd_ =
      std::make_shared<std::vector<EndCallback>>();
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_SRC_UTIL_HTTP_WEBSOCKET_QUERYID_H
