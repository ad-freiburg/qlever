// Copyright 2011 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2011 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de> [2017 - 2024]

#ifndef QLEVER_SRC_ENGINE_QUERYEXECUTIONCONTEXT_H
#define QLEVER_SRC_ENGINE_QUERYEXECUTIONCONTEXT_H

#include <chrono>
#include <memory>
#include <string>

#include "backports/three_way_comparison.h"
#include "engine/QueryPlanningCostFactors.h"
#include "engine/Result.h"
#include "engine/RuntimeInformation.h"
#include "engine/SortPerformanceEstimator.h"
#include "global/Id.h"
#include "index/DeltaTriples.h"
#include "index/Index.h"
#include "util/Cache.h"
#include "util/ConcurrentCache.h"

// The value of the `QueryResultCache` below. It consists of a `Result` together
// with its `RuntimeInfo`.
class CacheValue {
 private:
  std::shared_ptr<Result> result_;
  RuntimeInformation runtimeInfo_;

 public:
  explicit CacheValue(Result result, RuntimeInformation runtimeInfo)
      : result_{std::make_shared<Result>(std::move(result))},
        runtimeInfo_{std::move(runtimeInfo)} {}

  CacheValue(CacheValue&&) = default;
  CacheValue(const CacheValue&) = delete;
  CacheValue& operator=(CacheValue&&) = default;
  CacheValue& operator=(const CacheValue&) = delete;

  const Result& resultTable() const noexcept { return *result_; }

  std::shared_ptr<const Result> resultTablePtr() const noexcept {
    return result_;
  }

  const RuntimeInformation& runtimeInfo() const noexcept {
    return runtimeInfo_;
  }

  static ad_utility::MemorySize getSize(const IdTable& idTable) {
    return ad_utility::MemorySize::bytes(idTable.size() * idTable.numColumns() *
                                         sizeof(Id));
  }

  // Calculates the `MemorySize` taken up by an instance of `CacheValue`.
  struct SizeGetter {
    ad_utility::MemorySize operator()(const CacheValue& cacheValue) const {
      if (const auto& resultPtr = cacheValue.result_; resultPtr) {
        return getSize(resultPtr->idTable());
      } else {
        return 0_B;
      }
    }
  };
};

// The key for the `QueryResultCache` below. It consists of a `string` (the
// actual cache key of a `QueryExecutionTree` and the index of the
// `LocatedTriplesSnapshot` that was used to create the corresponding value.
// That way, two identical trees with different snapshot indices will have a
// different cache key. This has the (desired!) effect that UPDATE requests
// correctly invalidate preexisting cache results.
struct QueryCacheKey {
  std::string key_;
  size_t locatedTriplesSnapshotIndex_;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(QueryCacheKey, key_,
                                              locatedTriplesSnapshotIndex_)

  template <typename H>
  friend H AbslHashValue(H h, const QueryCacheKey& key) {
    return H::combine(std::move(h), key.key_, key.locatedTriplesSnapshotIndex_);
  }
};

// Threadsafe LRU cache for (partial) query results, that
// checks on insertion, if the result is currently being computed
// by another query.
using QueryResultCache = ad_utility::ConcurrentCache<
    ad_utility::LRUCache<QueryCacheKey, CacheValue, CacheValue::SizeGetter>>;

// Forward declaration because of cyclic dependency
class NamedResultCache;

// Execution context for queries.
// Holds references to index and engine, implements caching.
class QueryExecutionContext {
 public:
  QueryExecutionContext(
      const Index& index, QueryResultCache* const cache,
      ad_utility::AllocatorWithLimit<Id> allocator,
      SortPerformanceEstimator sortPerformanceEstimator,
      NamedResultCache* namedResultCache,
      std::function<void(std::string)> updateCallback =
          [](std::string) { /* No-op by default for testing */ },
      bool pinSubtrees = false, bool pinResult = false);

  QueryResultCache& getQueryTreeCache() { return *_subtreeCache; }

  [[nodiscard]] const Index& getIndex() const { return _index; }

  const LocatedTriplesSnapshot& locatedTriplesSnapshot() const {
    AD_CORRECTNESS_CHECK(sharedLocatedTriplesSnapshot_ != nullptr);
    return *sharedLocatedTriplesSnapshot_;
  }

  SharedLocatedTriplesSnapshot sharedLocatedTriplesSnapshot() const {
    return sharedLocatedTriplesSnapshot_;
  }

  // This function retrieves the most recent `LocatedTriplesSnapshot` and stores
  // it in the `QueryExecutionContext`. The new snapshot will be used for
  // evaluating queries after this call.
  //
  // NOTE: This is a dangerous function. It may only be called if no query with
  // the context is currently running.
  //
  // This function is only needed for chained updates, which have to see the
  // effect of previous updates but use the same execution context. Chained
  // updates are processed strictly sequentially, so this use case works.
  void updateLocatedTriplesSnapshot() {
    sharedLocatedTriplesSnapshot_ =
        _index.deltaTriplesManager().getCurrentSnapshot();
  }

  void clearCacheUnpinnedOnly() { getQueryTreeCache().clearUnpinnedOnly(); }

  [[nodiscard]] const SortPerformanceEstimator& getSortPerformanceEstimator()
      const {
    return _sortPerformanceEstimator;
  }

  [[nodiscard]] double getCostFactor(const std::string& key) const {
    return _costFactors.getCostFactor(key);
  };

  const ad_utility::AllocatorWithLimit<Id>& getAllocator() {
    return _allocator;
  }

  // Serialize the given `runtimeInformation` to a JSON string and send it
  // using `updateCallback_`. If `sendPriority` is set to `IfDue`, this only
  // happens if the last update was sent more than `websocketUpdateInterval_`
  // ago; if it is set to `Always`, the update is always sent.
  void signalQueryUpdate(const RuntimeInformation& runtimeInformation,
                         RuntimeInformation::SendPriority sendPriority) const;

  bool _pinSubtrees;
  bool _pinResult;

  // If false, then no updates of the runtime information should be sent via the
  // websocket connection for performance reasons.
  bool areWebsocketUpdatesEnabled() const {
    return areWebsocketUpdatesEnabled_;
  }

  // Access the cache for explicitly named query.
  NamedResultCache& namedResultCache() {
    AD_CORRECTNESS_CHECK(namedResultCache_ != nullptr);
    return *namedResultCache_;
  }

  // If `pinResultWithName_` is set, then the result of the query that is
  // executed using this context will be stored in the `namedQueryCache()` using
  // the string given in `PinResultWithName` as the query name. If
  // `geoIndexVar_` is also set, a geo index is built and cached in-memory on
  // the column of this variable. If `pinResultWithName_` is `nullopt`, no
  // pinning is done.
  struct PinResultWithName {
    std::string name_;
    std::optional<Variable> geoIndexVar_ = std::nullopt;
  };

  // Accessors; see `pinResultWithName_` for an explanation.
  auto& pinResultWithName() { return pinResultWithName_; }
  const auto& pinResultWithName() const { return pinResultWithName_; }

 private:
  // Helper functions to avoid including `global/RuntimeParameters.h` in this
  // header.
  static bool areWebSocketUpdatesEnabled();
  static std::chrono::milliseconds websocketUpdateInterval();
  const Index& _index;

  // When the `QueryExecutionContext` is constructed, get a stable read-only
  // snapshot of the current (located) delta triples. These can then be used
  // by the respective query without interfering with further incoming
  // update operations.
  SharedLocatedTriplesSnapshot sharedLocatedTriplesSnapshot_{
      _index.deltaTriplesManager().getCurrentSnapshot()};
  QueryResultCache* const _subtreeCache;
  // allocators are copied but hold shared state
  ad_utility::AllocatorWithLimit<Id> _allocator;
  QueryPlanningCostFactors _costFactors;
  SortPerformanceEstimator _sortPerformanceEstimator;
  std::function<void(std::string)> updateCallback_;

  // Cache the state of both runtime parameters to reduce the contention of the
  // mutex. `areWebsocketUpdatesEnabled_` is exposed so it can be disabled at a
  // later point in time.
 public:
  // Store the value of the `websocketUpdatesEnabled` runtime parameter. This
  // avoid synchronization overhead on each access and allows us to change the
  // value during query execution.
  bool areWebsocketUpdatesEnabled_ = areWebSocketUpdatesEnabled();

 private:
  // Store the value of the `websocketUpdateInterval` runtime parameter, for
  // the same reasons as above.
  std::chrono::milliseconds websocketUpdateInterval_ =
      websocketUpdateInterval();

  // The cache for named results.
  NamedResultCache* namedResultCache_ = nullptr;

  // Name (and optional variable for geometry index) under which the result of
  // the query that is executed using this context should be cached. When
  // `std::nullopt`, the result is not cached.
  std::optional<PinResultWithName> pinResultWithName_ = std::nullopt;

  // The last point in time when a websocket update was sent. This is used for
  // limiting the update frequency when `sendPriority` is `IfDue`.
  mutable std::chrono::steady_clock::time_point lastWebsocketUpdate_ =
      std::chrono::steady_clock::time_point::min();
};

#endif  // QLEVER_SRC_ENGINE_QUERYEXECUTIONCONTEXT_H
