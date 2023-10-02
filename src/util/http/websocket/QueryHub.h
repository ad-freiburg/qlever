//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_QUERYHUB_H
#define QLEVER_QUERYHUB_H

#include <absl/container/flat_hash_map.h>

#include "util/http/beast.h"
#include "util/http/websocket/QueryId.h"
#include "util/http/websocket/QueryToSocketDistributor.h"

namespace ad_utility::websocket {

/// Class that provides the functionality to create and/or acquire a
/// `QueryToSocketDistributor`. All operations are synchronized on an exclusive
/// strand for each instance. In the common case of this class being used
/// globally the provided thread-safety comes at a cost. So ideally this is only
/// used once and from then onwards only the `QueryToSocketDistributor` instance
/// is used.
class QueryHub {
  /// Helper type to make type T const if B is false
  template <bool B, typename T>
  using ConditionalConst = std::conditional_t<B, T, const T>;

  /// Stores a weak pointer and tracks if it was handed out for sending.
  struct WeakReferenceHolder {
    std::weak_ptr<QueryToSocketDistributor> pointer_;
    bool started_ = false;

    WeakReferenceHolder(std::weak_ptr<QueryToSocketDistributor> pointer,
                        bool started)
        : pointer_{std::move(pointer)}, started_{started} {}
  };

  net::io_context& ioContext_;
  /// Thread pool with single thread for synchronization
  net::strand<net::any_io_executor> globalStrand_;
  absl::flat_hash_map<QueryId, WeakReferenceHolder> socketDistributors_{};

  /// Implementation of createOrAcquireDistributor
  template <bool isSender>
  net::awaitable<
      std::shared_ptr<ConditionalConst<isSender, QueryToSocketDistributor>>>
      createOrAcquireDistributorInternal(QueryId);

 public:
  explicit QueryHub(net::io_context& ioContext)
      : ioContext_{ioContext}, globalStrand_{net::make_strand(ioContext)} {}

  /// Creates a new `QueryToSocketDistributor` or returns the pre-existing
  /// for the provided query id if there already is one. The bool parameter
  /// should be set to true if the caller of this methods intends to use
  /// the distributor object to send data, rather than just receiving it.
  net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
      createOrAcquireDistributorForSending(QueryId);

  /// Similar to `createOrAcquireDistributorForSending` but returns a const
  /// `QueryToSocketDistributor` that can only used for receiving instead.
  net::awaitable<std::shared_ptr<const QueryToSocketDistributor>>
      createOrAcquireDistributorForReceiving(QueryId);
  /// Expose strand for testing
  auto getStrand() const { return globalStrand_; }
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_QUERYHUB_H
