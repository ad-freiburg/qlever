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
  /// Strand for synchronization
  net::strand<net::any_io_executor> globalStrand_;
  absl::flat_hash_map<QueryId, WeakReferenceHolder> socketDistributors_{};

  // Expose internal API for testing
  friend net::awaitable<void>
  QueryHub_testCorrectReschedulingForEmptyPointerOnDestruct_coroutine(
      net::io_context&);

  /// Implementation of createOrAcquireDistributorForSending and
  /// createOrAcquireDistributorForReceiving, without thread safety,
  /// exposed for testing
  template <bool isSender>
  net::awaitable<
      std::shared_ptr<ConditionalConst<isSender, QueryToSocketDistributor>>>
      createOrAcquireDistributorInternalUnsafe(QueryId);

  /// createOrAcquireDistributorInternalUnsafe, but dispatched on global strand
  template <bool isSender>
  net::awaitable<
      std::shared_ptr<ConditionalConst<isSender, QueryToSocketDistributor>>>
      createOrAcquireDistributorInternal(QueryId);

 public:
  explicit QueryHub(net::io_context& ioContext)
      : ioContext_{ioContext}, globalStrand_{net::make_strand(ioContext)} {}

  /// Create a new `QueryToSocketDistributor` or return a pre-existing one for
  /// the provided query id if there already is one. This can only ever be
  /// called once per query session, otherwise there will be an exception. There
  /// can only ever be one sender.
  net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
      createOrAcquireDistributorForSending(QueryId);

  /// Returns a const `QueryToSocketDistributor` that can only used to receive
  /// messages. In contrast to `createOrAcquireDistributorForSending` this can
  /// be called arbitrarily often during the lifetime of a single query session.
  net::awaitable<std::shared_ptr<const QueryToSocketDistributor>>
      createOrAcquireDistributorForReceiving(QueryId);
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_QUERYHUB_H
