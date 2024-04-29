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
 private:
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

  using MapType = ad_utility::Synchronized<
      absl::flat_hash_map<QueryId, WeakReferenceHolder>>;

  net::io_context& ioContext_;
  /// Guard to block destruction of the underlying io_context, to allow
  /// to gracefully destroy objects that might depend on the io_context.
  std::shared_ptr<MapType> socketDistributors_{std::make_shared<MapType>()};

 public:
  explicit QueryHub(net::io_context& ioContext) : ioContext_{ioContext} {}

  /// Create a new `QueryToSocketDistributor` or return a pre-existing one for
  /// the provided query id if there already is one. This can only ever be
  /// called once per query session, otherwise there will be an exception. There
  /// can only ever be one sender.
  std::shared_ptr<QueryToSocketDistributor>
  createOrAcquireDistributorForSending(const QueryId& queryId);

  /// Returns a const `QueryToSocketDistributor` that can only used to receive
  /// messages. In contrast to `createOrAcquireDistributorForSending` this can
  /// be called arbitrarily often during the lifetime of a single query session.
  std::shared_ptr<const QueryToSocketDistributor>
  createOrAcquireDistributorForReceiving(const QueryId& queryId);

 private:
  /// Implementation of `createOrAcquireDistributorForSending` and
  /// `createOrAcquireDistributorForReceiving`.
  template <bool isSender>
  std::shared_ptr<
      QueryHub::ConditionalConst<isSender, QueryToSocketDistributor>>
  createOrAcquireDistributorInternal(const QueryId& queryId);

  // If the `socketDistributors` are still valid, lock them as well as the
  // `mutex, and then delete the `queryId` from the `socketDistributor`s, but if
  // the corresponding distributor is either expired, or if `alwaysDelete` is
  // set to true.
  static void deleteFromDistributors(
      std::weak_ptr<QueryHub::MapType> socketDistributors,
      const QueryId& queryId, bool alwaysDelete);

  // Expose internal API for testing
  friend net::awaitable<void> QueryHub_verifyNoOpOnDestroyedQueryHub_impl(
      net::io_context&);
  friend void QueryHub_verifyNoOpOnDestroyedQueryHubAfterSchedule_impl(
      net::io_context&);
  friend void QueryHub_verifyNoErrorWhenQueryIdMissing_impl(net::io_context&);
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_QUERYHUB_H
