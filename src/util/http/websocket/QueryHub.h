//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_QUERYHUB_H
#define QLEVER_QUERYHUB_H

#include <absl/container/flat_hash_map.h>

#include "util/http/beast.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/QueryToSocketDistributor.h"

namespace ad_utility::websocket {
using StrandType = net::strand<net::any_io_executor>;

/// Class that provides the functionality to create and/or acquire a
/// `QueryToSocketDistributor`. All operations are synchronized on an exclusive
/// strand for each instance. In the common case of this class being used
/// globally the provided thread-safety comes at a cost. So ideally this is only
/// used once and from then onwards only the `QueryToSocketDistributor` instance
/// is used.
class QueryHub {
  uint64_t counter = 0;
  struct IdentifiablePointer {
    std::weak_ptr<QueryToSocketDistributor> pointer_;
    uint64_t id_;
  };

  net::io_context& ioContext_;
  StrandType globalStrand_;
  absl::flat_hash_map<QueryId, IdentifiablePointer> socketDistributors_{};

  /// Implementation of createOrAcquireDistributor
  net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
      createOrAcquireDistributorInternal(QueryId);

 public:
  explicit QueryHub(net::io_context& ioContext)
      : ioContext_{ioContext}, globalStrand_{net::make_strand(ioContext)} {}

  /// Creates a new `QueryToSocketDistributor` or returns the pre-existing
  /// for the provided query id if there already is one.
  net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
      createOrAcquireDistributor(QueryId);
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_QUERYHUB_H
