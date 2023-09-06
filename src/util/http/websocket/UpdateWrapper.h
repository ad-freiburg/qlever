//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_UPDATEWRAPPER_H
#define QLEVER_UPDATEWRAPPER_H

#include <boost/asio/co_spawn.hpp>

#include "util/UniqueCleanup.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/QueryHub.h"
#include "util/http/websocket/QueryToSocketDistributor.h"

namespace ad_utility::websocket {
using unique_cleanup::UniqueCleanup;

/// This class provides a convenient wrapper that extracts the proper
/// `QueryToSocketDistributor` of the passed `QueryHub` reference
/// and provides a generic operator() interface to call addQueryStatusUpdate()
/// from the non-asio world.
class UpdateWrapper {
  UniqueCleanup<std::shared_ptr<QueryToSocketDistributor>> distributor_;
  net::any_io_executor executor_;

  UpdateWrapper(std::shared_ptr<QueryToSocketDistributor> distributor,
                net::any_io_executor executor)
      : distributor_{std::move(distributor),
                     [executor](auto&& distributor) {
                       auto coroutine =
                           [](auto distributor) -> net::awaitable<void> {
                         co_await distributor->signalEnd();
                       };
                       net::co_spawn(
                           executor,
                           coroutine(std::forward<decltype(distributor)>(
                               distributor)),
                           net::detached);
                     }},
        executor_{std::move(executor)} {}

 public:
  UpdateWrapper(UpdateWrapper&&) noexcept = default;

  /// Asynchronously creates an instance of this class. This is because because
  /// creating a distributor for this class needs to be done in a synchronized
  /// way.
  static net::awaitable<UpdateWrapper> create(common::QueryId queryId,
                                              QueryHub& queryHub);

  /// Broadcast the string to all listeners of this query asynchronously.
  void operator()(std::string) const;
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_UPDATEWRAPPER_H
