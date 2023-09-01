//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_QUERYTOSOCKETDISTRIBUTOR_H
#define QLEVER_QUERYTOSOCKETDISTRIBUTOR_H

#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

#include "util/http/websocket/Common.h"

namespace ad_utility::websocket {
using websocket::common::QueryId;
namespace net = boost::asio;
class QueryToSocketDistributor {
  QueryId queryId_;
  net::strand<net::io_context::executor_type> strand_;
  std::vector<std::function<void()>> wakeupCalls_{};
  std::vector<std::shared_ptr<const std::string>> data_{};
  std::atomic_bool finished_ = false;

  /// Wakes up all websockets that are currently "blocked" and waiting for an
  /// update of the given query. After being woken up they will check for
  /// updates and resume execution.
  void runAndEraseWakeUpCallsSynchronously();

 public:
  QueryToSocketDistributor(QueryId queryId, net::io_context& ioContext)
      : queryId_{std::move(queryId)}, strand_{net::make_strand(ioContext)} {}

  /// Calls runAndEraseWakeUpCallsSynchronously asynchronously and thread-safe.
  void wakeUpWebSocketsForQuery();

  void callOnUpdate(std::function<void()>);

  void addQueryStatusUpdate(std::string payload);

  void signalEnd();

  bool isFinished() const;

  const std::vector<std::shared_ptr<const std::string>>& getData();
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_QUERYTOSOCKETDISTRIBUTOR_H
