//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_QUERYTOSOCKETDISTRIBUTOR_H
#define QLEVER_QUERYTOSOCKETDISTRIBUTOR_H

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <functional>

#include "util/UniqueCleanup.h"
#include "util/http/websocket/Common.h"

namespace ad_utility::websocket {
using websocket::common::QueryId;
namespace net = boost::asio;

/// Class that temporarily holds live information of a single query to allow
/// the individual websockets to query it, and await status updates.
/// It also provides its own strand so operations on this class do not need
/// to be synchronized globally. The public API is thread-safe, but you
/// will end up on a different executor when awaiting it, so make sure
/// to use a wrapper like `sameExecutor()` to stay on your executor!
class QueryToSocketDistributor {
  struct IdentifiableFunction {
    std::function<void()> function_;
    uint64_t id_;
  };
  /// Strand to synchronize all operations on this class
  net::strand<net::any_io_executor> strand_;
  /// Vector to store all callbacks of websockets waiting for an update.
  std::shared_ptr<std::vector<IdentifiableFunction>> wakeupCalls_ =
      std::make_unique<std::vector<IdentifiableFunction>>();
  /// Vector that stores the actual data, so all websockets can read it at
  /// their own pace.
  std::vector<std::shared_ptr<const std::string>> data_{};
  /// Flag to indicate if a query ended and won't receive any more updates.
  bool finished_ = false;

  unique_cleanup::UniqueCleanup<std::function<void()>> cleanupCall_;

  uint64_t counter = 0;

  /// Wakes up all websockets that are currently "blocked" and waiting for an
  /// update of the given query. After being woken up they will check for
  /// updates and resume execution.
  void runAndEraseWakeUpCallsSynchronously();

  /// Function that can be used to "block" in boost asio until there's a new
  /// update to the data.
  template <typename CompletionToken>
  auto waitForUpdate(CompletionToken&& token);

 public:
  /// Constructor that builds a new strand from the provided io context.
  explicit QueryToSocketDistributor(net::io_context& ioContext,
                                    std::function<void()> cleanupCall)
      : strand_{net::make_strand(ioContext)},
        cleanupCall_{std::move(cleanupCall),
                     [](auto&& cleanupCall) { std::invoke(cleanupCall); }} {}

  /// Appends specified data to the vector and signals all waiting websockets
  /// that new data is available
  net::awaitable<void> addQueryStatusUpdate(std::string payload);

  /// Sets the signal that no new updates will be pushed. This causes any
  /// subsequent calls to waitForUpdate to return immediately if all data
  /// has already been consumed
  net::awaitable<void> signalEnd();

  /// Awaitable object to wait for and fetch the next available piece of data
  /// for the websocket. co_returns a nullptr if no more data is available.
  net::awaitable<std::shared_ptr<const std::string>> waitForNextDataPiece(
      size_t index);
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_QUERYTOSOCKETDISTRIBUTOR_H
