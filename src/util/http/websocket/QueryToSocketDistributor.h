//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_QUERYTOSOCKETDISTRIBUTOR_H
#define QLEVER_QUERYTOSOCKETDISTRIBUTOR_H

#include <memory>
#include <vector>

#include "backports/functional.h"
#include "util/UniqueCleanup.h"
#include "util/http/beast.h"

namespace ad_utility::websocket {
namespace net = boost::asio;

/// Class that temporarily holds live information of a single query to allow
/// the individual websockets to query it, and await status updates.
/// Note: all `awaitable` functions (in particular `waitForNextDataPiece`) must
/// be awaited on the strand that is exposed via the `strand()` member function.
/// Otherwise, an exception is thrown because the thread-safety would be
/// violated and because there is no good way in Boost::ASIO to change the
/// executor of a coroutine stack.
class QueryToSocketDistributor
    : public std::enable_shared_from_this<QueryToSocketDistributor> {
  /// Strand to synchronize all operations on this class
  net::strand<net::any_io_executor> strand_;
  mutable net::steady_timer infiniteTimer_;
  /// Vector that stores the actual data, so all websockets can read it at
  /// their own pace.
  std::vector<std::shared_ptr<const std::string>> data_{};
  /// Flag to indicate if a query ended and won't receive any more updates.
  std::atomic_flag finished_ = false;

  /// Function to remove this distributor from the `QueryHub` when it is
  /// destructed.
  unique_cleanup::UniqueCleanup<std::function<void(bool)>> cleanupCall_;
  std::function<void()> signalEndCall_;

  /// Wakes up all websockets that are currently "blocked" and waiting for an
  /// update of the given query. After being woken up they will check for
  /// updates and resume execution.
  void wakeUpWaitingListeners();

  /// Function that can be used to "block" in boost asio until there's a new
  /// update to the data.
  net::awaitable<void> waitForUpdate() const;

 public:
  /// Constructor that builds a new strand from the provided io context and
  /// `cleanupCall`. The cleanup call is invoked with `true` as an argument when
  /// `signalEnd()` is called and with `false` In the destructor if there where
  /// no explicit calls to `signalEnd()` before.
  explicit QueryToSocketDistributor(
      net::io_context& ioContext, const std::function<void(bool)>& cleanupCall);

  /// Appends specified data to the vector and signals all waiting websockets
  /// that new data is available
  void addQueryStatusUpdate(std::string payload);

  /// Sets the signal that no new updates will be pushed. This causes any
  /// subsequent calls to waitForUpdate to return immediately if all data
  /// has already been consumed
  void signalEnd();

  /// Expose the strand that is used to synchronize access to this class. All
  /// calls to `waitForNextDataPiece` must be awaited on the returned `strand`.
  auto strand() const { return strand_; }

  /// Awaitable object to wait for and fetch the next available piece of data
  /// for the websocket. co_returns a nullptr if no more data is available.
  /// Must be awaited on the strand returned by `strand()`, else an exception is
  /// thrown.
  net::awaitable<std::shared_ptr<const std::string>> waitForNextDataPiece(
      size_t index) const;
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_QUERYTOSOCKETDISTRIBUTOR_H
