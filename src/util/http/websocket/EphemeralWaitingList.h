//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_EPHEMERALWAITINGLIST_H
#define QLEVER_EPHEMERALWAITINGLIST_H

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <unordered_map>

#include "util/http/websocket/Common.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
using websocket::common::QueryId;

/// Class that allows boost asio to wait for a certain query to start, with
/// support for cancellation which frees the memory properly.
/// `WebSocketManager` implicitly cancels this operation by composing awaitables
/// with websocket io operations with the `&&` operator, which cancels
/// the other awaitable if one of them is aborted.
/// Note: This class is not thread safe, so make sure to invoke it on the
/// same strand on each invocation.
class EphemeralWaitingList {
  std::atomic_uint64_t idCounter_{0};

  /// Typed wrapper for uint64_t
  class FunctionId {
    friend EphemeralWaitingList;

    uint64_t id_;
    explicit FunctionId(uint64_t id) : id_{id} {}

   public:
    // Starting with gcc 12 and clang 15 this can be constexpr
    bool operator==(const FunctionId&) const noexcept = default;
  };

  /// Helper struct to bundle a std::function with a unique id
  struct IdentifiableFunction {
    std::function<void()> func_;
    FunctionId id_;
  };

  /// Multimap that maps query ids to multiple callbacks of waiting websockets.
  std::unordered_multimap<QueryId, IdentifiableFunction, absl::Hash<QueryId>>
      waitingCallbacks_{};

  /// Helper function that removes just a single entry from the multimap
  void removeCallback(const QueryId& queryId,
                      const FunctionId& functionId) noexcept;

  /// The main logic that makes the awaitable "block" until signalQueryStart
  /// is called.
  template <typename CompletionToken>
  net::awaitable<void> registerCallbackAndWait(const QueryId& queryId,
                                               CompletionToken&& token);

 public:
  /// Signals the query has started and resumes execution of all awaitables
  /// waiting for the given query.
  void signalQueryStart(const QueryId& queryId);

  /// A wrapper for registerCallbackAndWait with hardcoded completion token.
  /// This allows the implementation to reside outside of the header file.
  net::awaitable<void> waitForQueryStart(const QueryId& queryId);
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_EPHEMERALWAITINGLIST_H
