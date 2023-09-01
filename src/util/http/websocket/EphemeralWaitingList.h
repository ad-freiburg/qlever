//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_EPHEMERALWAITINGLIST_H
#define QLEVER_EPHEMERALWAITINGLIST_H

#include <absl/container/flat_hash_map.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <unordered_map>

#include "util/http/websocket/Common.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
using websocket::common::QueryId;
class FunctionId {
  friend class EphemeralWaitingList;

  uint64_t id_;
  explicit FunctionId(uint64_t id) : id_{id} {}

 public:
  template <typename H>
  friend H AbslHashValue(H h, const FunctionId& fid) {
    return H::combine(std::move(h), fid.id_);
  }
  // Starting with gcc 12 and clang 15 this can be constexpr
  bool operator==(const FunctionId&) const noexcept = default;
};

class EphemeralWaitingList {
  std::atomic_uint64_t idCounter_{0};

  struct IdentifiableFunction {
    std::function<void()> func_;
    FunctionId id_;
  };

  std::unordered_multimap<QueryId, IdentifiableFunction, absl::Hash<QueryId>>
      waitingCallbacks_{};

  void removeCallback(const QueryId& queryId, const FunctionId& functionId);

  template <typename CompletionToken>
  net::awaitable<void> registerCallbackAndWait(const QueryId& queryId,
                                               const FunctionId& functionId,
                                               CompletionToken&& token);

 public:
  void signalQueryStart(const QueryId& queryId);
  net::awaitable<void> waitForQueryStart(const QueryId& queryId);
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_EPHEMERALWAITINGLIST_H
