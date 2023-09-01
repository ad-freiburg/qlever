//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_EPHEMERALWAITINGLIST_H
#define QLEVER_EPHEMERALWAITINGLIST_H

#include <absl/container/flat_hash_map.h>

#include <unordered_map>

#include "util/http/websocket/Common.h"

namespace ad_utility::websocket {

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
  absl::flat_hash_map<FunctionId, QueryId> functionIdToQueryId_{};

 public:
  void signalQueryUpdate(const QueryId& queryId);
  FunctionId callOnQueryUpdate(const QueryId& queryId, std::function<void()>);
  void removeCallback(const FunctionId&);
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_EPHEMERALWAITINGLIST_H
