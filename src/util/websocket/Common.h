//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <chrono>
#include <cstdint>
#include <random>

#include "../../engine/RuntimeInformation.h"

namespace ad_utility::websocket::common {
class QueryId {
  std::string id_;
  explicit QueryId(std::string id) : id_{std::move(id)} {}

 public:
  static std::optional<QueryId> uniqueIdFromString(std::string id) {
    // TODO implement system ensuring unique ids
    //  where something keeps track of active queries
    return QueryId{std::move(id)};
  }
  static QueryId idFromString(std::string id) {
    return QueryId{std::move(id)};
  }

  static QueryId uniqueId() {
    static std::mt19937 generator(std::random_device{}());
    static std::uniform_int_distribution<uint64_t> distrib{};
    return QueryId{std::to_string(distrib(generator))};
  }

  // TODO analyze why these can't be constexpr
  bool operator==(const QueryId&) const noexcept = default;
  bool operator!=(const QueryId&) const noexcept = default;
  // required for multimap
  auto operator<=>(const QueryId&) const noexcept = default;

  friend std::hash<QueryId>;
};

using TimeStamp = std::chrono::time_point<std::chrono::steady_clock>;

struct TimedClientPayload {
  std::string payload;
  TimeStamp updateMoment;
};
}  // namespace ad_utility::websocket::common

template <>
struct std::hash<ad_utility::websocket::common::QueryId> {
  auto operator()(
      const ad_utility::websocket::common::QueryId& queryId) const noexcept {
    return std::hash<std::string>{}(queryId.id_);
  }
};
