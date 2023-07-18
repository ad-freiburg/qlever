//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <cstdint>

#include "util/Date.h"
#include "util/json.h"

namespace qlever {
// Return information about the last breaking change in QLever's index
// structure. If the contents in this struct differ between a built index and a
// server executable, then the index has to be rebuilt.
struct IndexVersion {
  uint64_t prNumber_;
  Date date_{1900, 1, 1};
  friend void to_json(nlohmann::json& j, const IndexVersion& version) {
    j["date"] = version.date_.toStringAndType().first;
    j["date-bits"] = version.date_.toBits();
    j["pull-request-number"] = version.prNumber_;
  }

  friend void from_json(const nlohmann::json& j, IndexVersion& version) {
    version.prNumber_ = static_cast<uint64_t>(j["pull-request-number"]);
    version.date_ = Date::fromBits(static_cast<uint64_t>(j["date-bits"]));
  }
  bool operator==(const IndexVersion&) const = default;
};

inline const IndexVersion& indexVersion() {
  static IndexVersion version{1004, Date{2023, 6, 16}};
  return version;
}

}  // namespace qlever
