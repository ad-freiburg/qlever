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
  // The number of the pull request that changed the index format most recently.
  uint64_t prNumber_;
  // The date of the last breaking change of the index format.
  Date date_{1900, 1, 1};

  // Conversion To JSON.
  friend void to_json(nlohmann::json& j, const IndexVersion& version) {
    j["date"] = version.date_.toStringAndType().first;
    j["date-bits"] = version.date_.toBits();
    j["pull-request-number"] = version.prNumber_;
  }

  // Conversion from JSON.
  friend void from_json(const nlohmann::json& j, IndexVersion& version) {
    version.prNumber_ = static_cast<uint64_t>(j["pull-request-number"]);
    version.date_ = Date::fromBits(static_cast<uint64_t>(j["date-bits"]));
  }
  bool operator==(const IndexVersion&) const = default;
};

// The actual index version. Change it once the binary format of the index
// changes.
inline const IndexVersion& indexVersion{1004, Date{2023, 6, 16}};

}  // namespace qlever
