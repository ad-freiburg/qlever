//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "util/json.h"

namespace qlever {
// Return information about the last breaking change in QLever's index
// structure. If the contents in this struct differ between a built index and a
// server executable, then the index has to be rebuilt.
inline const nlohmann::json& indexVersion() {
  static nlohmann::json version = []() {
    nlohmann::json j;
    j["date"] = "2023-06-16";
    j["pull-request-number"] = 1004;
    return j;
  }();
  return version;
}

}  // namespace qlever
