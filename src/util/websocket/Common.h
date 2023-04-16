//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <chrono>
#include <cstdint>
#include "../../engine/RuntimeInformation.h"

namespace ad_utility::websocket::common {
// TODO make type with strict type safety?
typedef uint32_t QueryId;

using TimeStamp = std::chrono::time_point<std::chrono::steady_clock>;

struct RuntimeInformationSnapshot {
  RuntimeInformation runtimeInformation;
  TimeStamp updateMoment;
};
}
