//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/QueryExecutionContext.h"

#include "global/RuntimeParameters.h"

bool QueryExecutionContext::areWebSocketUpdatesEnabled() {
  return RuntimeParameters().get<"websocket-updates-enabled">();
}
