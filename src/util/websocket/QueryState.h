//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <optional>

#include "util/websocket/Common.h"

namespace ad_utility::query_state {

using websocket::common::QueryId;
using websocket::common::SharedPayloadAndTimestamp;

void signalUpdateForQuery(const QueryId& queryId,
                          const RuntimeInformation& runtimeInformation);
void clearQueryInfo(const QueryId& queryId);
SharedPayloadAndTimestamp getIfUpdatedSince(
    const QueryId& queryId, websocket::common::Timestamp timeStamp);

}  // namespace ad_utility::query_state
