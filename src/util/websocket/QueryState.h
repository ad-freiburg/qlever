//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once


#include <optional>
#include "./Common.h"

namespace ad_utility::query_state {

using websocket::common::RuntimeInformationSnapshot;
using websocket::common::QueryId;

void signalUpdateForQuery(QueryId queryId, RuntimeInformation runtimeInformation);
void clearQueryInfo(QueryId queryId);
std::optional<RuntimeInformationSnapshot> getIfUpdatedSince(QueryId queryId, websocket::common::TimeStamp timeStamp);

}  // namespace ad_utility::query_state
