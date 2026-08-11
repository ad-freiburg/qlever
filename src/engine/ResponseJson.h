// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_RESPONSEJSON_H
#define QLEVER_SRC_ENGINE_RESPONSEJSON_H

#include <optional>
#include <string>

#include "engine/NamedResultCache.h"
#include "engine/QueryExecutionContext.h"
#include "index/Index.h"
#include "util/ParseException.h"
#include "util/Timer.h"
#include "util/json.h"

// Compose the JSON response bodies for `Server`'s
// query-processing HTTP endpoints.
namespace responseJson {
using nlohmann::json;

// Compose the response for the `?cmd=stats` endpoint (and for the endpoints
// that set the index resp. text index description): name, git hash, and
// version of the index and server, as well as counts of predicates,
// subjects, objects, and triples.
json composeStats(const Index& index);

// Compose the response for the `?cmd=cache-stats` endpoint, as well as the
// `?cmd=clear-cache`, `?cmd=clear-cache-complete`, and
// `?cmd=clear-named-cache` endpoints (which return the cache stats after
// clearing): the number and total size of pinned and unpinned cache
// entries.
json composeCacheStats(const QueryResultCache& cache,
                       const NamedResultCache& namedResultCache);

// Compose the JSON error response sent to the client when processing a
// query or an update fails: the (truncated) operation string, the error
// message, the elapsed time, and (if available and not truncated) the
// location of the error inside the operation string.
json composeError(
    const std::string& query, const std::string& errorMsg,
    const ad_utility::Timer& requestTimer,
    const std::optional<ExceptionMetadata>& metadata = std::nullopt);

}  // namespace responseJson

#endif  // QLEVER_SRC_ENGINE_RESPONSEJSON_H
