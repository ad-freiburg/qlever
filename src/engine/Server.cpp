// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include "./Server.h"

#include <re2/re2.h>

#include <algorithm>
#include <cstring>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../parser/ParseException.h"
#include "../util/HttpServer/UrlParser.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"
#include "../util/json.h"
#include "QueryPlanner.h"

// _____________________________________________________________________________
void Server::initialize(const string& ontologyBaseName, bool useText,
                        bool usePatterns, bool usePatternTrick) {
  LOG(INFO) << "Initializing server..." << std::endl;

  _enablePatternTrick = usePatternTrick;
  _index.setUsePatterns(usePatterns);

  // Init the index.
  _index.createFromOnDiskIndex(ontologyBaseName);
  if (useText) {
    _index.addTextFromOnDiskIndex();
  }

  _sortPerformanceEstimator.computeEstimatesExpensively(
      _allocator,
      _index.getNofTriples() * PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);

  // Set flag.
  _initialized = true;
  LOG(INFO) << "Done initializing server." << std::endl;
}

// _____________________________________________________________________________
void Server::run() {
  if (!_initialized) {
    LOG(ERROR) << "Cannot start an uninitialized server!" << std::endl;
    exit(1);
  }

  auto httpSessionHandler =
      [this](auto request, auto&& send) -> boost::asio::awaitable<void> {
    co_await process(std::move(request), send);
  };
  auto httpServer = HttpServer{static_cast<unsigned short>(_port), "0.0.0.0",
                               std::move(httpSessionHandler)};

  // The first argument is the number of threads, the second one is the maximum
  // number of simultaneous TCP connections. Technically, these can be
  // different, but this is equal to the behavior of QLever's previous server.
  // TODO<joka921> Make this obsolete by implementing better concurrency
  // management in the actual query processing.
  httpServer.run(_numThreads, _numThreads);
}

// _____________________________________________________________________________
boost::asio::awaitable<void> Server::process(
    const ad_utility::httpUtils::HttpRequest auto& request, auto&& send) {
  using namespace ad_utility::httpUtils;
  ad_utility::Timer requestTimer;
  requestTimer.start();

  auto filenameAndParams = ad_utility::UrlParser::parseTarget(request.target());
  const auto& params = filenameAndParams._parameters;

  auto sendWithCors = [&send](auto response) -> boost::asio::awaitable<void> {
    response.set(http::field::access_control_allow_origin, "*");
    co_return co_await send(std::move(response));
  };

  // If there is a command like "clear_cache" which might be combined with a
  // query, track if there is such a command but no query and still send
  // a useful response.
  std::optional<http::response<http::string_body>> responseFromCommand;
  if (params.contains("cmd")) {
    const auto& cmd = params.at("cmd");
    if (cmd == "stats") {
      LOG(INFO) << "Supplying index stats..." << std::endl;
      auto response = createJsonResponse(composeStatsJson(), request);
      co_return co_await sendWithCors(std::move(response));
    } else if (cmd == "cache-stats") {
      LOG(INFO) << "Supplying cache stats..." << std::endl;
      auto response = createJsonResponse(composeCacheStatsJson(), request);
      co_return co_await sendWithCors(std::move(response));
    } else if (cmd == "clear-cache") {
      LOG(INFO) << "Clearing the cache, unpinned elements only" << std::endl;
      _cache.clearUnpinnedOnly();
      responseFromCommand =
          createJsonResponse(composeCacheStatsJson(), request);
    } else if (cmd == "clear-cache-complete") {
      LOG(INFO) << "Clearing the cache completely, including unpinned elements"
                << std::endl;
      _cache.clearAll();
      responseFromCommand =
          createJsonResponse(composeCacheStatsJson(), request);
    } else if (cmd == "get-settings") {
      LOG(INFO) << "Supplying settings..." << std::endl;
      json settingsJson = RuntimeParameters().toMap();
      co_await sendWithCors(createJsonResponse(settingsJson, request));
      co_return;
    } else {
      co_await sendWithCors(createBadRequestResponse(
          R"(Unknown value for query parameter "cmd": ")" + cmd + '\"',
          request));
      co_return;
    }
  }

  // TODO<joka921> Restrict this access by a token.
  // TODO<joka921> Warn about unknown parameters
  bool anyParamWasChanged = false;
  for (const auto& [key, value] : params) {
    if (RuntimeParameters().getKeys().contains(key)) {
      RuntimeParameters().set(key, value);
      anyParamWasChanged = true;
    }
  }

  if (anyParamWasChanged) {
    json settingsJson = RuntimeParameters().toMap();
    responseFromCommand = createJsonResponse(settingsJson, request);
  }

  if (params.contains("query")) {
    if (params.at("query").empty()) {
      co_return co_await sendWithCors(createBadRequestResponse(
          "Parameter \"query\" must not have an empty value", request));
    }

    co_return co_await processQuery(params, requestTimer, std::move(request),
                                    sendWithCors);
  } else if (responseFromCommand.has_value()) {
    co_return co_await sendWithCors(std::move(responseFromCommand.value()));
  }

  // Neither a query nor a command were specified, simply serve a file.
  // Note that `makeFileServer` returns a function.
  // The first argument is the document root, the second one is the whitelist.
  co_await makeFileServer(".", ad_utility::HashSet<std::string>{
                                   "index.html", "script.js",
                                   "style.css"})(std::move(request), send);
}

// _____________________________________________________________________________
json Server::composeResponseQleverJson(const ParsedQuery& query,
                                       const QueryExecutionTree& qet,
                                       ad_utility::Timer& requestTimer,
                                       size_t maxSend) {
  shared_ptr<const ResultTable> rt = qet.getResult();
  requestTimer.stop();
  off_t compResultUsecs = requestTimer.usecs();
  size_t resultSize = rt->size();

  nlohmann::json j;

  j["query"] = query._originalString;
  j["status"] = "OK";
  j["resultsize"] = resultSize;
  j["warnings"] = qet.collectWarnings();
  j["selected"] = query._selectClause._selectedVariables;

  j["runtimeInformation"] = RuntimeInformation::ordered_json(
      qet.getRootOperation()->getRuntimeInfo());

  {
    size_t limit = query._limit.value_or(MAX_NOF_ROWS_IN_RESULT);
    size_t offset = query._offset.value_or(0);
    requestTimer.cont();
    j["res"] =
        qet.writeResultAsQLeverJson(query._selectClause._selectedVariables,
                                    std::min(limit, maxSend), offset);
    requestTimer.stop();
  }

  requestTimer.stop();
  j["time"]["total"] =
      std::to_string(static_cast<double>(requestTimer.usecs()) / 1000.0) + "ms";
  j["time"]["computeResult"] =
      std::to_string(static_cast<double>(compResultUsecs) / 1000.0) + "ms";

  return j;
}

// _____________________________________________________________________________
json Server::composeResponseSparqlJson(const ParsedQuery& query,
                                       const QueryExecutionTree& qet,
                                       ad_utility::Timer& requestTimer,
                                       size_t maxSend) {
  shared_ptr<const ResultTable> rt = qet.getResult();
  requestTimer.stop();
  nlohmann::json j;
  size_t limit = query._limit.value_or(MAX_NOF_ROWS_IN_RESULT);
  size_t offset = query._offset.value_or(0);
  requestTimer.cont();
  j = qet.writeResultAsSparqlJson(query._selectClause._selectedVariables,
                                  std::min(limit, maxSend), offset);
  requestTimer.stop();
  return j;
}

// _____________________________________________________________________________
ad_utility::stream_generator::stream_generator Server::composeResponseSepValues(
    const ParsedQuery& query, const QueryExecutionTree& qet, char sep) {
  size_t limit = query._limit.value_or(MAX_NOF_ROWS_IN_RESULT);
  size_t offset = query._offset.value_or(0);
  return qet.generateResults(query._selectClause._selectedVariables, limit,
                             offset, sep);
}

// _____________________________________________________________________________
json Server::composeExceptionJson(const string& query, const std::exception& e,
                                  ad_utility::Timer& requestTimer) {
  requestTimer.stop();

  json j;
  j["query"] = query;
  j["status"] = "ERROR";
  j["resultsize"] = 0;
  j["time"]["total"] = requestTimer.msecs();
  j["time"]["computeResult"] = requestTimer.msecs();
  j["exception"] = e.what();
  return j;
}

// _____________________________________________________________________________
json Server::composeStatsJson() const {
  json result;
  result["kbindex"] = _index.getKbName();
  result["permutations"] = (_index.hasAllPermutations() ? 6 : 2);
  if (_index.hasAllPermutations()) {
    result["nofsubjects"] = _index.getNofSubjects();
    result["nofpredicates"] = _index.getNofPredicates();
    result["nofobjects"] = _index.getNofObjects();
  }

  auto [actualTriples, addedTriples] = _index.getNumTriplesActuallyAndAdded();
  result["noftriples"] = _index.getNofTriples();
  result["nofActualTriples"] = actualTriples;
  result["nofAddedTriples"] = addedTriples;
  result["textindex"] = _index.getTextName();
  result["nofrecords"] = _index.getNofTextRecords();
  result["nofwordpostings"] = _index.getNofWordPostings();
  result["nofentitypostings"] = _index.getNofEntityPostings();
  return result;
}

// _______________________________________
nlohmann::json Server::composeCacheStatsJson() const {
  nlohmann::json result;
  result["num-non-pinned-entries"] = _cache.numNonPinnedEntries();
  result["num-pinned-entries"] = _cache.numPinnedEntries();
  result["non-pinned-size"] = _cache.nonPinnedSize();
  result["pinned-size"] = _cache.pinnedSize();
  result["num-pinned-index-scan-sizes"] = _cache.pinnedSizes().rlock()->size();
  return result;
}

// ____________________________________________________________________________
boost::asio::awaitable<void> Server::processQuery(
    const ParamValueMap& params, ad_utility::Timer& requestTimer,
    const ad_utility::httpUtils::HttpRequest auto& request, auto&& send) {
  using namespace ad_utility::httpUtils;
  AD_CHECK(params.contains("query"));
  const auto& query = params.at("query");
  AD_CHECK(!query.empty());

  auto sendJson = [&request, &send](
                      const json& jsonString,
                      http::status status =
                          http::status::ok) -> boost::asio::awaitable<void> {
    auto response = createJsonResponse(jsonString, request, status);
    co_return co_await send(std::move(response));
  };

  std::optional<json> errorResponse;

  try {
    ad_utility::SharedConcurrentTimeoutTimer timeoutTimer =
        std::make_shared<ad_utility::ConcurrentTimeoutTimer>(
            ad_utility::TimeoutTimer::unlimited());
    if (params.contains("timeout")) {
      timeoutTimer = std::make_shared<ad_utility::ConcurrentTimeoutTimer>(
          ad_utility::TimeoutTimer::fromSeconds(
              std::stof(params.at("timeout"))));
    }

    auto containsParam = [&params](const std::string& param,
                                   const std::string& expected) {
      return params.contains(param) && params.at(param) == expected;
    };
    size_t maxSend = params.contains("send") ? std::stoul(params.at("send"))
                                             : MAX_NOF_ROWS_IN_RESULT;
    const bool pinSubtrees = containsParam("pinsubtrees", "true");
    const bool pinResult = containsParam("pinresult", "true");
    LOG(INFO) << "Query" << ((pinSubtrees) ? " (Cache pinned)" : "")
              << ((pinResult) ? " (Result pinned)" : "") << ": " << query
              << '\n';
    ParsedQuery pq = SparqlParser(query).parse();
    pq.expandPrefixes();

    QueryExecutionContext qec(_index, _engine, &_cache, _allocator,
                              _sortPerformanceEstimator, pinSubtrees,
                              pinResult);
    // start the shared timeout timer here to also include
    // the query planning
    timeoutTimer->wlock()->start();

    QueryPlanner qp(&qec);
    qp.setEnablePatternTrick(_enablePatternTrick);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    qet.isRoot() = true;  // allow pinning of the final result
    qet.recursivelySetTimeoutTimer(timeoutTimer);
    LOG(TRACE) << qet.asString() << std::endl;

    using ad_utility::MediaType;
    // Determine the result media type.

    // TODO<joka921> qleverJson should not be the default as soon
    // as the UI explicitly requests it.
    // TODO<joka921> Add sparqlJson as soon as it is supported.
    const auto supportedMediaTypes = []() {
      static const std::vector<MediaType> mediaTypes{
          ad_utility::MediaType::qleverJson, ad_utility::MediaType::sparqlJson,
          ad_utility::MediaType::tsv, ad_utility::MediaType::csv};
      return mediaTypes;
    };

    std::optional<MediaType> mediaType = std::nullopt;

    // The explicit `action=..._export` parameter have precedence over the
    // `Accept:...` header field
    if (containsParam("action", "csv_export")) {
      mediaType = ad_utility::MediaType::csv;
    } else if (containsParam("action", "tsv_export")) {
      mediaType = ad_utility::MediaType::tsv;
    } else if (containsParam("action", "qlever_json_export")) {
      mediaType = ad_utility::MediaType::qleverJson;
    } else if (containsParam("action", "sparql_json_export")) {
      mediaType = ad_utility::MediaType::sparqlJson;
    }

    std::string_view acceptHeader = request.base()[http::field::accept];

    if (!mediaType.has_value()) {
      mediaType = ad_utility::getMediaTypeFromAcceptHeader(
          acceptHeader, supportedMediaTypes());
    }

    if (!mediaType.has_value()) {
      co_return co_await send(createBadRequestResponse(
          "Did not find any supported media type "
          "in this \'Accept:\' header field: \"" +
              std::string{acceptHeader} + "\". " +
              ad_utility::getErrorMessageForSupportedMediaTypes(
                  supportedMediaTypes()),
          request));
    }

    AD_CHECK(mediaType.has_value());
    switch (mediaType.value()) {
      case ad_utility::MediaType::csv: {
        auto responseGenerator = composeResponseSepValues(pq, qet, ',');
        auto response = createOkResponse(std::move(responseGenerator), request,
                                         ad_utility::MediaType::csv);
        co_await send(std::move(response));
      } break;
      case ad_utility::MediaType::tsv: {
        auto responseGenerator = composeResponseSepValues(pq, qet, '\t');
        auto response = createOkResponse(std::move(responseGenerator), request,
                                         ad_utility::MediaType::tsv);
        co_await send(std::move(response));
      } break;
      case ad_utility::MediaType::qleverJson: {
        // Normal case: JSON response
        auto responseString =
            composeResponseQleverJson(pq, qet, requestTimer, maxSend);
        co_await sendJson(std::move(responseString));
      } break;
      case ad_utility::MediaType::sparqlJson: {
        auto responseString =
            composeResponseSparqlJson(pq, qet, requestTimer, maxSend);
        co_await sendJson(std::move(responseString));
      }
      default:
        // This should never happen, because we have carefully restricted the
        // subset of mediaTypes that can occur here.
        AD_CHECK(false);
    }
    // Print the runtime info. This needs to be done after the query
    // was computed.
    LOG(INFO) << '\n' << qet.getRootOperation()->getRuntimeInfo().toString();
  } catch (const ad_semsearch::Exception& e) {
    errorResponse = composeExceptionJson(query, e, requestTimer);
  } catch (const std::exception& e) {
    errorResponse = composeExceptionJson(query, e, requestTimer);
  }
  if (errorResponse.has_value()) {
    co_return co_await sendJson(errorResponse.value(),
                                http::status::bad_request);
  }
}
