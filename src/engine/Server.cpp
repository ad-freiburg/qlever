// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include "./Server.h"

#include <re2/re2.h>

#include <algorithm>
#include <cstring>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../parser/ParseException.h"
#include "../util/HttpServer/UrlParser.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"
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
  if (params.contains("cmd")) {
    const auto& cmd = params.at("cmd");
    if (cmd == "stats") {
      LOG(INFO) << "Supplying index stats..." << std::endl;
      auto response = createJsonResponse(composeStatsJson(), request);
      co_return co_await send(std::move(response));
    } else if (cmd == "cachestats") {
      LOG(INFO) << "Supplying cache stats..." << std::endl;
      auto response =
          createJsonResponse(composeCacheStatsJson().dump(), request);
      co_return co_await send(std::move(response));
    } else if (cmd == "clearcache") {
      _cache.clearUnpinnedOnly();
    } else if (cmd == "clearcachecomplete") {
      // The _pinnedSizes are not part of the (otherwise threadsafe) _cache
      // and thus have to be manually locked.
      // TODO<joka921> make _pinnedSizes part of the cache, or eliminate them
      // entirely.
      auto lock = _pinnedSizes.wlock();
      _cache.clearAll();
      lock->clear();
    }
    co_return;
  }

  if (params.contains("query")) {
    if (params.at("query").empty()) {
      co_return co_await send(createBadRequestResponse(
          "Parameter \"query\" must not have an empty value", request));
    }

    co_return co_await processQuery(params, requestTimer, std::move(request),
                                    send);
  }

  // Neither a query nor a command were specified, simply serve a file.
  // Note that `makeFileServer` returns a function.
  // The first argument is the document root, the second one is the whitelist.
  co_await makeFileServer(".", ad_utility::HashSet<std::string>{
                                   "index.html", "script.js",
                                   "style.css"})(std::move(request), send);
}

// _____________________________________________________________________________
string Server::composeResponseJson(const ParsedQuery& query,
                                   const QueryExecutionTree& qet,
                                   ad_utility::Timer& requestTimer,
                                   size_t maxSend) const {
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
    size_t limit = MAX_NOF_ROWS_IN_RESULT;
    size_t offset = 0;
    if (query._limit.size() > 0) {
      limit = static_cast<size_t>(atol(query._limit.c_str()));
    }
    if (query._offset.size() > 0) {
      offset = static_cast<size_t>(atol(query._offset.c_str()));
    }
    requestTimer.cont();
    j["res"] = qet.writeResultAsJson(query._selectClause._selectedVariables,
                                     std::min(limit, maxSend), offset);
    requestTimer.stop();
  }

  requestTimer.stop();
  j["time"]["total"] = std::to_string(requestTimer.usecs() / 1000.0) + "ms";
  j["time"]["computeResult"] = std::to_string(compResultUsecs / 1000.0) + "ms";

  return j.dump(4);
}

// _____________________________________________________________________________
string Server::composeResponseSepValues(const ParsedQuery& query,
                                        const QueryExecutionTree& qet,
                                        char sep) const {
  std::ostringstream os;
  size_t limit = std::numeric_limits<size_t>::max();
  size_t offset = 0;
  if (query._limit.size() > 0) {
    limit = static_cast<size_t>(atol(query._limit.c_str()));
  }
  if (query._offset.size() > 0) {
    offset = static_cast<size_t>(atol(query._offset.c_str()));
  }
  qet.writeResultToStream(os, query._selectClause._selectedVariables, limit,
                          offset, sep);

  return os.str();
}

// _____________________________________________________________________________
string Server::composeResponseJson(const string& query,
                                   const std::exception& exception,
                                   ad_utility::Timer& requestTimer) const {
  std::ostringstream os;
  requestTimer.stop();

  json j;
  j["query"] = query;
  j["status"] = "ERROR";
  j["resultsize"] = 0;
  j["time"]["total"] = requestTimer.msecs();
  j["time"]["computeResult"] = requestTimer.msecs();
  j["exception"] = exception.what();
  return j.dump(4);
}

// _____________________________________________________________________________
string Server::composeStatsJson() const {
  std::ostringstream os;
  os << "{\n"
     << "\"kbindex\": \"" << _index.getKbName() << "\",\n"
     << "\"permutations\": \"" << (_index.hasAllPermutations() ? "6" : "2")
     << "\",\n";
  if (_index.hasAllPermutations()) {
    os << "\"nofsubjects\": \"" << _index.getNofSubjects() << "\",\n";
    os << "\"nofpredicates\": \"" << _index.getNofPredicates() << "\",\n";
    os << "\"nofobjects\": \"" << _index.getNofObjects() << "\",\n";
  }

  auto [actualTriples, addedTriples] = _index.getNumTriplesActuallyAndAdded();
  os << "\"noftriples\": \"" << _index.getNofTriples() << "\",\n"
     << "\"nofActualTriples\": \"" << actualTriples << "\",\n"
     << "\"nofAddedTriples\": \"" << addedTriples << "\",\n"
     << "\"textindex\": \"" << _index.getTextName() << "\",\n"
     << "\"nofrecords\": \"" << _index.getNofTextRecords() << "\",\n"
     << "\"nofwordpostings\": \"" << _index.getNofWordPostings() << "\",\n"
     << "\"nofentitypostings\": \"" << _index.getNofEntityPostings() << "\"\n"
     << "}\n";
  return os.str();
}

// _______________________________________
nlohmann::json Server::composeCacheStatsJson() const {
  nlohmann::json result;
  result["num-non-pinned-entries"] = _cache.numNonPinnedEntries();
  result["num-pinned-entries"] = _cache.numPinnedEntries();
  result["non-pinned-size"] = _cache.nonPinnedSize();
  result["pinned-size"] = _cache.pinnedSize();
  result["num-pinned-index-scan-sizes"] = _pinnedSizes.rlock()->size();
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

  auto sendJson =
      [&request,
       &send](std::string&& jsonString) -> boost::asio::awaitable<void> {
    auto response = createJsonResponse(std::move(jsonString), request);
    co_return co_await send(std::move(response));
  };

  std::optional<std::string> errorResponse;

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

    QueryExecutionContext qec(_index, _engine, &_cache, &_pinnedSizes,
                              _allocator, _sortPerformanceEstimator,
                              pinSubtrees, pinResult);
    // start the shared timeout timer here to also include
    // the query planning
    timeoutTimer->wlock()->start();

    QueryPlanner qp(&qec);
    qp.setEnablePatternTrick(_enablePatternTrick);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    qet.isRoot() = true;  // allow pinning of the final result
    qet.recursivelySetTimeoutTimer(timeoutTimer);
    LOG(TRACE) << qet.asString() << std::endl;

    if (containsParam("action", "csv_export")) {
      // CSV export
      auto responseString = composeResponseSepValues(pq, qet, ',');
      auto response = createOkResponse(std::move(responseString), request,
                                       ad_utility::MediaType::csv);
      co_await send(std::move(response));
    } else if (containsParam("action", "tsv_export")) {
      // TSV export
      auto responseString = composeResponseSepValues(pq, qet, '\t');
      auto response = createOkResponse(std::move(responseString), request,
                                       ad_utility::MediaType::tsv);
      co_await send(std::move(response));
    } else {
      // Normal case: JSON response
      auto responseString = composeResponseJson(pq, qet, requestTimer, maxSend);
      co_await sendJson(std::move(responseString));
    }
    // Print the runtime info. This needs to be done after the query
    // was computed.
    LOG(INFO) << '\n' << qet.getRootOperation()->getRuntimeInfo().toString();
  } catch (const ad_semsearch::Exception& e) {
    errorResponse = composeResponseJson(query, e, requestTimer);
  } catch (const std::exception& e) {
    errorResponse = composeResponseJson(query, e, requestTimer);
  }
  if (errorResponse.has_value()) {
    co_return co_await sendJson(std::move(errorResponse.value()));
  }
}
