// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include "./Server.h"

#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "QueryPlanner.h"

template <typename T>
using Awaitable = Server::Awaitable<T>;

// __________________________________________________________________________
void Server::initialize(const string& indexBaseName, bool useText,
                        bool usePatterns, bool usePatternTrick,
                        bool loadAllPermutations) {
  LOG(INFO) << "Initializing server ..." << std::endl;

  _enablePatternTrick = usePatternTrick;
  _index.setUsePatterns(usePatterns);
  _index.setLoadAllPermutations(loadAllPermutations);

  // Init the index.
  _index.createFromOnDiskIndex(indexBaseName);
  if (useText) {
    _index.addTextFromOnDiskIndex();
  }

  _sortPerformanceEstimator.computeEstimatesExpensively(
      _allocator,
      _index.getNofTriples() * PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);

  // Set flag.
  _initialized = true;
  LOG(INFO) << "Access token for restricted API calls is \"" << accessToken_
            << "\"" << std::endl;
  LOG(INFO) << "The server is ready, listening for requests on port "
            << std::to_string(_port) << " ..." << std::endl;
}

// _____________________________________________________________________________
void Server::run(const string& indexBaseName, bool useText, bool usePatterns,
                 bool usePatternTrick, bool loadAllPermutations) {
  using namespace ad_utility::httpUtils;

  // Function that handles a request asynchronously, will be passed as argument
  // to `HttpServer` below.
  auto httpSessionHandler =
      [this](auto request, auto&& send) -> boost::asio::awaitable<void> {
    // Version of send with maximally permissive CORS header (that allows the
    // client that receives the response to do with it what it wants).
    auto sendWithCors = [&send](auto response) -> boost::asio::awaitable<void> {
      response.set(http::field::access_control_allow_origin, "*");
      co_return co_await send(std::move(response));
    };
    // Process the request using the `process` method and if it throws an
    // exception, log the error message and send a HTTP/1.1 404 Bad Request
    // response with that message. Note that the C++ standard forbids co_await
    // in the catch block, hence the workaround with the `exceptionErrorMsg`.
    std::optional<std::string> exceptionErrorMsg;
    std::optional<http::response<http::string_body>> badRequestResponse;
    try {
      co_await process(std::move(request), sendWithCors);
    } catch (const std::exception& e) {
      exceptionErrorMsg = e.what();
    }
    if (exceptionErrorMsg) {
      LOG(ERROR) << exceptionErrorMsg.value() << std::endl;
      auto badRequestResponse = createBadRequestResponse(
          absl::StrCat(exceptionErrorMsg.value(), "\n"), request);
      co_await sendWithCors(std::move(badRequestResponse));
    }
  };

  // First set up the HTTP server, so that it binds to the socket, and
  // the "socket already in use" error appears quickly.
  auto httpServer = HttpServer{static_cast<unsigned short>(_port), "0.0.0.0",
                               _numThreads, std::move(httpSessionHandler)};

  // Initialize the index
  initialize(indexBaseName, useText, usePatterns, usePatternTrick,
             loadAllPermutations);

  // Start listening for connections on the server.
  httpServer.run();
}

// _____________________________________________________________________________
ad_utility::UrlParser::UrlPathAndParameters Server::getUrlPathAndParameters(
    const ad_utility::httpUtils::HttpRequest auto& request) {
  if (request.method() == http::verb::get) {
    // For a GET request, `request.target()` yields the part after the domain,
    // which is a concatenation of the path and the query string (the query
    // string starting with "?").
    return ad_utility::UrlParser::parseGetRequestTarget(request.target());
  }
  if (request.method() == http::verb::post) {
    // For a POST request, the content type *must* be either
    // "application/x-www-form-urlencoded" or "application/sparql-query". In
    // the first case, the body of the POST request contains a URL-encoded
    // query (just like in the part of a GET request after the "?"). In the
    // second case, the body of the POST request contains *only* the SPARQL
    // query, but not URL-encoded, and no other URL parameters. See Sections
    // 2.1.2 and 2.1.3 of the SPARQL 1.1 standard:
    // https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321
    std::string_view contentType = request.base()[http::field::content_type];
    LOG(DEBUG) << "Content-type: \"" << contentType << "\"" << std::endl;
    static constexpr std::string_view contentTypeUrlEncoded =
        "application/x-www-form-urlencoded";
    static constexpr std::string_view contentTypeSparqlQuery =
        "application/sparql-query";

    // In either of the two cases explained above, we convert the data to a
    // format as if it came from a GET request. The second argument to
    // `parseGetRequestTarget` says whether the function should apply URL
    // decoding.
    if (contentType == contentTypeUrlEncoded) {
      return ad_utility::UrlParser::parseGetRequestTarget(
          absl::StrCat(request.target(), "?", request.body()), true);
    }
    if (contentType == contentTypeSparqlQuery) {
      return ad_utility::UrlParser::parseGetRequestTarget(
          absl::StrCat(request.target(), "?query=", request.body()), false);
    }
    throw std::runtime_error(
        absl::StrCat("POST request with content type \"", contentType,
                     "\" not supported (must be \"", contentTypeUrlEncoded,
                     "\" or \"", contentTypeSparqlQuery, "\")"));
  }
  std::ostringstream requestMethodName;
  requestMethodName << request.method();
  throw std::runtime_error(
      absl::StrCat("Request method \"", requestMethodName.str(),
                   "\" not supported (has to be GET or POST)"));
};

// _____________________________________________________________________________
Awaitable<void> Server::process(
    const ad_utility::httpUtils::HttpRequest auto& request, auto&& send) {
  using namespace ad_utility::httpUtils;
  ad_utility::Timer requestTimer;
  requestTimer.start();

  // Parse the path and the URL parameters from the given request. Both GET and
  // POST requests are supported.
  LOG(DEBUG) << "Request method: \"" << request.method() << "\""
             << ", target: \"" << request.target() << "\""
             << ", body: \"" << request.body() << "\"" << std::endl;
  const auto urlPathAndParameters = getUrlPathAndParameters(request);
  const auto& parameters = urlPathAndParameters._parameters;

  // Lambda for checking if a URL parameter exists in the request and if we are
  // allowed to access it. If yes, return the value, otherwise return
  // `std::nullopt`.
  //
  // If `value` is `std::nullopt`, only check if the key exists.  We need this
  // because we have parameters like "cmd=stats", where a fixed combination of
  // the key and value determines the kind of action, as well as parameters
  // like "index-decription=...", where the key determines the kind of action.
  auto checkParameter =
      [&parameters](const std::string key, std::optional<std::string> value,
                    bool accessAllowed = true) -> std::optional<std::string> {
    // If the key is not found, always return std::nullopt.
    if (!parameters.contains(key)) {
      return std::nullopt;
    }
    // If value is given, but not equal to param value, return std::nullopt. If
    // no value is given, set it to param value.
    if (value == std::nullopt) {
      value = parameters.at(key);
    } else if (value != parameters.at(key)) {
      return std::nullopt;
    }
    // At this point, we have a value. Either return it or say access denied.
    if (accessAllowed == false) {
      LOG(INFO) << "Access to \"" << key << "=" << value.value() << "\""
                << "requires a valid access-token"
                << ", this URL parameter is ignored" << std::endl;
      return std::nullopt;
    } else {
      return value;
    }
  };

  // Check the access token. If an access token is provided and the check fails,
  // throw an exception and do not process any part of the query (even if the
  // processing would have been allowed without access token).
  auto accessToken = checkParameter("access-token", std::nullopt);
  bool accessTokenOk = false;
  if (accessToken) {
    auto accessTokenProvidedMsg = absl::StrCat(
        "Access token \"access-token=", accessToken.value(), "\" provided");
    auto requestIgnoredMsg = ", request is ignored";
    if (accessToken_.empty()) {
      throw std::runtime_error(absl::StrCat(
          accessTokenProvidedMsg,
          " but server was started without --access-token", requestIgnoredMsg));
    } else if (accessToken != accessToken_) {
      throw std::runtime_error(absl::StrCat(
          accessTokenProvidedMsg, " but not correct", requestIgnoredMsg));
    } else {
      LOG(DEBUG) << accessTokenProvidedMsg << " and correct" << std::endl;
      accessTokenOk = true;
    }
  }

  // Process all URL parameters known to QLever. If there is more than one,
  // QLever processes all of one, but only returns the result from the last one.
  // In particular, if there is a "query" parameter, it will be processed last
  // and its result returned.
  //
  // Some parameters require that "access-token" is set correctly. If not, that
  // parameter is ignored.
  std::optional<http::response<http::string_body>> response;

  // Execute commands (URL parameter with key "cmd").
  auto logCommand = [](std::optional<std::string>& cmd, std::string actionMsg) {
    LOG(INFO) << "Processing command \"" << cmd.value() << "\""
              << ": " << actionMsg << std::endl;
  };
  if (auto cmd = checkParameter("cmd", "stats")) {
    logCommand(cmd, "get index statistics");
    response = createJsonResponse(composeStatsJson(), request);
  } else if (auto cmd = checkParameter("cmd", "cache-stats")) {
    logCommand(cmd, "get cache statistics");
    response = createJsonResponse(composeCacheStatsJson(), request);
  } else if (auto cmd = checkParameter("cmd", "clear-cache")) {
    logCommand(cmd, "clear the cache (unpinned elements only)");
    _cache.clearUnpinnedOnly();
    response = createJsonResponse(composeCacheStatsJson(), request);
  } else if (auto cmd =
                 checkParameter("cmd", "clear-cache-complete", accessTokenOk)) {
    logCommand(cmd, "clear cache completely (including unpinned elements)");
    _cache.clearAll();
    response = createJsonResponse(composeCacheStatsJson(), request);
  } else if (auto cmd = checkParameter("cmd", "get-settings")) {
    logCommand(cmd, "get server settings");
    response = createJsonResponse(RuntimeParameters().toMap(), request);
  }

  // Set description of KB index.
  if (auto description =
          checkParameter("index-description", std::nullopt, accessTokenOk)) {
    LOG(INFO) << "Setting index description to: \"" << description.value()
              << "\"" << std::endl;
    _index.setKbName(description.value());
    response = createJsonResponse(composeStatsJson(), request);
  }

  // Set description of text index.
  if (auto description =
          checkParameter("text-description", std::nullopt, accessTokenOk)) {
    LOG(INFO) << "Setting text description to: \"" << description.value()
              << "\"" << std::endl;
    _index.setTextName(description.value());
    response = createJsonResponse(composeStatsJson(), request);
  }

  // Set one or several of the runtime parameters.
  for (auto key : RuntimeParameters().getKeys()) {
    if (auto value = checkParameter(key, std::nullopt, accessTokenOk)) {
      LOG(INFO) << "Setting runtime parameter \"" << key << "\""
                << " to value \"" << value.value() << "\"" << std::endl;
      RuntimeParameters().set(key, value.value());
      response = createJsonResponse(RuntimeParameters().toMap(), request);
    }
  }

  // If "query" parameter is given, process query.
  if (auto query = checkParameter("query", std::nullopt)) {
    if (query.value().empty()) {
      throw std::runtime_error(
          "Parameter \"query\" must not have an empty value");
    }
    co_return co_await processQuery(parameters, requestTimer,
                                    std::move(request), send);
  }

  // If there was no "query", but any of the URL parameters processed before
  // produced a `response`, send that now. Note that if multiple URL parameters
  // were processed, only the `response` from the last one is sent.
  if (response.has_value()) {
    co_return co_await send(std::move(response.value()));
  }

  // At this point, there were either no URL paraeters or none of them were
  // recognized by QLever. We then interpret the request as a request for a
  // file. However, only files from a very restricted whitelist (see below) will
  // actually be served.
  //
  // NOTE 1: `makeFileServer` returns a function.  The first argument is the
  // document root, the second one is the whitelist.
  //
  // NOTE 2: `co_await makeFileServer(....)(...)` doesn't compile in g++ 11.2.0,
  // probably because of the following bug:
  // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=98056
  // TODO<joka921>: Reinstate this one-liner as soon as this bug is fixed.
  //
  // TODO: The file server currently does not LOG much. For example, when a file
  // is not found, a corresponding response is returned to the requestion
  // client, but the log says nothing about it. The place to change this would
  // be in `src/util/HttpServer/HttpUtils.h`.
  auto serveFileRequest = makeFileServer(
      ".",
      ad_utility::HashSet<std::string>{"index.html", "script.js", "style.css"})(
      std::move(request), send);
  co_return co_await std::move(serveFileRequest);
}

// _____________________________________________________________________________
Awaitable<json> Server::composeResponseQleverJson(
    const ParsedQuery& query, const QueryExecutionTree& qet,
    ad_utility::Timer& requestTimer, size_t maxSend) const {
  auto compute = [&, maxSend] {
    shared_ptr<const ResultTable> resultTable = qet.getResult();
    requestTimer.stop();
    off_t compResultUsecs = requestTimer.usecs();
    size_t resultSize = resultTable->size();

    nlohmann::json j;

    j["query"] = query._originalString;
    j["status"] = "OK";
    j["warnings"] = qet.collectWarnings();
    if (query.hasSelectClause()) {
      j["selected"] =
          query.selectClause()._varsOrAsterisk.getSelectedVariables();
    } else {
      j["selected"] =
          std::vector<std::string>{"?subject", "?predicate", "?object"};
    }

    j["runtimeInformation"] = RuntimeInformation::ordered_json(
        qet.getRootOperation()->getRuntimeInfo());

    {
      size_t limit = std::min(query._limitOffset._limit, maxSend);
      size_t offset = query._limitOffset._offset;
      requestTimer.cont();
      j["res"] = query.hasSelectClause()
                     ? qet.writeResultAsQLeverJson(
                           query.selectClause()._varsOrAsterisk, limit, offset,
                           std::move(resultTable))
                     : qet.writeRdfGraphJson(query.constructClause(), limit,
                                             offset, std::move(resultTable));
      requestTimer.stop();
    }
    j["resultsize"] = query.hasSelectClause() ? resultSize : j["res"].size();

    requestTimer.stop();
    j["time"]["total"] =
        std::to_string(static_cast<double>(requestTimer.usecs()) / 1000.0) +
        "ms";
    j["time"]["computeResult"] =
        std::to_string(static_cast<double>(compResultUsecs) / 1000.0) + "ms";

    return j;
  };
  return computeInNewThread(compute);
}

// _____________________________________________________________________________
Awaitable<json> Server::composeResponseSparqlJson(
    const ParsedQuery& query, const QueryExecutionTree& qet,
    ad_utility::Timer& requestTimer, size_t maxSend) const {
  if (!query.hasSelectClause()) {
    throw std::runtime_error{
        "SPARQL-compliant JSON format is only supported for SELECT queries"};
  }
  auto compute = [&, maxSend] {
    shared_ptr<const ResultTable> resultTable = qet.getResult();
    requestTimer.stop();
    nlohmann::json j;
    size_t limit = std::min(query._limitOffset._limit, maxSend);
    size_t offset = query._limitOffset._offset;
    requestTimer.cont();
    j = qet.writeResultAsSparqlJson(query.selectClause()._varsOrAsterisk, limit,
                                    offset, std::move(resultTable));

    requestTimer.stop();
    return j;
  };
  return computeInNewThread(compute);
}

// _____________________________________________________________________________
template <QueryExecutionTree::ExportSubFormat format>
Awaitable<ad_utility::streams::stream_generator>
Server::composeResponseSepValues(const ParsedQuery& query,
                                 const QueryExecutionTree& qet) const {
  auto compute = [&] {
    size_t limit = query._limitOffset._limit;
    size_t offset = query._limitOffset._offset;
    return query.hasSelectClause()
               ? qet.generateResults<format>(
                     query.selectClause()._varsOrAsterisk, limit, offset)
               : qet.writeRdfGraphSeparatedValues<format>(
                     query.constructClause(), limit, offset, qet.getResult());
  };
  return computeInNewThread(compute);
}

// _____________________________________________________________________________

ad_utility::streams::stream_generator Server::composeTurtleResponse(
    const ParsedQuery& query, const QueryExecutionTree& qet) {
  if (!query.hasConstructClause()) {
    throw std::runtime_error{
        "RDF Turtle as an export format is only supported for CONSTRUCT "
        "queries"};
  }
  size_t limit = query._limitOffset._limit;
  size_t offset = query._limitOffset._offset;
  return qet.writeRdfGraphTurtle(query.constructClause(), limit, offset,
                                 qet.getResult());
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
              << ((pinResult) ? " (Result pinned)" : "") << ":\n"
              << query << std::endl;
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
          ad_utility::MediaType::qleverJson,
          ad_utility::MediaType::sparqlJson,
          ad_utility::MediaType::tsv,
          ad_utility::MediaType::csv,
          ad_utility::MediaType::turtle,
          ad_utility::MediaType::octetStream};
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
    } else if (containsParam("action", "turtle_export")) {
      mediaType = ad_utility::MediaType::turtle;
    } else if (containsParam("action", "binary_export")) {
      mediaType = ad_utility::MediaType::octetStream;
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
        auto responseGenerator = co_await composeResponseSepValues<
            QueryExecutionTree::ExportSubFormat::CSV>(pq, qet);

        auto response = createOkResponse(std::move(responseGenerator), request,
                                         ad_utility::MediaType::csv);
        co_await send(std::move(response));

      } break;
      case ad_utility::MediaType::tsv: {
        auto responseGenerator = co_await composeResponseSepValues<
            QueryExecutionTree::ExportSubFormat::TSV>(pq, qet);

        auto response = createOkResponse(std::move(responseGenerator), request,
                                         ad_utility::MediaType::tsv);
        co_await send(std::move(response));
      } break;
      case ad_utility::MediaType::octetStream: {
        auto responseGenerator = co_await composeResponseSepValues<
            QueryExecutionTree::ExportSubFormat::BINARY>(pq, qet);

        auto response = createOkResponse(std::move(responseGenerator), request,
                                         ad_utility::MediaType::octetStream);
        co_await send(std::move(response));
      } break;
      case ad_utility::MediaType::qleverJson: {
        // Normal case: JSON response
        auto responseString =
            co_await composeResponseQleverJson(pq, qet, requestTimer, maxSend);
        co_await sendJson(std::move(responseString));
      } break;
      case ad_utility::MediaType::turtle: {
        auto responseGenerator = composeTurtleResponse(pq, qet);

        auto response = createOkResponse(std::move(responseGenerator), request,
                                         ad_utility::MediaType::turtle);
        co_await send(std::move(response));
      } break;
      case ad_utility::MediaType::sparqlJson: {
        auto responseString =
            co_await composeResponseSparqlJson(pq, qet, requestTimer, maxSend);
        co_await sendJson(std::move(responseString));
      } break;
      default:
        // This should never happen, because we have carefully restricted the
        // subset of mediaTypes that can occur here.
        AD_CHECK(false);
    }
    // Print the runtime info. This needs to be done after the query
    // was computed.

    // TODO<joka921> Also log an identifier of the query.
    LOG(INFO) << "Done processing query and sending result"
              << ", total time was " << requestTimer.msecs() << " ms"
              << std::endl;
    LOG(DEBUG) << "Runtime Info:\n"
               << qet.getRootOperation()->getRuntimeInfo().toString()
               << std::endl;
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
