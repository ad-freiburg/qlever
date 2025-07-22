// Copyright 2015 - 2025 The QLever Authors, in particular:
//
// 2015 - 2017 Björn Buchhold, UFR
// 2020 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/Server.h"

#include <absl/functional/bind_front.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <sstream>
#include <string>
#include <vector>

#include "CompilationInfo.h"
#include "GraphStoreProtocol.h"
#include "engine/ExecuteUpdate.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/HttpError.h"
#include "engine/QueryPlanner.h"
#include "engine/SPARQLProtocol.h"
#include "global/RuntimeParameters.h"
#include "index/IndexImpl.h"
#include "parser/SparqlParser.h"
#include "util/AsioHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParseableDuration.h"
#include "util/TypeTraits.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"
#include "util/http/websocket/MessageSender.h"

using namespace std::string_literals;
using namespace ad_utility::url_parser::sparqlOperation;

template <typename T>
using Awaitable = Server::Awaitable<T>;
using ad_utility::MediaType;

// __________________________________________________________________________
Server::Server(unsigned short port, size_t numThreads,
               ad_utility::MemorySize maxMem, std::string accessToken,
               bool usePatternTrick)
    : numThreads_(numThreads),
      port_(port),
      accessToken_(std::move(accessToken)),
      allocator_{ad_utility::makeAllocationMemoryLeftThreadsafeObject(maxMem),
                 [this](ad_utility::MemorySize numMemoryToAllocate) {
                   cache_.makeRoomAsMuchAsPossible(MAKE_ROOM_SLACK_FACTOR *
                                                   numMemoryToAllocate);
                 }},
      index_{allocator_},
      enablePatternTrick_(usePatternTrick),
      // The number of server threads currently also is the number of queries
      // that can be processed simultaneously.
      queryThreadPool_{numThreads} {
  // This also directly triggers the update functions and propagates the
  // values of the parameters to the cache.
  RuntimeParameters().setOnUpdateAction<"cache-max-num-entries">(
      [this](size_t newValue) { cache_.setMaxNumEntries(newValue); });
  RuntimeParameters().setOnUpdateAction<"cache-max-size">(
      [this](ad_utility::MemorySize newValue) { cache_.setMaxSize(newValue); });
  RuntimeParameters().setOnUpdateAction<"cache-max-size-single-entry">(
      [this](ad_utility::MemorySize newValue) {
        cache_.setMaxSizeSingleEntry(newValue);
      });
}

// __________________________________________________________________________
void Server::initialize(const std::string& indexBaseName, bool useText,
                        bool usePatterns, bool loadAllPermutations,
                        bool persistUpdates) {
  LOG(INFO) << "Initializing server ..." << std::endl;

  index_.usePatterns() = usePatterns;
  index_.loadAllPermutations() = loadAllPermutations;

  // Init the index.
  index_.createFromOnDiskIndex(indexBaseName, persistUpdates);
  if (useText) {
    index_.addTextFromOnDiskIndex();
  }

  sortPerformanceEstimator_.computeEstimatesExpensively(
      allocator_, index_.numTriples().normalAndInternal_() *
                      PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);

  LOG(INFO) << "Access token for restricted API calls is \"" << accessToken_
            << "\"" << std::endl;
}

// _____________________________________________________________________________
void Server::run(const std::string& indexBaseName, bool useText,
                 bool usePatterns, bool loadAllPermutations,
                 bool persistUpdates) {
  using namespace ad_utility::httpUtils;

  // Function that handles a request asynchronously, will be passed as argument
  // to `HttpServer` below.
  auto httpSessionHandler =
      [this](auto request, auto&& send) -> boost::asio::awaitable<void> {
    // Version of send with maximally permissive CORS header (which allows the
    // client that receives the response to do with it what it wants).
    // NOTE: For POST and GET requests, the "allow origin" header is sufficient,
    // while the "allow headers" header is needed only for OPTIONS request. The
    // "allow methods" header is purely informational. To avoid two similar
    // lambdas here, we send the same headers for GET, POST, and OPTIONS.
    auto sendWithAccessControlHeaders =
        [&send](auto response) -> boost::asio::awaitable<void> {
      response.set(http::field::access_control_allow_origin, "*");
      response.set(http::field::access_control_allow_headers, "*");
      response.set(http::field::access_control_allow_methods,
                   "GET, POST, OPTIONS");
      co_return co_await send(std::move(response));
    };
    // Reply to OPTIONS requests immediately by allowing everything.
    // NOTE: Handling OPTIONS requests is necessary because some POST queries
    // (in particular, from the QLever UI) are preceded by an OPTIONS request (a
    // so-called "preflight" request, which asks permission for the POST query).
    if (request.method() == http::verb::options) {
      LOG(INFO) << std::endl;
      LOG(INFO) << "Request received via " << request.method()
                << ", allowing everything" << std::endl;
      co_return co_await sendWithAccessControlHeaders(
          createOkResponse("", request, MediaType::textPlain));
    }
    // Process the request using the `process` method and if it throws an
    // exception, log the error message and send a HTTP/1.1 400 Bad Request
    // response with that message. Note that the C++ standard forbids co_await
    // in the catch block, hence the workaround with the `exceptionErrorMsg`.
    std::optional<std::string> exceptionErrorMsg;
    std::optional<boost::beast::http::status> httpResponseStatus;
    try {
      co_await process(request, sendWithAccessControlHeaders);
    } catch (const HttpError& e) {
      httpResponseStatus = e.status();
      exceptionErrorMsg = e.what();
    } catch (const std::exception& e) {
      exceptionErrorMsg = e.what();
    }
    if (exceptionErrorMsg.has_value()) {
      LOG(ERROR) << exceptionErrorMsg.value() << std::endl;
      auto status =
          httpResponseStatus.value_or(boost::beast::http::status::bad_request);
      auto response = createHttpResponseFromString(
          exceptionErrorMsg.value(), status, request, MediaType::textPlain);
      co_return co_await sendWithAccessControlHeaders(std::move(response));
    }
  };

  auto webSocketSessionSupplier = [this](net::io_context& ioContext) {
    // This must only be called once
    AD_CONTRACT_CHECK(queryHub_.expired());
    auto queryHub =
        std::make_shared<ad_utility::websocket::QueryHub>(ioContext);
    // Make sure the `queryHub` does not outlive the ioContext it has a
    // reference to, by only storing a `weak_ptr` in the `queryHub_`. Note: This
    // `weak_ptr` may only be converted back to a `shared_ptr` inside a task
    // running on the `io_context`.
    queryHub_ = queryHub;
    return [this, queryHub = std::move(queryHub)](
               const http::request<http::string_body>& request,
               tcp::socket socket) {
      return ad_utility::websocket::WebSocketSession::handleSession(
          *queryHub, queryRegistry_, request, std::move(socket));
    };
  };

  // First set up the HTTP server, so that it binds to the socket, and
  // the "socket already in use" error appears quickly.
  auto httpServer = HttpServer{port_, "0.0.0.0", static_cast<int>(numThreads_),
                               std::move(httpSessionHandler),
                               std::move(webSocketSessionSupplier)};

  // Initialize the index
  initialize(indexBaseName, useText, usePatterns, loadAllPermutations,
             persistUpdates);

  LOG(INFO) << "The server is ready, listening for requests on port "
            << std::to_string(httpServer.getPort()) << " ..." << std::endl;

  // Start listening for connections on the server.
  httpServer.run();
}

// _____________________________________________________________________________
CPP_template_def(typename RequestT, typename ResponseT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    net::awaitable<std::optional<Server::TimeLimit>> Server::
        verifyUserSubmittedQueryTimeout(
            std::optional<std::string_view> userTimeout, bool accessTokenOk,
            const RequestT& request, ResponseT& send) const {
  auto defaultTimeout = RuntimeParameters().get<"default-query-timeout">();
  // TODO<GCC12> Use the monadic operations for std::optional
  if (userTimeout.has_value()) {
    auto timeoutCandidate =
        ad_utility::ParseableDuration<TimeLimit>::fromString(
            userTimeout.value());
    if (timeoutCandidate > defaultTimeout && !accessTokenOk) {
      co_await send(ad_utility::httpUtils::createForbiddenResponse(
          absl::StrCat("User submitted timeout was higher than what is "
                       "currently allowed by "
                       "this instance (",
                       defaultTimeout.toString(),
                       "). Please use a valid-access token to override this "
                       "server configuration."),
          request));
      co_return std::nullopt;
    }
    co_return timeoutCandidate;
  }
  co_return std::chrono::duration_cast<TimeLimit>(
      decltype(defaultTimeout)::DurationType{defaultTimeout});
}

// _____________________________________________
/// Special type of std::runtime_error used to indicate that there has been
/// a collision of query ids. This will happen when a HTTP client chooses an
/// explicit id that is currently already in use. In this case the server
/// will respond with HTTP status 409 Conflict and the client is encouraged
/// to re-submit their request with a different query id.
class QueryAlreadyInUseError : public std::runtime_error {
 public:
  explicit QueryAlreadyInUseError(std::string_view proposedQueryId)
      : std::runtime_error{"Query id '"s + proposedQueryId +
                           "' is already in use!"} {}
};

// _____________________________________________________________________________
auto Server::cancelAfterDeadline(
    std::weak_ptr<ad_utility::CancellationHandle<>> cancellationHandle,
    TimeLimit timeLimit)
    -> QL_CONCEPT_OR_NOTHING(
        ad_utility::InvocableWithExactReturnType<void>) auto {
  net::steady_timer timer{timerExecutor_, timeLimit};

  timer.async_wait([cancellationHandle = std::move(cancellationHandle)](
                       const boost::system::error_code&) {
    if (auto pointer = cancellationHandle.lock()) {
      pointer->cancel(ad_utility::CancellationState::TIMEOUT);
    }
  });
  return [timer = std::move(timer)]() mutable { timer.cancel(); };
}

// _____________________________________________________________________________
auto Server::setupCancellationHandle(
    const ad_utility::websocket::QueryId& queryId, TimeLimit timeLimit)
    -> QL_CONCEPT_OR_NOTHING(ad_utility::isInstantiation<
                             CancellationHandleAndTimeoutTimerCancel>) auto {
  auto cancellationHandle = queryRegistry_.getCancellationHandle(queryId);
  AD_CORRECTNESS_CHECK(cancellationHandle);
  cancellationHandle->startWatchDog();
  absl::Cleanup cancelCancellationHandle{
      cancelAfterDeadline(cancellationHandle, timeLimit)};
  return CancellationHandleAndTimeoutTimerCancel{
      std::move(cancellationHandle), std::move(cancelCancellationHandle)};
}

// ____________________________________________________________________________
auto Server::prepareOperation(
    std::string_view operationName, std::string_view operationSPARQL,
    ad_utility::websocket::MessageSender& messageSender,
    const ad_utility::url_parser::ParamValueMap& params, TimeLimit timeLimit) {
  auto [cancellationHandle, cancelTimeoutOnDestruction] =
      setupCancellationHandle(messageSender.getQueryId(), timeLimit);

  // Do the query planning. This creates a `QueryExecutionTree`, which will
  // then be used to process the query.
  auto [pinSubtrees, pinResult] = determineResultPinning(params);
  LOG(INFO) << "Processing the following " << operationName << ":"
            << (pinResult ? " [pin result]" : "")
            << (pinSubtrees ? " [pin subresults]" : "") << "\n"
            << ad_utility::truncateOperationString(operationSPARQL)
            << std::endl;
  QueryExecutionContext qec(index_, &cache_, allocator_,
                            sortPerformanceEstimator_, std::ref(messageSender),
                            pinSubtrees, pinResult);

  return std::tuple{std::move(qec), std::move(cancellationHandle),
                    std::move(cancelTimeoutOnDestruction)};
}

// _____________________________________________________________________________
CPP_template_def(typename RequestT, typename ResponseT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::process(const RequestT& request, ResponseT&& send) {
  using namespace ad_utility::httpUtils;

  // Log some basic information about the request. Start with an empty line so
  // that in a low-traffic scenario (or when the query processing is very fast),
  // we have one visual block per request in the log.
  std::string_view contentType = request.base()[http::field::content_type];
  LOG(INFO) << std::endl;
  LOG(INFO) << "Request received via " << request.method()
            << (contentType.empty()
                    ? absl::StrCat(", no content type specified")
                    : absl::StrCat(", content type \"", contentType, "\""))
            << std::endl;

  // Start timing.
  ad_utility::Timer requestTimer{ad_utility::Timer::Started};

  // Parse the path and the URL parameters from the given request. Works for GET
  // requests as well as the two kinds of POST requests allowed by the SPARQL
  // standard, see method `getUrlPathAndParameters`.
  auto parsedHttpRequest = SPARQLProtocol::parseHttpRequest(request);
  const auto& parameters = parsedHttpRequest.parameters_;

  // We always want to call `Server::checkParameter` with the same first
  // parameter.
  auto checkParameter = absl::bind_front(
      &ad_utility::url_parser::checkParameter, std::cref(parameters));

  // Check the access token. If an access token is provided and the check fails,
  // throw an exception and do not process any part of the query (even if the
  // processing had been allowed without access token).
  bool accessTokenOk = checkAccessToken(parsedHttpRequest.accessToken_);
  auto requireValidAccessToken = [&accessTokenOk](
                                     const std::string& actionName) {
    if (!accessTokenOk) {
      throw std::runtime_error(absl::StrCat(
          actionName,
          " requires a valid access token but no access token was provided"));
    }
  };

  // Process all URL parameters known to QLever. If there is more than one,
  // QLever processes all of them, but only returns the result from the last
  // one. In particular, if there is a "query" parameter, it will be processed
  // last and its result returned.
  //
  // Some parameters require that "access-token" is set correctly. If not, that
  // parameter is ignored.
  std::optional<http::response<http::string_body>> response;

  // Execute commands (URL parameter with key "cmd").
  auto logCommand = [](const std::optional<std::string_view>& cmd,
                       std::string_view actionMsg) {
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
    cache_.clearUnpinnedOnly();
    response = createJsonResponse(composeCacheStatsJson(), request);
  } else if (auto cmd = checkParameter("cmd", "clear-cache-complete")) {
    requireValidAccessToken("clear-cache-complete");
    logCommand(cmd, "clear cache completely (including unpinned elements)");
    cache_.clearAll();
    response = createJsonResponse(composeCacheStatsJson(), request);
  } else if (auto cmd = checkParameter("cmd", "clear-delta-triples")) {
    requireValidAccessToken("clear-delta-triples");
    logCommand(cmd, "clear delta triples");
    // The function requires a SharedCancellationHandle, but the operation is
    // not cancellable.
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    // We don't directly `co_await` because of lifetime issues (bugs) in the
    // Conan setup.
    auto coroutine = computeInNewThread(
        updateThreadPool_,
        [this] {
          // Use `this` explicitly to silence false-positive errors on the
          // captured `this` being unused.
          return this->index_.deltaTriplesManager().modify<DeltaTriplesCount>(
              [](auto& deltaTriples) {
                deltaTriples.clear();
                return deltaTriples.getCounts();
              });
        },
        handle);
    auto countAfterClear = co_await std::move(coroutine);
    response = createJsonResponse(nlohmann::json{countAfterClear}, request);
  } else if (auto cmd = checkParameter("cmd", "get-settings")) {
    logCommand(cmd, "get server settings");
    response = createJsonResponse(RuntimeParameters().toMap(), request);
  } else if (auto cmd = checkParameter("cmd", "get-index-id")) {
    logCommand(cmd, "get index ID");
    response =
        createOkResponse(index_.getIndexId(), request, MediaType::textPlain);
  } else if (auto cmd = checkParameter("cmd", "dump-active-queries")) {
    requireValidAccessToken("dump-active-queries");
    logCommand(cmd, "dump active queries");
    nlohmann::json json;
    for (auto& [key, value] : queryRegistry_.getActiveQueries()) {
      json[nlohmann::json(key)] = std::move(value);
    }
    response = createJsonResponse(json, request);
  }

  // Ping with or without message.
  if (parsedHttpRequest.path_ == "/ping") {
    if (auto msg = checkParameter("msg", std::nullopt)) {
      LOG(INFO) << "Alive check with message \"" << msg.value() << "\""
                << std::endl;
    } else {
      LOG(INFO) << "Alive check without message" << std::endl;
    }
    response = createOkResponse("This QLever server is up and running\n",
                                request, MediaType::textPlain);
  }

  // Set description of KB index.
  if (auto description = checkParameter("index-description", std::nullopt)) {
    requireValidAccessToken("index-description");
    LOG(INFO) << "Setting index description to: \"" << description.value()
              << "\"" << std::endl;
    index_.setKbName(std::string{description.value()});
    response = createJsonResponse(composeStatsJson(), request);
  }

  // Set description of text index.
  if (auto description = checkParameter("text-description", std::nullopt)) {
    requireValidAccessToken("text-description");
    LOG(INFO) << "Setting text description to: \"" << description.value()
              << "\"" << std::endl;
    index_.setTextName(std::string{description.value()});
    response = createJsonResponse(composeStatsJson(), request);
  }

  // Set one or several of the runtime parameters.
  for (auto key : RuntimeParameters().getKeys()) {
    if (auto value = checkParameter(key, std::nullopt)) {
      requireValidAccessToken("setting runtime parameters");
      LOG(INFO) << "Setting runtime parameter \"" << key << "\""
                << " to value \"" << value.value() << "\"" << std::endl;
      RuntimeParameters().set(key, std::string{value.value()});
      response = createJsonResponse(RuntimeParameters().toMap(), request);
    }
  }

  // Store the QueryExecutionTree outside the lambda, s.t. we have access in
  // case of errors to create an informative error message that includes the
  // runtime information.
  std::optional<PlannedQuery> plannedQuery;
  auto visitOperation =
      [&checkParameter, &accessTokenOk, &request, &send, &parameters,
       &requestTimer, &plannedQuery,
       this](std::vector<ParsedQuery> operations, std::string operationName,
             const std::string operationString,
             std::function<bool(const ParsedQuery&)> expectedOperation,
             const std::string msg) -> Awaitable<void> {
    auto timeLimit = co_await verifyUserSubmittedQueryTimeout(
        checkParameter("timeout", std::nullopt), accessTokenOk, request, send);
    if (!timeLimit.has_value()) {
      // If the optional is empty, this indicates an error response has been
      // sent to the client already. We can stop here.
      co_return;
    }
    ad_utility::websocket::MessageSender messageSender =
        createMessageSender(queryHub_, request, operationString);

    auto [qec, cancellationHandle, cancelTimeoutOnDestruction] =
        prepareOperation(operationName, operationString, messageSender,
                         parameters, timeLimit.value());
    if (!ql::ranges::all_of(operations, expectedOperation)) {
      throw std::runtime_error(absl::StrCat(
          msg, ad_utility::truncateOperationString(operationString)));
    }
    if (ql::ranges::all_of(operations, &ParsedQuery::hasUpdateClause)) {
      co_return co_await processUpdate(
          std::move(operations), requestTimer, cancellationHandle, qec,
          std::move(request), send, timeLimit.value(), plannedQuery);
    } else {
      AD_CORRECTNESS_CHECK(operations.size() == 1);
      ParsedQuery query = std::move(operations[0]);
      AD_CORRECTNESS_CHECK(query.hasSelectClause() || query.hasAskClause() ||
                           query.hasConstructClause());
      co_return co_await processQuery(
          parameters, std::move(query), requestTimer, cancellationHandle, qec,
          std::move(request), send, timeLimit.value(), plannedQuery);
    }
  };
  auto visitQuery = [&visitOperation](Query query) -> Awaitable<void> {
    // We need to copy the query string because `visitOperation` below also
    // needs it.
    auto parsedQuery =
        SparqlParser::parseQuery(query.query_, query.datasetClauses_);
    return visitOperation(
        {std::move(parsedQuery)}, "SPARQL Query", std::move(query.query_),
        std::not_fn(&ParsedQuery::hasUpdateClause),
        "SPARQL QUERY was request via the HTTP request, but the "
        "following update was sent instead of an query: ");
  };
  auto visitUpdate = [this, &visitOperation, &requireValidAccessToken](
                         Update update) -> Awaitable<void> {
    requireValidAccessToken("SPARQL Update");
    // We need to copy the update string because `visitOperation` below also
    // needs it.
    auto parsedUpdates = SparqlParser::parseUpdate(
        index().getBlankNodeManager(), update.update_, update.datasetClauses_);
    return visitOperation(
        std::move(parsedUpdates), "SPARQL Update", std::move(update.update_),
        &ParsedQuery::hasUpdateClause,
        "SPARQL UPDATE was request via the HTTP request, but the "
        "following query was sent instead of an update: ");
  };
  auto visitGraphStore = [&request, &visitOperation, &requireValidAccessToken](
                             GraphStoreOperation operation) -> Awaitable<void> {
    ParsedQuery parsedOperation =
        GraphStoreProtocol::transformGraphStoreProtocol(std::move(operation),
                                                        request);

    if (parsedOperation.hasUpdateClause()) {
      requireValidAccessToken("Update from Graph Store Protocol");
    }

    // Don't check for the `ParsedQuery`s actual type (Query or Update) here
    // because graph store operations can result in both.
    auto trueFunc = [](const ParsedQuery&) { return true; };
    std::string_view operationType =
        parsedOperation.hasUpdateClause() ? "Update" : "Query";
    std::string operationString = parsedOperation._originalString;
    return visitOperation({std::move(parsedOperation)},
                          absl::StrCat("Graph Store (", operationType, ")"),
                          std::move(operationString), trueFunc,
                          "Unused dummy message");
  };
  auto visitNone = [&response, &send, &request](None) -> Awaitable<void> {
    // If there was no "query", but any of the URL parameters processed before
    // produced a `response`, send that now. Note that if multiple URL
    // parameters were processed, only the `response` from the last one is sent.
    if (response.has_value()) {
      return send(std::move(response.value()));
    }

    // At this point, if there is a "?" in the query string, it means that there
    // are URL parameters which QLever does not know or did not process.
    if (request.target().find("?") != std::string::npos) {
      return send(createBadRequestResponse("Unknown query parameters",
                                           std::move(request)));
    }
    // No path matched up until this point, so return 404 to indicate the client
    // made an error and the server will not serve anything else.
    return send(createNotFoundResponse("Unknown path", std::move(request)));
  };

  co_return co_await processOperation(
      std::move(parsedHttpRequest.operation_),
      ad_utility::OverloadCallOperator{visitQuery, visitUpdate, visitGraphStore,
                                       visitNone},
      requestTimer, request, send, plannedQuery);
}

// ____________________________________________________________________________
std::pair<bool, bool> Server::determineResultPinning(
    const ad_utility::url_parser::ParamValueMap& params) {
  const bool pinSubtrees =
      ad_utility::url_parser::checkParameter(params, "pinsubtrees", "true")
          .has_value();
  const bool pinResult =
      ad_utility::url_parser::checkParameter(params, "pinresult", "true")
          .has_value();
  return {pinSubtrees, pinResult};
}

// ____________________________________________________________________________
Server::PlannedQuery Server::planQuery(
    ParsedQuery&& operation, const ad_utility::Timer& requestTimer,
    TimeLimit timeLimit, QueryExecutionContext& qec,
    ad_utility::SharedCancellationHandle handle) const {
  QueryPlanner qp(&qec, handle);
  auto executionTree = qp.createExecutionTree(operation);
  PlannedQuery plannedQuery{std::move(operation), std::move(executionTree)};
  handle->throwIfCancelled();
  // Set some additional attributes on the `PlannedQuery`.
  plannedQuery.queryExecutionTree_.getRootOperation()
      ->recursivelySetCancellationHandle(std::move(handle));
  plannedQuery.queryExecutionTree_.getRootOperation()
      ->recursivelySetTimeConstraint(timeLimit);
  auto& qet = plannedQuery.queryExecutionTree_;
  qet.isRoot() = true;  // allow pinning of the final result
  auto timeForQueryPlanning = requestTimer.msecs();
  auto& runtimeInfoWholeQuery =
      qet.getRootOperation()->getRuntimeInfoWholeQuery();
  runtimeInfoWholeQuery.timeQueryPlanning = timeForQueryPlanning;
  LOG(INFO) << "Query planning done in " << timeForQueryPlanning.count()
            << " ms" << std::endl;
  LOG(TRACE) << qet.getCacheKey() << std::endl;
  return plannedQuery;
}

// _____________________________________________________________________________
nlohmann::json Server::composeErrorResponseJson(
    const std::string& query, const std::string& errorMsg,
    const ad_utility::Timer& requestTimer,
    const std::optional<ExceptionMetadata>& metadata) {
  json j;
  using ad_utility::Timer;
  j["query"] = ad_utility::truncateOperationString(query);
  j["status"] = "ERROR";
  j["resultsize"] = 0;
  j["time"]["total"] = requestTimer.msecs().count();
  j["time"]["computeResult"] = requestTimer.msecs().count();
  j["exception"] = errorMsg;

  // If the error location is truncated don't send it's location.
  if (metadata.has_value() &&
      metadata.value().stopIndex_ < MAX_LENGTH_OPERATION_ECHO) {
    auto& value = metadata.value();
    j["metadata"]["startIndex"] = value.startIndex_;
    j["metadata"]["stopIndex"] = value.stopIndex_;
    j["metadata"]["line"] = value.line_;
    j["metadata"]["positionInLine"] = value.charPositionInLine_;
  }

  return j;
}

// _____________________________________________________________________________
nlohmann::json Server::composeStatsJson() const {
  json result;
  result["name-index"] = index_.getKbName();
  result["git-hash-index"] = index_.getGitShortHash();
  result["git-hash-server"] =
      *qlever::version::gitShortHashWithoutLinking.wlock();
  result["num-permutations"] = (index_.hasAllPermutations() ? 6 : 2);
  result["num-predicates-normal"] = index_.numDistinctPredicates().normal;
  result["num-predicates-internal"] = index_.numDistinctPredicates().internal;
  if (index_.hasAllPermutations()) {
    result["num-subjects-normal"] = index_.numDistinctSubjects().normal;
    result["num-subjects-internal"] = index_.numDistinctSubjects().internal;
    result["num-objects-normal"] = index_.numDistinctObjects().normal;
    result["num-objects-internal"] = index_.numDistinctObjects().internal;
  }

  auto numTriples = index_.numTriples();
  result["num-triples-normal"] = numTriples.normal;
  result["num-triples-internal"] = numTriples.internal;
  result["name-text-index"] = index_.getTextName();
  result["num-text-records"] = index_.getNofTextRecords();
  result["num-word-occurrences"] = index_.getNofWordPostings();
  result["num-entity-occurrences"] = index_.getNofEntityPostings();
  return result;
}

// _______________________________________
nlohmann::json Server::composeCacheStatsJson() const {
  nlohmann::json result;
  result["num-non-pinned-entries"] = cache_.numNonPinnedEntries();
  result["num-pinned-entries"] = cache_.numPinnedEntries();

  // TODO Get rid of the `getByte()`, once `MemorySize` has it's own json
  // converter.
  result["non-pinned-size"] = cache_.nonPinnedSize().getBytes();
  result["pinned-size"] = cache_.pinnedSize().getBytes();
  return result;
}

// _____________________________________________
CPP_template_def(typename RequestT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    ad_utility::websocket::OwningQueryId Server::getQueryId(
        const RequestT& request, std::string_view query) {
  using ad_utility::websocket::OwningQueryId;
  std::string_view queryIdHeader = request.base()["Query-Id"];
  if (queryIdHeader.empty()) {
    return queryRegistry_.uniqueId(query);
  }
  auto queryId =
      queryRegistry_.uniqueIdFromString(std::string(queryIdHeader), query);
  if (!queryId) {
    throw QueryAlreadyInUseError{queryIdHeader};
  }
  return std::move(queryId.value());
}

// _____________________________________________________________________________
CPP_template_def(typename RequestT, typename ResponseT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::sendStreamableResponse(
        const RequestT& request, ResponseT& send, MediaType mediaType,
        const PlannedQuery& plannedQuery, const QueryExecutionTree& qet,
        const ad_utility::Timer& requestTimer,
        SharedCancellationHandle cancellationHandle) const {
  auto responseGenerator = ExportQueryExecutionTrees::computeResult(
      plannedQuery.parsedQuery_, qet, mediaType, requestTimer,
      std::move(cancellationHandle));

  auto response = ad_utility::httpUtils::createOkResponse(
      std::move(responseGenerator), request, mediaType);
  try {
    co_await send(std::move(response));
  } catch (const boost::system::system_error& e) {
    // "Broken Pipe" errors are thrown and reported by `streamable_body`,
    // so we can safely ignore these kind of exceptions. In practice this
    // should only ever "commonly" happen with `CancellationException`s.
    if (e.code().value() == EPIPE) {
      co_return;
    }
    LOG(ERROR) << "Unexpected error while sending response: " << e.what()
               << std::endl;
  } catch (const std::exception& e) {
    // Even if an exception is thrown here for some unknown reason, don't
    // propagate it, and log it directly, so the code doesn't try to send
    // an HTTP response containing the error message onto a HTTP stream
    // that is already partially written. The only way to pass metadata
    // after the beginning is by using the trailer mechanism as described
    // here:
    // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Trailer#chunked_transfer_encoding_using_a_trailing_header
    // This won't be treated as an error by any regular HTTP client, so
    // while it might be worth implementing to have some sort of validation
    // check, it isn't even shown by curl by default let alone in the
    // browser. Currently though it looks like boost.beast does simply not
    // properly terminate the connection if an error occurs which does
    // provide a somewhat cryptic error message when using curl, but is
    // better than silently failing.
    LOG(ERROR) << e.what() << std::endl;
  }
}

// ____________________________________________________________________________
CPP_template_def(typename RequestT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    std::vector<ad_utility::MediaType> Server::determineMediaTypes(
        const ad_utility::url_parser::ParamValueMap& params,
        const RequestT& request) {
  using namespace ad_utility::url_parser;
  // The following code block determines the media type to be used for the
  // result. The media type is either determined by the "Accept:" header of
  // the request or by the URL parameter "action=..." (for TSV and CSV export,
  // for QLever-historical reasons).
  std::optional<MediaType> mediaType = std::nullopt;

  // The explicit `action=..._export` parameter have precedence over the
  // `Accept:...` header field
  if (checkParameter(params, "action", "csv_export")) {
    mediaType = MediaType::csv;
  } else if (checkParameter(params, "action", "tsv_export")) {
    mediaType = MediaType::tsv;
  } else if (checkParameter(params, "action", "qlever_json_export")) {
    mediaType = MediaType::qleverJson;
  } else if (checkParameter(params, "action", "sparql_json_export")) {
    mediaType = MediaType::sparqlJson;
  } else if (checkParameter(params, "action", "turtle_export")) {
    mediaType = MediaType::turtle;
  } else if (checkParameter(params, "action", "binary_export")) {
    mediaType = MediaType::octetStream;
  }

  std::string_view acceptHeader = request.base()[http::field::accept];

  if (mediaType.has_value()) {
    return {mediaType.value()};
  }

  try {
    return ad_utility::getMediaTypesFromAcceptHeader(acceptHeader);
  } catch (const std::exception& e) {
    throw HttpError(http::status::not_acceptable, e.what());
  }
}

// ____________________________________________________________________________
CPP_template_def(typename RequestT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    ad_utility::websocket::MessageSender Server::createMessageSender(
        const std::weak_ptr<ad_utility::websocket::QueryHub>& queryHub,
        const RequestT& request, std::string_view operation) {
  auto queryHubLock = queryHub.lock();
  AD_CORRECTNESS_CHECK(queryHubLock);
  ad_utility::websocket::MessageSender messageSender{
      getQueryId(request, operation), *queryHubLock};
  return messageSender;
}

// _____________________________________________________________________________
ad_utility::MediaType Server::chooseBestFittingMediaType(
    const std::vector<ad_utility::MediaType>& candidates,
    const ParsedQuery& parsedQuery) {
  if (!candidates.empty()) {
    auto it = ql::ranges::find_if(candidates, [&parsedQuery](
                                                  MediaType mediaType) {
      if (parsedQuery.hasAskClause()) {
        std::array supportedMediaTypes{
            MediaType::sparqlXml, MediaType::sparqlJson, MediaType::qleverJson};
        return ad_utility::contains(supportedMediaTypes, mediaType);
      }
      if (parsedQuery.hasSelectClause()) {
        std::array supportedMediaTypes{
            MediaType::octetStream, MediaType::csv,
            MediaType::tsv,         MediaType::qleverJson,
            MediaType::sparqlXml,   MediaType::sparqlJson};
        return ad_utility::contains(supportedMediaTypes, mediaType);
      }
      std::array supportedMediaTypes{MediaType::csv, MediaType::tsv,
                                     MediaType::qleverJson, MediaType::turtle};
      return ad_utility::contains(supportedMediaTypes, mediaType);
    });
    if (it != candidates.end()) {
      return *it;
    }
  }

  return parsedQuery.hasConstructClause() ? MediaType::turtle
                                          : MediaType::sparqlJson;
}

// ____________________________________________________________________________
CPP_template_def(typename RequestT, typename ResponseT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::processQuery(
        const ad_utility::url_parser::ParamValueMap& params,
        ParsedQuery&& query, const ad_utility::Timer& requestTimer,
        ad_utility::SharedCancellationHandle cancellationHandle,
        QueryExecutionContext& qec, const RequestT& request, ResponseT&& send,
        TimeLimit timeLimit, std::optional<PlannedQuery>& plannedQuery) {
  AD_CORRECTNESS_CHECK(!query.hasUpdateClause());

  auto mediaTypes = determineMediaTypes(params, request);
  AD_LOG_INFO << "Requested media types of the result are: "
              << absl::StrJoin(
                     mediaTypes | ql::views::transform([](MediaType mediaType) {
                       return absl::StrCat(
                           "\"", ad_utility::toString(mediaType), "\"");
                     }),
                     ", ")
              << std::endl;

  // The usage of an `optional` here is required because of a limitation in
  // Boost::Asio which forces us to use default-constructible result types with
  // `computeInNewThread`. We also can't unwrap the optional directly in this
  // function, because then the conan build fails in a very strange way,
  // probably related to issues in GCC's coroutine implementation.
  // For the same reason (crashes in the conanbuild) we store the coroutine in
  // an explicit variable instead of directly `co_await`-ing it.
  auto coroutine = computeInNewThread(
      queryThreadPool_,
      [this, &query, &requestTimer, &timeLimit, &qec,
       &cancellationHandle]() -> std::optional<PlannedQuery> {
        return this->planQuery(std::move(query), requestTimer, timeLimit, qec,
                               cancellationHandle);
      },
      cancellationHandle);
  plannedQuery = co_await std::move(coroutine);
  auto qet = plannedQuery.value().queryExecutionTree_;

  MediaType mediaType =
      chooseBestFittingMediaType(mediaTypes, plannedQuery.value().parsedQuery_);

  // Update the `PlannedQuery` with the export limit when the response
  // content-type is `application/qlever-results+json` and ensure that the
  // offset is not applied twice when exporting the query.
  adjustParsedQueryLimitOffset(plannedQuery.value(), mediaType, params);

  // This actually processes the query and sends the result in the
  // requested format.
  co_await sendStreamableResponse(request, AD_FWD(send), mediaType,
                                  plannedQuery.value(),
                                  plannedQuery.value().queryExecutionTree_,
                                  requestTimer, cancellationHandle);

  // Print the runtime info. This needs to be done after the query
  // was computed.
  LOG(INFO) << "Done processing query and sending result"
            << ", total time was " << requestTimer.msecs().count() << " ms"
            << std::endl;

  // Log that we are done with the query and how long it took.
  //
  // TODO<joka921> Also log an identifier of the query.
  LOG(DEBUG) << "Runtime Info:\n"
             << plannedQuery.value()
                    .queryExecutionTree_.getRootOperation()
                    ->runtimeInfo()
                    .toString()
             << std::endl;
  co_return;
}

nlohmann::json Server::createResponseMetadataForUpdate(
    const ad_utility::Timer& requestTimer, const Index& index,
    const DeltaTriples& deltaTriples, const PlannedQuery& plannedQuery,
    const QueryExecutionTree& qet, const DeltaTriplesCount& countBefore,
    const UpdateMetadata& updateMetadata, const DeltaTriplesCount& countAfter) {
  auto formatTime = [](std::chrono::milliseconds time) {
    return absl::StrCat(time.count(), "ms");
  };

  json response;
  response["update"] = ad_utility::truncateOperationString(
      plannedQuery.parsedQuery_._originalString);
  response["status"] = "OK";
  auto warnings = qet.collectWarnings();
  warnings.emplace(warnings.begin(),
                   "SPARQL 1.1 Update for QLever is experimental.");
  response["warnings"] = warnings;
  RuntimeInformationWholeQuery& runtimeInfoWholeOp =
      qet.getRootOperation()->getRuntimeInfoWholeQuery();
  RuntimeInformation& runtimeInfo = qet.getRootOperation()->runtimeInfo();
  response["runtimeInformation"]["meta"] =
      nlohmann::ordered_json(runtimeInfoWholeOp);
  response["runtimeInformation"]["query_execution_tree"] =
      nlohmann::ordered_json(runtimeInfo);
  response["delta-triples"]["before"] = nlohmann::json(countBefore);
  response["delta-triples"]["after"] = nlohmann::json(countAfter);
  response["delta-triples"]["difference"] =
      nlohmann::json(countAfter - countBefore);
  if (updateMetadata.inUpdate_.has_value()) {
    response["delta-triples"]["operation"] =
        json(updateMetadata.inUpdate_.value());
  }
  response["time"]["planning"] =
      formatTime(runtimeInfoWholeOp.timeQueryPlanning);
  response["time"]["where"] =
      formatTime(std::chrono::duration_cast<std::chrono::milliseconds>(
          runtimeInfo.totalTime_));
  json updateTime{
      {"total", formatTime(updateMetadata.triplePreparationTime_ +
                           updateMetadata.deletionTime_ +
                           updateMetadata.insertionTime_)},
      {"preparation", formatTime(updateMetadata.triplePreparationTime_)},
      {"delete", formatTime(updateMetadata.deletionTime_)},
      {"insert", formatTime(updateMetadata.insertionTime_)}};
  response["time"]["update"] = updateTime;
  response["time"]["total"] = formatTime(requestTimer.msecs());
  for (auto permutation : Permutation::ALL) {
    response["located-triples"][Permutation::toString(
        permutation)]["blocks-affected"] =
        deltaTriples.getLocatedTriplesForPermutation(permutation).numBlocks();
    auto numBlocks = index.getPimpl()
                         .getPermutation(permutation)
                         .metaData()
                         .blockData()
                         .size();
    response["located-triples"][Permutation::toString(permutation)]
            ["blocks-total"] = numBlocks;
  }
  return response;
}
// ____________________________________________________________________________
nlohmann::json Server::processUpdateImpl(
    const PlannedQuery& plannedUpdate, const ad_utility::Timer& requestTimer,
    ad_utility::SharedCancellationHandle cancellationHandle,
    DeltaTriples& deltaTriples) {
  const auto& qet = plannedUpdate.queryExecutionTree_;
  AD_CORRECTNESS_CHECK(plannedUpdate.parsedQuery_.hasUpdateClause());

  DeltaTriplesCount countBefore = deltaTriples.getCounts();
  UpdateMetadata updateMetadata =
      ExecuteUpdate::executeUpdate(index_, plannedUpdate.parsedQuery_, qet,
                                   deltaTriples, cancellationHandle);
  DeltaTriplesCount countAfter = deltaTriples.getCounts();

  LOG(INFO) << "Done processing update"
            << ", total time was " << requestTimer.msecs().count() << " ms"
            << std::endl;
  LOG(DEBUG) << "Runtime Info:\n"
             << qet.getRootOperation()->runtimeInfo().toString() << std::endl;

  // Clear the cache, because all cache entries have been invalidated by
  // the update anyway (The index of the located triples snapshot is
  // part of the cache key).
  cache_.clearAll();

  return createResponseMetadataForUpdate(requestTimer, index_, deltaTriples,
                                         plannedUpdate, qet, countBefore,
                                         updateMetadata, countAfter);
}

// ____________________________________________________________________________
CPP_template_def(typename RequestT, typename ResponseT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::processUpdate(
        std::vector<ParsedQuery>&& updates,
        const ad_utility::Timer& requestTimer,
        ad_utility::SharedCancellationHandle cancellationHandle,
        QueryExecutionContext& qec, const RequestT& request, ResponseT&& send,
        TimeLimit timeLimit, std::optional<PlannedQuery>& plannedUpdate) {
  AD_CORRECTNESS_CHECK(ql::ranges::all_of(
      updates, [](const ParsedQuery& p) { return p.hasUpdateClause(); }));

  // If multiple updates are part of a single request, those have to run
  // atomically. This is ensured, because the updates below are run on the
  // `updateThreadPool_`, which only has a single thread.
  static_assert(UPDATE_THREAD_POOL_SIZE == 1);
  auto coroutine = computeInNewThread(
      updateThreadPool_,
      [this, &requestTimer, &cancellationHandle, &updates, &qec, &timeLimit,
       &plannedUpdate]() {
        json results = json::array();
        // TODO<qup42> We currently create a new snapshot after each update in
        // the chain, which is expensive. Instead, the updates could operate
        // directly on the `DeltaTriples` (we have an exclusive lock on them
        // anyway).
        for (ParsedQuery& update : updates) {
          // Make the snapshot before the query planning. Otherwise, it could
          // happen that the query planner "knows" that a result is empty, when
          // actually it is not due to a preceding update in the chain. Also,
          // this improves the size estimates and hence the query plan.
          qec.updateLocatedTriplesSnapshot();
          plannedUpdate = planQuery(std::move(update), requestTimer, timeLimit,
                                    qec, cancellationHandle);
          // Update the delta triples.
          results.push_back(index_.deltaTriplesManager().modify<nlohmann::json>(
              [this, &requestTimer, &cancellationHandle,
               &plannedUpdate](auto& deltaTriples) {
                // Use `this` explicitly to silence false-positive
                // errors on captured `this` being unused.
                return this->processUpdateImpl(plannedUpdate.value(),
                                               requestTimer, cancellationHandle,
                                               deltaTriples);
              }));
        }
        return results;
      },
      cancellationHandle);
  auto response = co_await std::move(coroutine);

  // SPARQL 1.1 Protocol 2.2.4 Successful Responses: "The response body
  // of a successful update request is implementation defined."
  co_await send(
      ad_utility::httpUtils::createJsonResponse(std::move(response), request));
  co_return;
}

// ____________________________________________________________________________
CPP_template_def(typename VisitorT, typename RequestT, typename ResponseT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::processOperation(
        ad_utility::url_parser::sparqlOperation::Operation operation,
        VisitorT visitor, const ad_utility::Timer& requestTimer,
        const RequestT& request, ResponseT& send,
        const std::optional<PlannedQuery>& plannedQuery) {
  // Copy the operation string for the error case before processing the
  // operation, because processing moves it.
  const std::string operationString = [&operation] {
    if (auto* q = std::get_if<Query>(&operation)) {
      return q->query_;
    }
    if (auto* u = std::get_if<Update>(&operation)) {
      return u->update_;
    }
    if (std::holds_alternative<GraphStoreOperation>(operation)) {
      return std::string(
          "No operation string available for Graph Store Operation");
    }
    AD_CORRECTNESS_CHECK(std::holds_alternative<None>(operation));
    return std::string(
        "No operation string available, because operation type is "
        "unknown.");
  }();
  using namespace ad_utility::httpUtils;
  http::status responseStatus = http::status::ok;

  // Put the whole query processing in a try-catch block. If any
  // exception occurs, log the error message and send a JSON response
  // with all the details to the client. Note that the C++ standard
  // forbids co_await in the catch block, hence the workaround with the
  // optional `exceptionErrorMsg`.
  std::optional<std::string> exceptionErrorMsg;
  std::optional<ExceptionMetadata> metadata;
  try {
    co_return co_await std::visit(visitor, std::move(operation));
  } catch (const HttpError& e) {
    responseStatus = e.status();
    exceptionErrorMsg = e.what();
  } catch (const ParseException& e) {
    responseStatus = http::status::bad_request;
    exceptionErrorMsg = e.errorMessageWithoutPositionalInfo();
    metadata = e.metadata();
  } catch (const QueryAlreadyInUseError& e) {
    responseStatus = http::status::conflict;
    exceptionErrorMsg = e.what();
  } catch (const ad_utility::CancellationException& e) {
    // Send 429 status code to indicate that the time limit was reached
    // or the query was cancelled because of some other reason.
    responseStatus = http::status::too_many_requests;
    exceptionErrorMsg = e.what();
  } catch (const std::exception& e) {
    responseStatus = http::status::internal_server_error;
    exceptionErrorMsg = e.what();
  }
  // TODO<qup42> at this stage should probably have a wrapper that takes
  //  optional<errorMsg> and optional<metadata> and does this logic
  if (exceptionErrorMsg) {
    LOG(ERROR) << exceptionErrorMsg.value() << std::endl;
    if (metadata) {
      // The `coloredError()` message might fail because of the
      // different Unicode handling of QLever and ANTLR. Make sure to
      // detect this case so that we can fix it if it happens.
      try {
        LOG(ERROR) << metadata.value().coloredError() << std::endl;
      } catch (const std::exception& e) {
        exceptionErrorMsg.value().append(absl::StrCat(
            " Highlighting an error for the command line log failed: ",
            e.what()));
        LOG(ERROR) << "Failed to highlight error in operation. " << e.what()
                   << std::endl;
        LOG(ERROR) << metadata.value().query_ << std::endl;
      }
    }
    auto errorResponseJson = composeErrorResponseJson(
        operationString, exceptionErrorMsg.value(), requestTimer, metadata);
    if (plannedQuery.has_value()) {
      errorResponseJson["runtimeInformation"] =
          nlohmann::ordered_json(plannedQuery.value()
                                     .queryExecutionTree_.getRootOperation()
                                     ->runtimeInfo());
    }
    auto errResponse =
        createJsonResponse(errorResponseJson, request, responseStatus);
    co_return co_await send(std::move(errResponse));
  }
}

// _____________________________________________________________________________
template <std::invocable Function, typename T>
Awaitable<T> Server::computeInNewThread(net::static_thread_pool& threadPool,
                                        Function function,
                                        SharedCancellationHandle handle) {
  // `interruptible` will set the shared state of this promise
  // with a function that can be used to cancel the timer.
  std::promise<std::function<void()>> cancelTimerPromise{};
  auto cancelTimerFuture = cancelTimerPromise.get_future();

  auto inner = [function = std::move(function),
                cancelTimerFuture =
                    std::move(cancelTimerFuture)]() mutable -> T {
    // Ensure future is ready by the time this is called.
    AD_CORRECTNESS_CHECK(cancelTimerFuture.wait_for(std::chrono::milliseconds{
                             0}) == std::future_status::ready);
    cancelTimerFuture.get()();
    return std::invoke(std::move(function));
  };
  // interruptible doesn't make the awaitable return faster when
  // cancelled, this might still block. However it will make the code
  // check the cancellation handle while waiting for a thread in the
  // pool to become ready.
  return ad_utility::interruptible(
      ad_utility::runFunctionOnExecutor(threadPool.get_executor(),
                                        std::move(inner), net::use_awaitable),
      std::move(handle), std::move(cancelTimerPromise));
}

// _____________________________________________________________________________
bool Server::checkAccessToken(
    std::optional<std::string_view> accessToken) const {
  if (!accessToken) {
    return false;
  }
  const auto accessTokenProvidedMsg = "Access token was provided";
  if (accessToken_.empty()) {
    throw std::runtime_error(
        absl::StrCat(accessTokenProvidedMsg,
                     " but server was started without --access-token"));
  } else if (!ad_utility::constantTimeEquals(accessToken.value(),
                                             accessToken_)) {
    throw std::runtime_error(
        absl::StrCat(accessTokenProvidedMsg, " but it was invalid"));
  } else {
    LOG(DEBUG) << accessTokenProvidedMsg << " and correct" << std::endl;
    return true;
  }
}

// _____________________________________________________________________________
void Server::adjustParsedQueryLimitOffset(
    PlannedQuery& plannedQuery, const ad_utility::MediaType& mediaType,
    const ad_utility::url_parser::ParamValueMap& parameters) {
  // Read the export limit from the `send` parameter (historical name).
  // This limits the number of bindings exported in
  // `ExportQueryExecutionTrees`. It should only have an effect for the
  // QLever JSON export.
  auto& limitOffset = plannedQuery.parsedQuery_._limitOffset;
  auto& exportLimit = limitOffset.exportLimit_;
  auto sendParameter =
      ad_utility::url_parser::getParameterCheckAtMostOnce(parameters, "send");
  if (sendParameter.has_value() && mediaType == MediaType::qleverJson) {
    exportLimit = std::stoul(sendParameter.value());
  }
}
