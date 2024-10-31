// Copyright 2011 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/Server.h"

#include <boost/url.hpp>
#include <sstream>
#include <string>
#include <vector>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "global/RuntimeParameters.h"
#include "util/AsioHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/ParseableDuration.h"
#include "util/http/HttpUtils.h"
#include "util/http/websocket/MessageSender.h"

using namespace std::string_literals;

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
      threadPool_{numThreads} {
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
void Server::initialize(const string& indexBaseName, bool useText,
                        bool usePatterns, bool loadAllPermutations) {
  LOG(INFO) << "Initializing server ..." << std::endl;

  index_.usePatterns() = usePatterns;
  index_.loadAllPermutations() = loadAllPermutations;

  // Init the index.
  index_.createFromOnDiskIndex(indexBaseName);
  if (useText) {
    index_.addTextFromOnDiskIndex();
  }

  sortPerformanceEstimator_.computeEstimatesExpensively(
      allocator_, index_.numTriples().normalAndInternal_() *
                      PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);

  LOG(INFO) << "Access token for restricted API calls is \"" << accessToken_
            << "\"" << std::endl;
  LOG(INFO) << "The server is ready, listening for requests on port "
            << std::to_string(port_) << " ..." << std::endl;
}

// _____________________________________________________________________________
void Server::run(const string& indexBaseName, bool useText, bool usePatterns,
                 bool loadAllPermutations) {
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
    try {
      co_await process(request, sendWithAccessControlHeaders);
    } catch (const std::exception& e) {
      exceptionErrorMsg = e.what();
    }
    if (exceptionErrorMsg) {
      LOG(ERROR) << exceptionErrorMsg.value() << std::endl;
      auto badRequestResponse = createBadRequestResponse(
          absl::StrCat(exceptionErrorMsg.value(), "\n"), request);
      co_await sendWithAccessControlHeaders(std::move(badRequestResponse));
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
  initialize(indexBaseName, useText, usePatterns, loadAllPermutations);

  // Start listening for connections on the server.
  httpServer.run();
}

// _____________________________________________________________________________
ad_utility::url_parser::ParsedRequest Server::parseHttpRequest(
    const ad_utility::httpUtils::HttpRequest auto& request) {
  // For an HTTP request, `request.target()` yields the HTTP Request-URI.
  // This is a concatenation of the URL path and the query strings.
  using namespace ad_utility::url_parser::sparqlOperation;
  auto parsedUrl = ad_utility::url_parser::parseRequestTarget(request.target());
  ad_utility::url_parser::ParsedRequest parsedRequest{
      std::move(parsedUrl.path_), std::move(parsedUrl.parameters_), None{}};

  // Some valid requests (e.g. QLever's custom commands like retrieving index
  // statistics) don't have a query. So an empty operation is not necessarily an
  // error.
  auto setOperationIfSpecifiedInParams =
      [&parsedRequest]<typename Operation>(string_view paramName) {
        auto operation = ad_utility::url_parser::getParameterCheckAtMostOnce(
            parsedRequest.parameters_, paramName);
        if (operation.has_value()) {
          parsedRequest.operation_ = Operation{operation.value()};
          parsedRequest.parameters_.erase(paramName);
        }
      };

  if (request.method() == http::verb::get) {
    setOperationIfSpecifiedInParams.template operator()<Query>("query");
    if (parsedRequest.parameters_.contains("update")) {
      throw std::runtime_error("SPARQL Update is not allowed as GET request.");
    }
    return parsedRequest;
  }
  if (request.method() == http::verb::post) {
    // For a POST request, the content type *must* be either
    // "application/x-www-form-urlencoded" (1), "application/sparql-query"
    // (2) or "application/sparql-update" (3).
    //
    // (1) Section 2.1.2: The body of the POST request contains *all* parameters
    // (including the query or update) in an encoded form (just like in the part
    // of a GET request after the "?").
    //
    // (2) Section 2.1.3: The body of the POST request contains *only* the
    // unencoded SPARQL query. There may be additional HTTP query parameters.
    //
    // (3) Section 2.2.2: The body of the POST request contains *only* the
    // unencoded SPARQL update. There may be additional HTTP query parameters.
    //
    // Reference: https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321
    std::string_view contentType = request.base()[http::field::content_type];
    LOG(DEBUG) << "Content-type: \"" << contentType << "\"" << std::endl;
    static constexpr std::string_view contentTypeUrlEncoded =
        "application/x-www-form-urlencoded";
    static constexpr std::string_view contentTypeSparqlQuery =
        "application/sparql-query";
    static constexpr std::string_view contentTypeSparqlUpdate =
        "application/sparql-update";

    // Note: For simplicity we only check via `starts_with`. This ignores
    // additional parameters like `application/sparql-query;charset=utf8`. We
    // currently always expect UTF-8.
    // TODO<joka921> Implement more complete parsing that allows the checking of
    // these parameters.
    if (contentType.starts_with(contentTypeUrlEncoded)) {
      // All parameters must be included in the request body for URL-encoded
      // POST. The HTTP query string parameters must be empty. See SPARQL 1.1
      // Protocol Sections 2.1.2
      if (!parsedRequest.parameters_.empty()) {
        throw std::runtime_error(
            "URL-encoded POST requests must not contain query parameters in "
            "the URL.");
      }

      // Set the url-encoded parameters from the request body.
      // Note: previously we used `boost::urls::parse_query`, but that function
      // doesn't unescape the `+` which encodes a space character. The following
      // workaround of making the url-encoded parameters a complete relative url
      // and parsing this URL seems to work.
      // Note: We have to bind the result of `StrCat` to an explicit variable,
      // as the `boost::urls` parsing routines only give back a view, which
      // otherwise would be dangling.
      auto bodyAsQuery = absl::StrCat("/?", request.body());
      auto query = boost::urls::parse_origin_form(bodyAsQuery);
      if (!query) {
        throw std::runtime_error(
            "Invalid URL-encoded POST request, body was: " + request.body());
      }
      parsedRequest.parameters_ =
          ad_utility::url_parser::paramsToMap(query->params());

      if (parsedRequest.parameters_.contains("query") &&
          parsedRequest.parameters_.contains("update")) {
        throw std::runtime_error(
            R"(Request must only contain one of "query" and "update".)");
      }
      setOperationIfSpecifiedInParams.template operator()<Query>("query");
      setOperationIfSpecifiedInParams.template operator()<Update>("update");

      return parsedRequest;
    }
    if (contentType.starts_with(contentTypeSparqlQuery)) {
      parsedRequest.operation_ = Query{request.body()};
      return parsedRequest;
    }
    if (contentType.starts_with(contentTypeSparqlUpdate)) {
      parsedRequest.operation_ = Update{request.body()};
      return parsedRequest;
    }
    throw std::runtime_error(absl::StrCat(
        "POST request with content type \"", contentType,
        "\" not supported (must be \"", contentTypeUrlEncoded, "\", \"",
        contentTypeSparqlQuery, "\" or \"", contentTypeSparqlUpdate, "\")"));
  }
  std::ostringstream requestMethodName;
  requestMethodName << request.method();
  throw std::runtime_error(
      absl::StrCat("Request method \"", requestMethodName.str(),
                   "\" not supported (has to be GET or POST)"));
};

// _____________________________________________________________________________

net::awaitable<std::optional<Server::TimeLimit>>
Server::verifyUserSubmittedQueryTimeout(
    std::optional<std::string_view> userTimeout, bool accessTokenOk,
    const ad_utility::httpUtils::HttpRequest auto& request, auto& send) const {
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

// _____________________________________________________________________________
Awaitable<void> Server::process(
    const ad_utility::httpUtils::HttpRequest auto& request, auto&& send) {
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
  const auto parsedHttpRequest = parseHttpRequest(request);
  const auto& parameters = parsedHttpRequest.parameters_;

  auto checkParameterNotPresent = [&parameters](
                                      const std::string& parameterName) {
    if (parameters.contains(parameterName)) {
      throw NotSupportedException(absl::StrCat(
          parameterName, " parameter is currently not supported by QLever."));
    }
  };
  checkParameterNotPresent("default-graph-uri");
  checkParameterNotPresent("named-graph-uri");

  auto checkParameter = [&parameters](std::string_view key,
                                      std::optional<std::string> value,
                                      bool accessAllowed = true) {
    return Server::checkParameter(parameters, key, std::move(value),
                                  accessAllowed);
  };

  // Check the access token. If an access token is provided and the check fails,
  // throw an exception and do not process any part of the query (even if the
  // processing had been allowed without access token).
  bool accessTokenOk =
      checkAccessToken(checkParameter("access-token", std::nullopt));

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
  } else if (auto cmd =
                 checkParameter("cmd", "clear-cache-complete", accessTokenOk)) {
    logCommand(cmd, "clear cache completely (including unpinned elements)");
    cache_.clearAll();
    response = createJsonResponse(composeCacheStatsJson(), request);
  } else if (auto cmd = checkParameter("cmd", "get-settings")) {
    logCommand(cmd, "get server settings");
    response = createJsonResponse(RuntimeParameters().toMap(), request);
  } else if (auto cmd = checkParameter("cmd", "get-index-id")) {
    logCommand(cmd, "get index ID");
    response =
        createOkResponse(index_.getIndexId(), request, MediaType::textPlain);
  } else if (auto cmd =
                 checkParameter("cmd", "dump-active-queries", accessTokenOk)) {
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
  if (auto description =
          checkParameter("index-description", std::nullopt, accessTokenOk)) {
    LOG(INFO) << "Setting index description to: \"" << description.value()
              << "\"" << std::endl;
    index_.setKbName(std::string{description.value()});
    response = createJsonResponse(composeStatsJson(), request);
  }

  // Set description of text index.
  if (auto description =
          checkParameter("text-description", std::nullopt, accessTokenOk)) {
    LOG(INFO) << "Setting text description to: \"" << description.value()
              << "\"" << std::endl;
    index_.setTextName(std::string{description.value()});
    response = createJsonResponse(composeStatsJson(), request);
  }

  // Set one or several of the runtime parameters.
  for (auto key : RuntimeParameters().getKeys()) {
    if (auto value = checkParameter(key, std::nullopt, accessTokenOk)) {
      LOG(INFO) << "Setting runtime parameter \"" << key << "\""
                << " to value \"" << value.value() << "\"" << std::endl;
      RuntimeParameters().set(key, std::string{value.value()});
      response = createJsonResponse(RuntimeParameters().toMap(), request);
    }
  }

  auto visitQuery = [&checkParameter, &accessTokenOk, &request, &send,
                     &parameters, &requestTimer,
                     this](ad_utility::url_parser::sparqlOperation::Query query)
      -> Awaitable<void> {
    if (auto timeLimit = co_await verifyUserSubmittedQueryTimeout(
            checkParameter("timeout", std::nullopt), accessTokenOk, request,
            send)) {
      co_return co_await processQuery(parameters, query.query_, requestTimer,
                                      std::move(request), send,
                                      timeLimit.value());
    } else {
      // If the optional is empty, this indicates an error response has been
      // sent to the client already. We can stop here.
      co_return;
    }
  };
  auto visitUpdate = [](const ad_utility::url_parser::sparqlOperation::Update&)
      -> Awaitable<void> {
    throw std::runtime_error(
        "SPARQL 1.1 Update is  currently not supported by QLever.");
  };
  auto visitNone =
      [&response, &send, &request](
          ad_utility::url_parser::sparqlOperation::None) -> Awaitable<void> {
    // If there was no "query", but any of the URL parameters processed before
    // produced a `response`, send that now. Note that if multiple URL
    // parameters were processed, only the `response` from the last one is sent.
    if (response.has_value()) {
      co_return co_await send(std::move(response.value()));
    }

    // At this point, if there is a "?" in the query string, it means that there
    // are URL parameters which QLever does not know or did not process.
    if (request.target().find("?") != std::string::npos) {
      throw std::runtime_error(
          "Request with URL parameters, but none of them could be processed");
    }
    // No path matched up until this point, so return 404 to indicate the client
    // made an error and the server will not serve anything else.
    co_return co_await send(
        createNotFoundResponse("Unknown path", std::move(request)));
  };

  co_return co_await std::visit(
      ad_utility::OverloadCallOperator{visitQuery, visitUpdate, visitNone},
      parsedHttpRequest.operation_);
}

// _____________________________________________________________________________
json Server::composeErrorResponseJson(
    const string& query, const std::string& errorMsg,
    ad_utility::Timer& requestTimer,
    const std::optional<ExceptionMetadata>& metadata) {
  json j;
  using ad_utility::Timer;
  j["query"] = query;
  j["status"] = "ERROR";
  j["resultsize"] = 0;
  j["time"]["total"] = requestTimer.msecs().count();
  j["time"]["computeResult"] = requestTimer.msecs().count();
  j["exception"] = errorMsg;

  if (metadata.has_value()) {
    auto& value = metadata.value();
    j["metadata"]["startIndex"] = value.startIndex_;
    j["metadata"]["stopIndex"] = value.stopIndex_;
    j["metadata"]["line"] = value.line_;
    j["metadata"]["positionInLine"] = value.charPositionInLine_;
    // The ANTLR parser may not see the whole query. (The reason is value mixing
    // of the old and new parser.) To detect/work with this we also transmit
    // what ANTLR saw as query.
    // TODO<qup42> remove once the whole query is parsed with ANTLR.
    j["metadata"]["query"] = value.query_;
  }

  return j;
}

// _____________________________________________________________________________
json Server::composeStatsJson() const {
  json result;
  result["name-index"] = index_.getKbName();
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

// _____________________________________________

ad_utility::websocket::OwningQueryId Server::getQueryId(
    const ad_utility::httpUtils::HttpRequest auto& request,
    std::string_view query) {
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

auto Server::cancelAfterDeadline(
    std::weak_ptr<ad_utility::CancellationHandle<>> cancellationHandle,
    TimeLimit timeLimit)
    -> ad_utility::InvocableWithExactReturnType<void> auto {
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
    -> ad_utility::isInstantiation<
        CancellationHandleAndTimeoutTimerCancel> auto {
  auto cancellationHandle = queryRegistry_.getCancellationHandle(queryId);
  AD_CORRECTNESS_CHECK(cancellationHandle);
  cancellationHandle->startWatchDog();
  absl::Cleanup cancelCancellationHandle{
      cancelAfterDeadline(cancellationHandle, timeLimit)};
  return CancellationHandleAndTimeoutTimerCancel{
      std::move(cancellationHandle), std::move(cancelCancellationHandle)};
}

// _____________________________________________________________________________
Awaitable<void> Server::sendStreamableResponse(
    const ad_utility::httpUtils::HttpRequest auto& request, auto& send,
    MediaType mediaType, const PlannedQuery& plannedQuery,
    const QueryExecutionTree& qet, ad_utility::Timer& requestTimer,
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
boost::asio::awaitable<void> Server::processQuery(
    const ad_utility::url_parser::ParamValueMap& params, const string& query,
    ad_utility::Timer& requestTimer,
    const ad_utility::httpUtils::HttpRequest auto& request, auto&& send,
    TimeLimit timeLimit) {
  using namespace ad_utility::httpUtils;

  auto sendJson =
      [&request, &send](
          const json& jsonString,
          http::status responseStatus) -> boost::asio::awaitable<void> {
    auto response = createJsonResponse(jsonString, request, responseStatus);
    co_return co_await send(std::move(response));
  };

  http::status responseStatus = http::status::ok;

  // Put the whole query processing in a try-catch block. If any exception
  // occurs, log the error message and send a JSON response with all the details
  // to the client. Note that the C++ standard forbids co_await in the catch
  // block, hence the workaround with the optional `exceptionErrorMsg`.
  std::optional<std::string> exceptionErrorMsg;
  std::optional<ExceptionMetadata> metadata;
  // Also store the QueryExecutionTree outside the try-catch block to gain
  // access to the runtimeInformation in the case of an error.
  std::optional<PlannedQuery> plannedQuery;
  try {
    auto containsParam = [&params](const std::string& param,
                                   const std::string& expected) {
      auto parameterValue =
          ad_utility::url_parser::getParameterCheckAtMostOnce(params, param);
      return parameterValue.has_value() && parameterValue.value() == expected;
    };
    const bool pinSubtrees = containsParam("pinsubtrees", "true");
    const bool pinResult = containsParam("pinresult", "true");
    LOG(INFO) << "Processing the following SPARQL query:"
              << (pinResult ? " [pin result]" : "")
              << (pinSubtrees ? " [pin subresults]" : "") << "\n"
              << query << std::endl;

    // The following code block determines the media type to be used for the
    // result. The media type is either determined by the "Accept:" header of
    // the request or by the URL parameter "action=..." (for TSV and CSV export,
    // for QLever-historical reasons).

    std::optional<MediaType> mediaType = std::nullopt;

    // The explicit `action=..._export` parameter have precedence over the
    // `Accept:...` header field
    if (containsParam("action", "csv_export")) {
      mediaType = MediaType::csv;
    } else if (containsParam("action", "tsv_export")) {
      mediaType = MediaType::tsv;
    } else if (containsParam("action", "qlever_json_export")) {
      mediaType = MediaType::qleverJson;
    } else if (containsParam("action", "sparql_json_export")) {
      mediaType = MediaType::sparqlJson;
    } else if (containsParam("action", "turtle_export")) {
      mediaType = MediaType::turtle;
    } else if (containsParam("action", "binary_export")) {
      mediaType = MediaType::octetStream;
    }

    std::string_view acceptHeader = request.base()[http::field::accept];

    if (!mediaType.has_value()) {
      mediaType = ad_utility::getMediaTypeFromAcceptHeader(acceptHeader);
    }

    // TODO<c++23> use std::optional::transform
    std::optional<uint64_t> maxSend = std::nullopt;
    auto parameterValue =
        ad_utility::url_parser::getParameterCheckAtMostOnce(params, "send");
    if (parameterValue.has_value()) {
      maxSend = std::stoul(parameterValue.value());
    }
    // Limit JSON requests by default
    if (!maxSend.has_value() && (mediaType == MediaType::sparqlJson ||
                                 mediaType == MediaType::qleverJson)) {
      maxSend = MAX_NOF_ROWS_IN_RESULT;
    }

    if (!mediaType.has_value()) {
      co_return co_await send(createBadRequestResponse(
          absl::StrCat("Did not find any supported media type "
                       "in this \'Accept:\' header field: \"",
                       acceptHeader, "\". ",
                       ad_utility::getErrorMessageForSupportedMediaTypes()),
          request));
    }
    AD_CONTRACT_CHECK(mediaType.has_value());
    LOG(INFO) << "Requested media type of result is \""
              << ad_utility::toString(mediaType.value()) << "\"" << std::endl;

    auto queryHub = queryHub_.lock();
    AD_CORRECTNESS_CHECK(queryHub);
    ad_utility::websocket::MessageSender messageSender{
        getQueryId(request, query), *queryHub};
    // Do the query planning. This creates a `QueryExecutionTree`, which will
    // then be used to process the query.
    //
    // NOTE: This should come after determining the media type. Otherwise, it
    // might happen that the query planner runs for a while (recall that it many
    // do index scans) and then we get an error message afterwards that a
    // certain media type is not supported.
    QueryExecutionContext qec(index_, &cache_, allocator_,
                              sortPerformanceEstimator_,
                              std::ref(messageSender), pinSubtrees, pinResult);
    auto [cancellationHandle, cancelTimeoutOnDestruction] =
        setupCancellationHandle(messageSender.getQueryId(), timeLimit);

    auto queryDatasets = ad_utility::url_parser::parseDatasetClauses(params);
    plannedQuery = co_await parseAndPlan(query, queryDatasets, qec,
                                         cancellationHandle, timeLimit);
    AD_CORRECTNESS_CHECK(plannedQuery.has_value());
    auto& qet = plannedQuery.value().queryExecutionTree_;
    qet.isRoot() = true;  // allow pinning of the final result
    auto timeForQueryPlanning = requestTimer.msecs();
    auto& runtimeInfoWholeQuery =
        qet.getRootOperation()->getRuntimeInfoWholeQuery();
    runtimeInfoWholeQuery.timeQueryPlanning = timeForQueryPlanning;
    LOG(INFO) << "Query planning done in " << timeForQueryPlanning.count()
              << " ms" << std::endl;
    LOG(TRACE) << qet.getCacheKey() << std::endl;

    // Apply stricter limit for export if present
    if (maxSend.has_value()) {
      auto& pq = plannedQuery.value().parsedQuery_;
      pq._limitOffset._limit =
          std::min(maxSend.value(), pq._limitOffset.limitOrDefault());
    }
    // Make sure we don't underflow here
    AD_CORRECTNESS_CHECK(
        plannedQuery.value().parsedQuery_._limitOffset._offset >=
        qet.getRootOperation()->getLimit()._offset);
    // Don't apply offset twice, if the offset was not applied to the operation
    // then the exporter can safely apply it during export.
    plannedQuery.value().parsedQuery_._limitOffset._offset -=
        qet.getRootOperation()->getLimit()._offset;

    // This actually processes the query and sends the result in the requested
    // format.
    co_await sendStreamableResponse(request, send, mediaType.value(),
                                    plannedQuery.value(), qet, requestTimer,
                                    cancellationHandle);

    // Print the runtime info. This needs to be done after the query
    // was computed.

    // Log that we are done with the query and how long it took.
    //
    // NOTE: We need to explicitly stop the `requestTimer` here because in the
    // sending code above, it is done only in some cases and not in others (in
    // particular, not for TSV and CSV because for those, the result does not
    // contain timing information).
    //
    // TODO<joka921> Also log an identifier of the query.
    LOG(INFO) << "Done processing query and sending result"
              << ", total time was " << requestTimer.msecs().count() << " ms"
              << std::endl;
    LOG(DEBUG) << "Runtime Info:\n"
               << qet.getRootOperation()->runtimeInfo().toString() << std::endl;
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
      // The `coloredError()` message might fail because of the different
      // Unicode handling of QLever and ANTLR. Make sure to detect this case so
      // that we can fix it if it happens.
      try {
        LOG(ERROR) << metadata.value().coloredError() << std::endl;
      } catch (const std::exception& e) {
        exceptionErrorMsg.value().append(absl::StrCat(
            " Highlighting an error for the command line log failed: ",
            e.what()));
        LOG(ERROR) << "Failed to highlight error in query. " << e.what()
                   << std::endl;
        LOG(ERROR) << metadata.value().query_ << std::endl;
      }
    }
    auto errorResponseJson = composeErrorResponseJson(
        query, exceptionErrorMsg.value(), requestTimer, metadata);
    if (plannedQuery.has_value()) {
      errorResponseJson["runtimeInformation"] =
          nlohmann::ordered_json(plannedQuery.value()
                                     .queryExecutionTree_.getRootOperation()
                                     ->runtimeInfo());
    }
    co_return co_await sendJson(errorResponseJson, responseStatus);
  }
}

// _____________________________________________________________________________
template <std::invocable Function, typename T>
Awaitable<T> Server::computeInNewThread(Function function,
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
  // interruptible doesn't make the awaitable return faster when cancelled,
  // this might still block. However it will make the code check the
  // cancellation handle while waiting for a thread in the pool to become ready.
  return ad_utility::interruptible(
      ad_utility::runFunctionOnExecutor(threadPool_.get_executor(),
                                        std::move(inner), net::use_awaitable),
      std::move(handle), std::move(cancelTimerPromise));
}

// _____________________________________________________________________________
net::awaitable<std::optional<Server::PlannedQuery>> Server::parseAndPlan(
    const std::string& query, const vector<DatasetClause>& queryDatasets,
    QueryExecutionContext& qec, SharedCancellationHandle handle,
    TimeLimit timeLimit) {
  auto handleCopy = handle;

  // The usage of an `optional` here is required because of a limitation in
  // Boost::Asio which forces us to use default-constructible result types with
  // `computeInNewThread`. We also can't unwrap the optional directly in this
  // function, because then the conan build fails in a very strange way,
  // probably related to issues in GCC's coroutine implementation.
  return computeInNewThread(
      [&query, &qec, enablePatternTrick = enablePatternTrick_,
       handle = std::move(handle), timeLimit,
       &queryDatasets]() mutable -> std::optional<PlannedQuery> {
        auto pq = SparqlParser::parseQuery(query);
        handle->throwIfCancelled();
        // SPARQL Protocol 2.1.4 specifies that the dataset from the query
        // parameters overrides the dataset from the query itself.
        if (!queryDatasets.empty()) {
          pq.datasetClauses_ =
              parsedQuery::DatasetClauses::fromClauses(queryDatasets);
        }
        QueryPlanner qp(&qec, handle);
        qp.setEnablePatternTrick(enablePatternTrick);
        auto qet = qp.createExecutionTree(pq);
        handle->throwIfCancelled();
        PlannedQuery plannedQuery{std::move(pq), std::move(qet)};

        plannedQuery.queryExecutionTree_.getRootOperation()
            ->recursivelySetCancellationHandle(std::move(handle));
        plannedQuery.queryExecutionTree_.getRootOperation()
            ->recursivelySetTimeConstraint(timeLimit);
        return plannedQuery;
      },
      std::move(handleCopy));
}

// _____________________________________________________________________________
bool Server::checkAccessToken(
    std::optional<std::string_view> accessToken) const {
  if (!accessToken) {
    return false;
  }
  auto accessTokenProvidedMsg = absl::StrCat(
      "Access token \"access-token=", accessToken.value(), "\" provided");
  auto requestIgnoredMsg = ", request is ignored";
  if (accessToken_.empty()) {
    throw std::runtime_error(absl::StrCat(
        accessTokenProvidedMsg,
        " but server was started without --access-token", requestIgnoredMsg));
  } else if (!ad_utility::constantTimeEquals(accessToken.value(),
                                             accessToken_)) {
    throw std::runtime_error(absl::StrCat(
        accessTokenProvidedMsg, " but not correct", requestIgnoredMsg));
  } else {
    LOG(DEBUG) << accessTokenProvidedMsg << " and correct" << std::endl;
    return true;
  }
}

// _____________________________________________________________________________

std::optional<std::string> Server::checkParameter(
    const ad_utility::url_parser::ParamValueMap& parameters,
    std::string_view key, std::optional<std::string> value,
    bool accessAllowed) {
  auto param =
      ad_utility::url_parser::getParameterCheckAtMostOnce(parameters, key);
  if (!param.has_value()) {
    return std::nullopt;
  }
  std::string parameterValue = param.value();

  // If value is given, but not equal to param value, return std::nullopt. If
  // no value is given, set it to param value.
  if (value == std::nullopt) {
    value = parameterValue;
  } else if (value != parameterValue) {
    return std::nullopt;
  }
  // Now that we have the value, check if there is a problem with the access.
  // If yes, we abort the query processing at this point.
  if (!accessAllowed) {
    throw std::runtime_error(absl::StrCat(
        "Access to \"", key, "=", value.value(), "\" denied",
        " (requires a valid access token)", ", processing of request aborted"));
  }
  return value;
}
