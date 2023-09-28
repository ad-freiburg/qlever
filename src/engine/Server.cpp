// Copyright 2011 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/Server.h"

#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "util/AsioHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/http/websocket/MessageSender.h"

template <typename T>
using Awaitable = Server::Awaitable<T>;

// __________________________________________________________________________
Server::Server(unsigned short port, size_t numThreads,
               ad_utility::MemorySize maxMem, std::string accessToken,
               bool usePatternTrick)
    : numThreads_(numThreads),
      port_(port),
      accessToken_(std::move(accessToken)),
      allocator_{
          ad_utility::makeAllocationMemoryLeftThreadsafeObject(
              maxMem.getBytes()),
          [this](size_t numBytesToAllocate) {
            cache_.makeRoomAsMuchAsPossible(ad_utility::MemorySize::bytes(
                MAKE_ROOM_SLACK_FACTOR * numBytesToAllocate / sizeof(Id)));
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
  RuntimeParameters().setOnUpdateAction<"cache-max-size-gb">(
      [this](size_t newValue) {
        cache_.setMaxSize(ad_utility::MemorySize::gigabytes(newValue));
      });
  RuntimeParameters().setOnUpdateAction<"cache-max-size-gb-single-entry">(
      [this](size_t newValue) {
        cache_.setMaxSizeSingleEntry(
            ad_utility::MemorySize::gigabytes(newValue));
      });
}

// __________________________________________________________________________
void Server::initialize(const string& indexBaseName, bool useText,
                        bool usePatterns, bool loadAllPermutations) {
  LOG(INFO) << "Initializing server ..." << std::endl;

  index_.setUsePatterns(usePatterns);
  index_.setLoadAllPermutations(loadAllPermutations);

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
          createOkResponse("", request, ad_utility::MediaType::textPlain));
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

  // First set up the HTTP server, so that it binds to the socket, and
  // the "socket already in use" error appears quickly.
  auto httpServer = HttpServer{port_, "0.0.0.0", static_cast<int>(numThreads_),
                               std::move(httpSessionHandler)};
  queryHub_ = &httpServer.getQueryHub();

  // Initialize the index
  initialize(indexBaseName, useText, usePatterns, loadAllPermutations);

  // Start listening for connections on the server.
  httpServer.run();

  // This will probably never be executed, but to ensure there will never
  // be any dangling pointers, set it to zero after use.
  queryHub_ = nullptr;
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
    // Note: For simplicity we only check via `starts_with`. This ignores
    // additional parameters like `application/sparql-query;charset=utf8`. We
    // currently always expect UTF-8.
    // TODO<joka921> Implement more complete parsing that allows the checking of
    // these parameters.
    if (contentType.starts_with(contentTypeUrlEncoded)) {
      return ad_utility::UrlParser::parseGetRequestTarget(
          absl::StrCat(toStd(request.target()), "?", request.body()), true);
    }
    if (contentType.starts_with(contentTypeSparqlQuery)) {
      return ad_utility::UrlParser::parseGetRequestTarget(
          absl::StrCat(toStd(request.target()), "?query=", request.body()),
          false);
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
    // Now that we have the value, check if there is a problem with the access.
    // If yes, we abort the query processing at this point.
    if (accessAllowed == false) {
      throw std::runtime_error(absl::StrCat("Access to \"", key, "=",
                                            value.value(), "\" denied",
                                            " (requires a valid access token)",
                                            ", processing of request aborted"));
    }
    return value;
  };

  // Check the access token. If an access token is provided and the check fails,
  // throw an exception and do not process any part of the query (even if the
  // processing had been allowed without access token).
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
    } else if (!ad_utility::constantTimeEquals(accessToken.value(),
                                               accessToken_)) {
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
  }

  // Ping with or without messsage.
  if (urlPathAndParameters._path == "/ping") {
    if (auto msg = checkParameter("msg", std::nullopt)) {
      LOG(INFO) << "Alive check with message \"" << msg.value() << "\""
                << std::endl;
    } else {
      LOG(INFO) << "Alive check without message" << std::endl;
    }
    response = createOkResponse("This QLever server is up and running\n",
                                request, ad_utility::MediaType::textPlain);
  }

  // Set description of KB index.
  if (auto description =
          checkParameter("index-description", std::nullopt, accessTokenOk)) {
    LOG(INFO) << "Setting index description to: \"" << description.value()
              << "\"" << std::endl;
    index_.setKbName(description.value());
    response = createJsonResponse(composeStatsJson(), request);
  }

  // Set description of text index.
  if (auto description =
          checkParameter("text-description", std::nullopt, accessTokenOk)) {
    LOG(INFO) << "Setting text description to: \"" << description.value()
              << "\"" << std::endl;
    index_.setTextName(description.value());
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

  // At this point, if there is a "?" in the query string, it means that there
  // are URL parameters which QLever does not know or did not process.
  if (request.target().find("?") != std::string::npos) {
    throw std::runtime_error(
        "Request with URL parameters, but none of them could be processed");
  }

  // At this point, we only have a path and no URL paraeters. We then interpret
  // the request as a request for a file. However, only files from a very
  // restricted whitelist (see below) will actually be served.
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
  // be in `src/util/http/HttpUtils.h`.
  LOG(INFO) << "Treating request target \"" << request.target() << "\""
            << " as a request for a file with that name" << std::endl;
  auto serveFileRequest = makeFileServer(
      ".",
      ad_utility::HashSet<std::string>{"index.html", "script.js", "style.css"})(
      std::move(request), send);
  co_return co_await std::move(serveFileRequest);
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
  j["time"]["total"] = Timer::toMilliseconds(requestTimer.value());
  j["time"]["computeResult"] = Timer::toMilliseconds(requestTimer.value());
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
  result["num-predicates-normal"] = index_.numDistinctPredicates().normal_;
  result["num-predicates-internal"] = index_.numDistinctPredicates().internal_;
  if (index_.hasAllPermutations()) {
    result["num-subjects-normal"] = index_.numDistinctSubjects().normal_;
    result["num-subjects-internal"] = index_.numDistinctSubjects().internal_;
    result["num-objects-normal"] = index_.numDistinctObjects().normal_;
    result["num-objects-internal"] = index_.numDistinctObjects().internal_;
  }

  auto numTriples = index_.numTriples();
  result["num-triples-normal"] = numTriples.normal_;
  result["num-triples-internal"] = numTriples.internal_;
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
  result["num-pinned-index-scan-sizes"] = cache_.pinnedSizes().rlock()->size();
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
      : std::runtime_error{"Query id '" + proposedQueryId +
                           "' is already in use!"} {}
};

// _____________________________________________

ad_utility::websocket::OwningQueryId Server::getQueryId(
    const ad_utility::httpUtils::HttpRequest auto& request) {
  using ad_utility::websocket::OwningQueryId;
  std::string_view queryIdHeader = request.base()["Query-Id"];
  if (queryIdHeader.empty()) {
    return queryRegistry_.uniqueId();
  }
  auto queryId = queryRegistry_.uniqueIdFromString(std::string(queryIdHeader));
  if (!queryId) {
    throw QueryAlreadyInUseError{queryIdHeader};
  }
  return std::move(queryId.value());
}

// ____________________________________________________________________________
boost::asio::awaitable<void> Server::processQuery(
    const ParamValueMap& params, ad_utility::Timer& requestTimer,
    const ad_utility::httpUtils::HttpRequest auto& request, auto&& send) {
  using namespace ad_utility::httpUtils;
  AD_CONTRACT_CHECK(params.contains("query"));
  const auto& query = params.at("query");
  AD_CONTRACT_CHECK(!query.empty());

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
  std::optional<QueryExecutionTree> queryExecutionTree;
  try {
    ad_utility::SharedConcurrentTimeoutTimer timeoutTimer = [&]() {
      auto t = ad_utility::TimeoutTimer::unlimited();
      if (params.contains("timeout")) {
        ad_utility::Timer::Seconds timeout{std::stof(params.at("timeout"))};
        t = ad_utility::TimeoutTimer{timeout, ad_utility::Timer::Started};
      }
      return std::make_shared<ad_utility::ConcurrentTimeoutTimer>(std::move(t));
    }();

    auto containsParam = [&params](const std::string& param,
                                   const std::string& expected) {
      return params.contains(param) && params.at(param) == expected;
    };
    size_t maxSend = params.contains("send") ? std::stoul(params.at("send"))
                                             : MAX_NOF_ROWS_IN_RESULT;
    const bool pinSubtrees = containsParam("pinsubtrees", "true");
    const bool pinResult = containsParam("pinresult", "true");
    LOG(INFO) << "Processing the following SPARQL query:"
              << (pinResult ? " [pin result]" : "")
              << (pinSubtrees ? " [pin subresults]" : "") << "\n"
              << query << std::endl;
    ParsedQuery pq = SparqlParser::parseQuery(query);

    // The following code block determines the media type to be used for the
    // result. The media type is either determined by the "Accept:" header of
    // the request or by the URL parameter "action=..." (for TSV and CSV export,
    // for QLever-historical reasons).
    using ad_utility::MediaType;

    // The first media type in this list is the default, if no other type is
    // specified in the request. It's "application/sparql-results+json", as
    // required by the SPARQL standard.
    const auto supportedMediaTypes = []() {
      static const std::vector<MediaType> mediaTypes{
          ad_utility::MediaType::sparqlJson,
          ad_utility::MediaType::qleverJson,
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
    AD_CONTRACT_CHECK(mediaType.has_value());
    LOG(INFO) << "Requested media type of result is \""
              << ad_utility::toString(mediaType.value()) << "\"" << std::endl;

    AD_CORRECTNESS_CHECK(queryHub_ != nullptr);
    auto messageSender = co_await ad_utility::websocket::MessageSender::create(
        getQueryId(request), *queryHub_);
    // Do the query planning. This creates a `QueryExecutionTree`, which will
    // then be used to process the query. Start the shared `timeoutTimer` here
    // to also include the query planning.
    //
    // NOTE: This should come after determining the media type. Otherwise, it
    // might happen that the query planner runs for a while (recall that it many
    // do index scans) and then we get an error message afterwards that a
    // certain media type is not supported.
    QueryExecutionContext qec(index_, &cache_, allocator_,
                              sortPerformanceEstimator_,
                              std::ref(messageSender), pinSubtrees, pinResult);
    QueryPlanner qp(&qec);
    qp.setEnablePatternTrick(enablePatternTrick_);
    queryExecutionTree = qp.createExecutionTree(pq);
    auto& qet = queryExecutionTree.value();
    qet.isRoot() = true;  // allow pinning of the final result
    qet.recursivelySetTimeoutTimer(timeoutTimer);
    qet.getRootOperation()->createRuntimeInfoFromEstimates();
    size_t timeForQueryPlanning = requestTimer.msecs();
    auto& runtimeInfoWholeQuery =
        qet.getRootOperation()->getRuntimeInfoWholeQuery();
    runtimeInfoWholeQuery.timeQueryPlanning = timeForQueryPlanning;
    LOG(INFO) << "Query planning done in " << timeForQueryPlanning << " ms"
              << std::endl;
    LOG(TRACE) << qet.asString() << std::endl;

    // Common code for sending responses for the streamable media types
    // (tsv, csv, octet-stream, turtle).
    auto sendStreamableResponse =
        [&](ad_utility::MediaType mediaType) -> Awaitable<void> {
      auto responseGenerator = co_await computeInNewThread([&] {
        return ExportQueryExecutionTrees::computeResultAsStream(pq, qet,
                                                                mediaType);
      });

      // The `streamable_body` that is used internally turns all exceptions that
      // occur while generating the results into "broken pipe". We store the
      // actual exceptions and manually rethrow them to propagate the correct
      // error messages to the user.
      // TODO<joka921> What happens, when part of the TSV export has already
      // been sent and an exception occurs after that?
      std::exception_ptr exceptionPtr;
      responseGenerator.assignExceptionToThisPointer(&exceptionPtr);
      try {
        auto response =
            createOkResponse(std::move(responseGenerator), request, mediaType);
        co_await send(std::move(response));
      } catch (...) {
        if (exceptionPtr) {
          std::rethrow_exception(exceptionPtr);
        }
        throw;
      }
    };

    // This actually processes the query and sends the result in the requested
    // format.
    switch (mediaType.value()) {
      case ad_utility::MediaType::csv:
      case ad_utility::MediaType::tsv:
      case ad_utility::MediaType::octetStream:
      case ad_utility::MediaType::turtle: {
        co_await sendStreamableResponse(mediaType.value());
      } break;
      case ad_utility::MediaType::qleverJson:
      case ad_utility::MediaType::sparqlJson: {
        // Normal case: JSON response
        auto responseString = co_await computeInNewThread([&, maxSend] {
          return ExportQueryExecutionTrees::computeResultAsJSON(
              pq, qet, requestTimer, maxSend, mediaType.value());
        });
        co_await sendJson(std::move(responseString), responseStatus);
      } break;
      default:
        // This should never happen, because we have carefully restricted the
        // subset of mediaTypes that can occur here.
        AD_FAIL();
    }
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
              << ", total time was " << requestTimer.msecs() << " ms"
              << std::endl;
    LOG(DEBUG) << "Runtime Info:\n"
               << qet.getRootOperation()->getRuntimeInfo().toString()
               << std::endl;
  } catch (const ParseException& e) {
    responseStatus = http::status::bad_request;
    exceptionErrorMsg = e.errorMessageWithoutPositionalInfo();
    metadata = e.metadata();
  } catch (const QueryAlreadyInUseError& e) {
    responseStatus = http::status::conflict;
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
    if (queryExecutionTree) {
      errorResponseJson["runtimeInformation"] = nlohmann::ordered_json(
          queryExecutionTree->getRootOperation()->getRuntimeInfo());
    }
    co_return co_await sendJson(errorResponseJson, responseStatus);
  }
}

// _____________________________________________________________________________
template <typename Function, typename T>
Awaitable<T> Server::computeInNewThread(Function function) const {
  auto runOnExecutor = [](auto executor, Function func) -> net::awaitable<T> {
    co_await net::post(net::bind_executor(executor, net::use_awaitable));
    co_return std::invoke(func);
  };
  return ad_utility::sameExecutor(
      runOnExecutor(threadPool_.get_executor(), std::move(function)));
}
