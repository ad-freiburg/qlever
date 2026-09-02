// Copyright 2015 - 2026 The QLever Authors, in particular:
//
// 2015 - 2017 Björn Buchhold, UFR
// 2020 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/Server.h"

#include <absl/functional/bind_front.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <array>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "backports/filesystem.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/GraphStoreProtocol.h"
#include "engine/HttpApiHelpers.h"
#include "engine/HttpError.h"
#include "engine/MaterializedViews.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryPlanner.h"
#include "engine/ResponseJson.h"
#include "engine/SparqlProtocol.h"
#include "engine/UpdateMetadata.h"
#include "global/RuntimeParameters.h"
#include "libqlever/Qlever.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlParser.h"
#include "util/AsioHelpers.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParseableDuration.h"
#include "util/QueryEventLog.h"
#include "util/ResourceMonitor.h"
#include "util/TimeTracer.h"
#include "util/TypeTraits.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"
#include "util/http/websocket/MessageSender.h"

using namespace std::string_literals;
using namespace ad_utility::url_parser::sparqlOperation;
using namespace ad_utility::metrics;

template <typename T>
using Awaitable = Server::Awaitable<T>;
using ad_utility::MediaType;

// _____________________________________________________________________________
Server::Server(
    unsigned short port, size_t numThreads, std::string accessToken,
    const qlever::EngineConfig& config, bool noAccessCheck,
    std::shared_ptr<ad_utility::metrics::MetricsReader> metricsReader,
    std::shared_ptr<ad_utility::RebuildTracker> rebuildTracker)
    : qlever_(config),
      numThreads_(numThreads),
      port_(port),
      accessToken_(std::move(accessToken)),
      noAccessCheck_(noAccessCheck),
      queryThreadPool_{numThreads},
      rebuildIndexStrategy_(config.rebuildIndexStrategy_),
      keepPreviousIndexDirs_(config.keepPreviousIndexDirs_),
      metricsReader_(std::move(metricsReader)),
      rebuildTracker_(rebuildTracker
                          ? std::move(rebuildTracker)
                          : std::make_shared<ad_utility::RebuildTracker>()) {
  AD_LOG_INFO << "Initializing server ..." << std::endl;

  initializeServerMetrics(config.memoryLimit_);

  if (noAccessCheck_) {
    AD_LOG_INFO << "No access token required for restricted API calls"
                << std::endl;
  } else {
    AD_LOG_INFO << "Access token for restricted API calls is \"" << accessToken_
                << "\"" << std::endl;
  }
}

// _____________________________________________________________________________
void Server::initializeServerMetrics(
    std::optional<ad_utility::MemorySize> memoryLimit) {
  metrics_ = std::make_unique<ServerMetrics>(
      [this]() {
        auto counts = indexAndViewsSnapshot()
                          ->index_.deltaTriplesManager()
                          .getCurrentLocatedTriplesSharedState()
                          ->counts_;
        AD_CORRECTNESS_CHECK(counts.has_value());
        auto [ins, del] = counts.value();
        return ins + del;
      },
      [this]() -> int64_t { return allocator().amountMemoryLeft().getBytes(); },
      [this]() -> int64_t {
        return (cache().nonPinnedSize() + cache().pinnedSize()).getBytes();
      },
      [this]() -> int64_t { return cache().getMaxSize().getBytes(); },
      [this]() -> int64_t {
        return static_cast<int64_t>(rebuildTracker_->poll().has_value());
      },
      memoryLimit);
  metrics_->registerCallbacks();
}

// _____________________________________________________________________________
void Server::configureQueryEventLog(const ql::filesystem::path& path) {
  // One log, owned by a `shared_ptr` copied into both callbacks, so its
  // lifetime follows the callbacks (and thus the registry).
  auto log = std::make_shared<ad_utility::QueryEventLog>();
  log->setOutputFile(path);
  // One generic lambda for both events: serialize the info struct (via its
  // `to_json`) and push it; the log appends the trailing newline.
  auto logEvent = [log](const auto& info) {
    nlohmann::ordered_json line = info;
    log->push(line.dump());
  };
  queryRegistry_.addOnStart(logEvent);
  queryRegistry_.addOnEnd(std::move(logEvent));
}

// _____________________________________________________________________________
CPP_template_def(typename RequestT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>) Server::ResponseT
    Server::reportHttpError(std::string_view message, http::status status,
                            const RequestT& request,
                            const MetricLabel& errorType) const {
  using namespace ad_utility::httpUtils;
  AD_LOG_ERROR << message << std::endl;
  metrics_->httpErrors_->Add(1, {errorType});
  return createHttpResponseFromString(std::string{message}, status, request,
                                      MediaType::textPlain);
}

// _____________________________________________________________________________
CPP_template_def(typename RequestT, typename SendT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::handleHttpRequest(RequestT request, SendT& send) {
  using namespace ad_utility::httpUtils;

  auto sendWithAccessControlHeaders =
      [&send](auto response) -> boost::asio::awaitable<void> {
    response.set(http::field::access_control_allow_origin, "*");
    response.set(http::field::access_control_allow_headers, "*");
    response.set(http::field::access_control_allow_methods,
                 "GET, POST, OPTIONS");
    co_return co_await send(std::move(response));
  };

  if (request.method() == http::verb::options) {
    AD_LOG_INFO << std::endl;
    AD_LOG_INFO << "Request received via " << request.method()
                << ", allowing everything" << std::endl;
    co_return co_await sendWithAccessControlHeaders(
        createOkResponse("", request, MediaType::textPlain));
  }

  // The C++ standard forbids a suspend point (`co_await`) inside a `catch`
  // block, so the actual `send` cannot happen here. Building the error
  // response, however, is synchronous and can happen right in the `catch`
  // block.
  std::optional<ResponseT> errorResponse;
  try {
    co_await process(request, sendWithAccessControlHeaders);
  } catch (const HttpError& e) {
    errorResponse =
        reportHttpError(e.what(), e.status(), request, HttpErrorType::http);
  } catch (const std::exception& e) {
    errorResponse = reportHttpError(e.what(), http::status::bad_request,
                                    request, HttpErrorType::internal);
  }
  if (errorResponse.has_value()) {
    co_return co_await sendWithAccessControlHeaders(
        std::move(errorResponse.value()));
  }
}

// Explicit instantiation so that friend test code (`ServerForTesting`, the
// `IndexRebuilder` `FRIEND_TEST`s), which cannot see this template's
// definition, can call `handleHttpRequest` directly with a `MockSend` to
// capture the response instead of sending it.
template Server::Awaitable<void> Server::handleHttpRequest<
    Server::StringBodyRequest, Server::MockSend>(StringBodyRequest, MockSend&);

// _____________________________________________________________________________
std::function<Server::Awaitable<void>(const Server::StringBodyRequest&,
                                      tcp::socket)>
Server::makeWebSocketSessionSupplier(net::any_io_executor& ioExecutor) {
  AD_CONTRACT_CHECK(queryHub_.expired(),
                    "`queryHub_` has already been initialized; "
                    "`makeWebSocketSessionSupplier` must only be called once.");
  auto queryHub = std::make_shared<ad_utility::websocket::QueryHub>(ioExecutor);
  // Make sure the `queryHub` does not outlive the ioContext it has a
  // reference to, by only storing a `weak_ptr` in the `queryHub_`. Note: This
  // `weak_ptr` may only be converted back to a `shared_ptr` inside a task
  // running on the `io_context`.
  queryHub_ = queryHub;
  return [this, queryHub = std::move(queryHub)](
             const StringBodyRequest& request, tcp::socket socket) {
    return ad_utility::websocket::WebSocketSession::handleSession(
        *queryHub, queryRegistry_, request, std::move(socket));
  };
}

// _____________________________________________________________________________
void Server::run() {
  auto httpSessionHandler = [this](auto request, auto&& send) {
    return handleHttpRequest(std::move(request), AD_FWD(send));
  };

  // `HttpServer`'s constructor binds the socket synchronously; keep this as
  // the first statement in `run()` so a port already in use fails fast,
  // before any other startup work.
  auto httpServer =
      HttpServer{port_, "0.0.0.0", static_cast<int>(numThreads_),
                 std::move(httpSessionHandler),
                 absl::bind_front(&Server::makeWebSocketSessionSupplier, this)};

  AD_LOG_INFO << "The server is ready, listening for requests on port "
              << std::to_string(httpServer.getPort()) << " ..." << std::endl;

  // Start listening for connections on the server.
  httpServer.run();
}

// _____________________________________________________________________________
Server::TimeLimit Server::verifyUserSubmittedQueryTimeout(
    std::optional<std::string_view> userTimeout, bool accessTokenOk) const {
  auto defaultTimeout =
      getRuntimeParameter<&RuntimeParameters::defaultQueryTimeout_>();
  if (userTimeout.has_value()) {
    auto timeoutCandidate =
        ad_utility::ParseableDuration<TimeLimit>::fromString(
            userTimeout.value());
    if (timeoutCandidate > defaultTimeout && !accessTokenOk) {
      throw HttpError(
          boost::beast::http::status::forbidden,
          absl::StrCat(
              "User submitted timeout was higher than what is currently "
              "allowed by this instance (",
              defaultTimeout.toString(),
              "). Please use a valid-access token to override this server "
              "configuration."));
    }
    return timeoutCandidate;
  }
  return std::chrono::duration_cast<TimeLimit>(
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
  // The timer must not be moved once `async_wait` has registered a
  // `wait_op` against its implementation: the queued op references the
  // original impl by address, and moving the timer leaves the op
  // dangling in the scheduler's queue. Wrap in `shared_ptr` so the
  // timer object stays put.
  auto timer = std::make_shared<net::steady_timer>(timerExecutor_, timeLimit);

  timer->async_wait([cancellationHandle = std::move(cancellationHandle)](
                        const boost::system::error_code&) {
    if (auto pointer = cancellationHandle.lock()) {
      pointer->cancel(ad_utility::CancellationState::TIMEOUT);
    }
  });
  return [timer = std::move(timer)]() { timer->cancel(); };
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
    ad_utility::websocket::MessageSender messageSender,
    const ParamValueMap& params, TimeLimit timeLimit, bool accessTokenOk,
    std::string_view clientIp) {
  auto [cancellationHandle, cancelTimeoutOnDestruction] =
      setupCancellationHandle(messageSender.getQueryId(), timeLimit);
  auto resultPinning = qlever::http_api_helpers::determineResultPinning(params);

  AD_LOG_INFO << "Processing the following " << operationName
              << (clientIp.empty() ? std::string{}
                                   : absl::StrCat(" from ", clientIp))
              << ":" << resultPinning.describeForLog() << "\n"
              << ad_utility::truncateOperationString(operationSPARQL)
              << std::endl;

  auto sharedMessageSender =
      std::make_shared<ad_utility::websocket::MessageSender>(
          std::move(messageSender));
  // Return a factory rather than a ready-made context, so the caller can bind
  // it to whichever snapshot is current when the operation runs (see
  // `processUpdate`).
  MakeQueryExecutionContext makeQec =
      [this, sharedMessageSender = std::move(sharedMessageSender),
       resultPinning = std::move(resultPinning),
       accessTokenOk](SharedIndexAndView indexAndViews) mutable {
        auto qec = qlever().createQueryExecutionContext(
            std::move(indexAndViews),
            [sharedMessageSender](std::string json) {
              (*sharedMessageSender)(std::move(json));
            },
            resultPinning.pinSubtrees_, resultPinning.pinResult_);
        configurePinnedResultWithName(
            std::move(resultPinning.pinResultWithName_), accessTokenOk, *qec);
        return qec;
      };
  return std::make_tuple(std::move(makeQec), std::move(cancellationHandle),
                         std::move(cancelTimeoutOnDestruction));
}

// _____________________________________________________________________________
void Server::configurePinnedResultWithName(
    std::optional<QueryExecutionContext::PinResultWithName> pinResultWithName,
    bool accessTokenOk, QueryExecutionContext& qec) {
  if (!pinResultWithName.has_value()) {
    return;
  }
  if (!accessTokenOk) {
    throw HttpError(
        http::status::forbidden,
        "Pinning a result with a name requires a valid access token");
  }
  qec.pinResultWithName() = std::move(pinResultWithName);
}

// _____________________________________________________________________________
Awaitable<DeltaTriplesCount> Server::processClearDeltaTriples() {
  // The function requires a SharedCancellationHandle, but the operation is
  // not cancellable.
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  // We don't directly `co_await` because of lifetime issues (bugs) in the
  // Conan setup.
  auto coroutine = computeInNewThread(
      updateThreadPool_,
      // Call `qlever().clearDeltaTriples()` here, on the (single-threaded)
      // `updateThreadPool_`, so its snapshot reflects the currently active
      // index and not a stale one that a concurrent rebuild may have swapped
      // out (whose changes would be lost).
      [this] { return qlever().clearDeltaTriples(); }, handle);
  auto countAfterClear = co_await std::move(coroutine);
  co_return countAfterClear;
}

// _____________________________________________________________________________
Awaitable<nlohmann::json> Server::processVacuumDeltaTriples(
    std::optional<std::string_view> userTimeout, bool accessTokenOk) {
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  TimeLimit timeLimit =
      verifyUserSubmittedQueryTimeout(userTimeout, accessTokenOk);
  auto cancelTimeoutOnDestruction = cancelAfterDeadline(handle, timeLimit);

  auto coroutine = computeInNewThread(
      updateThreadPool_,
      [this, handle] { return qlever().vacuumDeltaTriples(handle); }, handle);
  co_return co_await std::move(coroutine);
}

// _____________________________________________________________________________
Awaitable<nlohmann::json> Server::processWriteMaterializedView(
    const ParamValueMap& parameters, const SparqlOperation& operation,
    bool accessTokenOk, const ad_utility::Timer& requestTimer) {
  auto name =
      qlever::http_api_helpers::getViewNameParameter(parameters, "Writing");
  AD_CONTRACT_CHECK(name != "", "The name for the view may not be empty");

  // Extract query body.
  auto query = std::visit(
      [](const auto& op) -> Query {
        using T = std::decay_t<decltype(op)>;
        if constexpr (std::is_same_v<T, Query>) {
          return op;
        } else {
          static_assert(
              ad_utility::SameAsAny<T, Update, GraphStoreOperation, None>);
          throw std::runtime_error(
              "Action 'write-materialized-view' requires a 'SELECT' query.");
        }
      },
      operation);

  // Extract time limit.
  auto timeLimit =
      verifyUserSubmittedQueryTimeout(ad_utility::url_parser::checkParameter(
                                          parameters, "timeout", std::nullopt),
                                      accessTokenOk);

  // Call `Qlever::writeMaterializedView` with the extracted parameters. This
  // assumes that the access token has already been checked. Note that storing
  // the coroutine in a variable first and then awaiting it is required due to
  // lifetime issues on certain compilers.
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  auto coroutine = computeInNewThread(
      queryThreadPool_,
      [name, query, requestTimer, cancellationHandle, timeLimit,
       this]() mutable {
        qlever().writeMaterializedView(
            name, std::move(query.query_), query.datasetClauses_,
            std::move(cancellationHandle), timeLimit, requestTimer);
      },
      cancellationHandle);
  co_await std::move(coroutine);

  co_return nlohmann::json{{"materialized-view-written", name}};
}

// _____________________________________________________________________________
nlohmann::json Server::processLoadMaterializedView(
    const ParamValueMap& parameters, const SharedIndexAndView& indexAndViews) {
  auto name =
      qlever::http_api_helpers::getViewNameParameter(parameters, "Loading");

  auto qec = qlever().createQueryExecutionContext(indexAndViews);
  indexAndViews->materializedViewsManager_.loadView(name, qec.get());

  return json{{"materialized-view-loaded", name}};
}

// _____________________________________________________________________________
nlohmann::json Server::processDeleteMaterializedView(
    const ParamValueMap& parameters) const {
  auto name =
      qlever::http_api_helpers::getViewNameParameter(parameters, "Deleting");

  // Snapshot again instead of reusing the snapshot taken at the beginning of
  // `process()` (see `clear-delta-triples` above for the same pattern), so
  // that we delete the view from the index that is currently being served
  // and not from a stale one that a concurrent rebuild has swapped out in the
  // meantime. Deleting from a stale manager is not unsafe (the rebuild called
  // `MaterializedViewsManager::retireOnDiskFiles` on it, which makes
  // `deleteView` throw), it would just needlessly fail.
  indexAndViewsSnapshot()->materializedViewsManager_.deleteView(name);

  return json{{"materialized-view-deleted", name}};
}

// _____________________________________________________________________________
CPP_template_def(typename RequestT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Server::ResponseT Server::processPing(std::optional<std::string> msg,
                                          const RequestT& request) const {
  using namespace ad_utility::httpUtils;
  if (msg.has_value()) {
    AD_LOG_INFO << "Alive check with message \"" << msg.value() << "\""
                << std::endl;
  } else {
    AD_LOG_INFO << "Alive check without message" << std::endl;
  }
  return createOkResponse("This QLever server is up and running\n", request,
                          MediaType::textPlain);
}

namespace {
// Helpers used only by `Server::process` below, for dispatching its `cmd=`
// URL parameter.
namespace serverProcessHelpers {
// Metadata for a `cmd=<name>` URL parameter handled by `Server::process`:
// the log message and whether it requires a valid access token.
struct CommandMeta {
  std::string_view name_;
  std::string_view description_;
  bool requiresAuth_;
};

constexpr std::array commands = {
    CommandMeta{"stats", "get index statistics", false},
    CommandMeta{"cache-stats", "get cache statistics", false},
    CommandMeta{"clear-cache", "clear the cache (unpinned elements only)",
                false},
    CommandMeta{"clear-cache-complete",
                "clear cache completely (including unpinned elements)", true},
    CommandMeta{"clear-named-cache", "clear the cache for named results", true},
    CommandMeta{"clear-delta-triples", "clear delta triples", true},
    CommandMeta{"vacuum-delta-triples",
                "vacuum (remove redundant) delta triples", true},
    CommandMeta{"get-settings", "get server settings", false},
    CommandMeta{"get-index-id", "get index ID", false},
    CommandMeta{"dump-active-queries", "dump active queries", true},
    CommandMeta{"rebuild-index", "rebuilding index", true},
    CommandMeta{"write-materialized-view", "write materialized view", true},
    CommandMeta{"load-materialized-view", "explicitly load materialized view",
                true},
    CommandMeta{"delete-materialized-view", "delete materialized view", true},
};

// Throw a 403 `HttpError` if `accessTokenOk` is false; `actionName` names the
// action being authorized, for the error message.
void requireValidAccessToken(bool accessTokenOk, std::string_view actionName) {
  if (!accessTokenOk) {
    throw HttpError(boost::beast::http::status::forbidden,
                    absl::StrCat(actionName,
                                 " requires a valid access token but no "
                                 "access token was provided"));
  }
}

// Check if `paramName=<newValue>` is set in `parameters`. If so, verify the
// access token (using `actionName` if given, `paramName` otherwise for the
// error message on invalid access), log the `<newValue>` and return it. Return
// `std::nullopt` if no such parameter is found.
std::optional<std::string> checkAndLogParameterSetting(
    const ad_utility::url_parser::ParamValueMap& parameters,
    std::string_view paramName, bool accessTokenOk,
    std::optional<std::string_view> actionName = std::nullopt) {
  auto value = ad_utility::url_parser::checkParameter(parameters, paramName,
                                                      std::nullopt);
  if (value.has_value()) {
    requireValidAccessToken(accessTokenOk, actionName.value_or(paramName));
    AD_LOG_INFO << "Setting \"" << paramName << "\" to: \"" << value.value()
                << "\"" << std::endl;
  }
  return value;
}

// Look up metadata for `cmd` in `commands`, run the access-token check (if
// required), and log it. `cmd` must name an entry in `commands`. It always
// comes from a literal used in the `process()` dispatch below.
void dispatchLog(std::string_view cmd, bool accessTokenOk) {
  auto it = ql::ranges::find(commands, cmd, &CommandMeta::name_);
  AD_CORRECTNESS_CHECK(it != commands.end());
  if (it->requiresAuth_) {
    requireValidAccessToken(accessTokenOk, it->name_);
  }
  AD_LOG_INFO << "Processing command \"" << it->name_
              << "\": " << it->description_ << std::endl;
}
}  // namespace serverProcessHelpers
}  // namespace

// _____________________________________________________________________________
CPP_template_def(typename RequestT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>) Server::ResponseT
    Server::processMetrics(bool accessTokenOk, const RequestT& request) const {
  using namespace ad_utility::httpUtils;
  serverProcessHelpers::requireValidAccessToken(accessTokenOk, "metrics");
  if (!metricsReader_) {
    return createNotFoundResponse("Metrics not enabled (use --enable-metrics)",
                                  request);
  }
  return createOkResponse(metricsReader_->getMetricsText(), request,
                          MediaType::textPlain);
}

// _____________________________________________________________________________
std::optional<nlohmann::json> Server::processSetRuntimeParameters(
    const ParamValueMap& parameters, bool accessTokenOk) const {
  bool parameterChanged = false;
  for (const auto& key : globalRuntimeParameters.rlock()->getKeys()) {
    if (auto value = serverProcessHelpers::checkAndLogParameterSetting(
            parameters, key, accessTokenOk, "setting runtime parameters")) {
      globalRuntimeParameters.wlock()->setFromString(key, value.value());
      parameterChanged = true;
    }
  }
  if (!parameterChanged) {
    return std::nullopt;
  }
  return nlohmann::json(globalRuntimeParameters.rlock()->toMap());
}

// _____________________________________________________________________________
CPP_template_def(typename RequestT, typename SendT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::process(RequestT& request, SendT&& send) {
  using namespace ad_utility::httpUtils;
  using namespace responseJson;
  using namespace serverProcessHelpers;
  // Acquire the current index and the materialized views manager exactly once
  // for the whole request, under a single read lock. This way a concurrent
  // rebuild that swaps both in cannot make different helpers observe a
  // mismatched (index, manager) pair.
  auto indexAndViews = indexAndViewsSnapshot();
  auto& index = indexAndViews->index_;

  // Log some basic information about the request. Start with an empty line so
  // that in a low-traffic scenario (or when the query processing is very fast),
  // we have one visual block per request in the log.
  std::string_view contentType = request.base()[http::field::content_type];
  AD_LOG_INFO << std::endl;
  AD_LOG_INFO << "Request received via " << request.method()
              << (contentType.empty()
                      ? absl::StrCat(", no content type specified")
                      : absl::StrCat(", content type \"", contentType, "\""))
              << std::endl;

  // Start timing.
  ad_utility::Timer requestTimer{ad_utility::Timer::Started};

  // Parse the path and the URL parameters from the given request. Works for GET
  // requests as well as the two kinds of POST requests allowed by the SPARQL
  // standard, see method `getUrlPathAndParameters`.
  auto parsedHttpRequest = SparqlProtocol::parseHttpRequest(request);
  const auto& parameters = parsedHttpRequest.parameters_;

  // We always want to call `Server::checkParameter` with the same first
  // parameter.
  auto checkParameter = absl::bind_front(
      &ad_utility::url_parser::checkParameter, std::cref(parameters));

  // Check the access token. If an access token is provided and the check fails,
  // throw an exception and do not process any part of the query (even if the
  // processing had been allowed without access token).
  bool accessTokenOk = checkAccessToken(parsedHttpRequest.accessToken_);

  // We always want to call `serverProcessHelpers::requireValidAccessToken`
  // with the same `accessTokenOk`.
  auto requireValidAccessToken = absl::bind_front(
      &serverProcessHelpers::requireValidAccessToken, accessTokenOk);

  // We always want to call `serverProcessHelpers::checkAndLogParameterSetting`
  // with the same `parameters` and `accessTokenOk`.
  auto checkAndLogParameterSetting =
      [&parameters, accessTokenOk](std::string_view paramName) {
        return serverProcessHelpers::checkAndLogParameterSetting(
            parameters, paramName, accessTokenOk);
      };

  // Check if the current command is selected in the parameters from the
  // `parsedHttpRequest.parameters_`. If so, log this information via
  // `dispatchLog()` and return true. Return false otherwise.
  auto commandIs = [accessTokenOk, &checkParameter](std::string_view cmd) {
    if (checkParameter("cmd", std::string{cmd})) {
      dispatchLog(cmd, accessTokenOk);
      return true;
    }
    return false;
  };

  // We call `createJsonResponse` always with the same `request` parameter.
  auto jsonResponse = [&request](const json& j) {
    return createJsonResponse(j, request);
  };

  // We call `composeCacheStats()` always with the same parameters:
  // `qlever().cache()` and `qlever().namedResultCache()`.
  auto cacheStats = [&cache = qlever().cache(),
                     &namedResultCache = qlever().namedResultCache()]() {
    return composeCacheStats(cache, namedResultCache);
  };
  std::optional<http::response<streamable_body>> response;

  // Process all URL parameters known to QLever. If there is more than one,
  // QLever processes all of them, but only returns the result from the last
  // one. In particular, if there is a "query" parameter, it will be processed
  // last and its result returned.
  //
  // Some parameters require that "access-token" is set correctly. If not, that
  // parameter is ignored.
  if (commandIs("stats")) {
    response = jsonResponse(composeIndexStats(index));
  } else if (commandIs("cache-stats")) {
    response = jsonResponse(cacheStats());
  } else if (commandIs("clear-cache")) {
    cache().clearUnpinnedOnly();
    response = jsonResponse(cacheStats());
  } else if (commandIs("clear-cache-complete")) {
    cache().clearAll();
    response = jsonResponse(cacheStats());
  } else if (commandIs("clear-named-cache")) {
    namedResultCache().clear();
    response = jsonResponse(cacheStats());
  } else if (commandIs("clear-delta-triples")) {
    auto countAfterClear = co_await processClearDeltaTriples();
    response = jsonResponse(json(countAfterClear));
  } else if (commandIs("vacuum-delta-triples")) {
    auto vacuumStats = co_await processVacuumDeltaTriples(
        checkParameter("timeout", std::nullopt), accessTokenOk);
    response = jsonResponse(vacuumStats);
  } else if (commandIs("get-settings")) {
    response = jsonResponse(json(globalRuntimeParameters.rlock()->toMap()));
  } else if (commandIs("get-index-id")) {
    response =
        createOkResponse(index.getIndexId(), request, MediaType::textPlain);
  } else if (commandIs("dump-active-queries")) {
    auto json = nlohmann::json::object();
    for (auto& [key, value] : queryRegistry_.getActiveQueries()) {
      json[nlohmann::json(key)] = std::move(value);
    }
    response = jsonResponse(json);
  } else if (commandIs("rebuild-index")) {
    response = co_await processRebuildIndex(parameters, request);
  } else if (commandIs("write-materialized-view")) {
    auto materializedViewStats = co_await processWriteMaterializedView(
        parameters, parsedHttpRequest.operation_, accessTokenOk, requestTimer);
    response = jsonResponse(materializedViewStats);
    // Prevent regular query processing by removing the query from the
    // request.
    parsedHttpRequest.operation_ = None{};
  } else if (commandIs("load-materialized-view")) {
    response =
        jsonResponse(processLoadMaterializedView(parameters, indexAndViews));
    // Prevent regular query processing by removing the query from the
    // request.
    parsedHttpRequest.operation_ = None{};
  } else if (commandIs("delete-materialized-view")) {
    response = jsonResponse(processDeleteMaterializedView(parameters));
    // Prevent regular query processing by removing the query from the
    // request.
    parsedHttpRequest.operation_ = None{};
  }

  // Ping with or without message.
  if (parsedHttpRequest.path_ == "/ping") {
    response = processPing(checkParameter("msg", std::nullopt), request);
  }

  // Prometheus metrics scrape endpoint.
  if (parsedHttpRequest.path_ == "/metrics") {
    response = processMetrics(accessTokenOk, request);
  }

  // Set description of KB index.
  if (auto description = checkAndLogParameterSetting("index-description")) {
    index.setKbName(description.value());
    response = jsonResponse(composeIndexStats(index));
  }

  // Set description of text index.
  if (auto description = checkAndLogParameterSetting("text-description")) {
    index.setTextName(description.value());
    response = jsonResponse(composeIndexStats(index));
  }

  // Set one or several of the runtime parameters.
  if (auto updatedSettings =
          processSetRuntimeParameters(parameters, accessTokenOk)) {
    response = jsonResponse(updatedSettings.value());
  }

  // Store the QueryExecutionTree outside the lambda, s.t. we have access in
  // case of errors to create an informative error message that includes the
  // runtime information.
  std::optional<PlannedQuery> plannedQuery;
  auto visitOperation =
      [&checkParameter, &accessTokenOk, &request, &send, &parameters,
       &requestTimer, &plannedQuery, &indexAndViews, this](
          std::vector<ParsedQuery> operations, std::string operationName,
          const std::string operationString,
          std::function<bool(const ParsedQuery&)> expectedOperation,
          const std::string msg, SharedTimeTracer tracer) -> Awaitable<void> {
    auto timeLimit = verifyUserSubmittedQueryTimeout(
        checkParameter("timeout", std::nullopt), accessTokenOk);
    // Empty when the header is absent.
    std::string_view clientIp = request.base()["X-Real-IP"];
    ad_utility::websocket::MessageSender messageSender =
        createMessageSender(queryHub_, request, operationString, clientIp);
    // Grab the shared handle before `messageSender` is moved below.
    using enum ad_utility::websocket::QueryStatus;
    auto queryStatus = messageSender.sharedStatus();
    // Outside the `try`: `qecPtr` owns the id whose destructor writes the
    // `end` event, so the status must be set before it unwinds.
    // Workaround for a GCC 15/16 bug: the hidden object of a by-value
    // structured binding is not destroyed when the coroutine frame is
    // destroyed while suspended (gcc.gnu.org bug 124584).
    auto preparedOp = prepareOperation(operationName, operationString,
                                       std::move(messageSender), parameters,
                                       timeLimit, accessTokenOk, clientIp);
    auto& [makeQec, cancellationHandle, cancelTimeoutOnDestruction] =
        preparedOp;
    try {
      if (!ql::ranges::all_of(operations, expectedOperation)) {
        throw std::runtime_error(absl::StrCat(
            msg, ad_utility::truncateOperationString(operationString)));
      }
      if (ql::ranges::all_of(operations, &ParsedQuery::hasUpdateClause)) {
        metrics_->startedSparqlOperations_->Add(1, {OperationType::update});
        co_await processUpdate(std::move(makeQec), std::move(operations),
                               requestTimer, tracer, cancellationHandle,
                               std::move(request), send, timeLimit,
                               plannedQuery);
      } else {
        AD_CORRECTNESS_CHECK(operations.size() == 1);
        ParsedQuery query = std::move(operations[0]);
        AD_CORRECTNESS_CHECK(query.hasSelectClause() || query.hasAskClause() ||
                             query.hasConstructClause());
        metrics_->startedSparqlOperations_->Add(1, {OperationType::query});
        // Queries run against a consistent snapshot taken at the start of the
        // request, so build the execution context from that snapshot here.
        auto qecPtr = makeQec(indexAndViews);
        co_await processQuery(parameters, std::move(query), requestTimer,
                              cancellationHandle, *qecPtr, std::move(request),
                              send, timeLimit, plannedQuery);
      }
      queryStatus->store(OK);
      co_return;
    } catch (const ad_utility::CancellationException& e) {
      queryStatus->store(e.state() == ad_utility::CancellationState::TIMEOUT
                             ? TIMEOUT
                             : CANCELLED);
      throw;
    }
  };
  auto visitQuery = [&index, &visitOperation](Query query) -> Awaitable<void> {
    // We need to copy the query string because `visitOperation` below also
    // needs it.
    auto parsedQuery = SparqlParser::parseQuery(
        &index.encodedIriManager(), query.query_, query.datasetClauses_);
    auto dummy = std::make_shared<ad_utility::timer::TimeTracer>("dummy");
    return visitOperation(
        {std::move(parsedQuery)}, "SPARQL query", std::move(query.query_),
        std::not_fn(&ParsedQuery::hasUpdateClause),
        "SPARQL QUERY was requested via the HTTP request, but the "
        "following update was sent instead of an query: ",
        dummy);
  };
  auto visitUpdate = [&index, &visitOperation, &requireValidAccessToken](
                         Update update) -> Awaitable<void> {
    requireValidAccessToken("SPARQL Update");
    // We need to copy the update string because `visitOperation` below also
    // needs it.
    auto tracer = std::make_shared<ad_utility::timer::TimeTracer>("update");
    tracer->beginTrace("parsing");
    auto parsedUpdates = SparqlParser::parseUpdate(
        index.getBlankNodeManager(), &index.encodedIriManager(), update.update_,
        update.datasetClauses_);
    tracer->endTrace("parsing");
    return visitOperation(
        std::move(parsedUpdates), "SPARQL update", std::move(update.update_),
        &ParsedQuery::hasUpdateClause,
        "SPARQL UPDATE was requested via the HTTP request, but the "
        "following query was sent instead of an update: ",
        tracer);
  };
  auto visitGraphStore =
      [&request, &visitOperation, &requireValidAccessToken,
       &index](GraphStoreOperation operation) -> Awaitable<void> {
    auto tracer = std::make_shared<ad_utility::timer::TimeTracer>("update");
    tracer->beginTrace("parsing");
    std::vector<ParsedQuery> parsedOperations =
        GraphStoreProtocol::transformGraphStoreProtocol(std::move(operation),
                                                        request, index);
    tracer->endTrace("parsing");

    if (ql::ranges::any_of(parsedOperations, &ParsedQuery::hasUpdateClause)) {
      AD_CORRECTNESS_CHECK(
          ql::ranges::all_of(parsedOperations, &ParsedQuery::hasUpdateClause));
      requireValidAccessToken("Update from Graph Store Protocol");
    }

    // Don't check for the `ParsedQuery`s actual type (Query or Update) here
    // because graph store operations can result in both.
    auto trueFunc = [](const ParsedQuery&) { return true; };
    std::string operationString = parsedOperations[0]._originalString;
    return visitOperation(
        std::move(parsedOperations),
        absl::StrCat("Graph Store (", std::string_view{request.method_string()},
                     ")"),
        std::move(operationString), trueFunc, "Unused dummy message", tracer);
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

// Explicit instantiation so that friend test code (`ServerForTesting`, the
// `IndexRebuilder` `FRIEND_TEST`s), which cannot see this template's
// definition, can call `process` directly with a `MockSend` to capture the
// response instead of sending it.
template Server::Awaitable<void>
Server::process<Server::StringBodyRequest, Server::MockSend&>(
    StringBodyRequest&, MockSend&);

// ____________________________________________________________________________
Server::PlannedQuery Server::planQuery(
    ParsedQuery&& operation, QueryExecutionContext& qec,
    ad_utility::SharedCancellationHandle handle, TimeLimit timeLimit,
    const ad_utility::Timer& requestTimer) const {
  PlannedQuery plannedQuery = qlever().planQuery(
      std::move(operation), qec, std::move(handle), timeLimit, requestTimer);

  const auto& qet = plannedQuery.queryExecutionTree();
  const auto& runtimeInfoWholeQuery =
      qet.getRootOperation()->getRuntimeInfoWholeQuery();
  auto timeForQueryPlanning = runtimeInfoWholeQuery.timeQueryPlanning;
  AD_LOG_INFO << "Query planning done in " << timeForQueryPlanning.count()
              << " ms" << std::endl;
  AD_LOG_TRACE << qet.getCacheKey() << std::endl;
  return plannedQuery;
}

// _____________________________________________
CPP_template_def(typename RequestT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    ad_utility::websocket::OwningQueryId Server::getQueryId(
        const RequestT& request, std::string_view query,
        std::string_view clientIp) {
  using ad_utility::websocket::OwningQueryId;
  std::string_view queryIdHeader = request.base()["Query-Id"];
  if (queryIdHeader.empty()) {
    return queryRegistry_.uniqueId(query, clientIp);
  }
  auto queryId = queryRegistry_.uniqueIdFromString(std::string(queryIdHeader),
                                                   query, clientIp);
  if (!queryId) {
    throw QueryAlreadyInUseError{queryIdHeader};
  }
  return std::move(queryId.value());
}

// _____________________________________________________________________________
CPP_template_def(typename RequestT, typename SendT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::sendStreamableResponse(
        const RequestT& request, SendT& send, MediaType mediaType,
        const PlannedQuery plannedQuery, const ad_utility::Timer requestTimer,
        SharedCancellationHandle cancellationHandle) const {
  auto responseGenerator = ExportQueryExecutionTrees::computeResult(
      plannedQuery.parsedQuery(), plannedQuery.queryExecutionTree(), mediaType,
      requestTimer, std::move(cancellationHandle));

  auto response = ad_utility::httpUtils::createOkResponse(
      std::move(responseGenerator), request, mediaType);
  if (plannedQuery.parsedQuery().responseMiddleware_.has_value()) {
    response =
        plannedQuery.parsedQuery().responseMiddleware_.value().applyQuery(
            std::move(response));
  }
  try {
    co_await send(std::move(response));
  } catch (const boost::system::system_error& e) {
    // "Broken Pipe" errors are thrown and reported by `streamable_body`,
    // so we can safely ignore these kind of exceptions. In practice this
    // should only ever "commonly" happen with `CancellationException`s.
    if (e.code().value() == EPIPE) {
      co_return;
    }
    AD_LOG_ERROR << "Unexpected error while sending response: " << e.what()
                 << std::endl;
    metrics_->sparqlErrors_->Add(1, {SparqlErrorType::systemError});
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
    AD_LOG_ERROR << e.what() << std::endl;
    metrics_->sparqlErrors_->Add(1, {SparqlErrorType::sendStreamableResponse});
  }
}

// ____________________________________________________________________________
CPP_template_def(typename RequestT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    ad_utility::websocket::MessageSender Server::createMessageSender(
        const std::weak_ptr<ad_utility::websocket::QueryHub>& queryHub,
        const RequestT& request, std::string_view operation,
        std::string_view clientIp) {
  auto queryHubLock = queryHub.lock();
  AD_CORRECTNESS_CHECK(queryHubLock);
  ad_utility::websocket::MessageSender messageSender{
      getQueryId(request, operation, clientIp), *queryHubLock};
  return messageSender;
}

// _____________________________________________________________________________
ad_utility::MediaType Server::chooseBestFittingMediaType(
    const std::vector<ad_utility::MediaType>& candidates,
    const ParsedQuery& parsedQuery) {
  if (!candidates.empty()) {
    using enum ad_utility::MediaType;
    static constexpr auto askTypes =
        ExportQueryExecutionTrees::supportedMediaTypesForAskQueries;
    static constexpr auto selectTypes =
        ExportQueryExecutionTrees::supportedMediaTypesForSelectQueries;
    static constexpr auto constructTypes =
        ExportQueryExecutionTrees::supportedMediaTypesForConstructQueries;

    ql::span<const MediaType> supported;
    if (parsedQuery.hasAskClause()) {
      supported = askTypes;
    } else if (parsedQuery.hasSelectClause()) {
      supported = selectTypes;
    } else {
      AD_CORRECTNESS_CHECK(parsedQuery.hasConstructClause());
      supported = constructTypes;
    }

    auto it = ql::ranges::find_if(candidates, [supported](MediaType mediaType) {
      return ad_utility::contains(supported, mediaType);
    });
    if (it != candidates.end()) {
      return *it;
    }
  }

  return parsedQuery.hasConstructClause() ? MediaType::turtle
                                          : MediaType::sparqlJson;
}

// ____________________________________________________________________________
CPP_template_def(typename RequestT, typename SendT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::processQuery(
        const ParamValueMap& params, ParsedQuery&& query,
        const ad_utility::Timer& requestTimer,
        ad_utility::SharedCancellationHandle cancellationHandle,
        QueryExecutionContext& qec, const RequestT& request, SendT&& send,
        TimeLimit timeLimit, std::optional<PlannedQuery>& plannedQuery) {
  AD_CORRECTNESS_CHECK(!query.hasUpdateClause());
  ad_utility::metrics::ActiveCounterGuard queryGuard{
      *metrics_->runningSparqlOperations_, "query"};

  auto mediaTypes = qlever::http_api_helpers::determineMediaTypes(
      params, request.base()[http::field::accept]);
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
        return this->planQuery(std::move(query), qec, cancellationHandle,
                               timeLimit, requestTimer);
      },
      cancellationHandle);
  plannedQuery = co_await std::move(coroutine);
  auto qet = plannedQuery.value().queryExecutionTree();

  MediaType mediaType = chooseBestFittingMediaType(
      mediaTypes, plannedQuery.value().parsedQuery());

  // Only post updates when we export `qlever-results+json` or
  // 'sparql-results+json`.
  if (mediaType != MediaType::qleverJson &&
      mediaType != MediaType::sparqlJson) {
    qec.areWebsocketUpdatesEnabled_ = false;
  }

  // Update the `ParsedQuery` with the export limit when the response
  // content-type is `application/qlever-results+json` (or, if enabled via
  // runtime parameter, `application/sparql-results+json`). The `send`
  // parameter is validated regardless of the content-type.
  plannedQuery->parsedQuery().updateExportLimit(
      qlever::http_api_helpers::determineSendLimit(params, mediaType));

  // This actually processes the query and sends the result in the
  // requested format.
  co_await sendStreamableResponse(request, AD_FWD(send), mediaType,
                                  plannedQuery.value(), requestTimer,
                                  cancellationHandle);
  // Print the runtime info. This needs to be done after the query
  // was computed.
  AD_LOG_INFO << "Done processing query and sending result"
              << ", total time was " << requestTimer.msecs().count() << " ms"
              << std::endl;
  metrics_->sparqlOperationDuration_->Record(
      static_cast<double>(requestTimer.msecs().count()),
      {OperationType::query});
  metrics_->finishedSparqlOperations_->Add(1, {OperationType::query});

  // Log that we are done with the query and how long it took.
  //
  // TODO<joka921> Also log an identifier of the query.
  AD_LOG_DEBUG << "Runtime Info:\n"
               << plannedQuery.value()
                      .queryExecutionTree()
                      .getRootOperation()
                      ->runtimeInfo()
                      .toString()
               << std::endl;
  co_return;
}

// ____________________________________________________________________________
nlohmann::ordered_json Server::createResponseMetadataForUpdate(
    const LocatedTriplesState& locatedTriples, const PlannedQuery& plannedQuery,
    const UpdateMetadata& updateMetadata,
    const ad_utility::timer::TimeTracer& tracer) {
  AD_CORRECTNESS_CHECK(updateMetadata.countBefore_.has_value());
  AD_CORRECTNESS_CHECK(updateMetadata.inUpdate_.has_value());
  AD_CORRECTNESS_CHECK(updateMetadata.countAfter_.has_value());
  auto& qet = plannedQuery.queryExecutionTree();
  auto warnings = qet.collectWarnings();
  warnings.emplace(warnings.begin(),
                   "SPARQL 1.1 Update for QLever is experimental.");
  nlohmann::ordered_json response;
  response["update"] = ad_utility::truncateOperationString(
      plannedQuery.parsedQuery()._originalString);
  response["status"] = "OK";
  response["warnings"] = warnings;
  response["runtimeInformation"]["meta"] = nlohmann::ordered_json(
      qet.getRootOperation()->getRuntimeInfoWholeQuery());
  response["runtimeInformation"]["query_execution_tree"] =
      nlohmann::ordered_json(qet.getRootOperation()->runtimeInfo());
  auto setIfHasValue = [&response, &updateMetadata](
                           auto field, const std::string& fieldName) {
    const auto& countOpt = std::invoke(field, updateMetadata);
    if (countOpt.has_value()) {
      response["delta-triples"][fieldName] = nlohmann::json(countOpt.value());
    }
  };
  setIfHasValue(&UpdateMetadata::countBefore_, "before");
  setIfHasValue(&UpdateMetadata::countAfter_, "after");
  setIfHasValue(&UpdateMetadata::inUpdate_, "operation");
  if (updateMetadata.countAfter_.has_value() &&
      updateMetadata.countBefore_.has_value()) {
    response["delta-triples"]["difference"] =
        nlohmann::json(updateMetadata.countAfter_.value() -
                       updateMetadata.countBefore_.value());
  }
  response["time"] = tracer.getJSONShort()["update"];
  for (auto permutation : Permutation::ALL) {
    response["located-triples"][Permutation::toString(
        permutation)]["blocks-affected"] =
        locatedTriples.getLocatedTriplesForPermutation<false>(permutation)
            .numBlocks();
    auto numBlocks = plannedQuery.getIndex()
                         .getPimpl()
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
CPP_template_def(typename RequestT, typename SendT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::processUpdate(
        MakeQueryExecutionContext makeQec, std::vector<ParsedQuery>&& updates,
        const ad_utility::Timer& requestTimer, SharedTimeTracer outerTracer,
        ad_utility::SharedCancellationHandle cancellationHandle,
        const RequestT& request, SendT&& send, TimeLimit timeLimit,
        std::optional<PlannedQuery>& plannedUpdate) {
  outerTracer->beginTrace("waitingForUpdateThread");
  ad_utility::metrics::ActiveCounterGuard updateGuard{
      *metrics_->runningSparqlOperations_, "update"};
  AD_CORRECTNESS_CHECK(ql::ranges::all_of(
      updates, [](const ParsedQuery& p) { return p.hasUpdateClause(); }));

  auto responseMiddlewares =
      ad_utility::RvalueView(
          updates | ql::views::transform(&ParsedQuery::responseMiddleware_) |
          ql::views::filter(ad_utility::hasValue) |
          ql::views::transform(ad_utility::value)) |
      ::ranges::to<std::vector>();

  std::vector<UpdateMetadata> metadatas;

  // If multiple updates are part of a single request, those have to run
  // atomically. This is ensured, because the updates below are run on the
  // `updateThreadPool_`, which only has a single thread.
  static_assert(UPDATE_THREAD_POOL_SIZE == 1);
  auto coroutine = computeInNewThread(
      updateThreadPool_,
      [this, &makeQec, &requestTimer, &cancellationHandle, &updates, &timeLimit,
       &plannedUpdate, outerTracer, &metadatas]() {
        outerTracer->endTrace("waitingForUpdateThread");
        // Snapshot and build the context on the update thread (see
        // `clear-delta-triples`), so the update sees and modifies the currently
        // active index. The resulting `plannedUpdate` keeps the context alive
        // past this lambda via `PlannedQuery`'s shared ownership.
        auto indexAndViews = indexAndViewsSnapshot();
        auto& index = indexAndViews->index_;
        auto qecPtr = makeQec(indexAndViews);
        auto& qec = *qecPtr;
        return index.deltaTriplesManager().modify<json>(
            [this, &cancellationHandle, &plannedUpdate, &updates, &requestTimer,
             &timeLimit, &qec, &metadatas](DeltaTriples& deltaTriples) {
              qec.setLocatedTriplesForEvaluation(
                  deltaTriples.getLocatedTriplesSharedStateReference());
              json results = json::array();
              for (auto&& [i, update] : ranges::views::enumerate(updates)) {
                auto tracer = ad_utility::timer::TimeTracer("update");
                // The augmented metadata is invalidated by any update. It is
                // only updated automatically at the end of modify. Updates with
                // non-empty graph patterns need the augmented metadata. Update
                // the augmented metadata before executing those updates.
                tracer.beginTrace("updateMetadata");
                if (i != 0 &&
                    !update._rootGraphPattern._graphPatterns.empty()) {
                  deltaTriples.updateAugmentedMetadata();
                }
                tracer.endTrace("updateMetadata");
                tracer.beginTrace("planning");
                plannedUpdate =
                    planQuery(std::move(update), qec, cancellationHandle,
                              timeLimit, requestTimer);
                tracer.endTrace("planning");
                tracer.beginTrace("execution");
                // Update the delta triples.
                // Use `this` explicitly to silence false-positive
                // errors on captured `this` being unused.
                auto updateMetadata = this->qlever().applyUpdate(
                    plannedUpdate.value(), cancellationHandle, deltaTriples,
                    tracer);
                tracer.endTrace("execution");

                tracer.endTrace("update");
                results.push_back(createResponseMetadataForUpdate(
                    *deltaTriples.getLocatedTriplesSharedStateReference(),
                    *plannedUpdate, updateMetadata, tracer));
                metadatas.push_back(std::move(updateMetadata));

                AD_LOG_INFO << "Done processing update, total time was "
                            << requestTimer.msecs().count() << " ms"
                            << std::endl;
                AD_LOG_DEBUG << "Runtime Info:\n"
                             << plannedUpdate->queryExecutionTree()
                                    .getRootOperation()
                                    ->runtimeInfo()
                                    .toString()
                             << std::endl;
              }
              return results;
            },
            true, true, *outerTracer);
      },
      cancellationHandle);
  auto operations = co_await std::move(coroutine);
  metrics_->sparqlOperationDuration_->Record(
      static_cast<double>(requestTimer.msecs().count()),
      {OperationType::update});
  metrics_->finishedSparqlOperations_->Add(1, {OperationType::update});
  // With `--rebuild-index-strategy` set, an update can bring the delta triples
  // to a state where the strategy asks for a rebuild, in which case one is
  // started in the background here (without delaying the response below).
  if (!metadatas.empty() && metadatas.back().countAfter_.has_value()) {
    auto numIndexTriples = static_cast<size_t>(
        indexAndViewsSnapshot()->index_.numTriples().normal);
    triggerRebuildIfStrategySaysSo(metadatas.back().countAfter_.value(),
                                   numIndexTriples);
  }
  auto responseJson = nlohmann::ordered_json();
  responseJson["operations"] = operations;
  outerTracer->endTrace("update");
  responseJson["time"] = outerTracer->getJSONShort()["update"];

  // SPARQL 1.1 Protocol 2.2.4 Successful Responses: "The responses body of a
  // successful update request is implementation defined."
  auto response = ad_utility::httpUtils::createJsonResponse(
      std::move(responseJson), request);
  for (auto& middleware : responseMiddlewares) {
    response = middleware.applyUpdate(std::move(response), metadatas);
  }
  co_await send(std::move(response));
  co_return;
}

// ____________________________________________________________________________
CPP_template_def(typename VisitorT, typename RequestT, typename SendT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<void> Server::processOperation(
        SparqlOperation operation, VisitorT visitor,
        const ad_utility::Timer& requestTimer, const RequestT& request,
        SendT& send, const std::optional<PlannedQuery>& plannedQuery) {
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
    metrics_->sparqlErrors_->Add(1, {SparqlErrorType::protocol});
  } catch (const ParseException& e) {
    responseStatus = http::status::bad_request;
    exceptionErrorMsg = e.errorMessageWithoutPositionalInfo();
    metadata = e.metadata();
    metrics_->sparqlErrors_->Add(1, {SparqlErrorType::syntax});
  } catch (const QueryAlreadyInUseError& e) {
    // No `OwningQueryId` exists for this request (creation was rejected).
    responseStatus = http::status::conflict;
    exceptionErrorMsg = e.what();
    metrics_->sparqlErrors_->Add(1, {SparqlErrorType::inUse});
  } catch (const ad_utility::CancellationException& e) {
    // Send 429 status code to indicate that the time limit was reached
    // or the query was cancelled because of some other reason.
    responseStatus = http::status::too_many_requests;
    exceptionErrorMsg = e.what();
    metrics_->sparqlErrors_->Add(1, {SparqlErrorType::timeout});
  } catch (const std::exception& e) {
    responseStatus = http::status::internal_server_error;
    exceptionErrorMsg = e.what();
    metrics_->sparqlErrors_->Add(1, {SparqlErrorType::internal});
  }
  // TODO<qup42> at this stage should probably have a wrapper that takes
  //  optional<errorMsg> and optional<metadata> and does this logic
  if (to_status_class(responseStatus) == http::status_class::informational ||
      responseStatus == http::status::no_content ||
      responseStatus == http::status::not_modified) {
    // HTTP mandates empty response bodies for the status codes 1xx, 204 and
    // 304.
    auto resp =
        createResponseWithEmptyBody(responseStatus, request, std::nullopt);
    co_return co_await send(std::move(resp));
  }
  if (exceptionErrorMsg) {
    AD_LOG_ERROR << exceptionErrorMsg.value() << std::endl;
    if (metadata) {
      // The `coloredError()` message might fail because of the
      // different Unicode handling of QLever and ANTLR. Make sure to
      // detect this case so that we can fix it if it happens.
      try {
        AD_LOG_ERROR << metadata.value().coloredError() << std::endl;
      } catch (const std::exception& e) {
        exceptionErrorMsg.value().append(absl::StrCat(
            " Highlighting an error for the command line log failed: ",
            e.what()));
        AD_LOG_ERROR << "Failed to highlight error in operation. " << e.what()
                     << std::endl;
        AD_LOG_ERROR << metadata.value().query_ << std::endl;
      }
    }
    auto errorResponseJson = responseJson::composeError(
        operationString, exceptionErrorMsg.value(), requestTimer, metadata);
    if (plannedQuery.has_value()) {
      errorResponseJson["runtimeInformation"] =
          nlohmann::ordered_json(plannedQuery.value()
                                     .queryExecutionTree()
                                     .getRootOperation()
                                     ->runtimeInfo());
    }
    auto errResponse =
        createJsonResponse(errorResponseJson, request, responseStatus);
    co_return co_await send(std::move(errResponse));
  }
}

// _____________________________________________________________________________
CPP_template_def(typename Function,
                 typename T)(requires ql::concepts::invocable<Function>)
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
  if (noAccessCheck_) {
    AD_LOG_DEBUG << "Skipping access check" << std::endl;
    return true;
  }
  if (!accessToken) {
    return false;
  }
  const auto accessTokenProvidedMsg = "Access token was provided";
  if (accessToken_.empty()) {
    throw HttpError(
        http::status::forbidden,
        absl::StrCat(accessTokenProvidedMsg,
                     " but server was started without --access-token"));
  } else if (!ad_utility::constantTimeEquals(accessToken.value(),
                                             accessToken_)) {
    throw HttpError(
        http::status::forbidden,
        absl::StrCat(accessTokenProvidedMsg, " but it was invalid"));
  } else {
    AD_LOG_DEBUG << accessTokenProvidedMsg << " and correct" << std::endl;
    return true;
  }
}

// _____________________________________________________________________________
template ad_utility::websocket::MessageSender
Server::createMessageSender<Server::StringBodyRequest>(
    const std::weak_ptr<ad_utility::websocket::QueryHub>&,
    const StringBodyRequest&, std::string_view, std::string_view);

// _____________________________________________________________________________
Awaitable<qlever::IndexSwapConfig> Server::rebuildIndex(
    std::optional<std::string> rebuildTmpDir,
    std::optional<std::string> rebuildPreviousIndexDir) {
  // There is no mechanism to actually cancel the handle.
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  auto indexAndViews = indexAndViewsSnapshot();
  auto& [index, oldManager] = *indexAndViews;

  // Turn the two directories that can be set via command parameters into the
  // base names of the indexes involved in the rebuild. The new index ends up at
  // the base name `index` is currently served from (which is the base name the
  // server was started on, because a rebuild re-anchors the new index to
  // exactly that place, see `Qlever::moveRebuiltIndexIntoPlace`), so that a
  // later restart loads it.
  auto config = qlever::Qlever::makeIndexRebuildConfig(
      index, std::move(rebuildTmpDir), std::move(rebuildPreviousIndexDir));

  // Warn if state that won't carry over to the rebuilt index was previously
  // loaded: the new index never calls `addTextFromOnDiskIndex()` and is paired
  // with a fresh, empty `MaterializedViewsManager`.
  if (index.getNofTextRecords() > 0) {
    AD_LOG_WARN << "A text index was loaded for the current index, but text "
                   "search will no longer work after the rebuild completes. "
                   "Restart the server using the original index to re-enable "
                   "text search."
                << std::endl;
  }
  if (oldManager.hasLoadedViews()) {
    AD_LOG_WARN
        << "Materialized views were loaded for the current index, but they "
           "will no longer be available after the rebuild completes. You'll "
           "have to recompute them on the rebuilt index."
        << std::endl;
  }
  // NOTE: We deliberately use the plain `runFunctionOnExecutor` and not
  // `computeInNewThread` here: the latter wraps the awaitable in
  // `ad_utility::interruptible`, whose cancellation-check timer is useless on
  // this path (the `handle` above can never be cancelled) and whose
  // timer/parallel-group machinery was the prime suspect in a rare, hard to
  // reproduce case where a completed rebuild never resumed this coroutine
  // (all rebuild work done, all threads idle, "Registered ..." never logged).
  //
  // We don't directly `co_await` because of lifetime issues (bugs) in the
  // Conan setup.
  auto coroutine = ad_utility::runFunctionOnExecutor(
      queryThreadPool_.get_executor(),
      [this, &index, &handle, &config] {
        return qlever().rebuildIndexToDisk(index, config, handle);
      },
      net::use_awaitable);
  auto rebuildResult = co_await std::move(coroutine);
  // It is important that the swap is done in the update thread pool, because it
  // prevents other updates from being applied while the diff is computed for
  // the new index. Otherwise, the new index would be out of sync with the
  // current index.
  auto swapRoutine = ad_utility::runFunctionOnExecutor(
      updateThreadPool_.get_executor(),
      [this, &index, &oldManager, rebuildResult = std::move(rebuildResult),
       &handle, &config]() mutable {
        // The swap below moves all files of the old index to a different base
        // name and installs the new index at the base name of the old one. Any
        // view file that `oldManager` created after that point would silently
        // become a view of the NEW index, even though its `Id`s refer to the
        // vocabulary of the old one. `oldManager` outlives the swap (queries
        // that started before it still hold a snapshot of it), so close it for
        // writing first. This blocks until a concurrent
        // `write-materialized-view` or `delete-materialized-view` has finished;
        // the files it created are then moved along with the rest of the old
        // index.
        //
        // NOTE: The other on-disk state of the old index (its persisted delta
        // triples and allocated graph names) needs no such protection, because
        // it is only written from this very executor, which has a single
        // thread.
        oldManager.retireOnDiskFiles();
        // The swap also applies the configured policy for which `previous.*`
        // index directories to keep. Deleting a directory there blocks this
        // single-threaded executor for a bit, but the swap blocks updates
        // anyway, and doing it in the same step avoids an extra executor
        // hop. A cleanup failure is only logged; it does not fail the
        // request, because the new index is already in place.
        qlever().swapInRebuiltIndex(index, std::move(rebuildResult), handle,
                                    config, keepPreviousIndexDirs_);
        auto now = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
        metrics_->indexLoadMetric_->Record(now);
      },
      net::use_awaitable);
  co_await std::move(swapRoutine);
  co_return config;
}

// _____________________________________________________________________________
CPP_template_def(typename RequestT)(
    requires ad_utility::httpUtils::HttpRequest<RequestT>)
    Awaitable<Server::ResponseT> Server::processRebuildIndex(
        const ParamValueMap& parameters, const RequestT& request) {
  using namespace ad_utility::httpUtils;
  auto config = co_await rebuildIndexUnlessInProgress(
      ad_utility::url_parser::checkParameter(parameters, "rebuild-tmp-dir",
                                             std::nullopt),
      ad_utility::url_parser::checkParameter(
          parameters, "rebuild-previous-index-dir", std::nullopt));
  if (!config.has_value()) {
    co_return createHttpResponseFromString(
        "Another rebuild is currently in progress!",
        http::status::too_many_requests, request, MediaType::textPlain);
  }
  co_return createJsonResponse(responseJson::composeRebuildSuccess(*config),
                               request);
}

// _____________________________________________________________________________
Awaitable<std::optional<qlever::IndexSwapConfig>>
Server::rebuildIndexUnlessInProgress(
    std::optional<std::string> rebuildTmpDir,
    std::optional<std::string> rebuildPreviousIndexDir) {
  // The rebuild counts as running until this goes out of scope, which happens
  // whether the rebuild succeeds, throws, or is cancelled.
  auto runningRebuild = rebuildTracker_->tryBegin();
  if (!runningRebuild.has_value()) {
    co_return std::nullopt;
  }
  co_return co_await rebuildIndex(std::move(rebuildTmpDir),
                                  std::move(rebuildPreviousIndexDir));
}

// _____________________________________________________________________________
void Server::triggerRebuildIfStrategySaysSo(const DeltaTriplesCount& count,
                                            size_t numIndexTriples) {
  if (!rebuildIndexStrategy_.has_value()) {
    return;
  }
  // NOTE: Cast before adding: the counts are non-negative here (they are set
  // sizes), and the unsigned addition cannot overflow.
  auto numDeltaTriples = static_cast<size_t>(count.triplesInserted_) +
                         static_cast<size_t>(count.triplesDeleted_);
  if (!rebuildIndexStrategy_->shouldTriggerRebuild(numDeltaTriples,
                                                   numIndexTriples)) {
    return;
  }
  // Cheap early return while a rebuild is running, so that the updates that
  // arrive during it (whose delta triples are carried over into the new index
  // by the swap) do not each spawn a coroutine only to find the guard taken.
  // The authoritative check is the guard in `rebuildIndexUnlessInProgress`,
  // which is shared with the `cmd=rebuild-index` HTTP request, so that a
  // manual and an automatic rebuild can never run concurrently.
  if (rebuildTracker_->poll().has_value()) {
    return;
  }
  AD_LOG_INFO << "Triggering an automatic index rebuild, the number of delta "
                 "triples ("
              << numDeltaTriples << ") has reached the threshold ("
              << rebuildIndexStrategy_->rebuildThreshold(numIndexTriples)
              << ") for the current index size (" << numIndexTriples
              << " triples)" << std::endl;
  net::co_spawn(queryThreadPool_, runAutomaticRebuild(),
                &Server::logAutomaticRebuildFailure);
}

// _____________________________________________________________________________
Awaitable<void> Server::runAutomaticRebuild() {
  auto config =
      co_await rebuildIndexUnlessInProgress(std::nullopt, std::nullopt);
  if (config.has_value()) {
    AD_LOG_INFO << "Automatic index rebuild completed, the new index "
                   "has been swapped in"
                << std::endl;
  } else {
    AD_LOG_INFO << "Automatic index rebuild skipped, another rebuild "
                   "started concurrently"
                << std::endl;
  }
}

// _____________________________________________________________________________
void Server::logAutomaticRebuildFailure(std::exception_ptr exception) {
  if (!exception) {
    return;
  }
  try {
    std::rethrow_exception(exception);
  } catch (const std::exception& e) {
    AD_LOG_ERROR << "Automatic index rebuild failed: " << e.what() << std::endl;
  }
}
