// Copyright 2015 - 2026 The QLever Authors, in particular:
//
// 2015 - 2017 Björn Buchhold, UFR
// 2020 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_SERVER_H
#define QLEVER_SRC_ENGINE_SERVER_H

#include <absl/functional/any_invocable.h>

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "backports/filesystem.h"
#include "engine/HttpApiHelpers.h"
#include "engine/KeepPreviousIndexDirs.h"
#include "engine/MaterializedViews.h"
#include "engine/NamedResultCache.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SortPerformanceEstimator.h"
#include "index/IdTableUtils.h"
#include "index/Index.h"
#include "libqlever/Qlever.h"
#include "libqlever/QleverTypes.h"
#include "util/AllocatorWithLimit.h"
#include "util/ParseException.h"
#include "util/TypeTraits.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"
#include "util/http/streamable_body.h"
#include "util/http/websocket/MessageSender.h"
#include "util/http/websocket/QueryHub.h"
#include "util/json.h"
#include "util/metrics/Metrics.h"
#include "util/metrics/ServerMetrics.h"

template <typename Operation>
CPP_concept QueryOrUpdate =
    ad_utility::SameAsAny<Operation,
                          ad_utility::url_parser::sparqlOperation::Query,
                          ad_utility::url_parser::sparqlOperation::Update>;

// Forward declaration for testing.
namespace serverTestHelpers {
class ServerForTesting;
}

//! The HTTP Server used.
class Server {
  using json = nlohmann::json;
  using SharedIndexAndView = std::shared_ptr<qlever::Qlever::IndexAndViews>;
  using ParamValueMap = ad_utility::url_parser::ParamValueMap;
  using SparqlOperation = ad_utility::url_parser::sparqlOperation::Operation;
  // Build a `QueryExecutionContext` for a given `IndexAndViews` snapshot,
  // capturing the request-specific settings (message sender, pinning). This
  // lets the caller bind the context to whichever snapshot is current when the
  // operation actually runs (see `processUpdate`).
  using MakeQueryExecutionContext =
      absl::AnyInvocable<std::shared_ptr<QueryExecutionContext>(
          SharedIndexAndView)>;
  FRIEND_TEST(ServerTest, getQueryId);
  FRIEND_TEST(ServerTest, createMessageSender);
  FRIEND_TEST(ServerTest, configurePinnedResultWithName);
  FRIEND_TEST(IndexRebuilder, serverIntegration);
  FRIEND_TEST(IndexRebuilder, serverIntegrationDroppedStateWarnings);
  FRIEND_TEST(IndexRebuilder, serverIntegrationAutomaticRebuild);
  FRIEND_TEST(IndexRebuilder, serverIntegrationKeepPreviousIndexDirs);
  friend serverTestHelpers::ServerForTesting;

 public:
  explicit Server(unsigned short port, size_t numThreads,
                  std::string accessToken, const qlever::EngineConfig& config,
                  bool noAccessCheck = false,
                  std::shared_ptr<ad_utility::metrics::MetricsReader>
                      metricsReader = nullptr);

  virtual ~Server() = default;

  // First initialize the server. Then loop, wait for requests and trigger
  // processing. This method never returns except when throwing an exception.
  void run();

  // Open `path` and register start/end callbacks on the query registry that
  // write one JSONL line per query event to it. Call once, after construction.
  void configureQueryEventLog(const ql::filesystem::path& path);

 private:
  qlever::Qlever qlever_;
  const size_t numThreads_;
  unsigned short port_;
  std::string accessToken_;
  bool noAccessCheck_;
  ad_utility::websocket::QueryRegistry queryRegistry_{};

  /// Non-owning reference to the `QueryHub` instance living inside
  /// the `WebSocketHandler` created for `HttpServer`.
  std::weak_ptr<ad_utility::websocket::QueryHub> queryHub_;

  boost::asio::static_thread_pool queryThreadPool_;
  // The update thread pool size has to be `1` s.t. UPDATE operations are run
  // atomically under all circumstances.
  static constexpr size_t UPDATE_THREAD_POOL_SIZE = 1;
  boost::asio::static_thread_pool updateThreadPool_{UPDATE_THREAD_POOL_SIZE};

  /// Executor with a single thread that is used to run timers asynchronously.
  boost::asio::static_thread_pool timerExecutor_{1};

  // Indicates if an index rebuild is currently in progress so that we prevent
  // triggering this twice.
  std::atomic_bool rebuildInProgress_{false};

  // If set, an index rebuild is triggered automatically after an update
  // whenever the strategy says so, see `triggerRebuildIfStrategySaysSo`. Set
  // via the `--rebuild-index-strategy` option of `qlever-server`.
  std::optional<qlever::RebuildIndexStrategy> rebuildIndexStrategy_;

  // Which `previous.*` index directories to keep after a successful rebuild
  // (manual or automatic), see `KeepPreviousIndexDirs`. Set via the
  // `--rebuild-keep-previous-index-dirs` option of `qlever-server`.
  qlever::KeepPreviousIndexDirs keepPreviousIndexDirs_ =
      qlever::KeepPreviousIndexDirs::OriginalAndMostRecent;

  // MetricsReader for serving the /metrics endpoint. `nullptr` when metrics are
  // disabled (--enable-metrics not passed).
  std::shared_ptr<ad_utility::metrics::MetricsReader> metricsReader_;

  // Deregisters callbacks on destruction. Declared after `qlever_` so that it
  // is destroyed before `qlever_` which the callbacks access.
  std::unique_ptr<ServerMetrics> metrics_;

  template <typename T>
  using Awaitable = boost::asio::awaitable<T>;

  using TimeLimit = std::chrono::milliseconds;

  using SharedCancellationHandle = ad_utility::SharedCancellationHandle;
  using SharedTimeTracer = std::shared_ptr<ad_utility::timer::TimeTracer>;
  using PlannedQuery = qlever::PlannedQuery;
  using ResponseT = ad_utility::httpUtils::ResponseT;
  using StringBodyRequest =
      boost::beast::http::request<boost::beast::http::string_body>;

  // A `send` callable for `process`/`handleHttpRequest` that captures
  // whatever response it is invoked with into `response_` instead of
  // actually sending it. Used by friend test code (`ServerForTesting` and the
  // `FRIEND_TEST`s above) to call `process`/`handleHttpRequest` directly and
  // inspect the response that would have been sent. A named type is required
  // here (rather than an ad-hoc lambda) because `process`/`handleHttpRequest`
  // are only defined in `Server.cpp`, so callers in other translation units
  // can only invoke them through an explicit template instantiation, which in
  // turn requires a type with linkage.
  class MockSend {
   public:
    Awaitable<void> operator()(auto response) {
      response_ = std::move(response);
      co_return;
    }

    ResponseT response_;
  };

  CPP_template(typename CancelTimeout)(
      requires ad_utility::isInstantiation<
          CancelTimeout,
          absl::Cleanup>) struct CancellationHandleAndTimeoutTimerCancel {
    SharedCancellationHandle handle_;
    /// Object of type `absl::Cleanup` that when destroyed cancels the timer
    /// that would otherwise invoke the cancellation of the `handle_` via the
    /// time limit.
    CancelTimeout cancelTimeout_;
  };

  // Clang doesn't seem to be able to automatically deduce the type correctly.
  // and GCC 11 thinks deduction guides are not allowed within classes.
#ifdef __clang__
  CPP_template(typename CancelTimeout)(
      requires ad_utility::isInstantiation<CancelTimeout, absl::Cleanup>)
      CancellationHandleAndTimeoutTimerCancel(SharedCancellationHandle,
                                              CancelTimeout)
          -> CancellationHandleAndTimeoutTimerCancel<CancelTimeout>;
#endif

  // Run `qlever().clearDeltaTriples()` on `updateThreadPool_` and return the
  // resulting counts. Not cancellable. Unlike `processVacuumDeltaTriples`
  // below, this is unconditional and has no timeout, so it can never fail
  // partway through.
  Awaitable<DeltaTriplesCount> processClearDeltaTriples();

  // Vacuum (remove redundant) delta triples of the currently active index,
  // honoring a user-submitted timeout (see `verifyUserSubmittedQueryTimeout`).
  // Unlike `processClearDeltaTriples` above, this can fail (because of an
  // invalid timeout), in which case `verifyUserSubmittedQueryTimeout` throws
  // an `HttpError` that unwinds out of `process()`, all the way up to
  // `handleHttpRequest`. Otherwise the resulting vacuum stats are returned.
  Awaitable<json> processVacuumDeltaTriples(
      std::optional<std::string_view> userTimeout, bool accessTokenOk);

  // Handle a `write-materialized-view` command: extract the view name, query,
  // and timeout from `parameters`/`operation`, execute the query, and store
  // its result as a named materialized view. Like `processVacuumDeltaTriples`
  // above, a rejected timeout throws an `HttpError` that unwinds out of
  // `process()`, all the way up to `handleHttpRequest`. Otherwise the
  // resulting materialized-view stats are returned. On success, the caller is
  // responsible for resetting the request's operation to `None{}` so that
  // `process()` doesn't also try to execute it as a regular query.
  Awaitable<json> processWriteMaterializedView(
      const ParamValueMap& parameters, const SparqlOperation& operation,
      bool accessTokenOk, const ad_utility::Timer& requestTimer);

  // Handle a `load-materialized-view` command: extract the view name from
  // `parameters` and load it via `indexAndViews`'s materialized views
  // manager. The caller is responsible for resetting the request's operation
  // to `None{}` so that `process()` doesn't also try to execute it as a
  // regular query. Unlike `processWriteMaterializedView` above, this neither
  // executes a query nor honors a timeout, so it runs synchronously and
  // either returns its result or throws.
  json processLoadMaterializedView(const ParamValueMap& parameters,
                                   const SharedIndexAndView& indexAndViews);

  // Handle a `delete-materialized-view` command: extract the view name from
  // `parameters`, delete it via a freshly taken index/views snapshot (not the
  // one from the beginning of `process()`, so that a concurrent rebuild
  // cannot make this operate on a stale manager). The caller is responsible
  // for resetting the request's operation to `None{}`, like
  // `processLoadMaterializedView` above.
  json processDeleteMaterializedView(const ParamValueMap& parameters) const;

  // Handle the `/ping` endpoint: log the alive check (with or without an
  // accompanying "msg" parameter) and return a fixed confirmation response.
  CPP_template(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) ResponseT
      processPing(std::optional<std::string> msg,
                  const RequestT& request) const;

  // Handle the `/metrics` endpoint: require a valid access token, then
  // return Prometheus-formatted metrics text if enabled
  // (`--enable-metrics`), or a 404 response otherwise.
  CPP_template(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) ResponseT
      processMetrics(bool accessTokenOk, const RequestT& request) const;

  // Set every runtime parameter that's present in `parameters`, verifying the
  // access token if there is at least one such runtime parameter. If any
  // runtime parameter was changed, return the representation of all runtime
  // parameters, else `nullopt`.
  std::optional<json> processSetRuntimeParameters(
      const ParamValueMap& parameters, bool accessTokenOk) const;

  // Handle a `rebuild-index` command: extract the tmp-dir/previous-index-dir
  // parameters and trigger a rebuild unless one is already in progress.
  // Unlike `processVacuumDeltaTriples`/`processWriteMaterializedView` above,
  // this never needs to bypass query processing, so it returns the response
  // directly.
  CPP_template(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<ResponseT> processRebuildIndex(const ParamValueMap& parameters,
                                               const RequestT& request);

  // Result of `processCommands` below.
  struct ProcessCommandsResult {
    // The response produced by the matched `cmd=` URL parameter, if any.
    std::optional<ResponseT> response_;

    // The commands `write-materialized-view`, `load-materialized-view`, and
    // `delete-materialized-view` already execute the given query themselves.
    // Set to true by one of them to tell `process()` not to run the
    // operation again via `processOperation`.
    bool consumedQueryOperation_ = false;
  };

  // Handle the `cmd=<name>` URL parameter (see `serverProcessHelpers::
  // commands` in `Server.cpp` for the full list); throws an `HttpError` if
  // `cmd` is set but not one of those. `operation` is the parsed
  // "query"/"update"/graph-store operation of the same request, if any; for
  // `write-materialized-view` it doubles as the view-defining query. For
  // that command as well as `load-materialized-view` and
  // `delete-materialized-view`, the returned
  // `ProcessCommandsResult::consumedQueryOperation_` is set to tell
  // `process()` not to also execute it as a regular query.
  CPP_template(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<ProcessCommandsResult> processCommands(
          const SharedIndexAndView& indexAndViews,
          const ParamValueMap& parameters, const SparqlOperation& operation,
          bool accessTokenOk, const ad_utility::Timer& requestTimer,
          RequestT& request);

  // Initialize and register server metrics which are stored in `metrics_`.
  void initializeServerMetrics(
      std::optional<ad_utility::MemorySize> memoryLimit);

  // Log `message`, record it under `errorType` in the HTTP error metrics,
  // and build the corresponding HTTP error response for `request`.
  CPP_template(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) ResponseT
      reportHttpError(std::string_view message,
                      ad_utility::httpUtils::http::status status,
                      const RequestT& request,
                      const ad_utility::metrics::MetricLabel& errorType) const;

  // The `HttpHandler` passed to `HttpServer` in `run()`. This function
  // satisfies the constraints for the `HttpHandler` in `HttpServer.h`.
  //
  // Reply to OPTIONS requests immediately by allowing everything. This is
  // necessary because some POST queries (in particular, from the QLever UI)
  // are preceded by an OPTIONS request (a so-called "preflight" request,
  // which asks permission for the POST query).
  //
  // Process all other requests using `process()`. If that throws, turn the
  // exception into an HTTP error response via `reportHttpError` (which also
  // logs it and updates the error metrics).
  //
  // Send every response (including error responses) with a maximally
  // permissive CORS header, which allows the client that receives the
  // response to do with it what it wants. Strictly, only OPTIONS requests
  // need the "allow headers" header, while GET and POST only need "allow
  // origin"; the same headers are sent for all three to avoid two similar
  // code paths.
  CPP_template(typename RequestT, typename SendT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> handleHttpRequest(RequestT request, SendT& send);

  // Build the `WebSocketHandler` passed to `HttpServer` in `run()`. Call once
  // at server startup with the server's `io_context` executor; set up the
  // `QueryHub` for that executor and return the handler that dispatches
  // individual WebSocket sessions to it.
  std::function<Awaitable<void>(const StringBodyRequest&,
                                boost::asio::ip::tcp::socket)>
  makeWebSocketSessionSupplier(boost::asio::any_io_executor& ioExecutor);
  FRIEND_TEST(ServerTest, makeWebSocketSessionSupplier);

  /// Handle a single HTTP request. Check whether a file request or a query was
  /// sent, and dispatch to functions handling these cases. This function
  /// requires the constraints for the `HttpHandler` in `HttpServer.h`.
  /// \param req The HTTP request.
  /// \param send The action that sends a http:response. (see the
  ///             `HttpServer.h` for documentation).
  CPP_template(typename RequestT, typename SendT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> process(RequestT& request, SendT&& send);

  // The final step of `process()`: by this point the operation type (which also
  // can be `no-operation`) is known, so this builds the
  // query/update/graph-store-protocol/no-operation visitors and hands them,
  // together with `operation`, to `processOperation`.
  CPP_template(typename RequestT, typename SendT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> processSparqlOperation(
          SparqlOperation operation, const ParamValueMap& parameters,
          bool accessTokenOk, const ad_utility::Timer& requestTimer,
          SharedIndexAndView indexAndViews, RequestT& request, SendT&& send,
          std::optional<ResponseT> response);

  // Wraps the error handling around the processing of operations. Calls the
  // visitor on the given operation.
  CPP_template(typename VisitorT, typename RequestT, typename SendT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> processOperation(
          SparqlOperation operation, VisitorT visitor,
          const ad_utility::Timer& requestTimer, const RequestT& request,
          SendT& send, const std::optional<PlannedQuery>& plannedQuery);

  // Out of a list of allowed media types, choose the one that best fits the
  // given query type. Currently it just chooses the first from the list. If the
  // list is empty, just choose one that works for the given query type.
  static ad_utility::MediaType chooseBestFittingMediaType(
      const std::vector<ad_utility::MediaType>& candidates,
      const ParsedQuery& parsedQuery);
  FRIEND_TEST(ServerTest, chooseBestFittingMediaType);

  // Do the actual execution of a query.
  CPP_template(typename RequestT, typename SendT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> processQuery(
          const ParamValueMap& params, ParsedQuery&& query,
          const ad_utility::Timer& requestTimer,
          ad_utility::SharedCancellationHandle cancellationHandle,
          QueryExecutionContext& qec, const RequestT& request, SendT&& send,
          TimeLimit timeLimit, std::optional<PlannedQuery>& plannedQuery);
  // For an executed update create a JSON with some stats on the update (timing,
  // number of changed triples, etc.).
  static nlohmann::ordered_json createResponseMetadataForUpdate(
      const LocatedTriplesState& locatedTriples,
      const PlannedQuery& plannedQuery, const UpdateMetadata& updateMetadata,
      const ad_utility::timer::TimeTracer& tracer);
  FRIEND_TEST(ServerTest, createResponseMetadata);
  // Do the actual execution of an update.
  CPP_template(typename RequestT, typename SendT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> processUpdate(
          MakeQueryExecutionContext makeQec, std::vector<ParsedQuery>&& updates,
          const ad_utility::Timer& requestTimer, SharedTimeTracer tracer,
          ad_utility::SharedCancellationHandle cancellationHandle,
          const RequestT& request, SendT&& send, TimeLimit timeLimit,
          std::optional<PlannedQuery>& plannedUpdate);

  //  Prepare the execution of an operation.
  auto prepareOperation(std::string_view operationName,
                        std::string_view operationSPARQL,
                        ad_utility::websocket::MessageSender messageSender,
                        const ParamValueMap& params, TimeLimit timeLimit,
                        bool accessTokenOk, std::string_view clientIp);

  // Configure pinning of a named result on the `qec`. If `pinResultWithName`
  // is set, then the `qec` is configured such that the query result will be
  // stored in the named result cache accordingly. Throw if `pinResultWithName`
  // is set, but the access token is not okay.
  static void configurePinnedResultWithName(
      std::optional<QueryExecutionContext::PinResultWithName> pinResultWithName,
      bool accessTokenOk, QueryExecutionContext& qec);

  // Plan a parsed query.
  PlannedQuery planQuery(ParsedQuery&& operation, QueryExecutionContext& qec,
                         SharedCancellationHandle handle, TimeLimit timeLimit,
                         const ad_utility::Timer& requestTimer) const;
  // Creates a `MessageSender` for the given operation.
  CPP_template(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      ad_utility::websocket::MessageSender createMessageSender(
          const std::weak_ptr<ad_utility::websocket::QueryHub>& queryHub,
          const RequestT& request, std::string_view operation,
          std::string_view clientIp = {});
  /// Invoke `function` on `threadPool_`, and return an awaitable to wait for
  /// its completion, wrapping the result.
  CPP_template(typename Function, typename T = std::invoke_result_t<Function>)(
      requires ql::concepts::invocable<Function>)
      Awaitable<T> computeInNewThread(
          boost::asio::static_thread_pool& threadPool, Function function,
          SharedCancellationHandle handle);

  /// This method extracts a client-defined query id from the passed HTTP
  /// request if it is present. If it is not present or empty, a new
  /// pseudo-random id will be chosen by the server. Note that this id is not
  /// communicated to the client in any way. It ensures that every query has a
  /// unique id and therefore that the code doesn't need to check for an empty
  /// case. In the case of conflict when using a manual id, a
  /// `QueryAlreadyInUseError` exception is thrown.
  ///
  /// \param request The HTTP request to extract the id from.
  /// \param query A string representation of the query to register an id for.
  ///
  /// \return An OwningQueryId object. It removes itself from the registry
  ///         on destruction.
  CPP_template(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      ad_utility::websocket::OwningQueryId
      getQueryId(const RequestT& request, std::string_view query,
                 std::string_view clientIp = {});

  /// Schedule a task to trigger the timeout after the `timeLimit`.
  /// The returned callback can be used to prevent this task from executing
  /// either because the `cancellationHandle` has been aborted by some other
  /// means or because the task has been completed successfully.
  auto cancelAfterDeadline(
      std::weak_ptr<ad_utility::CancellationHandle<>> cancellationHandle,
      TimeLimit timeLimit)
      -> QL_CONCEPT_OR_NOTHING(
          ad_utility::InvocableWithExactReturnType<void>) auto;

  /// Acquire the `CancellationHandle` for the given `QueryId`, start the
  /// watchdog and call `cancelAfterDeadline` to set the timeout after
  /// `timeLimit`. Return an object of type
  /// `CancellationHandleAndTimeoutTimerCancel`, where the `cancelTimeout_`
  /// member can be invoked to cancel the imminent cancellation via timeout.
  auto setupCancellationHandle(const ad_utility::websocket::QueryId& queryId,
                               TimeLimit timeLimit)
      -> QL_CONCEPT_OR_NOTHING(ad_utility::isInstantiation<
                               CancellationHandleAndTimeoutTimerCancel>) auto;

  /// Check if the access token is valid. Return true if the access token
  /// exists and is valid. Return false if there's no access token passed.
  /// Throw an exception if there is a token passed but it doesn't match,
  /// or there is no access token set by the server config. The error message is
  /// formulated towards end users, it can be sent directly as the text of an
  /// HTTP error response.
  bool checkAccessToken(std::optional<std::string_view> accessToken) const;
  FRIEND_TEST(ServerTest, checkAccessToken);

  /// Check if user-provided timeout is authorized with a valid access-token or
  /// lower than the server default. Throw an `HttpError` (403 Forbidden) if
  /// the change is not allowed. For queries and updates, `processOperation`
  /// catches it and builds the standard JSON error response. On the `cmd=`
  /// paths it unwinds to `handleHttpRequest`, which builds a plain-text
  /// response. Return the new timeout otherwise.
  TimeLimit verifyUserSubmittedQueryTimeout(
      std::optional<std::string_view> userTimeout, bool accessTokenOk) const;

  /// Send response for the streamable media types (tsv, csv, octet-stream,
  /// turtle, sparqlJson, qleverJson).
  CPP_template(typename RequestT, typename SendT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> sendStreamableResponse(
          const RequestT& request, SendT& send, ad_utility::MediaType mediaType,
          const PlannedQuery plannedQuery, const ad_utility::Timer requestTimer,
          SharedCancellationHandle cancellationHandle) const;

  FRIEND_TEST(MaterializedViewsTest, serverIntegration);

  // Trigger an index rebuild: build a new index from the current state
  // (including updates) in a temporary directory, swap it in, move the old
  // index to the directory for the old index, and move the new index to the
  // place of the old one (see `Qlever::swapInRebuiltIndex`). The two optional
  // arguments override the defaults for the temporary directory and the
  // directory for the old index; the full resolved configuration is returned.
  // This assumes that the access token has already been checked and no other
  // rebuild is currently in progress.
  Awaitable<qlever::IndexSwapConfig> rebuildIndex(
      std::optional<std::string> rebuildTmpDir,
      std::optional<std::string> rebuildPreviousIndexDir);

  // Like `rebuildIndex` above, but do nothing and return `std::nullopt` if
  // another rebuild is currently in progress (the `rebuildInProgress_` flag
  // is held for the duration of the rebuild). This is the common
  // implementation behind the two ways of triggering a rebuild: the manual
  // `cmd=rebuild-index` HTTP request and the automatic trigger below.
  Awaitable<std::optional<qlever::IndexSwapConfig>>
  rebuildIndexUnlessInProgress(
      std::optional<std::string> rebuildTmpDir,
      std::optional<std::string> rebuildPreviousIndexDir);

  // If `rebuildIndexStrategy_` is set and it says a rebuild should be
  // triggered for `count` (the number of delta triples after an update) and
  // the given number of triples in the current index, trigger an index
  // rebuild in the background, unless one is already in progress. Returns
  // immediately; the rebuild runs detached and logs its success or failure.
  void triggerRebuildIfStrategySaysSo(const DeltaTriplesCount& count,
                                      size_t numIndexTriples);

  // The background coroutine spawned by `triggerRebuildIfStrategySaysSo`:
  // run the rebuild (unless one is already in progress) and log the outcome.
  Awaitable<void> runAutomaticRebuild();

  // Completion handler of that coroutine: log the exception, if there is one.
  static void logAutomaticRebuildFailure(std::exception_ptr exception);

  // Getters for the `Qlever` instance, as well as its data members.
  qlever::Qlever& qlever() { return qlever_; }
  const qlever::Qlever& qlever() const { return qlever_; }

  QueryResultCache& cache() { return qlever().cache(); }
  const QueryResultCache& cache() const { return qlever().cache(); }
  ad_utility::AllocatorWithLimit<Id>& allocator() {
    return qlever().allocator();
  }
  const ad_utility::AllocatorWithLimit<Id>& allocator() const {
    return qlever().allocator();
  }
  SortPerformanceEstimator& sortPerformanceEstimator() {
    return qlever().sortPerformanceEstimator();
  }
  const SortPerformanceEstimator& sortPerformanceEstimator() const {
    return qlever().sortPerformanceEstimator();
  }
  NamedResultCache& namedResultCache() { return qlever().namedResultCache(); }
  const NamedResultCache& namedResultCache() const {
    return qlever().namedResultCache();
  }

  // Atomically snapshot both the `Index` and the `MaterializedViewsManager`
  // under a single read lock, so that all code paths handling a single request
  // observe a matching pair even if a concurrent rebuild swaps the pointers
  // between two reads.
  SharedIndexAndView indexAndViewsSnapshot() const {
    return qlever().indexAndViewsSnapshot();
  }
};

#endif  // QLEVER_SRC_ENGINE_SERVER_H
