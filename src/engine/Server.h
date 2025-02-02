// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach<kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <util/http/websocket/MessageSender.h>

#include <string>
#include <vector>

#include "ExecuteUpdate.h"
#include "engine/Engine.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SortPerformanceEstimator.h"
#include "index/Index.h"
#include "parser/SparqlParser.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParseException.h"
#include "util/TypeTraits.h"
#include "util/http/HttpServer.h"
#include "util/http/streamable_body.h"
#include "util/http/websocket/QueryHub.h"
#include "util/json.h"

using nlohmann::json;
using std::string;
using std::vector;

//! The HTTP Server used.
class Server {
  FRIEND_TEST(ServerTest, parseHttpRequest);
  FRIEND_TEST(ServerTest, getQueryId);
  FRIEND_TEST(ServerTest, createMessageSender);
  FRIEND_TEST(ServerTest, extractAccessToken);

 public:
  explicit Server(unsigned short port, size_t numThreads,
                  ad_utility::MemorySize maxMem, std::string accessToken,
                  bool usePatternTrick = true);

  virtual ~Server() = default;

 private:
  //! Initialize the server.
  void initialize(const string& indexBaseName, bool useText,
                  bool usePatterns = true, bool loadAllPermutations = true);

 public:
  //! First initialize the server. Then loop, wait for requests and trigger
  //! processing. This method never returns except when throwing an exception.
  void run(const string& indexBaseName, bool useText, bool usePatterns = true,
           bool loadAllPermutations = true);

  Index& index() { return index_; }
  const Index& index() const { return index_; }

  /// Helper struct bundling a parsed query with a query execution tree.
  struct PlannedQuery {
    ParsedQuery parsedQuery_;
    QueryExecutionTree queryExecutionTree_;
  };

 private:
  const size_t numThreads_;
  unsigned short port_;
  std::string accessToken_;
  QueryResultCache cache_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  SortPerformanceEstimator sortPerformanceEstimator_;
  Index index_;
  ad_utility::websocket::QueryRegistry queryRegistry_{};

  bool enablePatternTrick_;

  /// Non-owning reference to the `QueryHub` instance living inside
  /// the `WebSocketHandler` created for `HttpServer`.
  std::weak_ptr<ad_utility::websocket::QueryHub> queryHub_;

  net::static_thread_pool queryThreadPool_;
  net::static_thread_pool updateThreadPool_{1};

  /// Executor with a single thread that is used to run timers asynchronously.
  net::static_thread_pool timerExecutor_{1};

  template <typename T>
  using Awaitable = boost::asio::awaitable<T>;

  using TimeLimit = std::chrono::milliseconds;

  using SharedCancellationHandle = ad_utility::SharedCancellationHandle;

  template <ad_utility::isInstantiation<absl::Cleanup> CancelTimeout>
  struct CancellationHandleAndTimeoutTimerCancel {
    SharedCancellationHandle handle_;
    /// Object of type `absl::Cleanup` that when destroyed cancels the timer
    /// that would otherwise invoke the cancellation of the `handle_` via the
    /// time limit.
    CancelTimeout cancelTimeout_;
  };

  // Clang doesn't seem to be able to automatically deduce the type correctly.
  // and GCC 11 thinks deduction guides are not allowed within classes.
#ifdef __clang__
  template <ad_utility::isInstantiation<absl::Cleanup> CancelTimeout>
  CancellationHandleAndTimeoutTimerCancel(SharedCancellationHandle,
                                          CancelTimeout)
      -> CancellationHandleAndTimeoutTimerCancel<CancelTimeout>;
#endif

  /// Parse the path and URL parameters from the given request. Supports both
  /// GET and POST request according to the SPARQL 1.1 standard.
  static ad_utility::url_parser::ParsedRequest parseHttpRequest(
      const ad_utility::httpUtils::HttpRequest auto& request);

  /// Extract the Access token for that request from the `Authorization` header
  /// or the URL query parameters.
  static std::optional<std::string> extractAccessToken(
      const ad_utility::httpUtils::HttpRequest auto& request,
      const ad_utility::url_parser::ParamValueMap& params);

  /// Handle a single HTTP request. Check whether a file request or a query was
  /// sent, and dispatch to functions handling these cases. This function
  /// requires the constraints for the `HttpHandler` in `HttpServer.h`.
  /// \param req The HTTP request.
  /// \param send The action that sends a http:response. (see the
  ///             `HttpServer.h` for documentation).
  Awaitable<void> process(
      const ad_utility::httpUtils::HttpRequest auto& request, auto&& send);

  /// Handle a http request that asks for the processing of an query or update.
  /// This is only a wrapper for `processQuery` and `processUpdate` which
  /// does the error handling.
  /// \param params The key-value-pairs  sent in the HTTP GET request.
  /// \param operation Must be Query or Update.
  /// \param requestTimer Timer that measure the total processing
  ///                     time of this request.
  /// \param request The HTTP request.
  /// \param send The action that sends a http:response (see the
  ///             `HttpServer.h` for documentation).
  /// \param timeLimit Duration in seconds after which the query will be
  ///                  cancelled.
  template <typename Operation>
  Awaitable<void> processQueryOrUpdate(
      const ad_utility::url_parser::ParamValueMap& params,
      const Operation& operation, ad_utility::Timer& requestTimer,
      const ad_utility::httpUtils::HttpRequest auto& request, auto&& send,
      TimeLimit timeLimit);
  // Do the actual execution of a query.
  Awaitable<void> processQuery(
      const ad_utility::url_parser::ParamValueMap& params,
      const ad_utility::url_parser::sparqlOperation::Query& query,
      ad_utility::Timer& requestTimer,
      const ad_utility::httpUtils::HttpRequest auto& request, auto&& send,
      TimeLimit timeLimit);
  // For an executed update create a json with some stats on the update (timing,
  // number of changed triples, etc.).
  static json createResponseMetadataForUpdate(
      const ad_utility::Timer& requestTimer, const Index& index,
      const DeltaTriples& deltaTriples, const PlannedQuery& plannedQuery,
      const QueryExecutionTree& qet, const DeltaTriplesCount& countBefore,
      const UpdateMetadata& updateMetadata,
      const DeltaTriplesCount& countAfter);
  FRIEND_TEST(ServerTest, createResponseMetadata);
  // Do the actual execution of an update.
  Awaitable<void> processUpdate(
      const ad_utility::url_parser::ParamValueMap& params,
      const ad_utility::url_parser::sparqlOperation::Update& update,
      ad_utility::Timer& requestTimer,
      const ad_utility::httpUtils::HttpRequest auto& request, auto&& send,
      TimeLimit timeLimit);

  // Determine the media type to be used for the result. The media type is
  // determined (in this order) by the current action (e.g.,
  // "action=csv_export") and by the "Accept" header of the request.
  static ad_utility::MediaType determineMediaType(
      const ad_utility::url_parser::ParamValueMap& params,
      const ad_utility::httpUtils::HttpRequest auto& request);
  FRIEND_TEST(ServerTest, determineMediaType);
  // Determine whether the subtrees and the result should be pinned.
  static std::pair<bool, bool> determineResultPinning(
      const ad_utility::url_parser::ParamValueMap& params);
  FRIEND_TEST(ServerTest, determineResultPinning);
  // Sets up the PlannedQuery s.t. it is ready to be executed.
  PlannedQuery setupPlannedQuery(
      const std::vector<DatasetClause>& queryDatasets,
      const std::string& operation, QueryExecutionContext& qec,
      SharedCancellationHandle handle, TimeLimit timeLimit,
      const ad_utility::Timer& requestTimer) const;
  // Creates a `MessageSender` for the given operation.
  ad_utility::websocket::MessageSender createMessageSender(
      const std::weak_ptr<ad_utility::websocket::QueryHub>& queryHub,
      const ad_utility::httpUtils::HttpRequest auto& request,
      const string& operation);
  // Execute an update operation. The function must have exclusive access to the
  // DeltaTriples object.
  json processUpdateImpl(
      const ad_utility::url_parser::ParamValueMap& params,
      const ad_utility::url_parser::sparqlOperation::Update& update,
      ad_utility::Timer& requestTimer, TimeLimit timeLimit, auto& messageSender,
      ad_utility::SharedCancellationHandle cancellationHandle,
      DeltaTriples& deltaTriples);

  static json composeErrorResponseJson(
      const string& query, const std::string& errorMsg,
      ad_utility::Timer& requestTimer,
      const std::optional<ExceptionMetadata>& metadata = std::nullopt);

  json composeStatsJson() const;

  json composeCacheStatsJson() const;

  /// Invoke `function` on `threadPool_`, and return an awaitable to wait for
  /// its completion, wrapping the result.
  template <std::invocable Function,
            typename T = std::invoke_result_t<Function>>
  Awaitable<T> computeInNewThread(net::static_thread_pool& threadPool,
                                  Function function,
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
  ad_utility::websocket::OwningQueryId getQueryId(
      const ad_utility::httpUtils::HttpRequest auto& request,
      std::string_view query);

  /// Schedule a task to trigger the timeout after the `timeLimit`.
  /// The returned callback can be used to prevent this task from executing
  /// either because the `cancellationHandle` has been aborted by some other
  /// means or because the task has been completed successfully.
  auto cancelAfterDeadline(
      std::weak_ptr<ad_utility::CancellationHandle<>> cancellationHandle,
      TimeLimit timeLimit)
      -> ad_utility::InvocableWithExactReturnType<void> auto;

  /// Run the SPARQL parser and then the query planner on the `query`. All
  /// computation is performed on the `threadPool_`.
  PlannedQuery parseAndPlan(const std::string& query,
                            const vector<DatasetClause>& queryDatasets,
                            QueryExecutionContext& qec,
                            SharedCancellationHandle handle,
                            TimeLimit timeLimit) const;

  /// Acquire the `CancellationHandle` for the given `QueryId`, start the
  /// watchdog and call `cancelAfterDeadline` to set the timeout after
  /// `timeLimit`. Return an object of type
  /// `CancellationHandleAndTimeoutTimerCancel`, where the `cancelTimeout_`
  /// member can be invoked to cancel the imminent cancellation via timeout.
  auto setupCancellationHandle(const ad_utility::websocket::QueryId& queryId,
                               TimeLimit timeLimit)
      -> ad_utility::isInstantiation<
          CancellationHandleAndTimeoutTimerCancel> auto;

  /// Check if the access token is valid. Return true if the access token
  /// exists and is valid. Return false if there's no access token passed.
  /// Throw an exception if there is a token passed but it doesn't match,
  /// or there is no access token set by the server config. The error message is
  /// formulated towards end users, it can be sent directly as the text of an
  /// HTTP error response.
  bool checkAccessToken(std::optional<std::string_view> accessToken) const;

  /// Check if user-provided timeout is authorized with a valid access-token or
  /// lower than the server default. Return an empty optional and send a 403
  /// Forbidden HTTP response if the change is not allowed. Return the new
  /// timeout otherwise.
  net::awaitable<std::optional<Server::TimeLimit>>
  verifyUserSubmittedQueryTimeout(
      std::optional<std::string_view> userTimeout, bool accessTokenOk,
      const ad_utility::httpUtils::HttpRequest auto& request, auto& send) const;

  /// Send response for the streamable media types (tsv, csv, octet-stream,
  /// turtle, sparqlJson, qleverJson).
  Awaitable<void> sendStreamableResponse(
      const ad_utility::httpUtils::HttpRequest auto& request, auto& send,
      ad_utility::MediaType mediaType, const PlannedQuery& plannedQuery,
      const QueryExecutionTree& qet, ad_utility::Timer& requestTimer,
      SharedCancellationHandle cancellationHandle) const;
};
