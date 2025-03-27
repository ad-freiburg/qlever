// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach<kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SERVER_H
#define QLEVER_SRC_ENGINE_SERVER_H

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
#include "util/http/HttpUtils.h"
#include "util/http/streamable_body.h"
#include "util/http/websocket/QueryHub.h"
#include "util/json.h"

using nlohmann::json;
using std::string;
using std::vector;

template <typename Operation>
CPP_concept QueryOrUpdate =
    ad_utility::SameAsAny<Operation,
                          ad_utility::url_parser::sparqlOperation::Query,
                          ad_utility::url_parser::sparqlOperation::Update>;

//! The HTTP Server used.
class Server {
  FRIEND_TEST(ServerTest, getQueryId);
  FRIEND_TEST(ServerTest, createMessageSender);

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

  boost::asio::static_thread_pool queryThreadPool_;
  boost::asio::static_thread_pool updateThreadPool_{1};

  /// Executor with a single thread that is used to run timers asynchronously.
  boost::asio::static_thread_pool timerExecutor_{1};

  template <typename T>
  using Awaitable = boost::asio::awaitable<T>;

  using TimeLimit = std::chrono::milliseconds;

  using SharedCancellationHandle = ad_utility::SharedCancellationHandle;

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
  CPP_template_2(typename CancelTimeout)(
      requires ad_utility::isInstantiation<CancelTimeout, absl::Cleanup>)
      CancellationHandleAndTimeoutTimerCancel(SharedCancellationHandle,
                                              CancelTimeout)
          -> CancellationHandleAndTimeoutTimerCancel<CancelTimeout>;
#endif

  /// Handle a single HTTP request. Check whether a file request or a query was
  /// sent, and dispatch to functions handling these cases. This function
  /// requires the constraints for the `HttpHandler` in `HttpServer.h`.
  /// \param req The HTTP request.
  /// \param send The action that sends a http:response. (see the
  ///             `HttpServer.h` for documentation).
  CPP_template_2(typename RequestT, typename ResponseT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> process(const RequestT& request, ResponseT&& send);

  // Wraps the error handling around the processing of operations. Calls the
  // visitor on the given operation.
  CPP_template_2(typename VisitorT, typename RequestT, typename ResponseT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> processOperation(
          ad_utility::url_parser::sparqlOperation::Operation operation,
          VisitorT visitor, const ad_utility::Timer& requestTimer,
          const RequestT& request, ResponseT& send);
  // Do the actual execution of a query.
  CPP_template_2(typename RequestT, typename ResponseT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> processQuery(
          const ad_utility::url_parser::ParamValueMap& params,
          ParsedQuery&& query, const ad_utility::Timer& requestTimer,
          ad_utility::SharedCancellationHandle cancellationHandle,
          QueryExecutionContext& qec, const RequestT& request, ResponseT&& send,
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
  CPP_template_2(typename RequestT, typename ResponseT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> processUpdate(
          ParsedQuery&& update, const ad_utility::Timer& requestTimer,
          ad_utility::SharedCancellationHandle cancellationHandle,
          QueryExecutionContext& qec, const RequestT& request, ResponseT&& send,
          TimeLimit timeLimit);

  // Determine the media type to be used for the result. The media type is
  // determined (in this order) by the current action (e.g.,
  // "action=csv_export") and by the "Accept" header of the request.
  CPP_template_2(typename RequestT)(requires ad_utility::httpUtils::HttpRequest<
                                    RequestT>) static ad_utility::MediaType
      determineMediaType(const ad_utility::url_parser::ParamValueMap& params,
                         const RequestT& request);
  FRIEND_TEST(ServerTest, determineMediaType);
  // Determine whether the subtrees and the result should be pinned.
  static std::pair<bool, bool> determineResultPinning(
      const ad_utility::url_parser::ParamValueMap& params);
  FRIEND_TEST(ServerTest, determineResultPinning);
  //  Prepare the execution of an operation
  auto prepareOperation(std::string_view operationName,
                        std::string_view operationSPARQL,
                        ad_utility::websocket::MessageSender& messageSender,
                        const ad_utility::url_parser::ParamValueMap& params,
                        TimeLimit timeLimit);

  // Plan a parsed query.
  Awaitable<PlannedQuery> planQuery(
      boost::asio::static_thread_pool& thread_pool, ParsedQuery&& operation,
      const ad_utility::Timer& requestTimer, TimeLimit timeLimit,
      QueryExecutionContext& qec, SharedCancellationHandle handle);
  // Creates a `MessageSender` for the given operation.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      ad_utility::websocket::MessageSender createMessageSender(
          const std::weak_ptr<ad_utility::websocket::QueryHub>& queryHub,
          const RequestT& request, const string& operation);
  // Execute an update operation. The function must have exclusive access to the
  // DeltaTriples object.
  json processUpdateImpl(
      const PlannedQuery& plannedUpdate, const ad_utility::Timer& requestTimer,
      ad_utility::SharedCancellationHandle cancellationHandle,
      DeltaTriples& deltaTriples);

  static json composeErrorResponseJson(
      const string& query, const std::string& errorMsg,
      const ad_utility::Timer& requestTimer,
      const std::optional<ExceptionMetadata>& metadata = std::nullopt);

  json composeStatsJson() const;

  json composeCacheStatsJson() const;

  /// Invoke `function` on `threadPool_`, and return an awaitable to wait for
  /// its completion, wrapping the result.
  template <std::invocable Function,
            typename T = std::invoke_result_t<Function>>
  Awaitable<T> computeInNewThread(boost::asio::static_thread_pool& threadPool,
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
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      ad_utility::websocket::OwningQueryId
      getQueryId(const RequestT& request, std::string_view query);

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

  /// Check if user-provided timeout is authorized with a valid access-token or
  /// lower than the server default. Return an empty optional and send a 403
  /// Forbidden HTTP response if the change is not allowed. Return the new
  /// timeout otherwise.
  CPP_template_2(typename RequestT, typename ResponseT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) boost::asio::
      awaitable<std::optional<Server::TimeLimit>> verifyUserSubmittedQueryTimeout(
          std::optional<std::string_view> userTimeout, bool accessTokenOk,
          const RequestT& request, ResponseT& send) const;

  /// Send response for the streamable media types (tsv, csv, octet-stream,
  /// turtle, sparqlJson, qleverJson).
  CPP_template_2(typename RequestT, typename ResponseT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>)
      Awaitable<void> sendStreamableResponse(
          const RequestT& request, ResponseT& send,
          ad_utility::MediaType mediaType, const PlannedQuery& plannedQuery,
          const QueryExecutionTree& qet, const ad_utility::Timer& requestTimer,
          SharedCancellationHandle cancellationHandle) const;
};

#endif  // QLEVER_SRC_ENGINE_SERVER_H
