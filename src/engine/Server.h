// Copyright 2021 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach<kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <string>
#include <vector>

#include "engine/Engine.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SortPerformanceEstimator.h"
#include "index/Index.h"
#include "nlohmann/json.hpp"
#include "parser/ParseException.h"
#include "parser/SparqlParser.h"
#include "util/AllocatorWithLimit.h"
#include "util/Timer.h"
#include "util/http/HttpServer.h"
#include "util/http/streamable_body.h"

using nlohmann::json;
using std::string;
using std::vector;

//! The HTTP Server used.
class Server {
 public:
  explicit Server(const int port, const int numThreads, size_t maxMemGB,
                  std::string accessToken);

  virtual ~Server() = default;

  using ParamValueMap = ad_utility::HashMap<string, string>;

 private:
  //! Initialize the server.
  void initialize(const string& indexBaseName, bool useText,
                  bool usePatterns = true, bool usePatternTrick = true,
                  bool loadAllPermutations = true);

 public:
  //! First initialize the server. Then loop, wait for requests and trigger
  //! processing. This method never returns except when throwing an exception.
  void run(const string& indexBaseName, bool useText, bool usePatterns = true,
           bool usePatternTrick = true, bool loadAllPermutations = true);

  Index& index() { return _index; }
  const Index& index() const { return _index; }

 private:
  const int _numThreads;
  int _port;
  std::string accessToken_;
  QueryResultCache _cache;
  ad_utility::AllocatorWithLimit<Id> _allocator;
  SortPerformanceEstimator _sortPerformanceEstimator;
  Index _index;
  Engine _engine;

  bool _initialized;
  bool _enablePatternTrick;

  // Semaphore for the number of queries that can be processed at once.
  mutable std::counting_semaphore<std::numeric_limits<int>::max()>
      _queryProcessingSemaphore;

  template <typename T>
  using Awaitable = boost::asio::awaitable<T>;

  /// Parse the path and URL parameters from the given request. Supports both
  /// GET and POST request according to the SPARQL 1.1 standard.
  ad_utility::UrlParser::UrlPathAndParameters getUrlPathAndParameters(
      const ad_utility::httpUtils::HttpRequest auto& request);

  /// Handle a single HTTP request. Check whether a file request or a query was
  /// sent, and dispatch to functions handling these cases. This function
  /// requires the constraints for the `HttpHandler` in `HttpServer.h`.
  /// \param req The HTTP request.
  /// \param send The action that sends a http:response. (see the
  ///             `HttpServer.h` for documentation).
  Awaitable<void> process(
      const ad_utility::httpUtils::HttpRequest auto& request, auto&& send);

  /// Handle a http request that asks for the processing of a query.
  /// \param params The key-value-pairs  sent in the HTTP GET request. When this
  /// function is called, we already know that a parameter "query" is contained
  /// in `params`.
  /// \param requestTimer Timer that measure the total processing
  ///                     time of this request.
  /// \param request The HTTP request.
  /// \param send The action that sends a http:response (see the
  ///             `HttpServer.h` for documentation).
  Awaitable<void> processQuery(
      const ParamValueMap& params, ad_utility::Timer& requestTimer,
      const ad_utility::httpUtils::HttpRequest auto& request, auto&& send);

  static json composeErrorResponseJson(
      const string& query, const std::string& errorMsg,
      ad_utility::Timer& requestTimer,
      const std::optional<ExceptionMetadata>& metadata = std::nullopt);

  static ad_utility::streams::stream_generator composeTurtleResponse(
      const ParsedQuery& query, const QueryExecutionTree& qet);

  json composeStatsJson() const;

  json composeCacheStatsJson() const;

  // Perform the following steps: Acquire a token from the
  // _queryProcessingSemaphore, run `function`, and release the token. These
  // steps are performed on a new thread (not one of the server threads).
  // Returns an awaitable of the return value of `function`
  template <typename Function, typename T = std::invoke_result_t<Function>>
  Awaitable<T> computeInNewThread(Function function) const;
};
