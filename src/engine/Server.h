// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>
// Author: Johannes Kalmbach<kalmbach@cs.uni-freiburg.de>

#pragma once

#include <string>
#include <vector>

#include "../engine/Engine.h"
#include "../index/Index.h"
#include "../parser/ParseException.h"
#include "../parser/SparqlParser.h"
#include "../util/AllocatorWithLimit.h"
#include "../util/HttpServer/HttpServer.h"
#include "../util/Socket.h"
#include "../util/Timer.h"
#include "./QueryExecutionContext.h"
#include "./QueryExecutionTree.h"
#include "./SortPerformanceEstimator.h"

using std::string;
using std::vector;

using ad_utility::Socket;

//! The HTTP Sever used.
class Server {
 public:
  explicit Server(const int port, const int numThreads, size_t maxMemGB,
                  size_t cacheMaxSizeGB, size_t cacheMaxSizeGBSingleEntry,
                  size_t cacheMaxNumEntries)
      : _numThreads(numThreads),
        _port(port),
        _cache(cacheMaxNumEntries, cacheMaxSizeGB * (1ull << 30u) / sizeof(Id),
               cacheMaxSizeGBSingleEntry * (1ull << 30u) / sizeof(Id)),
        _allocator{ad_utility::makeAllocationMemoryLeftThreadsafeObject(
                       maxMemGB * (1ull << 30u)),
                   [this](size_t numBytesToAllocate) {
                     _cache.makeRoomAsMuchAsPossible(
                         static_cast<double>(numBytesToAllocate) / sizeof(Id) *
                         MAKE_ROOM_SLACK_FACTOR);
                   }},
        _sortPerformanceEstimator(),
        _index(),
        _engine(),
        _initialized(false) {}

  virtual ~Server() = default;

  using ParamValueMap = ad_utility::HashMap<string, string>;

  // Initialize the server.
  void initialize(const string& ontologyBaseName, bool useText,
                  bool usePatterns = true, bool usePatternTrick = true);

  //! Loop, wait for requests and trigger processing. This method never returns
  //! except when throwing an exceptiob
  void run();

 private:
  const int _numThreads;
  int _port;
  QueryResultCache _cache;
  ad_utility::AllocatorWithLimit<Id> _allocator;
  SortPerformanceEstimator _sortPerformanceEstimator;
  Index _index;
  Engine _engine;

  bool _initialized;
  bool _enablePatternTrick;

  /// Handle a single HTTP request. Check whether a file request or a query was
  /// sent, and dispatch to functions handling these cases. This function
  /// requires the constraints for the `HttpHandler` in `HttpServer.h`.
  /// \param req The HTTP request.
  /// \param send The action that sends a http:response. (see the
  ///             `HttpServer.h` for documentation).
  boost::asio::awaitable<void> process(
      const ad_utility::httpUtils::HttpRequest auto& req, auto&& send);

  /// Handle a http request that asks for the processing of a query.
  /// \param params The key-value-pairs  sent in the HTTP GET request. When this
  /// function is called, we already know that a parameter "query" is contained
  /// in `params`.
  /// \param requestTimer Timer that measure the total processing
  ///                     time of this request.
  /// \param request The HTTP request.
  /// \param send The action that sends a http:response (see the
  ///             `HttpServer.h` for documentation).
  boost::asio::awaitable<void> processQuery(
      const ParamValueMap& params, ad_utility::Timer& requestTimer,
      const ad_utility::httpUtils::HttpRequest auto& request, auto&& send);

  static json composeResponseJson(const ParsedQuery& query,
                                  const QueryExecutionTree& qet,
                                  ad_utility::Timer& requestTimer,
                                  size_t sendMax = MAX_NOF_ROWS_IN_RESULT);

  static string composeResponseSepValues(const ParsedQuery& query,
                                         const QueryExecutionTree& qet,
                                         char sep);

  static json composeResponseJson(const string& query, const std::exception& e,
                                  ad_utility::Timer& requestTimer);

  json composeStatsJson() const;

  json composeCacheStatsJson() const;
};
