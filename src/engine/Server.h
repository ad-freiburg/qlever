// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <string>
#include <vector>

#include "../engine/Engine.h"
#include "../index/Index.h"
#include "../parser/ParseException.h"
#include "../parser/SparqlParser.h"
#include "../util/AllocatorWithLimit.h"
#include "../util/HttpServer/WebServer.h"
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

  typedef ad_utility::HashMap<string, string> ParamValueMap;

  struct FilenameAndParamValueMap {
    std::string _filename;
    ParamValueMap _paramValueMap;
  };

  // Initialize the server.
  void initialize(const string& ontologyBaseName, bool useText,
                  bool usePatterns = true, bool usePatternTrick = true);

  //! Loop, wait for requests and trigger processing. This method never returns
  //! except when throwing an exceptiob
  void run();

 private:
  const int _numThreads;
  int _port;
  ConcurrentLruCache _cache;
  PinnedSizes _pinnedSizes;
  ad_utility::AllocatorWithLimit<Id> _allocator;
  SortPerformanceEstimator _sortPerformanceEstimator;
  Index _index;
  Engine _engine;

  bool _initialized;
  bool _enablePatternTrick;

  void runAcceptLoop();

  template <typename Body, typename Allocator, typename Send>
  boost::asio::awaitable<void> process(
      http::request<Body, http::basic_fields<Allocator>>&& req, Send&& send);

  template <typename Body, typename Allocator, typename Send>
  boost::asio::awaitable<void> processQuery(
      const ParamValueMap& params, ad_utility::Timer& requestTimer,
      http::request<Body, http::basic_fields<Allocator>>&& request,
      Send&& send);

  void serveFile(Socket* client, const string& requestedFile) const;

  FilenameAndParamValueMap parseHttpRequest(std::string_view request) const;

  string createQueryFromHttpParams(const ParamValueMap& params) const;

  string createHttpResponse(const string& content,
                            const string& contentType) const;

  string create404HttpResponse() const;
  string create400HttpResponse() const;

  string composeResponseJson(const ParsedQuery& query,
                             const QueryExecutionTree& qet,
                             ad_utility::Timer& requestTimer,
                             size_t sendMax = MAX_NOF_ROWS_IN_RESULT) const;

  string composeResponseSepValues(const ParsedQuery& query,
                                  const QueryExecutionTree& qet,
                                  char sep) const;

  string composeResponseJson(const string& query, const std::exception& e,
                             ad_utility::Timer& requestTimer) const;

  string composeStatsJson() const;

  json composeCacheStatsJson() const;
};
