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
#include "../util/Socket.h"
#include "../util/Timer.h"
#include "./QueryExecutionContext.h"
#include "./QueryExecutionTree.h"

using std::string;
using std::vector;

using ad_utility::Socket;

//! The HTTP Sever used.
class Server {
 public:
  explicit Server(const int port, const int numThreads)
      : _numThreads(numThreads),
        _serverSocket(),
        _port(port),
        _index(),
        _engine(),
        _initialized(false) {}

  typedef ad_utility::HashMap<string, string> ParamValueMap;

  // Initialize the server.
  void initialize(const string& ontologyBaseName, bool useText,
                  bool allPermutations = false, bool usePatterns = false);

  //! Loop, wait for requests and trigger processing. This method never returns
  //! except when throwing an exceptiob
  void run();

 private:
  const int _numThreads;
  Socket _serverSocket;
  int _port;
  Index _index;
  Engine _engine;

  bool _initialized;

  void runAcceptLoop(QueryExecutionContext* qec);

  void process(Socket* client, QueryExecutionContext* qec) const;

  void serveFile(Socket* client, const string& requestedFile) const;

  ParamValueMap parseHttpRequest(const string& request) const;

  string createQueryFromHttpParams(const ParamValueMap& params) const;

  string createHttpResponse(const string& content,
                            const string& contentType) const;

  string create404HttpResponse() const;
  string create400HttpResponse() const;

  string composeResponseJson(const ParsedQuery& query,
                             const QueryExecutionTree& qet,
                             size_t sendMax = MAX_NOF_ROWS_IN_RESULT) const;

  string composeResponseSepValues(const ParsedQuery& query,
                                  const QueryExecutionTree& qet,
                                  char sep) const;

  string composeResponseJson(const string& query,
                             const ad_semsearch::Exception& e) const;

  string composeResponseJson(const string& query,
                             const std::exception* e) const;

  string composeStatsJson() const;

  mutable ad_utility::Timer _requestProcessingTimer;
};
