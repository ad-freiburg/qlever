// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include "./Server.h"

#include <algorithm>
#include <cstring>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../parser/ParseException.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"
#include "./Server.h"
#include "QueryPlanner.h"

// _____________________________________________________________________________
Server::~Server() {
  if (_initialized) {
    _serverSocket.close();
  }
}

// _____________________________________________________________________________
void Server::initialize(const string& ontologyBaseName, bool useText,
                        bool usePatterns, bool usePatternTrick) {
  LOG(INFO) << "Initializing server..." << std::endl;

  _enablePatternTrick = usePatternTrick;
  _index.setUsePatterns(usePatterns);

  // Init the index.
  _index.createFromOnDiskIndex(ontologyBaseName);
  if (useText) {
    _index.addTextFromOnDiskIndex();
  }
  _sortPerformanceEstimator.computeEstimatesExpensively(
      _allocator,
      _index.getNofTriples() * PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);
  _initialized = true;
  LOG(INFO) << "Done initializing server." << std::endl;
}

// _____________________________________________________________________________
void Server::run() {
  if (!_initialized) {
    LOG(ERROR) << "Cannot start an uninitialized server!" << std::endl;
    exit(1);
  }

  for (int i = 0; i < _numThreads; ++i) {
    uWS::App()
        .get("/*", [this](uWS::HttpResponse<false>* resp,
                          uWS::HttpRequest* req) { process(resp, req); })
        .listen(_port,
                [this](auto* token) {
                  if (!token) {
                    LOG(ERROR) << "Failed to create socket on port " << _port
                               << "." << std::endl;
                    exit(1);
                  }
                })
        .run();
  }
}

// _____________________________________________________________________________
void Server::process(uWS::HttpResponse<false>* resp, uWS::HttpRequest* req) {
  ad_utility::Timer requestTimer;
  requestTimer.start();
  string file = std::string(req->getUrl());
  if (file == "/") {
    file = "/index.html";
  }
  LOG(INFO) << "GET request for " << file << std::endl;
  // Use hardcoded white-listing for index.html and style.css
  // can be changed if more should ever be needed, for now keep it simple.
  if (req->getQuery().empty()) {
    LOG(DEBUG) << "file: " << file << '\n';
    if (file == "/index.html" || file == "/style.css" || file == "/script.js") {
      serveFile(resp, "." + file);
      return;
    } else {
      LOG(INFO) << "Responding with 404 for file " << file << '\n';
      create404HttpResponse(resp);
      return;
    }
  }

  std::string query;

  try {
    resp->writeHeader("Access-Control-Allow-Origin", "*");
    if (ad_utility::getLowercase(req->getQuery("cmd")) == "stats") {
      LOG(INFO) << "Supplying index stats..." << std::endl;
      std::string statsJson = composeStatsJson();
      resp->writeStatus("200 Ok");
      resp->writeHeader("Content-Type", "application/json");
      resp->end(statsJson);
      LOG(INFO) << "Sent stats to client." << std::endl;
      return;
    }

    if (ad_utility::getLowercase(req->getQuery("cmd")) == "cachestats") {
      LOG(INFO) << "Supplying cache stats..." << std::endl;
      json statsJson = composeCacheStatsJson();
      resp->writeStatus("200 Ok");
      resp->writeHeader("Content-Type", "application/json");
      resp->end(statsJson.dump());
      LOG(INFO) << "Sent cache stats to client." << std::endl;
      return;
    }

    if (ad_utility::getLowercase(req->getQuery("cmd")) == "clearcache") {
      _cache.clearUnpinnedOnly();
      resp->writeStatus("200 Ok");
      resp->end();
    }

    if (ad_utility::getLowercase(req->getQuery("cmd")) ==
        "clearcachecomplete") {
      auto lock = _pinnedSizes.wlock();
      _cache.clearAll();
      lock->clear();
    }

    ad_utility::SharedConcurrentTimeoutTimer timeoutTimer =
        std::make_shared<ad_utility::ConcurrentTimeoutTimer>(
            ad_utility::TimeoutTimer::unlimited());
    if (std::string_view t = req->getQuery("timeout"); !t.empty()) {
      timeoutTimer = std::make_shared<ad_utility::ConcurrentTimeoutTimer>(
          ad_utility::TimeoutTimer::fromSeconds(stof(std::string(t))));
    }

    std::string_view send = req->getQuery("send");
    size_t maxSend = MAX_NOF_ROWS_IN_RESULT;
    if (!send.empty()) {
      maxSend = static_cast<size_t>(stoul(std::string(send)));
    }

    const bool pinSubtrees =
        ad_utility::getLowercase(req->getQuery("pinsubtrees")) == "true";
    const bool pinResult =
        ad_utility::getLowercase(req->getQuery("pinresult")) == "true";
    query = createQueryFromHttpParams(req);
    LOG(INFO) << "Query" << ((pinSubtrees) ? " (Cache pinned)" : "")
              << ((pinResult) ? " (Result pinned)" : "") << ": " << query
              << '\n';
    ParsedQuery pq = SparqlParser(query).parse();
    pq.expandPrefixes();

    QueryExecutionContext qec(_index, _engine, &_cache, &_pinnedSizes,
                              _allocator, _sortPerformanceEstimator,
                              pinSubtrees, pinResult);
    // start the shared timeout timer here to also include
    // the query planning
    timeoutTimer->wlock()->start();

    QueryPlanner qp(&qec);
    qp.setEnablePatternTrick(_enablePatternTrick);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    qet.isRoot() = true;  // allow pinning of the final result
    qet.recursivelySetTimeoutTimer(timeoutTimer);
    LOG(TRACE) << qet.asString() << std::endl;

    std::string response;
    std::string contentType;

    if (ad_utility::getLowercase(req->getQuery("action")) == "csv_export") {
      // CSV export
      response = composeResponseSepValues(pq, qet, ',');
      contentType =
          "text/csv\r\n"
          "Content-Disposition: attachment;filename=export.csv";
    } else if (ad_utility::getLowercase(req->getQuery("action")) ==
               "tsv_export") {
      // TSV export
      response = composeResponseSepValues(pq, qet, '\t');
      contentType =
          "text/tab-separated-values\r\n"
          "Content-Disposition: attachment;filename=export.tsv";
    } else if (ad_utility::getLowercase(req->getQuery("action")) ==
               "binary_export") {
      // binary export
      response = composeResponseSepValues(pq, qet, 'b');
      contentType =
          "application/octet-stream\r\n"
          "Content-Disposition: attachment;filename=export.dat";
    } else {
      // Normal case: JSON response
      response = composeResponseJson(pq, qet, requestTimer, maxSend);
      contentType = "application/json";
    }
    resp->writeStatus("200 Ok");
    resp->writeHeader("Content-Type", contentType);
    resp->end(response);
    // Print the runtime info. This needs to be done after the query
    // was computed.
    LOG(INFO) << '\n' << qet.getRootOperation()->getRuntimeInfo().toString();
  } catch (const std::exception& e) {
    resp->writeStatus("500 Internal Server Error");
    resp->end(composeResponseJson(query, e, requestTimer));
  }
}

// _____________________________________________________________________________
string Server::createQueryFromHttpParams(uWS::HttpRequest* req) {
  // Construct a Query object from the parsed request.
  std::string_view query = req->getQuery("query");
  if (query.empty()) {
    AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
             "Expected at least one non-empty attribute \"query\".");
  }
  return std::string(query);
}

// _____________________________________________________________________________
string Server::createHttpResponse(const string& content,
                                  const string& contentType) {
  std::ostringstream os;
  os << "HTTP/1.1 200 OK\r\n"
     << "Content-Length: " << content.size() << "\r\n"
     << "Connection: close\r\n"
     << "Content-Type: " << contentType << "; charset="
     << "utf-8"
     << "\r\n"
     << "Access-Control-Allow-Origin: *"
     << "\r\n"
     << "\r\n"
     << content;
  return os.str();
}

// _____________________________________________________________________________
void Server::create404HttpResponse(uWS::HttpResponse<false>* resp) {
  resp->writeStatus("404 Not Found");
  resp->end("404 Not Found");
}

// _____________________________________________________________________________
void Server::create400HttpResponse(uWS::HttpResponse<false>* resp) {
  resp->writeStatus("400 Bad Request");
  resp->end("400 Bad Request");
}

// _____________________________________________________________________________
string Server::composeResponseJson(const ParsedQuery& query,
                                   const QueryExecutionTree& qet,
                                   ad_utility::Timer& requestTimer,
                                   size_t maxSend) {
  shared_ptr<const ResultTable> rt = qet.getResult();
  requestTimer.stop();
  off_t compResultUsecs = requestTimer.usecs();
  size_t resultSize = rt->size();

  nlohmann::json j;

  j["query"] = query._originalString;
  j["status"] = "OK";
  j["resultsize"] = resultSize;
  j["warnings"] = qet.collectWarnings();
  j["selected"] = query._selectClause._selectedVariables;

  j["runtimeInformation"] = RuntimeInformation::ordered_json(
      qet.getRootOperation()->getRuntimeInfo());

  {
    auto [limit, offset] = parseLimitAndOffset(query, MAX_NOF_ROWS_IN_RESULT);
    requestTimer.cont();
    j["res"] = qet.writeResultAsJson(query._selectClause._selectedVariables,
                                     std::min(limit, maxSend), offset);
    requestTimer.stop();
  }

  requestTimer.stop();
  j["time"]["total"] =
      std::to_string(static_cast<double>(requestTimer.usecs()) / 1000.0) + "ms";
  j["time"]["computeResult"] =
      std::to_string(static_cast<double>(compResultUsecs) / 1000.0) + "ms";

  return j.dump(4);
}

// _________________________________________________________________________________
std::pair<size_t, size_t> Server::parseLimitAndOffset(const ParsedQuery& query,
                                                      size_t defaultLimit) {
  size_t limit = defaultLimit;
  if (!query._limit.empty() > 0) {
    limit = stoul(query._limit);
  }

  size_t offset = 0;
  if (!query._offset.empty()) {
    offset = stoul(query._offset);
  }
  return std::pair{limit, offset};
}

// _____________________________________________________________________________
string Server::composeResponseSepValues(const ParsedQuery& query,
                                        const QueryExecutionTree& qet,
                                        char sep) {
  std::ostringstream os;
  auto [limit, offset] = parseLimitAndOffset(query);
  qet.writeResultToStream(os, query._selectClause._selectedVariables, limit,
                          offset, sep);

  return os.str();
}

// _____________________________________________________________________________
string Server::composeResponseJson(const string& query,
                                   const std::exception& exception,
                                   ad_utility::Timer& requestTimer) {
  std::ostringstream os;
  requestTimer.stop();

  json j;
  j["query"] = query;
  j["status"] = "ERROR";
  j["resultsize"] = 0;
  j["time"]["total"] = requestTimer.msecs();
  j["time"]["computeResult"] = requestTimer.msecs();
  j["exception"] = exception.what();
  return j.dump(4);
}

// _____________________________________________________________________________
void Server::serveFile(uWS::HttpResponse<false>* resp,
                       const string& requestedFile) {
  string contentString;
  string contentType = "text/plain";
  string statusString = "200 OK";

  // CASE: file.
  LOG(DEBUG) << "Looking for file: \"" << requestedFile << "\" ... \n";
  std::ifstream in(requestedFile.c_str());
  if (!in) {
    statusString = "404 NOT FOUND";
    contentString = "404 NOT FOUND";
  } else {
    // File into string
    contentString = string((std::istreambuf_iterator<char>(in)),
                           std::istreambuf_iterator<char>());
    // Set content type
    if (ad_utility::endsWith(requestedFile, ".html")) {
      contentType = "text/html";
    } else if (ad_utility::endsWith(requestedFile, ".css")) {
      contentType = "text/css";
    } else if (ad_utility::endsWith(requestedFile, ".js")) {
      contentType = "application/javascript";
    }
    in.close();
  }

  resp->writeStatus(statusString);
  resp->writeHeader("Access-Control-Allow-Origin", "*");
  resp->writeHeader("Content-Type", contentType);
  resp->end(contentString);
}

// _____________________________________________________________________________
string Server::composeStatsJson() const {
  nlohmann::json result;
  result["kbindex"] = _index.getKbName();
  result["permutations"] = (_index.hasAllPermutations() ? "6" : "2");
  if (_index.hasAllPermutations()) {
    result["nofsubjects"] = _index.getNofSubjects();
    result["nofpredicates"] = _index.getNofPredicates();
    result["nofobjects"] = _index.getNofObjects();
  }
  auto [actualTriples, addedTriples] = _index.getNumTriplesActuallyAndAdded();
  result["noftriples"] = _index.getNofTriples();
  result["nofActualTriples"] = actualTriples;
  result["nofAddedTriples"] = addedTriples;
  result["textindex"] = _index.getTextName();
  result["nofrecords"] = _index.getNofTextRecords();
  result["nofwordpostings"] = _index.getNofWordPostings();
  result["nofentitypostings"] = _index.getNofEntityPostings();
  return result.dump(4);
}

// _______________________________________
nlohmann::json Server::composeCacheStatsJson() const {
  nlohmann::json result;
  result["num-non-pinned-entries"] = _cache.numNonPinnedEntries();
  result["num-pinned-entries"] = _cache.numPinnedEntries();
  result["non-pinned-size"] = _cache.nonPinnedSize();
  result["pinned-size"] = _cache.pinnedSize();
  result["num-pinned-index-scan-sizes"] = _pinnedSizes.rlock()->size();
  return result;
}
