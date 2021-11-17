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
#include "App.h"
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
  if (req->getQuery().size() == 0) {
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
#ifdef ALLOW_SHUTDOWN
    if (ad_utility::getLowercase(req->getQuery("cmd")) == "shutdown") {
      LOG(INFO) << "Shutdown triggered by HTTP request "
                << "(deactivate by compiling without -DALLOW_SHUTDOWN)"
                << std::endl;
      exit(0);
    }
#endif
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
  } catch (const ad_semsearch::Exception& e) {
    resp->writeStatus("500 Internal Server Error");
    resp->end(composeResponseJson(query, e, requestTimer));
  } catch (const std::exception& e) {
    resp->writeStatus("500 Internal Server Error");
    resp->end(composeResponseJson(query, e, requestTimer));
  }
}

// _____________________________________________________________________________
Server::ParamValueMap Server::parseHttpRequest(
    const string& httpRequest) const {
  LOG(DEBUG) << "Parsing HTTP Request." << endl;
  ParamValueMap params;
  // Parse the HTTP Request.

  size_t indexOfGET = httpRequest.find("GET");
  size_t indexOfHTTP = httpRequest.find("HTTP");

  if (indexOfGET == httpRequest.npos || indexOfHTTP == httpRequest.npos) {
    AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
             "Invalid request. Only supporting proper HTTP GET requests!\n" +
                 httpRequest);
  }

  string request =
      httpRequest.substr(indexOfGET + 3, indexOfHTTP - (indexOfGET + 3));

  size_t index = request.find("?");
  if (index == request.npos) {
    AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
             "Invalid request. At least one parameters is "
             "required for meaningful queries!\n" +
                 httpRequest);
  }
  size_t next = request.find('&', index + 1);
  while (next != request.npos) {
    size_t posOfEq = request.find('=', index + 1);
    if (posOfEq == request.npos) {
      AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
               "Parameter without \"=\" in HTTP Request.\n" + httpRequest);
    }
    string param = ad_utility::getLowercaseUtf8(
        request.substr(index + 1, posOfEq - (index + 1)));
    string value = ad_utility::decodeUrl(
        request.substr(posOfEq + 1, next - (posOfEq + 1)));
    if (params.count(param) > 0) {
      AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
               "Duplicate HTTP parameter: " + param);
    }
    params[param] = value;
    index = next;
    next = request.find('&', index + 1);
  }
  size_t posOfEq = request.find('=', index + 1);
  if (posOfEq == request.npos) {
    AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
             "Parameter without \"=\" in HTTP Request." + httpRequest);
  }
  string param = ad_utility::getLowercaseUtf8(
      request.substr(index + 1, posOfEq - (index + 1)));
  string value = ad_utility::decodeUrl(
      request.substr(posOfEq + 1, request.size() - 1 - (posOfEq + 1)));
  if (params.count(param) > 0) {
    AD_THROW(ad_semsearch::Exception::BAD_REQUEST, "Duplicate HTTP parameter.");
  }
  params[param] = value;

  LOG(DEBUG) << "Done parsing HTTP Request." << endl;
  return params;
}

// _____________________________________________________________________________
string Server::createQueryFromHttpParams(uWS::HttpRequest* req) const {
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
                                  const string& contentType) const {
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
void Server::create404HttpResponse(uWS::HttpResponse<false>* resp) const {
  resp->writeStatus("404 Not Found");
  resp->end("404 Not Found");
}

// _____________________________________________________________________________
void Server::create400HttpResponse(uWS::HttpResponse<false>* resp) const {
  resp->writeStatus("400 Bad Request");
  resp->end("400 Bad Request");
}

// _____________________________________________________________________________
string Server::composeResponseJson(const ParsedQuery& query,
                                   const QueryExecutionTree& qet,
                                   ad_utility::Timer& requestTimer,
                                   size_t maxSend) const {
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
    size_t limit = MAX_NOF_ROWS_IN_RESULT;
    size_t offset = 0;
    if (query._limit.size() > 0) {
      limit = static_cast<size_t>(atol(query._limit.c_str()));
    }
    if (query._offset.size() > 0) {
      offset = static_cast<size_t>(atol(query._offset.c_str()));
    }
    requestTimer.cont();
    j["res"] = qet.writeResultAsJson(query._selectClause._selectedVariables,
                                     std::min(limit, maxSend), offset);
    requestTimer.stop();
  }

  requestTimer.stop();
  j["time"]["total"] = std::to_string(requestTimer.usecs() / 1000.0) + "ms";
  j["time"]["computeResult"] = std::to_string(compResultUsecs / 1000.0) + "ms";

  return j.dump(4);
}

// _____________________________________________________________________________
string Server::composeResponseSepValues(const ParsedQuery& query,
                                        const QueryExecutionTree& qet,
                                        char sep) const {
  std::ostringstream os;
  size_t limit = std::numeric_limits<size_t>::max();
  size_t offset = 0;
  if (query._limit.size() > 0) {
    limit = static_cast<size_t>(atol(query._limit.c_str()));
  }
  if (query._offset.size() > 0) {
    offset = static_cast<size_t>(atol(query._offset.c_str()));
  }
  qet.writeResultToStream(os, query._selectClause._selectedVariables, limit,
                          offset, sep);

  return os.str();
}

// _____________________________________________________________________________
string Server::composeResponseJson(const string& query,
                                   const std::exception& exception,
                                   ad_utility::Timer& requestTimer) const {
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
                       const string& requestedFile) const {
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
  std::ostringstream os;
  os << "{\n"
     << "\"kbindex\": \"" << _index.getKbName() << "\",\n"
     << "\"permutations\": \"" << (_index.hasAllPermutations() ? "6" : "2")
     << "\",\n";
  if (_index.hasAllPermutations()) {
    os << "\"nofsubjects\": \"" << _index.getNofSubjects() << "\",\n";
    os << "\"nofpredicates\": \"" << _index.getNofPredicates() << "\",\n";
    os << "\"nofobjects\": \"" << _index.getNofObjects() << "\",\n";
  }

  auto [actualTriples, addedTriples] = _index.getNumTriplesActuallyAndAdded();
  os << "\"noftriples\": \"" << _index.getNofTriples() << "\",\n"
     << "\"nofActualTriples\": \"" << actualTriples << "\",\n"
     << "\"nofAddedTriples\": \"" << addedTriples << "\",\n"
     << "\"textindex\": \"" << _index.getTextName() << "\",\n"
     << "\"nofrecords\": \"" << _index.getNofTextRecords() << "\",\n"
     << "\"nofwordpostings\": \"" << _index.getNofWordPostings() << "\",\n"
     << "\"nofentitypostings\": \"" << _index.getNofEntityPostings() << "\"\n"
     << "}\n";
  return os.str();
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
