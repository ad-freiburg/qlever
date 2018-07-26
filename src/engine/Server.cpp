// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <algorithm>
#include <cstring>
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
void Server::initialize(const string& ontologyBaseName, bool useText,
                        bool allPermutations, bool onDiskLiterals,
                        bool optimizeOptionals, bool usePatterns) {
  LOG(INFO) << "Initializing server..." << std::endl;

  _optimizeOptionals = optimizeOptionals;

  _index.setUsePatterns(usePatterns);

  // Init the index.
  _index.setOnDiskLiterals(onDiskLiterals);
  _index.createFromOnDiskIndex(ontologyBaseName, allPermutations);
  if (useText) {
    _index.addTextFromOnDiskIndex();
  }

  // Init the server socket.
  bool ret = _serverSocket.create() && _serverSocket.bind(_port) &&
             _serverSocket.listen();
  if (!ret) {
    LOG(ERROR) << "Failed to create socket on port " << _port << "."
               << std::endl;
    exit(1);
  }

  // Set flag.
  _initialized = true;
  LOG(INFO) << "Done initializing server." << std::endl;
}

// _____________________________________________________________________________
void Server::run() {
  if (!_initialized) {
    LOG(ERROR) << "Cannot start an uninitialized server!" << std::endl;
    exit(1);
  }
  QueryExecutionContext qec(_index, _engine);
  std::vector<std::thread> threads;
  for (int i = 0; i < _numThreads; ++i) {
    threads.emplace_back(&Server::runAcceptLoop, this, &qec);
  }
  for (std::thread& worker : threads) {
    worker.join();
  }
}
// _____________________________________________________________________________
void Server::runAcceptLoop(QueryExecutionContext* qec) {
  // Loop and wait for queries. Run forever, for now.
  while (true) {
    // Wait for new query
    LOG(INFO) << "---------- WAITING FOR QUERY AT PORT \"" << _port << "\" ... "
              << std::endl;

    ad_utility::Socket client;
    // Concurrent accept used to be problematic because of the Thundering Herd
    // problem but this should be fixed on modern OSs
    bool success = _serverSocket.acceptClient(&client);
    if (!success) {
      LOG(ERROR) << "Socket error in acceot" << std::strerror(errno)
                 << std::endl;
      continue;
    }
    client.setKeepAlive(true);
    LOG(INFO) << "Incoming connection, processing..." << std::endl;
    process(&client, qec);
    client.close();
  }
}

// _____________________________________________________________________________
void Server::process(Socket* client, QueryExecutionContext* qec) const {
  string response;
  string query;
  string contentType;
  LOG(DEBUG) << "Waiting for receive call to complete." << endl;
  string request;
  string headers;
  client->getHTTPRequest(request, headers);
  LOG(DEBUG) << "Got request from client with size: " << request.size()
             << " and headers with total size: " << headers.size() << endl;

  size_t indexOfGET = request.find("GET");
  size_t indexOfHTTP = request.find("HTTP");
  size_t upper = indexOfHTTP;

  if (indexOfGET != request.npos && indexOfHTTP != request.npos) {
    size_t indexOfQuest = request.find("?", indexOfGET);
    if (indexOfQuest != string::npos && indexOfQuest < indexOfHTTP) {
      upper = indexOfQuest + 1;
    }
    string file = request.substr(indexOfGET + 5, upper - (indexOfGET + 5) - 1);
    if (file.size() == 0) {
      if (request[indexOfGET + 5] != '?') {
        file = "index.html";
      }
    }
    // Use hardcoded white-listing for index.html and style.css
    // can be changed if more should ever be needed, for now keep it simple.
    if (file.size() > 0) {
      LOG(DEBUG) << "file: " << file << '\n';
      if (file == "index.html" || file == "style.css" || file == "script.js") {
        serveFile(client, file);
        return;
      } else {
        LOG(INFO) << "Responding with 404 for file " << file << '\n';
        auto bytesSent = client->send(create404HttpResponse());
        LOG(DEBUG) << "Sent " << bytesSent << " bytes." << std::endl;
        return;
      }
    }

    try {
      ParamValueMap params = parseHttpRequest(request);

      if (ad_utility::getLowercase(params["cmd"]) == "stats") {
        LOG(INFO) << "Supplying index stats..." << std::endl;
        auto statsJson = composeStatsJson();
        contentType = "application/json";
        string httpResponse = createHttpResponse(statsJson, contentType);
        auto bytesSent = client->send(httpResponse);
        LOG(DEBUG) << "Sent " << bytesSent << " bytes." << std::endl;
        LOG(INFO) << "Sent stats to client." << std::endl;
        return;
      }

      if (ad_utility::getLowercase(params["cmd"]) == "clearcache") {
        qec->clearCache();
      }
      auto it = params.find("send");
      size_t maxSend = MAX_NOF_ROWS_IN_RESULT;
      if (it != params.end()) {
        maxSend = static_cast<size_t>(atol(it->second.c_str()));
      }
#ifdef ALLOW_SHUTDOWN
      if (ad_utility::getLowercase(params["cmd"]) == "shutdown") {
        LOG(INFO) << "Shutdown triggered by HTTP request "
                  << "(deactivate by compiling without -DALLOW_SHUTDOWN)"
                  << std::endl;
        exit(0);
      }
#endif
      query = createQueryFromHttpParams(params);
      LOG(INFO) << "Query: " << query << '\n';
      ParsedQuery pq = SparqlParser::parse(query);
      pq.expandPrefixes();

      // QueryGraph qg(qec);
      // qg.createFromParsedQuery(pq);
      // const QueryExecutionTree& qet = qg.getExecutionTree();
      QueryPlanner qp(qec, _optimizeOptionals);
      QueryExecutionTree qet = qp.createExecutionTree(pq);
      LOG(INFO) << qet.asString() << std::endl;

      if (ad_utility::getLowercase(params["action"]) == "csv_export") {
        // CSV export
        response = composeResponseSepValues(pq, qet, ',');
        contentType =
            "text/csv\r\n"
            "Content-Disposition: attachment;filename=export.csv";
      } else if (ad_utility::getLowercase(params["action"]) == "tsv_export") {
        // TSV export
        response = composeResponseSepValues(pq, qet, '\t');
        contentType =
            "text/tsv\r\n"
            "Content-Disposition: attachment;filename=export.tsv";
      } else {
        // Normal case: JSON response
        response = composeResponseJson(pq, qet, maxSend);
        contentType = "application/json";
      }
    } catch (const ad_semsearch::Exception& e) {
      response = composeResponseJson(query, e);
    } catch (const ParseException& e) {
      response = composeResponseJson(query, e);
    }
    string httpResponse = createHttpResponse(response, contentType);
    auto bytesSent = client->send(httpResponse);
    LOG(DEBUG) << "Sent " << bytesSent << " bytes." << std::endl;
  } else {
    LOG(INFO) << "Got invalid request " << request << '\n';
    LOG(INFO) << "Responding with 400 Bad Request.\n";
    auto bytesSent = client->send(create400HttpResponse());
    LOG(DEBUG) << "Sent " << bytesSent << " bytes." << std::endl;
  }
}

// _____________________________________________________________________________
Server::ParamValueMap Server::parseHttpRequest(
    const string& httpRequest) const {
  LOG(DEBUG) << "Parsing HTTP Request." << endl;
  ParamValueMap params;
  _requestProcessingTimer.start();
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
string Server::createQueryFromHttpParams(const ParamValueMap& params) const {
  string query;
  // Construct a Query object from the parsed request.
  auto it = params.find("query");
  if (it == params.end() || it->second == "") {
    AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
             "Expected at least one non-empty attribute \"query\".");
  }
  return it->second;
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
string Server::create404HttpResponse() const {
  std::ostringstream os;
  os << "HTTP/1.1 404 Not Found\r\n"
     << "Content-Length: 0\r\n"
     << "Connection: close\r\n"
     << "\r\n";
  return os.str();
}

// _____________________________________________________________________________
string Server::create400HttpResponse() const {
  std::ostringstream os;
  os << "HTTP/1.1 400 Bad Request\r\n"
     << "Content-Length: 0\r\n"
     << "Connection: close\r\n"
     << "\r\n";
  return os.str();
}

// _____________________________________________________________________________
string Server::composeResponseJson(const ParsedQuery& query,
                                   const QueryExecutionTree& qet,
                                   size_t maxSend) const {
  // TODO(schnelle) we really should use a json library
  // such as https://github.com/nlohmann/json
  shared_ptr<const ResultTable> rt = qet.getResult();
  _requestProcessingTimer.stop();
  off_t compResultUsecs = _requestProcessingTimer.usecs();
  size_t resultSize = rt->size();

  std::ostringstream os;
  os << "{\n"
     << "\"query\": \"" << ad_utility::escapeForJson(query._originalString)
     << "\",\n"
     << "\"status\": \"OK\",\n"
     << "\"resultsize\": \"" << resultSize << "\",\n";

  os << "\"selected\": ";
  if (query._selectedVariables.size()) {
    os << "[\"" << ad_utility::join(query._selectedVariables, "\", \"")
       << "\"],\n";
  } else {
    os << "[],\n";
  }

  os << "\"res\": ";
  size_t limit = MAX_NOF_ROWS_IN_RESULT;
  size_t offset = 0;
  if (query._limit.size() > 0) {
    limit = static_cast<size_t>(atol(query._limit.c_str()));
  }
  if (query._offset.size() > 0) {
    offset = static_cast<size_t>(atol(query._offset.c_str()));
  }
  _requestProcessingTimer.cont();
  qet.writeResultToStreamAsJson(os, query._selectedVariables, limit, offset,
                                maxSend);
  _requestProcessingTimer.stop();
  os << ",\n";

  os << "\"time\": {\n"
     << "\"total\": \"" << _requestProcessingTimer.usecs() / 1000.0 << "ms\",\n"
     << "\"computeResult\": \"" << compResultUsecs / 1000.0 << "ms\"\n"
     << "}\n"
     << "}\n";

  return os.str();
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
  qet.writeResultToStream(os, query._selectedVariables, limit, offset, sep);

  return os.str();
}

// _____________________________________________________________________________
string Server::composeResponseJson(
    const string& query, const ad_semsearch::Exception& exception) const {
  std::ostringstream os;
  _requestProcessingTimer.stop();

  os << "{\n"
     << "\"query\": \"" << ad_utility::escapeForJson(query) << "\",\n"
     << "\"status\": \"ERROR\",\n"
     << "\"resultsize\": \"0\",\n"
     << "\"time\": {\n"
     << "\"total\": \"" << _requestProcessingTimer.msecs() / 1000.0 << "ms\",\n"
     << "\"computeResult\": \"" << _requestProcessingTimer.msecs() / 1000.0
     << "ms\"\n"
     << "},\n";

  string msg = ad_utility::escapeForJson(exception.getFullErrorMessage());

  os << "\"exception\": \"" << msg << "\"\n"
     << "}\n";

  return os.str();
}

// _____________________________________________________________________________
string Server::composeResponseJson(const string& query,
                                   const ParseException& exception) const {
  std::ostringstream os;
  _requestProcessingTimer.stop();

  os << "{\n"
     << "\"query\": \"" << ad_utility::escapeForJson(query) << "\",\n"
     << "\"status\": \"ERROR\",\n"
     << "\"resultsize\": \"0\",\n"
     << "\"time\": {\n"
     << "\"total\": \"" << _requestProcessingTimer.msecs() << "ms\",\n"
     << "\"computeResult\": \"" << _requestProcessingTimer.msecs() << "ms\"\n"
     << "},\n";

  string msg = ad_utility::escapeForJson(exception.what());

  os << "\"exception\": \"" << msg << "\"\n"
     << "}\n";

  return os.str();
}

// _____________________________________________________________________________
void Server::serveFile(Socket* client, const string& requestedFile) const {
  string contentString;
  string contentType = "text/plain";
  string statusString = "HTTP/1.0 200 OK";

  // CASE: file.
  LOG(DEBUG) << "Looking for file: \"" << requestedFile << "\" ... \n";
  std::ifstream in(requestedFile.c_str());
  if (!in) {
    statusString = "HTTP/1.0 404 NOT FOUND";
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

  size_t contentLength = contentString.size();
  std::ostringstream headerStream;
  headerStream << statusString << "\r\n"
               << "Content-Length: " << contentLength << "\r\n"
               << "Content-Type: " << contentType << "\r\n"
               << "Access-Control-Allow-Origin: *\r\n"
               << "Connection: close\r\n"
               << "\r\n";

  string data = headerStream.str();
  data += contentString;
  client->send(data);
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

  os << "\"noftriples\": \"" << _index.getNofTriples() << "\",\n"
     << "\"textindex\": \"" << _index.getTextName() << "\",\n"
     << "\"nofrecords\": \"" << _index.getNofTextRecords() << "\",\n"
     << "\"nofwordpostings\": \"" << _index.getNofWordPostings() << "\",\n"
     << "\"nofentitypostings\": \"" << _index.getNofEntityPostings() << "\"\n"
     << "}\n";
  return os.str();
}
