// Copyright 2025 The QLever Authors, in particular:
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_INDEX_INPUTFILESERVER_H
#define QLEVER_INDEX_INPUTFILESERVER_H

#include "index/InputFileSpecification.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/ThreadSafeQueue.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"

class InputFileServer {
  using Request = http::request<http::string_body>;
  using Response = http::response<http::string_body>;

  ad_utility::data_structures::ThreadSafeQueue<qlever::InputFileSpecification>
      queue_;
  ad_utility::JThread serverThread_;
  unsigned short port_;
  size_t numReceivedFiles_ = 0;
  bool isRunning_ = false;
  std::function<void(void)> shutDown_;

  struct HttpHandler {
    InputFileServer* server_;
    net::awaitable<void> operator()(Request request, auto&& send) const {
      return server_->processRequest(std::move(request), AD_FWD(send));
    }
  };
  struct WebsocketHandlerDummy {
    net::awaitable<void> operator()(auto&&...) const {
      throw std::runtime_error("Websockets not implemented");
      co_return;
    }
  };
  struct WebsocketSupplier {
    WebsocketHandlerDummy operator()(net::io_context&) const { return {}; }
  };

  // Parse the Content-Type header to determine the RDF filetype. Throws
  // std::runtime_error if the header is absent or not one of the explicitly
  // supported values.
  static qlever::Filetype filetypeFromContentType(const Request& request);

  // Respond to a `Can-Upload` probe with 200 OK or 429 Too Many Requests.
  Response handleCanUploadQuery(const Request& request) const;

  // Handle a `Finish-Index-Building: true` signal, call `queue_.finish()`.
  Response handleFinishSignal(const Request& request);

  // Parse the request body as RDF data and enqueue it for index building.
  Response handleFileUpload(Request request);

  // Dispatch the incoming request to the appropriate handler.
  Response computeResponse(Request request);

  // Create a plain-text HTTP response from `body` and `status`, reusing the
  // keep-alive and version settings from `request`.
  static Response createStringResponse(std::string body, http::status status,
                                       const Request& request);

  net::awaitable<void> processRequest(Request request, auto&& send) {
    try {
      co_await send(computeResponse(std::move(request)));
    } catch (const std::exception& e) {
      AD_LOG_INFO << "InputFileServer::processRequest received an exception: "
                  << e.what() << std::endl;
    }
  }

 public:
  explicit InputFileServer(unsigned short port, size_t maxQueueSize = 20)
      : queue_{maxQueueSize}, port_{port} {
    if (maxQueueSize == 0) {
      AD_LOG_WARN << "InputFileServer created with a zero queue size; the "
                     "server will never accept file uploads."
                  << std::endl;
    }
  }

  // Start the HTTP server and return a range that yields the uploaded file
  // specifications as they arrive. The range is exhausted when
  // `Finish-Index-Building: true` is received.
  ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> run();

  ~InputFileServer();
};

#endif  // QLEVER_INDEX_INPUTFILESERVER_H
