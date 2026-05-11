//
// Created by kalmbacj on 2025-09-12.
//

#ifndef QLEVER_INPUTFILESERVER_H
#define QLEVER_INPUTFILESERVER_H

#include <absl/cleanup/cleanup.h>

#include "index/InputFileSpecification.h"
#include "util/Log.h"
#include "util/ThreadSafeQueue.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"

class InputFileServer {
  ad_utility::data_structures::ThreadSafeQueue<qlever::InputFileSpecification>
      queue_{20};
  ad_utility::JThread serverThread_;
  unsigned short port_;
  bool isRunning_ = false;
  std::function<void(void)> shutDown_;

  static constexpr auto websocketHandlerDummy =
      [](auto&&...) -> net::awaitable<void> {
    throw std::runtime_error("Websockets not implemented");
    co_return;
  };
  static constexpr auto websocketSupplier = [](net::io_context&) {
    return websocketHandlerDummy;
  };

  // Parse the Content-Type header to determine the RDF filetype. Defaults to
  // Turtle if the header is absent or contains an unrecognized value.
  static qlever::Filetype filetypeFromContentType(
      const http::request<http::string_body>& request);

  // Respond to a `Can-Upload` probe with 200 OK or 429 Too Many Requests.
  http::response<http::string_body> handleCanUploadQuery(
      const http::request<http::string_body>& request);

  // Handle a `Finish-Index-Building: true` signal, call `queue_.finish()`.
  http::response<http::string_body> handleFinishSignal(
      const http::request<http::string_body>& request);

  // Parse the request body as RDF data and enqueue it for index building.
  http::response<http::string_body> handleFileUpload(
      http::request<http::string_body> request);

  // Dispatch the incoming request to the appropriate handler.
  http::response<http::string_body> computeResponse(
      http::request<http::string_body> request);

  net::awaitable<void> processRequest(http::request<http::string_body> request,
                                      auto&& send) {
    try {
      co_await send(computeResponse(std::move(request)));
    } catch (const std::exception& e) {
      AD_LOG_INFO << "InputFileServer::processRequest received an exception: "
                  << e.what() << std::endl;
    }
  }

 public:
  explicit InputFileServer(unsigned short port) : port_{port} {}

  void run() {
    serverThread_ = ad_utility::JThread([this]() {
      auto handler = [this](auto request, auto&& send) {
        return processRequest(std::move(request), AD_FWD(send));
      };
      auto cleanup = absl::Cleanup{
          []() { AD_LOG_INFO << "Stopped running the HTTP Server"; }};
      HttpServer<decltype(handler), decltype(websocketHandlerDummy)> server{
          port_, "0.0.0.0", 1, handler, websocketSupplier};
      shutDown_ = [&server]() { server.shutDown(); };
      server.run();
    });
    isRunning_ = true;
  }

  using FileRange = cppcoro::generator<qlever::InputFileSpecification>;
  FileRange getFiles() {
    auto cleanup = absl::Cleanup{[]() {
      AD_LOG_INFO << "InputFileServer::getFiles was finished..." << std::endl;
    }};
    while (auto opt = queue_.pop()) {
      co_yield opt.value();
    }
  }

  ~InputFileServer() {
    AD_LOG_INFO << "Destroying the InputFileServer...." << std::endl;
    if (isRunning_) {
      shutDown_();
    }
    AD_LOG_INFO << "... Done destroying the InputFileServer...." << std::endl;
  }
};

#endif  // QLEVER_INPUTFILESERVER_H
