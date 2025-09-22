//
// Created by kalmbacj on 2025-09-12.
//

#ifndef QLEVER_INPUTFILESERVER_H
#define QLEVER_INPUTFILESERVER_H

#include <absl/functional/bind_front.h>

#include "index/InputFileSpecification.h"
#include "util/Log.h"
#include "util/ThreadSafeQueue.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"

class InputFileServer {
  ad_utility::data_structures::ThreadSafeQueue<
      qlever::InputFileSpecificationWithFileContent>
      queue{20};
  ad_utility::JThread serverThread_;
  unsigned short port = 9874;
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

  net::awaitable<void> processRequest(http::request<http::string_body> request,
                                      auto&& send) {
    auto it = request.find("Finish-Index-Building");
    if (it != request.end() && it->value() == "true") {
      queue.finish();
      co_await send(ad_utility::httpUtils::createOkResponse(
          "received signal for finishing", request,
          ad_utility::MediaType::textPlain));
      co_return;
    }
    std::optional<std::string> graph = [&request]() {
      auto it = request.find("graph");
      return it == request.end() ? std::nullopt
                                 : std::optional{std::string{it->value()}};
    }();

    qlever::InputFileSpecificationWithFileContent spec{
        std::move(request.body()), qlever::Filetype::Turtle, std::move(graph)};
    auto status = queue.pushIfNotFull(std::move(spec));
    switch (status) {
      using enum decltype(queue)::Status;
      case Pushed:
        co_await send(ad_utility::httpUtils::createOkResponse(
            "successfully registered a file for parsing", request,
            ad_utility::MediaType::textPlain));
        break;
      case Full:
        co_await send(ad_utility::httpUtils::createHttpResponseFromString(
            "input file queue is currently full, please send the file later",
            http::status::too_many_requests, request,
            ad_utility::MediaType::textPlain));
        break;
      case Finished:
        co_await send(ad_utility::httpUtils::createHttpResponseFromString(
            "tried to send a file after the signal for finishing was already "
            "sent",
            http::status::forbidden, request,
            ad_utility::MediaType::textPlain));
        break;
    }
    co_return;
  }

 public:
  void run() {
    serverThread_ = ad_utility::JThread([this]() {
      auto handler = [this](auto request, auto&& send) {
        return processRequest(std::move(request), AD_FWD(send));
      };
      HttpServer<decltype(handler), decltype(websocketHandlerDummy)> server{
          port, "0.0.0.0", 1, handler, websocketSupplier};
      shutDown_ = [&server]() { server.shutDown(); };
      server.run();
    });
    isRunning_ = true;
  }

  using FileRange =
      cppcoro::generator<qlever::InputFileSpecificationWithFileContent>;
  FileRange getFiles() {
    while (auto opt = queue.pop()) {
      co_yield opt.value();
    }
  }

  ~InputFileServer() {
    if (isRunning_) {
      shutDown_();
    }
  }
};

#endif  // QLEVER_INPUTFILESERVER_H
