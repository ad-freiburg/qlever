// Copyright 2025 The QLever Authors, in particular:
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_INDEX_INPUTFILESERVER_H
#define QLEVER_INDEX_INPUTFILESERVER_H

#include <absl/strings/str_cat.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "index/InputFileSpecification.h"
#include "parser/AsyncBlockSource.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/ThreadSafeQueue.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"

class InputFileServer {
 public:
  // Type-erased view of `bodyGetter`, used so that `HttpBodyBlockSource` (in
  // InputFileServer.cpp) doesn't need to know `HttpServer`'s internal
  // (deeply templated) `bodyGetter` type. Public because `HttpBodyBlockSource`
  // (a free class defined in InputFileServer.cpp, not a member of this class)
  // needs to name it.
  using BodyGetter =
      std::function<net::awaitable<std::optional<std::string_view>>()>;

  // Shared between `handleFileUpload` and `HttpBodyBlockSource`: the latter
  // signals (cancels the timer) once the request body has been fully
  // consumed or an error occurred, so that `handleFileUpload` -- which owns
  // the only valid reference to `bodyGetter` and the underlying stream/buffer
  // it reads from -- knows when it is safe to return. Public for the same
  // reason as `BodyGetter` above.
  struct UploadCompletion {
    net::steady_timer timer;
    std::exception_ptr error;
    explicit UploadCompletion(net::any_io_executor exec)
        : timer{std::move(exec), net::steady_timer::time_point::max()} {}
  };

 private:
  // In lazy mode the body is streamed separately; the handler receives a
  // header-only request and pulls the body itself via `bodyGetter`.
  using Request = http::request<http::empty_body>;
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
    net::awaitable<void> operator()(Request request, auto&& send,
                                    auto&& bodyGetter) const {
      return server_->processRequest(std::move(request), AD_FWD(send),
                                     AD_FWD(bodyGetter));
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

  // Create an InputFileSpecification backed by a `HttpBodyBlockSource` and
  // enqueue it for index building. The `HttpBodyBlockSource` pulls the
  // request body directly from `bodyGetter` (no intermediate queue or
  // thread); this coroutine stays suspended until it does so, since
  // `bodyGetter` and the stream/buffer it reads from are only valid for as
  // long as this coroutine frame is alive.
  net::awaitable<void> handleFileUpload(Request request, auto& send,
                                        auto& bodyGetter) {
    qlever::Filetype filetype;
    // `co_await` is not allowed inside a `catch` handler, so the error
    // response is only built here and sent afterwards.
    std::optional<Response> unsupportedMediaTypeResponse;
    try {
      filetype = filetypeFromContentType(request);
    } catch (const std::runtime_error& e) {
      unsupportedMediaTypeResponse = createStringResponse(
          e.what(), http::status::unsupported_media_type, request);
    }
    if (unsupportedMediaTypeResponse.has_value()) {
      co_await send(std::move(unsupportedMediaTypeResponse).value());
      co_return;
    }

    std::optional<std::string> graph = [&request]() {
      auto it = request.find("graph");
      return it == request.end() ? std::nullopt
                                 : std::optional{std::string{it->value()}};
    }();

    ++numReceivedFiles_;
    std::string description =
        absl::StrCat("<http-upload-", numReceivedFiles_, ">");

    // `bodyGetter` and this coroutine's executor are captured by the factory
    // below; `completion` is how the resulting `HttpBodyBlockSource` tells us
    // (from the parser side, running on a wholly different thread) that it is
    // done with `bodyGetter`, so we know when it is safe to return.
    auto sessionExecutor = co_await net::this_coro::executor;
    auto completion = std::make_shared<UploadCompletion>(sessionExecutor);
    BodyGetter erasedBodyGetter =
        [&bodyGetter]() -> net::awaitable<std::optional<std::string_view>> {
      co_return co_await bodyGetter();
    };
    auto factory = [erasedBodyGetter, sessionExecutor, completion](
                       const boost::asio::any_io_executor& /*unused*/,
                       ad_utility::MemorySize blocksize,
                       std::string_view /*unused*/) {
      return makeHttpBodyBlockSource(blocksize, erasedBodyGetter,
                                     sessionExecutor, completion);
    };
    qlever::InputFileSpecification spec{
        qlever::InputFileSpecification::BufferFactoryAndDescription{
            std::move(factory), std::move(description)},
        filetype, std::move(graph)};

    // Honour an explicit request to enable or disable parallel parsing.
    auto pit = request.find("Parse-In-Parallel");
    if (pit != request.end()) {
      spec.parseInParallel_ = (pit->value() == "true" || pit->value() == "1");
      spec.parseInParallelSetExplicitly_ = true;
    }

    auto pushStatus = queue_.pushIfNotFull(std::move(spec));
    switch (pushStatus) {
      using enum decltype(queue_)::Status;
      case Pushed:
        co_await send(
            createStringResponse("successfully registered a file for parsing",
                                 http::status::ok, request));
        break;
      case Full:
        co_await send(createStringResponse(
            "input file queue is currently full, please send the file later",
            http::status::too_many_requests, request));
        co_return;
      case Finished:
        co_await send(createStringResponse(
            "tried to send a file after the signal for finishing was already "
            "sent",
            http::status::forbidden, request));
        co_return;
    }

    // Only reached for `Pushed`: wait until the parser has fully consumed the
    // body (or failed to).
    co_await completion->timer.async_wait(
        boost::asio::as_tuple(net::use_awaitable));
    if (completion->error) {
      try {
        std::rethrow_exception(completion->error);
      } catch (const std::exception& e) {
        AD_LOG_WARN << "Error while streaming the request body for upload #"
                    << numReceivedFiles_ << ": " << e.what() << std::endl;
      } catch (...) {
        AD_LOG_WARN << "Unknown error while streaming the request body for "
                       "upload #"
                    << numReceivedFiles_ << std::endl;
      }
    }
  }

  // Create a plain-text HTTP response from `body` and `status`, reusing the
  // keep-alive and version settings from `request`.
  static Response createStringResponse(std::string body, http::status status,
                                       const Request& request);

  // Create the `AsyncBlockSource` that pulls chunks from `bodyGetter` via
  // `sessionExecutor` and signals `completion` on EOF/error (defined in
  // InputFileServer.cpp to keep `HttpBodyBlockSource` out of this header).
  static std::unique_ptr<qlever::parser::AsyncBlockSource>
  makeHttpBodyBlockSource(ad_utility::MemorySize blocksize,
                          BodyGetter bodyGetter,
                          net::any_io_executor sessionExecutor,
                          std::shared_ptr<UploadCompletion> completion);

  net::awaitable<void> processRequest(Request request, auto&& send,
                                      auto&& bodyGetter) {
    try {
      if (request.find("Can-Upload") != request.end()) {
        co_await send(handleCanUploadQuery(request));
        co_return;
      }
      auto it = request.find("Finish-Index-Building");
      if (it != request.end() && it->value() == "true") {
        co_await send(handleFinishSignal(request));
        co_return;
      }
      co_await handleFileUpload(std::move(request), send, bodyGetter);
    } catch (const std::exception& e) {
      AD_LOG_FATAL << "InputFileServer::processRequest: " << e.what()
                   << std::endl;
      std::terminate();
    } catch (...) {
      AD_LOG_FATAL << "InputFileServer::processRequest: unknown exception."
                   << std::endl;
      std::terminate();
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
