// Copyright 2025 The QLever Authors, in particular:
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/InputFileServer.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <chrono>
#include <memory>
#include <thread>

#include "util/http/HttpUtils.h"

using namespace ad_utility::httpUtils;

namespace {
// An `AsyncBlockSource` that pulls the bytes of an HTTP request body directly
// from `bodyGetter_`, with no intermediate queue or thread. `bodyGetter_` may
// only be safely called on `sessionExecutor_` (the strand the underlying
// `beast::tcp_stream` belongs to), so `asyncGetNextBlockImpl` `co_spawn`s a
// small coroutine onto that exact executor for every block; `net::co_spawn`
// takes care of running it there regardless of which thread requested the
// block. Once `bodyGetter_` signals EOF or throws, `completion_->timer` is
// cancelled to wake up `InputFileServer::handleFileUpload`, which is the sole
// owner of the (otherwise dangling-prone) `bodyGetter_` reference and must
// stay suspended for as long as this class might still call it.
class HttpBodyBlockSource : public qlever::parser::AsyncBlockSource {
  InputFileServer::BodyGetter bodyGetter_;
  net::any_io_executor sessionExecutor_;
  std::shared_ptr<InputFileServer::UploadCompletion> completion_;

 public:
  HttpBodyBlockSource(
      ad_utility::MemorySize blocksize, InputFileServer::BodyGetter bodyGetter,
      net::any_io_executor sessionExecutor,
      std::shared_ptr<InputFileServer::UploadCompletion> completion)
      : AsyncBlockSource{blocksize},
        bodyGetter_{std::move(bodyGetter)},
        sessionExecutor_{std::move(sessionExecutor)},
        completion_{std::move(completion)} {}

 protected:
  void asyncGetNextBlockImpl(Handler handler) override {
    net::co_spawn(
        sessionExecutor_,
        [this]() -> net::awaitable<std::optional<Block>> {
          auto chunk = co_await bodyGetter_();
          if (!chunk.has_value()) {
            co_return std::nullopt;
          }
          Block block;
          block.resize(chunk->size());
          std::ranges::copy(*chunk, block.data());
          co_return block;
        },
        [this, handler = std::move(handler)](
            std::exception_ptr ep, std::optional<Block> block) mutable {
          if (ep || !block.has_value()) {
            // EOF or error: wake up `handleFileUpload`, which is waiting on
            // `completion_->timer` for exactly this signal.
            completion_->error = ep;
            completion_->timer.cancel();
          }
          handler(ep, std::move(block));
        });
  }
};
}  // namespace

// _____________________________________________________________________________
std::unique_ptr<qlever::parser::AsyncBlockSource>
InputFileServer::makeHttpBodyBlockSource(
    ad_utility::MemorySize blocksize, BodyGetter bodyGetter,
    net::any_io_executor sessionExecutor,
    std::shared_ptr<UploadCompletion> completion) {
  return std::make_unique<HttpBodyBlockSource>(blocksize, std::move(bodyGetter),
                                               std::move(sessionExecutor),
                                               std::move(completion));
}

// _____________________________________________________________________________
InputFileServer::Response InputFileServer::createStringResponse(
    std::string body, http::status status, const Request& request) {
  return createHttpResponseFromString(std::move(body), status,
                                      ad_utility::MediaType::textPlain,
                                      request.keep_alive(), request.version());
}

// _____________________________________________________________________________
qlever::Filetype InputFileServer::filetypeFromContentType(
    const Request& request) {
  static constexpr std::string_view kSupportedValues =
      "Supported values: text/turtle, application/n-triples, "
      "application/n-quads.";
  auto it = request.find(http::field::content_type);
  if (it == request.end()) {
    throw std::runtime_error(
        absl::StrCat("A Content-Type header is required for file uploads. ",
                     kSupportedValues));
  }
  std::string_view contentType = it->value();
  // Reuse the same Accept-header-grammar-based parsing that
  // `GraphStoreProtocol::extractMediatype` uses for the `Content-Type`
  // header; this gives us `; charset=...`-parameter tolerance and
  // case-insensitivity for free.
  try {
    auto mediaTypes = ad_utility::getMediaTypesFromAcceptHeader(contentType);
    if (mediaTypes.size() == 1) {
      if (auto filetype = qlever::filetypeFromMediaType(mediaTypes.front())) {
        return filetype.value();
      }
    }
  } catch (const std::exception&) {
    // Fall through to the error below; an unparsable `Content-Type` is
    // reported the same way as an unsupported one.
  }
  throw std::runtime_error(absl::StrCat("Unsupported Content-Type: \"",
                                        contentType, "\". ", kSupportedValues));
}

// _____________________________________________________________________________
InputFileServer::UploadRequestParams InputFileServer::parseUploadRequestHeaders(
    const Request& request) {
  UploadRequestParams params;
  params.filetype_ = filetypeFromContentType(request);

  auto graphIt = request.find("graph");
  if (graphIt != request.end()) {
    params.graph_ = std::string{graphIt->value()};
  }

  // Honour an explicit request to enable or disable parallel parsing.
  auto parallelIt = request.find("Parse-In-Parallel");
  if (parallelIt != request.end()) {
    params.parseInParallel_ =
        (parallelIt->value() == "true" || parallelIt->value() == "1");
    params.parseInParallelSetExplicitly_ = true;
  }
  return params;
}

// _____________________________________________________________________________
InputFileServer::Response InputFileServer::handleCanUploadQuery(
    const Request& request) const {
  auto status =
      queue_.canPush() ? http::status::ok : http::status::too_many_requests;
  return createStringResponse("Responding to Can-Upload query", status,
                              request);
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<qlever::InputFileSpecification>
InputFileServer::run() {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  throw std::runtime_error(
      "InputFileServer::run() requires coroutine support (C++20).");
#else
  auto server = std::make_shared<
      HttpServer<BodyReadMode::Lazy, HttpHandler, NoWebSocketHandler>>(
      port_, "0.0.0.0", 1, HttpHandler{this}, NoWebSocketSupplier{});
  shutDown_ = [server]() { server->shutDown(); };
  serverThread_ = ad_utility::JThread{[server]() { server->run(); }};
  if (!server->waitUntilReady(std::chrono::milliseconds{500})) {
    throw std::runtime_error(
        "InputFileServer did not become ready within 500 ms.");
  }
  isRunning_ = true;
  auto filesGenerator =
      [](auto& queue) -> cppcoro::generator<qlever::InputFileSpecification> {
    while (auto opt = queue.pop()) {
      co_yield opt.value();
    }
  };
  return ad_utility::InputRangeTypeErased{filesGenerator(queue_)};
#endif
}

// _____________________________________________________________________________
InputFileServer::~InputFileServer() {
  if (isRunning_) {
    shutDown_();
  }
}
