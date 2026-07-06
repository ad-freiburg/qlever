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
#include <chrono>
#include <memory>
#include <thread>

#include "util/http/HttpUtils.h"

using namespace ad_utility::httpUtils;

namespace {
// An `AsyncBlockSource` that streams the bytes of an HTTP request body. The
// bytes themselves are delivered chunk by chunk into `queue` by
// `InputFileServer::handleFileUpload`, which pulls them from the HTTP
// session's `bodyGetter`; `getNextBlockImpl` simply pops them again on the
// (unrelated) executor that drives the RDF parser. The queue's `finish()`/
// `pushException()` signal EOF or an error to the parser thread.
class HttpBodyBlockSource : public qlever::parser::AsyncBlockSource {
  using ChunkQueue =
      ad_utility::data_structures::ThreadSafeQueue<std::vector<char>>;
  std::shared_ptr<ChunkQueue> queue_;

 public:
  HttpBodyBlockSource(const boost::asio::any_io_executor& exec,
                      ad_utility::MemorySize blocksize,
                      std::shared_ptr<ChunkQueue> queue)
      : AsyncBlockSource{exec, blocksize}, queue_{std::move(queue)} {}

 protected:
  std::optional<Block> getNextBlockImpl() override {
    auto chunk = queue_->pop();
    if (!chunk.has_value()) {
      return std::nullopt;
    }
    Block block;
    block.resize(chunk->size());
    std::ranges::copy(*chunk, block.data());
    return block;
  }
};
}  // namespace

// _____________________________________________________________________________
std::unique_ptr<qlever::parser::AsyncBlockSource>
InputFileServer::makeHttpBodyBlockSource(
    const boost::asio::any_io_executor& exec, ad_utility::MemorySize blocksize,
    SharedChunkQueue queue) {
  return std::make_unique<HttpBodyBlockSource>(exec, blocksize,
                                               std::move(queue));
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
  std::string_view ct = it->value();
  // Strip parameters (e.g., "; charset=utf-8").
  auto semi = ct.find(';');
  if (semi != std::string_view::npos) {
    ct = ct.substr(0, semi);
  }
  while (!ct.empty() && ct.back() == ' ') {
    ct.remove_suffix(1);
  }
  if (ct == "text/turtle" || ct == "application/n-triples") {
    return qlever::Filetype::Turtle;
  }
  if (ct == "application/n-quads") {
    return qlever::Filetype::NQuad;
  }
  throw std::runtime_error(absl::StrCat("Unsupported Content-Type: \"", ct,
                                        "\". ", kSupportedValues));
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
InputFileServer::Response InputFileServer::handleFinishSignal(
    const Request& request) {
  // In lazy mode the body is read after the response is sent, so we validate
  // via the Content-Length header instead of inspecting the body directly.
  auto it = request.find(http::field::content_length);
  if (it != request.end() && it->value() != "0") {
    return createStringResponse(
        "Finish-Index-Building must be sent with an empty body.",
        http::status::bad_request, request);
  }
  queue_.finish();
  return createStringResponse("received signal for finishing", http::status::ok,
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
      HttpServer<BodyReadMode::Lazy, HttpHandler, WebsocketHandlerDummy>>(
      port_, "0.0.0.0", 1, HttpHandler{this}, WebsocketSupplier{});
  shutDown_ = [server]() { server->shutDown(); };
  serverThread_ = ad_utility::JThread{[server]() { server->run(); }};
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds{500};
  while (!server->serverIsReady()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error(
          "InputFileServer did not become ready within 500 ms.");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
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
  AD_LOG_INFO << "Destroying the InputFileServer...." << std::endl;
  if (isRunning_) {
    shutDown_();
  }
  AD_LOG_INFO << "... Done destroying the InputFileServer...." << std::endl;
}
