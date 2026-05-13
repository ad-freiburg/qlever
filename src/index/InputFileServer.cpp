// Copyright 2025 The QLever Authors, in particular:
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/InputFileServer.h"

#include <absl/strings/str_cat.h>

#include <chrono>
#include <memory>
#include <thread>

#include "parser/ParallelBuffer.h"
#include "util/http/HttpUtils.h"

using namespace ad_utility::httpUtils;

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
InputFileServer::Response InputFileServer::handleFileUpload(
    Request request, SharedChunkQueue chunkQueue) {
  qlever::Filetype filetype;
  try {
    filetype = filetypeFromContentType(request);
  } catch (const std::runtime_error& e) {
    return createStringResponse(e.what(), http::status::unsupported_media_type,
                                request);
  }

  std::optional<std::string> graph = [&request]() {
    auto it = request.find("graph");
    return it == request.end() ? std::nullopt
                               : std::optional{std::string{it->value()}};
  }();

  ++numReceivedFiles_;
  std::string description =
      absl::StrCat("<http-upload-", numReceivedFiles_, ">");

  auto factory = [q = std::move(chunkQueue)](size_t blocksize,
                                             std::string_view) {
    return std::make_unique<HttpBodyParallelBuffer>(q, blocksize);
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
      return createStringResponse("successfully registered a file for parsing",
                                  http::status::ok, request);
    case Full:
      return createStringResponse(
          "input file queue is currently full, please send the file later",
          http::status::too_many_requests, request);
    case Finished:
      return createStringResponse(
          "tried to send a file after the signal for finishing was already "
          "sent",
          http::status::forbidden, request);
  }
  AD_FAIL();
}

// _____________________________________________________________________________
InputFileServer::Response InputFileServer::computeResponse(
    Request request, SharedChunkQueue chunkQueue) {
  if (request.find("Can-Upload") != request.end()) {
    return handleCanUploadQuery(request);
  }
  auto it = request.find("Finish-Index-Building");
  if (it != request.end() && it->value() == "true") {
    return handleFinishSignal(request);
  }
  return handleFileUpload(std::move(request), std::move(chunkQueue));
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<qlever::InputFileSpecification>
InputFileServer::run() {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  throw std::runtime_error(
      "InputFileServer::run() requires coroutine support (C++20).");
#else
  auto server = std::make_shared<
      HttpServer<BodyReadMode::Lazy, HttpHandler, WebsocketHandlerDummy> >(
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
