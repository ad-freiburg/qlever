// Copyright 2025 The QLever Authors, in particular:
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "InputFileServer.h"

#include "parser/ParallelBuffer.h"
#include "util/http/HttpUtils.h"

using namespace ad_utility::httpUtils;

// _____________________________________________________________________________
qlever::Filetype InputFileServer::filetypeFromContentType(
    const Request& request) {
  auto it = request.find(http::field::content_type);
  if (it == request.end()) {
    return qlever::Filetype::Turtle;
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
  if (ct == "application/n-quads") {
    return qlever::Filetype::NQuad;
  }
  // Both text/turtle and application/n-triples are handled by the Turtle
  // parser.
  return qlever::Filetype::Turtle;
}

// _____________________________________________________________________________
InputFileServer::Response InputFileServer::handleCanUploadQuery(
    const Request& request) {
  auto status =
      queue_.canPush() ? http::status::ok : http::status::too_many_requests;
  return createHttpResponseFromString("Responding to Can-Upload query", status,
                                      ad_utility::MediaType::textPlain,
                                      request.keep_alive(), request.version());
}

// _____________________________________________________________________________
InputFileServer::Response InputFileServer::handleFinishSignal(
    const Request& request) {
  AD_LOG_INFO << "Acquired the finishing signal" << std::endl;
  queue_.finish();
  return createHttpResponseFromString("received signal for finishing",
                                      http::status::ok,
                                      ad_utility::MediaType::textPlain,
                                      request.keep_alive(), request.version());
}

// _____________________________________________________________________________
InputFileServer::Response InputFileServer::handleFileUpload(Request request) {
  std::optional<std::string> graph = [&request]() {
    auto it = request.find("graph");
    return it == request.end() ? std::nullopt
                               : std::optional{std::string{it->value()}};
  }();

  qlever::Filetype filetype = filetypeFromContentType(request);

  auto bodyPtr = std::make_shared<std::string>(std::move(request.body()));
  auto factory = [bodyPtr](size_t blocksize, std::string_view) {
    return std::make_unique<StringParallelBuffer>(std::move(*bodyPtr),
                                                  blocksize);
  };
  qlever::InputFileSpecification spec{
      qlever::InputFileSpecification::BufferFactoryAndDescription{
          std::move(factory), "<http-request-body>"},
      filetype, std::move(graph)};

  auto pushStatus = queue_.pushIfNotFull(std::move(spec));
  switch (pushStatus) {
    using enum decltype(queue_)::Status;
    case Pushed:
      return createHttpResponseFromString(
          "successfully registered a file for parsing", http::status::ok,
          ad_utility::MediaType::textPlain, request.keep_alive(),
          request.version());
    case Full:
      return createHttpResponseFromString(
          "input file queue is currently full, please send the file later",
          http::status::too_many_requests, ad_utility::MediaType::textPlain,
          request.keep_alive(), request.version());
    case Finished:
      return createHttpResponseFromString(
          "tried to send a file after the signal for finishing was already "
          "sent",
          http::status::forbidden, ad_utility::MediaType::textPlain,
          request.keep_alive(), request.version());
  }
  AD_FAIL();
}

// _____________________________________________________________________________
InputFileServer::Response InputFileServer::computeResponse(Request request) {
  if (request.find("Can-Upload") != request.end()) {
    return handleCanUploadQuery(request);
  }
  auto it = request.find("Finish-Index-Building");
  if (it != request.end() && it->value() == "true") {
    return handleFinishSignal(request);
  }
  return handleFileUpload(std::move(request));
}

// _____________________________________________________________________________
cppcoro::generator<qlever::InputFileSpecification>
InputFileServer::getFilesRange() {
  auto cleanup = absl::Cleanup{[]() {
    AD_LOG_INFO << "InputFileServer::getFiles was finished..." << std::endl;
  }};
  while (auto opt = queue_.pop()) {
    co_yield opt.value();
  }
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<qlever::InputFileSpecification>
InputFileServer::run() {
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
  return ad_utility::InputRangeTypeErased<qlever::InputFileSpecification>{
      getFilesRange()};
}

// _____________________________________________________________________________
InputFileServer::~InputFileServer() {
  AD_LOG_INFO << "Destroying the InputFileServer...." << std::endl;
  if (isRunning_) {
    shutDown_();
  }
  AD_LOG_INFO << "... Done destroying the InputFileServer...." << std::endl;
}
