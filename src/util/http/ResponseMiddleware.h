// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_HTTP_RESPONSEMIDDLEWARE_H
#define QLEVER_SRC_UTIL_HTTP_RESPONSEMIDDLEWARE_H

#include <utility>

#include "engine/UpdateMetadata.h"
#include "util/http/HttpUtils.h"

// A `ResponseMiddleware` is a function that can modify a response right before
// it is sent out. `UpdateMiddleware` is used for updates and `QueryMiddleware`
// for queries.
struct ResponseMiddleware {
  using ResponseT = ad_utility::httpUtils::ResponseT;
  using UpdateMiddleware =
      std::function<ResponseT(ResponseT, const std::vector<UpdateMetadata>&)>;
  using QueryMiddleware = std::function<ResponseT(ResponseT)>;

 private:
  std::variant<UpdateMiddleware, QueryMiddleware> func_;

 public:
  // A middleware to be called after an Update. When this constructor is used,
  // the `UpdateMetadata` must be passed to `apply`.
  explicit ResponseMiddleware(UpdateMiddleware&& func)
      : func_(std::move(func)) {}
  // A middleware to be called after a Query. When this constructor is used,
  // *no* `UpdateMetadata` must be passed to `apply`.
  explicit ResponseMiddleware(QueryMiddleware&& func)
      : func_(std::move(func)) {}

  // Apply the middleware to a response. The current response is passed in and a
  // new one is returned.
  ResponseT apply(
      ResponseT response,
      const std::optional<std::vector<UpdateMetadata>>& metadataOpt) const {
    if (metadataOpt.has_value()) {
      AD_CONTRACT_CHECK(
          std::holds_alternative<UpdateMiddleware>(func_),
          "Got `UpdateMetadata` for but middleware takes no metadata.");
      const auto& updateMiddleware = std::get<UpdateMiddleware>(func_);
      return updateMiddleware(std::move(response), metadataOpt.value());
    }
    AD_CONTRACT_CHECK(
        std::holds_alternative<QueryMiddleware>(func_),
        "Got no `UpdateMetadata` for but middleware expects metadata.");
    const auto& queryMiddleware = std::get<QueryMiddleware>(func_);
    return queryMiddleware(std::move(response));
  }
};

#endif  // QLEVER_SRC_UTIL_HTTP_RESPONSEMIDDLEWARE_H
