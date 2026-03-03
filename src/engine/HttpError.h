// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <exception>

#include "util/http/beast.h"

#ifndef QLEVER_SRC_ENGINE_HTTPERROR_H
#define QLEVER_SRC_ENGINE_HTTPERROR_H

// An Error occurred that immediately results in a specific HTTP status code.
// This can be used to prematurely end the execution of a request in the Server
// and return a specific status code and response.
class HttpError : public std::exception {
 public:
  explicit HttpError(boost::beast::http::status status)
      : status_{status}, reason_{boost::beast::http::obsolete_reason(status)} {}
  explicit HttpError(boost::beast::http::status status, std::string_view extra)
      : status_{status}, reason_{extra} {}

  const char* what() const noexcept override { return reason_.c_str(); }

  [[nodiscard]] const boost::beast::http::status& status() const {
    return status_;
  }

 private:
  boost::beast::http::status status_;
  std::string reason_{};
};

#endif  // QLEVER_SRC_ENGINE_HTTPERROR_H
