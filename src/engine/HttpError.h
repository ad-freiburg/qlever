// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <exception>

#include "util/http/beast.h"

#ifndef QLEVER_HTTPERROR_H
#define QLEVER_HTTPERROR_H

// An Error occurred that results in a specific HTTP status code.
class HttpError : public std::exception {
 public:
  explicit HttpError(boost::beast::http::status status)
      : status_{status}, reason_{boost::beast::http::obsolete_reason(status)} {}
  explicit HttpError(boost::beast::http::status status, string_view extra)
      : status_{status},
        reason_{absl::StrCat(
            std::string_view{boost::beast::http::obsolete_reason(status)}, ": ",
            extra)} {}

  const char* what() const noexcept override { return reason_.c_str(); }

  [[nodiscard]] const boost::beast::http::status& status() const {
    return status_;
  }

 private:
  boost::beast::http::status status_;
  std::string reason_{};
};

#endif  // QLEVER_HTTPERROR_H
