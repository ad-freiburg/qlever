// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2022      Julian Mundhahs (mundhahj@tf.informatik.uni-freiburg.de)
#pragma once

#include <exception>
#include <string>
#include <string_view>

// Contains Metadata (position of the faulty clause) about a ParseException that
// occurred.
struct ExceptionMetadata {
  // Query that the exception occurred in. This is currently needed because the
  // parse doesn't parse the whole query in one piece. It therefore can only
  // determine the position in the current (sub-)query.
  std::string_view query_;
  // Start and Stop Index of the clause that cause the exception in query_.
  size_t startIndex_ = 0;
  size_t stopIndex_ = 0;

  [[nodiscard]] std::string coloredError() const {
    if (startIndex_ < stopIndex_) {
      auto first = query_.substr(0, startIndex_);
      auto middle = query_.substr(startIndex_, stopIndex_ - startIndex_ + 1);
      auto end = query_.substr(stopIndex_ + 1);

      return (std::string{first} + "\x1b[1m\x1b[4m\x1b[31m" +
              std::string{middle} + "\x1b[0m" + std::string{end});
    } else {
      return std::string{query_};
    }
  }
};

class ParseException : public std::exception {
 public:
  ParseException(std::string cause_, ExceptionMetadata metadata = {{}})
      : cause_(std::string("ParseException, cause: ") + cause_),
        metadata_(metadata){};

  virtual const char* what() const throw() { return cause_.c_str(); }
  [[nodiscard]] const ExceptionMetadata& metadata() const { return metadata_; }

 private:
  std::string cause_;
  ExceptionMetadata metadata_;
};
