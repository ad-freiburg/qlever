// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2022      Julian Mundhahs (mundhahj@tf.informatik.uni-freiburg.de)
#pragma once

#include <absl/strings/str_cat.h>

#include <exception>
#include <optional>
#include <string>
#include <string_view>

// Contains Metadata (position of the faulty clause) about a ParseException that
// occurred.
struct ExceptionMetadata {
  // Query that the exception occurred in. This is currently needed because the
  // parser doesn't parse the whole query in one piece. It therefore can only
  // determine the position in the current partial query.
  std::string query_;
  // Start and stop index of the clause that cause the exception in query_. The
  // indices are inclusive. The character at position `stopIndex` is part of the
  // clause.
  size_t startIndex_;
  size_t stopIndex_;
  // Start of the clause expressed as line (1-based) and position in the line
  // (0-based) in query_.
  size_t line_;
  size_t charPositionInLine_;

  bool operator==(const ExceptionMetadata& rhs) const = default;

  // Returns the query with the faulty clause highlighted using ANSI Escape
  // Sequences. The faulty clause is made bold, underlined and red.
  [[nodiscard]] std::string coloredError() const;
};

class ParseException : public std::exception {
 public:
  ParseException(std::string_view cause,
                 std::optional<ExceptionMetadata> metadata = std::nullopt)
      : cause_{absl::StrCat("ParseException, cause: ", cause)},
        metadata_{std::move(metadata)} {};

  virtual const char* what() const throw() { return cause_.c_str(); }
  [[nodiscard]] const std::optional<ExceptionMetadata>& metadata() const {
    return metadata_;
  }

  [[nodiscard]] const std::string& errorMessageWithoutPositionalInfo() const {
    return cause_;
  }

 private:
  std::string cause_;
  std::optional<ExceptionMetadata> metadata_;
};
