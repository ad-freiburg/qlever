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

  // Return the query with the faulty clause highlighted using ANSI Escape
  // Sequences. The faulty clause is made bold, underlined and red.
  std::string coloredError() const;

  // Return only the faulty clause.
  std::string_view offendingClause() const;
};

class ParseException : public std::exception {
 public:
  explicit ParseException(
      std::string_view cause,
      std::optional<ExceptionMetadata> metadata = std::nullopt,
      std::string_view prefix = "");

  const char* what() const noexcept override {
    return causeWithMetadata_.c_str();
  }
  [[nodiscard]] const std::optional<ExceptionMetadata>& metadata() const {
    return metadata_;
  }

  [[nodiscard]] const std::string& errorMessageWithoutPositionalInfo() const {
    return cause_;
  }

  const std::string& errorMessageWithoutPrefix() const { return causeRaw_; }

 private:
  std::string causeRaw_;
  std::string cause_;
  std::optional<ExceptionMetadata> metadata_;
  std::string causeWithMetadata_;
};

class InvalidSparqlQueryException : public ParseException {
 public:
  explicit InvalidSparqlQueryException(
      std::string_view cause,
      std::optional<ExceptionMetadata> metadata = std::nullopt)
      : ParseException{cause, std::move(metadata), "Invalid SPARQL query:"} {}
};

class NotSupportedException : public ParseException {
 public:
  explicit NotSupportedException(
      std::string_view cause,
      std::optional<ExceptionMetadata> metadata = std::nullopt)
      : ParseException{cause, std::move(metadata), "Not supported:"} {}
};
