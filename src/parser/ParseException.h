// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <exception>
#include <string>

struct ExceptionMetadata {
  std::string query_;
  size_t startIndex = 0;
  size_t stopIndex = 0;
};

class ParseException : public std::exception {
 public:
  ParseException(std::string _cause)
      : std::exception(),
        _cause(std::string("ParseException, cause: ") + _cause),
        metadata_({}){};

  ParseException(std::string _cause, ExceptionMetadata metadata_)
      : std::exception(),
        _cause(std::string("ParseException, cause: ") + _cause),
        metadata_(std::move(metadata_)){};

  virtual const char* what() const throw() { return _cause.c_str(); }

 private:
  std::string _cause;
  ExceptionMetadata metadata_;
};
