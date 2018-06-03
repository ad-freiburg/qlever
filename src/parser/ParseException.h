// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <exception>
#include <string>

class ParseException : public std::exception {
 public:
  ParseException(std::string _cause)
      : std::exception(),
        _cause(std::string("ParseException, cause: ") + _cause) {}

  virtual const char* what() const throw() { return _cause.c_str(); }

 private:
  std::string _cause;
};
