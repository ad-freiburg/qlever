// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <exception>

class ParseException : public std::exception {

public:

  ParseException(string _cause) : std::exception(),
    _cause(string("ParseException, cause: ") + _cause) {
  }

  virtual const char* what() const throw() {
    return _cause.c_str();
  }

private:
  string _cause;
};

