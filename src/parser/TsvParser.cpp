// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./TsvParser.h"

#include <cassert>
#include <iostream>

#include "../util/Log.h"

// _____________________________________________________________________________
TsvParser::TsvParser(const string& tsvFile) : _in(tsvFile) {}

// _____________________________________________________________________________
TsvParser::~TsvParser() { _in.close(); }

// _____________________________________________________________________________
bool TsvParser::getLine(array<string, 3>& res) {
  string line;
  if (std::getline(_in, line)) {
    size_t i = line.find('\t');
    assert(i != string::npos);
    size_t j = line.find('\t', i + 1);
    assert(j != string::npos);
    size_t k = line.find('\t', j + 1);
    assert(k != string::npos);
    res[0] = line.substr(0, i);
    res[1] = line.substr(i + 1, j - (i + 1));
    res[2] = line.substr(j + 1, k - (j + 1));
    return true;
  }
  return false;
}
