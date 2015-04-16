// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <cassert>
#include "../util/StringUtils.h"
#include "./ContextFileParser.h"


// _____________________________________________________________________________
ContextFileParser::ContextFileParser(const string& contextFile) :
    _in(contextFile) {
}

// _____________________________________________________________________________
ContextFileParser::~ContextFileParser() {
  _in.close();
}

// _____________________________________________________________________________
bool ContextFileParser::getLine(ContextFileParser::Line& line) {
  string l;
  if (std::getline(_in, l)) {
    size_t i = l.find('\t');
    assert(i != string::npos);
    size_t j = i + 2;
    assert(j + 3 < l.size() && l[j + 2] == '\t');
    size_t k = l.find('\t', j + 2);
    assert(k != string::npos);
    line._isEntity = (l[i + 1] == '1');
    line._word = (line._isEntity ?
        l.substr(0, i) : ad_utility::getLowercaseUtf8(l.substr(0, i)));
    line._contextId = static_cast<Id>(atol(l.substr(j + 1, k - j - 1).c_str()));
    line._score = static_cast<Score>(atol(l.substr(k + 1).c_str()));
    return true;
  }
  return false;
}
