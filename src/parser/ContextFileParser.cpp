// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./ContextFileParser.h"

#include <cassert>

#include "../util/Exception.h"
#include "../util/StringUtils.h"

// _____________________________________________________________________________
ContextFileParser::ContextFileParser(const string& contextFile,
                                     LocaleManager localeManager)
    : _in(contextFile), _localeManager(std::move(localeManager)) {}

// _____________________________________________________________________________
ContextFileParser::~ContextFileParser() { _in.close(); }

// _____________________________________________________________________________
bool ContextFileParser::getLine(ContextFileParser::Line& line) {
  string l;
  if (std::getline(_in, l)) {
    size_t i = l.find('\t');
    assert(i != string::npos);
    size_t j = i + 2;
    assert(j + 3 < l.size());
    size_t k = l.find('\t', j + 2);
    assert(k != string::npos);
    line._isEntity = (l[i + 1] == '1');
    line._word =
        (line._isEntity ? l.substr(0, i)
                        : _localeManager.getLowercaseUtf8(l.substr(0, i)));
    line._contextId =
        TextRecordIndex::make(atol(l.substr(j + 1, k - j - 1).c_str()));
    line._score = static_cast<Score>(atol(l.substr(k + 1).c_str()));
#ifndef NDEBUG
    if (_lastCId > line._contextId) {
      AD_THROW(ad_semsearch::Exception::BAD_INPUT,
               "ContextFile has to be sorted by context Id.");
    }
    _lastCId = line._contextId;
#endif
    return true;
  }
  return false;
}
