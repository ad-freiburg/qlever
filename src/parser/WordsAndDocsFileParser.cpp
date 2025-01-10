// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Felix Meisen (fesemeisen@outlook.de)

#include "parser/WordsAndDocsFileParser.h"

#include <cassert>

#include "util/Exception.h"
#include "util/StringUtils.h"

// _____________________________________________________________________________
WordsAndDocsFileParser::WordsAndDocsFileParser(const string& wordsOrDocsFile,
                                               LocaleManager localeManager)
    : in_(wordsOrDocsFile), localeManager_(std::move(localeManager)) {}

// _____________________________________________________________________________
ad_utility::InputRangeFromGet<WordsFileLine>::Storage WordsFileParser::get() {
  WordsFileLine line;
  string l;
  if (!std::getline(in_, l)) {
    return std::nullopt;
  };
  size_t i = l.find('\t');
  assert(i != string::npos);
  size_t j = i + 2;
  assert(j + 3 < l.size());
  size_t k = l.find('\t', j + 2);
  assert(k != string::npos);
  line.isEntity_ = (l[i + 1] == '1');
  line.word_ =
      (line.isEntity_ ? l.substr(0, i)
                      : localeManager_.getLowercaseUtf8(l.substr(0, i)));
  line.contextId_ =
      TextRecordIndex::make(atol(l.substr(j + 1, k - j - 1).c_str()));
  line.score_ = static_cast<Score>(atol(l.substr(k + 1).c_str()));
#ifndef NDEBUG
  if (lastCId_ > line.contextId_) {
    AD_THROW("ContextFile has to be sorted by context Id.");
  }
  lastCId_ = line.contextId_;
#endif
  return line;
}

// _____________________________________________________________________________
ad_utility::InputRangeFromGet<DocsFileLine>::Storage DocsFileParser::get() {
  DocsFileLine line;
  string l;
  if (!std::getline(in_, l)) {
    return std::nullopt;
  };
  size_t i = l.find('\t');
  assert(i != string::npos);
  line.docId_ = DocumentIndex::make(atol(l.substr(0, i).c_str()));
  line.docContent_ = l.substr(i + 1);
  return line;
}
