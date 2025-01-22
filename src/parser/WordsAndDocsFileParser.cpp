// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Felix Meisen (fesemeisen@outlook.de)

#include "parser/WordsAndDocsFileParser.h"

#include <cassert>

#include "util/Exception.h"
#include "util/StringUtils.h"

// _____________________________________________________________________________
WordsAndDocsFileParser::WordsAndDocsFileParser(
    const string& wordsOrDocsFile, const LocaleManager& localeManager)
    : in_(wordsOrDocsFile), localeManager_(localeManager) {}

// _____________________________________________________________________________
ad_utility::InputRangeFromGet<WordsFileLine>::Storage WordsFileParser::get() {
  WordsFileLine line;
  string l;
  if (!std::getline(getInputStream(), l)) {
    return std::nullopt;
  }
  std::string_view lineView(l);
  size_t i = lineView.find('\t');
  assert(i != string::npos);
  size_t j = i + 2;
  assert(j + 3 < lineView.size());
  size_t k = lineView.find('\t', j + 2);
  assert(k != string::npos);
  line.isEntity_ = (lineView[i + 1] == '1');
  line.word_ =
      (line.isEntity_
           ? lineView.substr(0, i)
           : getLocaleManager().getLowercaseUtf8(lineView.substr(0, i)));
  line.contextId_ =
      TextRecordIndex::make(atol(lineView.substr(j + 1, k - j - 1).data()));
  line.score_ = static_cast<Score>(atol(lineView.substr(k + 1).data()));
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
  string l;
  if (!std::getline(getInputStream(), l)) {
    return std::nullopt;
  }
  DocsFileLine line;
  size_t i = l.find('\t');
  assert(i != string::npos);
  line.docId_ = DocumentIndex::make(atol(l.substr(0, i).c_str()));
  line.docContent_ = l.substr(i + 1);
  return line;
}
