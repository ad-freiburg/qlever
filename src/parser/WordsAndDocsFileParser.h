// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <unicode/locid.h>

#include <fstream>
#include <string>

#include "global/Id.h"
#include "index/StringSortComparator.h"
#include "util/Iterators.h"

using std::string;

struct WordsFileLine {
  string word_;
  bool isEntity_;
  TextRecordIndex contextId_;
  Score score_;
  bool isLiteralEntity_ = false;
};

struct DocsFileLine {
  string docContent_;
  DocumentIndex docId_;
};

class WordsAndDocsFileParser {
 public:
  explicit WordsAndDocsFileParser(const string& wordsOrDocsFile,
                                  LocaleManager localeManager);
  ~WordsAndDocsFileParser();
  explicit WordsAndDocsFileParser(const WordsAndDocsFileParser& other) = delete;
  WordsAndDocsFileParser& operator=(const WordsAndDocsFileParser& other) =
      delete;

 protected:
  std::ifstream in_;
  LocaleManager localeManager_;
};

class WordsFileParser : public WordsAndDocsFileParser,
                        public ad_utility::InputRangeFromGet<WordsFileLine> {
 public:
  using WordsAndDocsFileParser::WordsAndDocsFileParser;
  Storage get() override;

 private:
#ifndef NDEBUG
  // Only used for sanity checks in debug builds
  TextRecordIndex lastCId_ = TextRecordIndex::make(0);
#endif
};

class DocsFileParser : public WordsAndDocsFileParser,
                       public ad_utility::InputRangeFromGet<DocsFileLine> {
 public:
  using WordsAndDocsFileParser::WordsAndDocsFileParser;
  Storage get() override;
};
