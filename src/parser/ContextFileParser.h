// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <unicode/locid.h>
#include <fstream>
#include <string>

#include "../global/Id.h"
#include "../index/StringSortComparator.h"

using std::string;

class ContextFileParser {
 public:
  struct Line {
    string _word;
    bool _isEntity;
    Id _contextId;
    Score _score;
  };

  explicit ContextFileParser(const string& contextFile,
                             LocaleManager localeManager);
  ~ContextFileParser();
  // Don't allow copy & assignment
  explicit ContextFileParser(const ContextFileParser& other) = delete;
  ContextFileParser& operator=(const ContextFileParser& other) = delete;

  // Get the next line from the file.
  // Returns true if something was stored.
  bool getLine(Line&);

 private:
  std::ifstream _in;
  LocaleManager _localeManager;
};
