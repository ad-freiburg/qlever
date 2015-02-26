// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>

using std::string;

class Index {
public:
  Index();
  void createFromTsvFile(const string& tsvFile, const string& onDiskBase);
private:
  string _onDiskBase;
};