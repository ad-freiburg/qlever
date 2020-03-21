// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <fstream>
#include <string>

using std::array;
using std::string;

class TsvParser {
 public:
  explicit TsvParser(const string& tsvFile);
  ~TsvParser();
  // Don't allow copy & assignment
  explicit TsvParser(const TsvParser& other) = delete;
  TsvParser& operator=(const TsvParser& other) = delete;
  TsvParser(TsvParser&& other) = default;
  TsvParser& operator=(TsvParser&& other) = default;

  // Get the next line from the file.
  // Returns true if something was stored.
  bool getLine(array<string, 3>&);

 private:
  std::ifstream _in;
};