// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <fstream>
#include <string>

using std::array;
using std::string;

class NTriplesParser {
 public:
  explicit NTriplesParser(const string& nTriplesFile);
  ~NTriplesParser();
  // Don't allow copy & assignment
  explicit NTriplesParser(const NTriplesParser& other) = delete;
  NTriplesParser& operator=(const NTriplesParser& other) = delete;

  // Get the next line from the file.
  // Returns true if something was stored.
  bool getLine(array<string, 3>&);

 private:
  std::ifstream _in;
};