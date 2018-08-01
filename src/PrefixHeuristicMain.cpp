// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "./index/PrefixHeuristic.h"
#include <iostream>

// _______________________________________________________________
int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: ./PrefixHeuristicMain <filename>\n";
    exit(1);
  }

  for (const auto& p : calculatePrefixes(argv[1], 127, 1)) {
    std::cout << p << '\n';
  }
}

