// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include <iostream>

#include "./global/Constants.h"
#include "./index/PrefixHeuristic.h"

// Reads a vocabulary of words from file, calculates the prefixes with which the
// greedy heuristic would compress this vocabulary and prints them on the
// screen (mostly for testing and evaluation purposes of the greedy algorithm)
//
// It is assumed, that there are 127 prefixes which are encoded by 1 byte each.
// Also prints some statistics about the compression (e.g. compression ratio)
//
// The vocabulary in the input file at argv[1] must be one word per line and
// alphabetically sorted
// _______________________________________________________________
int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: ./PrefixHeuristicEvaluatorMain <filename>\n";
    std::cerr << "Reads a vocabulary of words from file, calculates the "
                 "prefixes with which the greedy heuristic would compress this "
                 "vocabulary and prints them on the"
                 " screen (mostly for testing and evaluation purposes of the "
                 "greedy algorithm).\n"

                 " It is assumed, that there are 127 prefixes which are "
                 "encoded by 1 byte each."
                 " Also prints some statistics about the compression (e.g. "
                 "compression ratio)\n"

                 " The vocabulary in the input file at argv[1] must be one "
                 "word per line and alphabetically sorted";
    exit(1);
  }

  for (const auto& p :
       calculatePrefixes(argv[1], 127, NUM_COMPRESSION_PREFIXES, true)) {
    std::cout << p << '\n';
  }
}
