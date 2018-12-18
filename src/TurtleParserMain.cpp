// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#include <array>
#include <fstream>
#include <iostream>
#include <string>
#include "./parser/TurtleParser.h"

void writeNT(std::ostream& out, const std::string& filename) {
  TurtleParser p(filename);
  std::array<std::string, 3> triple;
  while (p.getLine(&triple)) {
    out << triple[0] << " " << triple[1] << " " << triple[2] << " .\n";
  }
}

int main(int argc, char** argv) {
  if (argc < 2 || argc > 3) {
    std::cerr << "Usage: ./TurtleParserMain <turtleInput> [<nTripleOutput>]"
              << std::endl;
    exit(1);
  }

  if (argc == 3) {
    std::ofstream of(argv[2]);
    if (!of) {
      std::cerr << "Error opening '" << argv[2] << "'" << std::endl;
      exit(1);
    }
    writeNT(of, argv[1]);
    of.close();
  } else {
    writeNT(std::cout, argv[1]);
  }
}
