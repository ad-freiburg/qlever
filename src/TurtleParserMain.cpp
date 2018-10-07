// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#include <array>
#include <iostream>
#include <string>
#include "./parser/TurtleParser.h"

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: ./TurtleParserMain <turtleInput>";
    exit(1);
  }
  TurtleParser p(argv[1]);
  std::array<std::string, 3> triple;
  while (p.getLine(&triple)) {
    std::cout << triple[0] << " " << triple[1] << " " << triple[2] << '\n';
  }
}
