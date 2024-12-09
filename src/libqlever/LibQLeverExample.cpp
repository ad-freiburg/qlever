//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <iostream>

#include "libqlever/Qlever.h"

int main() {
  qlever::QleverConfig config;
  config.baseName = "exampleIndex";
  config.inputFiles.emplace_back("/dev/stdin", qlever::Filetype::Turtle);
  qlever::Qlever::buildIndex(config);
  qlever::Qlever qlever{config};
  std::cout << qlever.query("SELECT * {?s ?p ?o}") << std::endl;
}
