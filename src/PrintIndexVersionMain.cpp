// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <kalmbach@cs.uni-freiburg.de>

// Simply print the index version to stdout. This is used in continuous
// integration tests to check whether the index version has changed.
#include <iostream>

#include "index/IndexFormatVersion.h"

int main() {
  std::cout << nlohmann::json{qlever::indexFormatVersion}.dump(4) << std::endl;
}
