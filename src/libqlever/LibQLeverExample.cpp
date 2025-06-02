//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/strings/str_replace.h>

#include <iostream>

#include "libqlever/Qlever.h"
#include "util/Exception.h"
#include "util/Timer.h"

static const std::string query = R"(
SELECT * WHERE {
 ?s ?p ?o
}
)";

std::vector<std::string> inputs{""};

int main(int argc, char** argv) {
  qlever::QleverConfig config;
  AD_CONTRACT_CHECK(argc >= 2);
  config.baseName = "exampleIndex";
  config.inputFiles.emplace_back(argv[1], qlever::Filetype::Turtle);
  qlever::Qlever::buildIndex(config);
  qlever::Qlever qlever{config};
  ad_utility::Timer t{ad_utility::Timer::Started};
  auto result = qlever.query(std::move(query));
  std::cout << "retrieved a query result of size " << result.size() << " in "
            << t.msecs().count() << "ms\n";
}
