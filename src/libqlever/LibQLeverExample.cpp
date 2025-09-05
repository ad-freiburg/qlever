// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <iostream>

#include "libqlever/Qlever.h"
#include "util/Exception.h"
#include "util/Timer.h"

static const std::string query = R"(
SELECT * WHERE {
 ?s ?p ?o
}
)";

int main(int argc, char** argv) {
  try {
    qlever::IndexBuilderConfig config;
    AD_CONTRACT_CHECK(argc >= 2);
    config.baseName_ = "exampleIndex";
    config.inputFiles_.emplace_back(argv[1], qlever::Filetype::Turtle);
    qlever::Qlever::buildIndex(config);
    qlever::EngineConfig engineConfig{config};
    qlever::Qlever qlever{engineConfig};
    ad_utility::Timer t{ad_utility::Timer::Started};
    auto result = qlever.query(query);
    std::cout << "Retrieved a query result of size " << result.size() << " in "
              << t.msecs().count() << "ms\n";
  } catch (const std::exception& e) {
    std::cerr << "An error occurred: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
