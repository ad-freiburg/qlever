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
SELECT (COUNT(*) AS ?count) WHERE {
 ?s ?p ?o
}
)";

int main(int argc, char** argv) {
  // Parse command line arguments.
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <input file> <index basename>"
              << std::endl;
    return 1;
  }
  std::string inputFile = argv[1];
  std::string indexBasename = argv[2];

  // Build index for the given input file and write index files to disk.
  std::cout << "\x1b[1mBuilding index for input file \"" << inputFile << "\""
            << " with basename \"" << indexBasename << "\x1b[0m" << std::endl;
  qlever::IndexBuilderConfig config;
  config.inputFiles_.push_back({inputFile, qlever::Filetype::Turtle});
  config.baseName_ = indexBasename;
  try {
    qlever::Qlever::buildIndex(config);
  } catch (const std::exception& e) {
    std::cerr << "Building the index failed: " << e.what() << std::endl;
    return 1;
  }
  std::cout << std::endl;

  // Load index.
  std::cout << "\x1b[1mLoading index with basename \"" << indexBasename
            << "\"\x1b[0m" << std::endl;
  qlever::EngineConfig engineConfig{config};
  qlever::Qlever qlever{engineConfig};
  std::cout << std::endl;

  // Execute query.
  std::cout << "\x1b[1mExecuting test query"
            << "\x1b[0m" << std::endl;
  std::string queryResult;
  ad_utility::Timer timer{ad_utility::Timer::Started};
  try {
    queryResult = qlever.query(query);
  } catch (const std::exception& e) {
    std::cerr << "Executing the query failed: " << e.what() << std::endl;
    return 1;
  }
  std::cout.imbue(std::locale(""));
  std::cout << "Query executed in " << timer.msecs().count() << "ms"
            << std::endl;
  std::cout << std::endl;

  // Show result.
  std::cout << "\x1b[1mResult string is:\x1b[0m" << std::endl;
  std::cout << queryResult << std::endl;
  std::cout << std::endl;
}
