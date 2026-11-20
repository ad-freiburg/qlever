// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Run a single SPARQL query on an existing index without starting a server,
// and write the result to standard output as `sparql-results+json`.

#include <exception>
#include <iostream>
#include <string>

#include "libqlever/Qlever.h"
#include "util/Log.h"

int main(int argc, char** argv) {
  // Parse the command line arguments.
  //
  // NOTE: The basename is used as a prefix of the names of the index files,
  // so a basename without a directory refers to the current directory.
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <index basename> <query>\n\n"
              << "Run the SPARQL query on the index with the given basename "
                 "(`/path/to/basename`, or just `basename` for an index in "
                 "the current directory) and write the result to standard "
                 "output as `sparql-results+json`."
              << std::endl;
    return 1;
  }
  qlever::EngineConfig config;
  config.baseName_ = argv[1];
  std::string query = argv[2];

  // Write the log to standard error (so that standard output contains nothing
  // but the query result).
  ad_utility::setGlobalLoggingStream(&std::cerr);

  // Load the index, run the query, and write the result.
  try {
    qlever::Qlever qlever{config};
    std::cout << qlever.query(std::move(query)) << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
