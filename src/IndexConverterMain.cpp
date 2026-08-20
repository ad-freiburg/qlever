// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Convert an index in the index format `{PR = 1572, Date = 2024-10-22}` to the
// index format `{PR = 3159, Date = 2026-08-14}`, so that it does not have to be
// rebuilt from its input files. See `index/IndexFormatConverter.h` for the
// details of the conversion, in particular for the difference between the two
// formats.

#include <absl/strings/str_cat.h>

#include <boost/program_options.hpp>
#include <iostream>
#include <string>

#include "global/RuntimeParameters.h"
#include "index/IndexFormatConverter.h"
#include "util/Forward.h"
#include "util/Log.h"
#include "util/ProgramOptionsHelpers.h"

namespace po = boost::program_options;

// _____________________________________________________________________________
int main(int argc, char** argv) {
  std::string oldIndexBasename;
  std::string newIndexBasename;

  ad_utility::ParameterToProgramOptionFactory optionFactory{
      &globalRuntimeParameters};

  // The overview message of the help states which index formats this converter
  // converts between, and what the difference between them is.
  po::options_description boostOptions{
      absl::StrCat("qlever-convert-index\n\n",
                   qlever::indexFormatConverter::conversionDescription(),
                   "\n\nOptions for qlever-convert-index")};
  auto add = [&boostOptions](auto&&... args) {
    boostOptions.add_options()(AD_FWD(args)...);
  };
  add("help,h", "Produce this help message.");
  add("index-basename,i", po::value(&oldIndexBasename)->required(),
      "The basename of the index that is converted, including the name of the "
      "index itself, for example `index-dir/wikidata` (required). This index "
      "is "
      "not modified.");
  add("output-index-basename,o", po::value(&newIndexBasename)->required(),
      "The basename of the converted index (required). None of the files with "
      "this basename may exist yet.");
  add("log-level",
      optionFactory.getProgramOption<&RuntimeParameters::logLevel_>(),
      "Runtime log level: FATAL, ERROR, WARN, INFO, DEBUG, TIMING, or TRACE. "
      "Default is INFO.");

  po::variables_map optionsMap;
  try {
    po::store(po::parse_command_line(argc, argv, boostOptions), optionsMap);
    if (optionsMap.count("help")) {
      std::cout << boostOptions << std::endl;
      return EXIT_SUCCESS;
    }
    po::notify(optionsMap);
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line argument: " << e.what() << std::endl;
    std::cerr << boostOptions << std::endl;
    return EXIT_FAILURE;
  }

  try {
    qlever::indexFormatConverter::convertIndexToCurrentFormat(oldIndexBasename,
                                                              newIndexBasename);
  } catch (const std::exception& e) {
    AD_LOG_ERROR << "Converting the index failed with the following exception: "
                 << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
