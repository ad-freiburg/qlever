// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Upgrade an index in the index format `{PR = 1572, Date = 2024-10-22}` to the
// index format `{PR = 3159, Date = 2026-09-01}` in place, so that it does not
// have to be rebuilt from its input files. See `index/IndexFormatConverter.h`
// for the details, in particular for the difference between the two formats
// and for how the upgraded index is staged in a subdirectory and only swapped
// into place after a check.

#include <absl/strings/str_cat.h>

#include <boost/program_options.hpp>
#include <iostream>
#include <string>

#include "CompilationInfo.h"
#include "global/RuntimeParameters.h"
#include "index/IndexFormatConverter.h"
#include "util/Forward.h"
#include "util/Log.h"
#include "util/ProgramOptionsHelpers.h"

namespace po = boost::program_options;

// _____________________________________________________________________________
int main(int argc, char** argv) {
  std::string indexBasename;

  ad_utility::ParameterToProgramOptionFactory optionFactory{
      &globalRuntimeParameters};

  // The overview message of the help states which index formats this upgrader
  // converts between, what the difference between them is, and how the upgrade
  // proceeds (staged in a subdirectory, swapped into place after a check).
  po::options_description boostOptions{
      absl::StrCat("Usage: qlever-upgrade-index <index-basename>\n\n",
                   qlever::indexFormatConverter::conversionDescription(),
                   "\n\nOptions for qlever-upgrade-index")};
  auto add = [&boostOptions](auto&&... args) {
    boostOptions.add_options()(AD_FWD(args)...);
  };
  add("help,h", "Produce this help message.");
  add("index-basename", po::value(&indexBasename)->required(),
      "The basename of the index that is upgraded in place, including the "
      "name of the index itself, for example `index-dir/wikidata` (positional "
      "argument, required).");
  add("log-level",
      optionFactory.getProgramOption<&RuntimeParameters::logLevel_>(),
      "Runtime log level: FATAL, ERROR, WARN, INFO, DEBUG, TIMING, or TRACE. "
      "Default is INFO.");

  // The base name is given as a positional argument, not via an option.
  po::positional_options_description positionalOptions;
  positionalOptions.add("index-basename", 1);

  po::variables_map optionsMap;
  try {
    po::store(po::command_line_parser(argc, argv)
                  .options(boostOptions)
                  .positional(positionalOptions)
                  .run(),
              optionsMap);
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

  AD_LOG_INFO << EMPH_ON << "QLever index upgrader "
              << qlever::version::ProjectVersion << ", compiled on "
              << qlever::version::DatetimeOfCompilation << " using git hash "
              << qlever::version::GitShortHash << EMPH_OFF << std::endl;

  try {
    qlever::indexFormatConverter::upgradeIndexInPlace(indexBasename);
  } catch (const std::exception& e) {
    AD_LOG_ERROR << "Upgrading the index failed with the following exception: "
                 << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
