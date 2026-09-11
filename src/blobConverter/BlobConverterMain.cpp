// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Convert a blob of the vocabulary and the named cached queries from the legacy
// format of the `demo-v1-c++17_unimodel` branch of the `qlever-bmw` fork to the
// current format of `Qlever::serializeVocabAndNamedCacheToCompressedBlob`. See
// `blobConverter/BlobConverter.h` for the differences between the two formats.

#include <algorithm>
#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "blobConverter/BlobConverter.h"
#include "global/RuntimeParameters.h"
#include "libqlever/Qlever.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/ProgramOptionsHelpers.h"

namespace po = boost::program_options;

namespace {
// Read the complete contents of the binary file at `path`.
std::vector<char> readBinaryFile(const std::string& path) {
  auto stream = ad_utility::makeIfstream(path, std::ios::binary);
  return std::vector<char>{std::istreambuf_iterator<char>{stream},
                           std::istreambuf_iterator<char>{}};
}

// The prefix of the variables that the query planner introduces internally and
// that are not part of the projection when printing results.
constexpr std::string_view internalVariablePrefix =
    "?_QLever_internal_variable_qp_";

// Load the converted `blob` into a blob-only `Qlever` instance (no index files
// on disk) and print the first `numRows` rows of every named cached query in
// the `statistics`, as TSV.
void printResults(const std::vector<char>& blob,
                  const qlever::blobConverter::ConversionStatistics& statistics,
                  size_t numRows) {
  qlever::Qlever qlever{qlever::EngineConfig{}, /*skipLoading=*/true};
  qlever.deserializeVocabAndNamedCacheFromCompressedBlob(blob);
  for (const auto& entry : statistics.entries_) {
    std::cout << "--- " << entry.name_ << " (first " << numRows << " of "
              << entry.numRows_ << " rows) ---\n";
    std::string query = "SELECT";
    size_t numVariables = 0;
    for (const auto& variable : entry.variables_) {
      if (ql::starts_with(variable, internalVariablePrefix)) {
        continue;
      }
      query.append(" ").append(variable);
      ++numVariables;
    }
    if (numVariables == 0) {
      std::cout << "(this result only has internal variables)\n";
      continue;
    }
    query.append(" WHERE { SERVICE ql:cached-result-with-name-")
        .append(entry.name_)
        .append(" {} } LIMIT ")
        .append(std::to_string(numRows));
    // NOTE: The name is part of the query, so a name that is not a valid
    // SPARQL token leads to a parse error; report it and continue with the
    // other entries.
    try {
      std::cout << qlever.query(std::move(query), ad_utility::MediaType::tsv);
    } catch (const std::exception& e) {
      std::cout << "(querying this result failed: " << e.what() << ")\n";
    }
  }
}
}  // namespace

// _____________________________________________________________________________
int main(int argc, char** argv) {
  std::string inputFile;
  std::string outputFile;
  size_t numVocabularyWordsToPrint = 0;
  size_t numResultRowsToPrint = 0;
  ad_utility::ParameterToProgramOptionFactory optionFactory{
      &globalRuntimeParameters};

  po::options_description boostOptions{
      "Usage: qlever-blob-converter <legacy-blob> <output-blob>\n\n"
      "Convert a blob (vocabulary + named cached queries) that was written by "
      "the legacy `qlever-bmw` fork (magic header `QLVUBLOB`) to the current "
      "blob format (magic header `QLVRBLOB`), which can be loaded via "
      "`Qlever::deserializeVocabAndNamedCacheFromCompressedBlob`.\n\n"
      "Options for qlever-blob-converter"};
  auto add = [&boostOptions](auto&&... args) {
    boostOptions.add_options()(AD_FWD(args)...);
  };
  add("help,h", "Produce this help message.");
  add("input", po::value(&inputFile)->required(),
      "The legacy blob file (positional argument, required).");
  add("output", po::value(&outputFile)->required(),
      "The file to which the converted blob is written (positional argument, "
      "required).");
  add("print-vocabulary,p",
      po::value(&numVocabularyWordsToPrint)->default_value(0),
      "Print the first N words of the vocabulary.");
  add("print-results,r", po::value(&numResultRowsToPrint)->default_value(0),
      "Load the converted blob into a blob-only QLever instance and print the "
      "first N rows of every named cached query (as a check that the "
      "converted blob works).");
  add("log-level",
      optionFactory.getProgramOption<&RuntimeParameters::logLevel_>(),
      "Runtime log level: FATAL, ERROR, WARN, INFO, DEBUG, TIMING, or TRACE. "
      "Default is INFO.");

  po::positional_options_description positionalOptions;
  positionalOptions.add("input", 1);
  positionalOptions.add("output", 1);

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

  try {
    using namespace qlever::blobConverter;
    auto input = readBinaryFile(inputFile);
    auto legacyBlob = readLegacyBlobFromCompressed(input);
    auto result = convertLegacyBlob(legacyBlob);
    {
      auto stream = ad_utility::makeOfstream(outputFile, std::ios::binary);
      stream.write(result.blob_.data(),
                   static_cast<std::streamsize>(result.blob_.size()));
    }
    std::cout << "Converted " << inputFile << " (" << input.size()
              << " bytes) to " << outputFile << " (" << result.blob_.size()
              << " bytes)\n";
    std::cout << "Index metadata of the converted blob: "
              << result.metadata_.dump() << "\n";
    std::cout << result.statistics_.toString();
    size_t numWords =
        std::min(numVocabularyWordsToPrint, legacyBlob.numWords());
    for (size_t i = 0; i < numWords; ++i) {
      std::cout << i << "\t" << legacyBlob.word(i) << "\n";
    }
    if (numResultRowsToPrint > 0) {
      printResults(result.blob_, result.statistics_, numResultRowsToPrint);
    }
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
