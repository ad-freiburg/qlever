// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "CompilationInfo.h"
#include "libqlever/Qlever.h"
#include "rdfTypes/Variable.h"
#include "util/Exception.h"
#include "util/ProgramOptionsHelpers.h"

namespace po = boost::program_options;

// Struct to hold a cached query configuration.
struct CachedQueryConfig {
  std::string name_;
  std::string query_;
  std::optional<Variable> geoIndexVar_;
};

// Struct to hold parsed command-line arguments.
struct CommandLineArgs {
  std::string baseName_;
  std::vector<std::string> inputFiles_;
  std::vector<std::string> filetypes_;
  std::string jsonQueries_;
  std::string outputFile_;
};

// Helper to parse the filetype from string.
qlever::Filetype getFiletype(std::optional<std::string_view> filetype,
                             std::string_view filename) {
  auto impl = [](std::string_view s) -> std::optional<qlever::Filetype> {
    if (s == "ttl" || s == "nt") {
      return qlever::Filetype::Turtle;
    } else if (s == "nq") {
      return qlever::Filetype::NQuad;
    } else {
      return std::nullopt;
    }
  };
  if (filetype.has_value()) {
    auto result = impl(filetype.value());
    if (result.has_value()) {
      return result.value();
    } else {
      throw std::runtime_error{
          absl::StrCat("The value of --file-format or -F must be one of "
                       "`ttl`, `nt`, or `nq`, but is `",
                       filetype.value(), "`")};
    }
  }

  auto posOfDot = filename.rfind('.');
  auto throwNotDeducable = [&filename]() {
    throw std::runtime_error{absl::StrCat(
        "Could not deduce the file format from the filename \"", filename,
        "\". Either use files with names that end on `.ttl`, `.nt`, or `.nq`, "
        "or explicitly set the format of the file via --file-format or -F")};
  };
  if (posOfDot == std::string::npos) {
    throwNotDeducable();
  }
  auto deducedType = impl(filename.substr(posOfDot + 1));
  if (deducedType.has_value()) {
    return deducedType.value();
  } else {
    throwNotDeducable();
  }
  AD_FAIL();
}

// Parse JSON queries into a vector of CachedQueryConfig.
std::vector<CachedQueryConfig> parseJsonQueries(const std::string& jsonString) {
  nlohmann::json queries;
  try {
    queries = nlohmann::json::parse(jsonString);
  } catch (const nlohmann::json::parse_error& e) {
    throw std::runtime_error{
        absl::StrCat("Failed to parse JSON queries: ", e.what())};
  }

  if (!queries.is_array()) {
    throw std::runtime_error{"JSON queries must be an array."};
  }

  std::vector<CachedQueryConfig> result;
  for (const auto& queryObj : queries) {
    if (!queryObj.is_object()) {
      throw std::runtime_error{"Each query entry must be an object."};
    }

    if (!queryObj.contains("name") || !queryObj["name"].is_string()) {
      throw std::runtime_error{
          "Each query entry must have a 'name' field (string)."};
    }
    if (!queryObj.contains("query") || !queryObj["query"].is_string()) {
      throw std::runtime_error{
          "Each query entry must have a 'query' field (string)."};
    }

    CachedQueryConfig config;
    config.name_ = queryObj["name"];
    config.query_ = queryObj["query"];

    // Check for optional geo-index-var.
    if (queryObj.contains("geo-index-var")) {
      if (!queryObj["geo-index-var"].is_string()) {
        throw std::runtime_error{
            "The 'geo-index-var' field must be a string if present."};
      }
      std::string geoIndexVarStr = queryObj["geo-index-var"];
      config.geoIndexVar_ = Variable{geoIndexVarStr};
    }

    result.push_back(std::move(config));
  }

  return result;
}

// Parse command-line arguments.
CommandLineArgs parseCommandLine(int argc, char** argv) {
  CommandLineArgs args;

  po::options_description options("Options for CreateBlobMain");
  auto add = [&options](auto&&... args) {
    options.add_options()(AD_FWD(args)...);
  };

  add("help,h", "Produce this help message.");
  add("index-basename,i", po::value(&args.baseName_)->required(),
      "The basename for temporary index files (required).");
  add("input-file,f", po::value(&args.inputFiles_)->required(),
      "RDF input file(s) to build index from (required).");
  add("file-format,F", po::value(&args.filetypes_),
      "The format of the input files [nt|ttl|nq]. Can be specified once for "
      "all files, or once per file, or not at all (deduced from filename).");
  add("json-queries,j", po::value(&args.jsonQueries_)->required(),
      "JSON string with cached queries (required). Format: [{\"name\": "
      "\"queryA\", \"query\":\"SELECT ?s WHERE {...}\", \"geo-index-var\": "
      "\"?s\"}, ...]");
  add("output,o", po::value(&args.outputFile_)->required(),
      "Output file for the serialized blob (required).");

  po::variables_map optionsMap;
  po::store(po::parse_command_line(argc, argv, options), optionsMap);

  if (optionsMap.count("help")) {
    std::cout << options << std::endl;
    std::exit(0);
  }

  po::notify(optionsMap);

  // Validate file-format arguments.
  if (!args.filetypes_.empty() && args.filetypes_.size() != 1 &&
      args.filetypes_.size() != args.inputFiles_.size()) {
    throw std::runtime_error{
        "The parameter --file-format/-F must be specified either exactly "
        "once (for all files) or exactly as many times as there are input "
        "files."};
  }

  return args;
}

int main(int argc, char** argv) {
  // Copy the git hash and datetime of compilation.
  qlever::version::copyVersionInfo();
  setlocale(LC_CTYPE, "");

  try {
    // Parse command-line arguments.
    CommandLineArgs args = parseCommandLine(argc, argv);

    // Build index configuration.
    qlever::IndexBuilderConfig config;
    config.baseName_ = args.baseName_;
    config.vocabType_ = ad_utility::VocabularyType{
        ad_utility::VocabularyType::Enum::InMemoryCompressed};

    // Parse input file specifications.
    for (size_t i = 0; i < args.inputFiles_.size(); ++i) {
      std::optional<std::string_view> filetype = std::nullopt;
      if (!args.filetypes_.empty()) {
        filetype = args.filetypes_.size() == 1 ? args.filetypes_[0]
                                               : args.filetypes_[i];
      }
      config.inputFiles_.emplace_back(
          args.inputFiles_[i], getFiletype(filetype, args.inputFiles_[i]),
          std::nullopt);
    }

    // Build the index.
    std::cout << "Building index from " << args.inputFiles_.size()
              << " file(s)..." << std::endl;
    qlever::Qlever::buildIndex(config);
    std::cout << "Index built successfully." << std::endl;

    // Load the index.
    std::cout << "Loading index..." << std::endl;
    qlever::EngineConfig engineConfig{config};
    qlever::Qlever qlever{engineConfig};
    std::cout << "Index loaded successfully." << std::endl;

    // Parse JSON queries and populate named result cache.
    std::cout << "Parsing cached queries..." << std::endl;
    auto queryConfigs = parseJsonQueries(args.jsonQueries_);
    std::cout << "Populating named result cache with " << queryConfigs.size()
              << " queries..." << std::endl;

    for (const auto& queryConfig : queryConfigs) {
      QueryExecutionContext::PinResultWithName options;
      options.name_ = queryConfig.name_;
      options.geoIndexVar_ = queryConfig.geoIndexVar_;

      std::cout << "  Pinning query '" << queryConfig.name_ << "'..."
                << std::endl;
      qlever.queryAndPinResultWithName(options, queryConfig.query_);
    }

    // Serialize to blob.
    std::cout << "Serializing to blob..." << std::endl;
    std::vector<char> blob = qlever.serializeToBlob();
    std::cout << "Serialization complete. Blob size: " << blob.size()
              << " bytes." << std::endl;

    // Write blob to output file.
    std::cout << "Writing blob to file '" << args.outputFile_ << "'..."
              << std::endl;
    std::ofstream outStream(args.outputFile_, std::ios::binary);
    if (!outStream) {
      throw std::runtime_error{
          absl::StrCat("Failed to open output file: ", args.outputFile_)};
    }
    outStream.write(blob.data(), blob.size());
    if (!outStream) {
      throw std::runtime_error{
          absl::StrCat("Failed to write to output file: ", args.outputFile_)};
    }
    outStream.close();
    std::cout << "Blob written successfully." << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
