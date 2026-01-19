// Copyright 2014 - 2025 University of Freiburg
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2014 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/functional/bind_front.h>

#include <boost/program_options.hpp>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include "CompilationInfo.h"
#include "global/Constants.h"
#include "index/ConstantsIndexBuilding.h"
#include "libqlever/Qlever.h"
#include "util/ProgramOptionsHelpers.h"
#include "util/ReadableNumberFacet.h"

using std::string;

namespace po = boost::program_options;

// Check that `values` has exactly one or `numFiles` many entries. An empty
// vector will also be accepted. If this condition is violated, throw an
// exception. This is used to validate the parameters for file types and default
// graphs.
static constexpr auto checkNumParameterValues =
    [](size_t numFiles, const auto& values, std::string_view parameterName) {
      if (values.empty()) {
        return;
      }
      if (values.size() == 1 || values.size() == numFiles) {
        return;
      }
      auto error = absl::StrCat(
          "The parameter \"", parameterName,
          "\" must be specified either exactly once (in which case it is "
          "used for all input files) or exactly as many times as there are "
          "input files, in which case each input file has its own value.",
          " The parameter can also be omitted entirely, in which "
          " case a default value is used for all input files.");
      throw std::runtime_error{error};
    };

// Convert the `filetype` string, which must be "ttl", "nt", or "nq" to the
// corresponding `qlever::Filetype` value. If no filetyp is given, try to deduce
// the type from the filename.
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
  // The following line is necessary because Clang and GCC currently can't
  // deduce that the above `else` case always throws and there is currently no
  // way to mark the `throwNotDeducable` lambda as `[[noreturn]]`.
  AD_FAIL();
}

// Get the parameter value at the given index. If the vector is empty, return
// the given `defaultValue`. If the vector has exactly one element, return that
// element, no matter what the index is.
template <typename T, typename TsList>
T getParameterValue(size_t idx, const TsList& values, const T& defaultValue) {
  if (values.empty()) {
    return defaultValue;
  }
  if (values.size() == 1) {
    return values.at(0);
  }
  return values.at(idx);
}
// Convert the parameters for the filenames, file types, and default graphs
// into a `vector<InputFileSpecification>`.
auto getFileSpecifications = [](const auto& filetype, auto& inputFile,
                                const auto& defaultGraphs,
                                const auto& parseParallel) {
  auto check = absl::bind_front(checkNumParameterValues, inputFile.size());
  check(filetype, "--file-format, -F");
  check(defaultGraphs, "--default-graph, -g");
  check(parseParallel, "--parse-parallel, p");

  std::vector<qlever::InputFileSpecification> fileSpecs;
  for (size_t i = 0; i < inputFile.size(); ++i) {
    auto type = getParameterValue<std::optional<std::string_view>>(
        i, filetype, std::nullopt);

    auto defaultGraph = getParameterValue<std::optional<string>>(
        i, defaultGraphs, std::nullopt);
    if (defaultGraph == "-") {
      defaultGraph = std::nullopt;
    }

    bool parseInParallel = getParameterValue(i, parseParallel, false);
    bool parseInParallelSetExplicitly = i < parseParallel.size();
    auto& filename = inputFile.at(i);
    if (filename == "-") {
      filename = "/dev/stdin";
    }
    fileSpecs.emplace_back(filename, getFiletype(type, filename),
                           std::move(defaultGraph), parseInParallel,
                           parseInParallelSetExplicitly);
  }
  return fileSpecs;
};

// Main function.
int main(int argc, char** argv) {
  // Copy the git hash and datetime of compilation (which require relinking)
  // to make them accessible to other parts of the code
  qlever::version::copyVersionInfo();
  setlocale(LC_CTYPE, "");

  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);

  qlever::IndexBuilderConfig config;
  std::vector<string> filetype;
  std::vector<string> inputFile;
  std::vector<string> defaultGraphs;
  std::vector<bool> parseParallel;

  boost::program_options::options_description boostOptions(
      "Options for qlever-index");
  auto add = [&boostOptions](auto&&... args) {
    boostOptions.add_options()(AD_FWD(args)...);
  };
  add("help,h", "Produce this help message.");
  add("index-basename,i", po::value(&config.baseName_)->required(),
      "The basename of the output files (required).");
  add("kg-input-file,f", po::value(&inputFile),
      "The file with the knowledge graph data to be parsed from. If omitted, "
      "will read from stdin.");
  add("file-format,F", po::value(&filetype),
      "The format of the input file with the knowledge graph data. Must be one "
      "of [nt|ttl|nq]. Can be specified once (then all files use that format), "
      "or once per file, or not at all (in that case, the format is deduced "
      "from the filename suffix if possible).");
  add("default-graph,g", po::value(&defaultGraphs),
      "The graph IRI without angle brackets. Write `-` for the default graph. "
      "Can be omitted (then all files use the default graph), specified once "
      "(then all files use that graph), or once per file.");
  add("parse-parallel,p", po::value(&parseParallel),
      "Enable or disable the parallel parser for all files (if specified once) "
      "or once per input file. Parallel parsing works for all input files "
      "using the N-Triples or N-Quads format, as well as for well-behaved "
      "Turtle files, where all the prefix declarations come in one block at "
      "the beginning and there are no multiline literals");
  add("kg-index-name,K", po::value(&config.kbIndexName_),
      "The name of the knowledge graph index (default: basename of "
      "`kg-input-file`).");

  // Options for the text index.
  add("text-docs-input-file,d", po::value(&config.docsfile_),
      "The full text of the text records from which to build the text index.");
  add("text-words-input-file,w", po::value(&config.wordsfile_),
      "Words of the text records from which to build the text index.");
  add("text-words-from-literals,W",
      po::bool_switch(&config.addWordsFromLiterals_),
      "Consider all literals from the internal vocabulary as text records. Can "
      "be combined with `text-docs-input-file` and `text-words-input-file`");
  add("text-index-name,T", po::value(&config.textIndexName_),
      "The name of the text index (default: basename of "
      "text-words-input-file).");
  add("add-text-index,A", po::bool_switch(&config.onlyAddTextIndex_),
      "Only build the text index. Assumes that a knowledge graph index with "
      "the same `index-basename` already exists.");
  add("bm25-b", po::value(&config.bScoringParam_),
      "Sets the b param in the BM25 scoring metric for the fulltext index."
      " This has to be between (including) 0 and 1.");
  add("bm25-k", po::value(&config.kScoringParam_),
      "Sets the k param in the BM25 scoring metric for the fulltext index."
      "This has to be greater than or equal to 0.");
  add("set-scoring-metric,S", po::value(&config.textScoringMetric_),
      R"(Sets the scoring metric used. Options are "explicit" for explicit )"
      "scores that are read from the wordsfile, "
      R"("tf-idf" for tf idf )"
      R"(and "bm25" for bm25. The default is "explicit".)");

  // Options for the knowledge graph index.
  add("settings-file,s", po::value(&config.settingsFile_),
      "A JSON file, where various settings can be specified (see the QLever "
      "documentation).");
  add("no-patterns", po::bool_switch(&config.noPatterns_),
      "Disable the precomputation for `ql:has-predicate`.");
  add("only-pso-and-pos-permutations,o",
      po::bool_switch(&config.onlyPsoAndPos_),
      "Only build the PSO and POS permutations. This is faster, but then "
      "queries with predicate variables are not supported");
  auto msg = absl::StrCat(
      "The vocabulary implementation for strings in qlever, can be any of ",
      ad_utility::VocabularyType::getListOfSupportedValues());
  add("vocabulary-type", po::value(&config.vocabType_), msg.c_str());

  add("encode-as-id",
      po::value(&config.prefixesForIdEncodedIris_)->composing()->multitoken(),
      "Space-separated list of IRI prefixes (without angle brackets). "
      "IRIs that start with one of these prefixes, followed by a sequence of "
      "digits, do not require a vocabulary entry, but are directly encoded "
      "in the ID. NOTE: When using ORDER BY, the order among encoded IRIs and "
      "among non-encoded IRIs is correct, but the order between encoded "
      "and non-encoded IRIs is not");

  // Options for the index building process.
  add("stxxl-memory,m", po::value(&config.memoryLimit_),
      "The amount of memory in to use for sorting during the index build. "
      "Decrease if the index builder runs out of memory.");
  add("parser-buffer-size,b", po::value(&config.parserBufferSize_),
      "The size of the buffer used for parsing the input files. This must be "
      "large enough to hold a single input triple. Default: 10 MB.");
  add("keep-temporary-files,k", po::bool_switch(&config.keepTemporaryFiles_),
      "Do not delete temporary files from index creation for debugging.");

  // Process command line arguments.
  po::variables_map optionsMap;

  try {
    po::store(po::parse_command_line(argc, argv, boostOptions), optionsMap);
    if (optionsMap.count("help")) {
      std::cout << boostOptions << '\n';
      return EXIT_SUCCESS;
    }
    po::notify(optionsMap);
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line argument: " << e.what() << '\n';
    std::cerr << boostOptions << '\n';
    return EXIT_FAILURE;
  }

  AD_LOG_INFO << EMPH_ON << "QLever IndexBuilder, compiled on "
              << qlever::version::DatetimeOfCompilation << " using git hash "
              << qlever::version::GitShortHash << EMPH_OFF << std::endl;

  try {
    config.inputFiles_ = getFileSpecifications(filetype, inputFile,
                                               defaultGraphs, parseParallel);
    config.validate();
    qlever::Qlever::buildIndex(config);

  } catch (std::exception& e) {
    AD_LOG_ERROR << "Creating the index for QLever failed with the following "
                    "exception: "
                 << e.what() << std::endl;
    return 2;
  }
  return 0;
}
