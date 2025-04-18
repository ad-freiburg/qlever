// Copyright 2014 - 2025 University of Freiburg
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2014 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <boost/program_options.hpp>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include "CompilationInfo.h"
#include "global/Constants.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"
#include "util/File.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ProgramOptionsHelpers.h"
#include "util/ReadableNumberFacet.h"

using std::string;

namespace po = boost::program_options;

// Check that `values` has exactly one or `numFiles` many entries. An empty
// vector will also be accepted. If this condition is violated, throw an
// exception. This is used to validate the parameters for file types and default
// graphs.
template <typename T>
static void checkNumParameterValues(const T& values, size_t numFiles,
                                    std::string_view parameterName) {
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
}

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

  string baseName;
  string wordsfile;
  string docsfile;
  string textIndexName;
  string kbIndexName;
  string settingsFile;
  string scoringMetric = "explicit";
  std::vector<string> filetype;
  std::vector<string> inputFile;
  std::vector<string> defaultGraphs;
  std::vector<bool> parseParallel;
  bool noPatterns = false;
  bool onlyAddTextIndex = false;
  bool keepTemporaryFiles = false;
  bool onlyPsoAndPos = false;
  bool addWordsFromLiterals = false;
  float bScoringParam = 0.75;
  float kScoringParam = 1.75;
  std::optional<ad_utility::MemorySize> indexMemoryLimit;
  std::optional<ad_utility::MemorySize> parserBufferSize;
  optind = 1;

  Index index{ad_utility::makeUnlimitedAllocator<Id>()};

  boost::program_options::options_description boostOptions(
      "Options for IndexBuilderMain");
  auto add = [&boostOptions](auto&&... args) {
    boostOptions.add_options()(AD_FWD(args)...);
  };
  add("help,h", "Produce this help message.");
  add("index-basename,i", po::value(&baseName)->required(),
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
  add("kg-index-name,K", po::value(&kbIndexName),
      "The name of the knowledge graph index (default: basename of "
      "`kg-input-file`).");

  // Options for the text index.
  add("text-docs-input-file,d", po::value(&docsfile),
      "The full text of the text records from which to build the text index.");
  add("text-words-input-file,w", po::value(&wordsfile),
      "Words of the text records from which to build the text index.");
  add("text-words-from-literals,W", po::bool_switch(&addWordsFromLiterals),
      "Consider all literals from the internal vocabulary as text records. Can "
      "be combined with `text-docs-input-file` and `text-words-input-file`");
  add("text-index-name,T", po::value(&textIndexName),
      "The name of the text index (default: basename of "
      "text-words-input-file).");
  add("add-text-index,A", po::bool_switch(&onlyAddTextIndex),
      "Only build the text index. Assumes that a knowledge graph index with "
      "the same `index-basename` already exists.");
  add("bm25-b", po::value(&bScoringParam),
      "Sets the b param in the BM25 scoring metric for the fulltext index."
      " This has to be between (including) 0 and 1.");
  add("bm25-k", po::value(&kScoringParam),
      "Sets the k param in the BM25 scoring metric for the fulltext index."
      "This has to be greater than or equal to 0.");
  add("set-scoring-metric,S", po::value(&scoringMetric),
      R"(Sets the scoring metric used. Options are "explicit" for explicit )"
      "scores that are read from the wordsfile, "
      R"("tf-idf" for tf idf )"
      R"(and "bm25" for bm25. The default is "explicit".)");

  // Options for the knowledge graph index.
  add("settings-file,s", po::value(&settingsFile),
      "A JSON file, where various settings can be specified (see the QLever "
      "documentation).");
  add("no-patterns", po::bool_switch(&noPatterns),
      "Disable the precomputation for `ql:has-predicate`.");
  add("only-pso-and-pos-permutations,o", po::bool_switch(&onlyPsoAndPos),
      "Only build the PSO and POS permutations. This is faster, but then "
      "queries with predicate variables are not supported");

  // Options for the index building process.
  add("stxxl-memory,m", po::value(&indexMemoryLimit),
      "The amount of memory in to use for sorting during the index build. "
      "Decrease if the index builder runs out of memory.");
  add("parser-buffer-size,b", po::value(&parserBufferSize),
      "The size of the buffer used for parsing the input files. This must be "
      "large enough to hold a single input triple. Default: 10 MB.");
  add("keep-temporary-files,k", po::bool_switch(&keepTemporaryFiles),
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
    if (kScoringParam < 0) {
      throw std::invalid_argument("The value of bm25-k must be >= 0");
    }
    if (bScoringParam < 0 || bScoringParam > 1) {
      throw std::invalid_argument(
          "The value of bm25-b must be between and "
          "including 0 and 1");
    }
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line argument: " << e.what() << '\n';
    std::cerr << boostOptions << '\n';
    return EXIT_FAILURE;
  }
  if (indexMemoryLimit.has_value()) {
    index.memoryLimitIndexBuilding() = indexMemoryLimit.value();
  }
  if (parserBufferSize.has_value()) {
    index.parserBufferSize() = parserBufferSize.value();
  }

  // If no text index name was specified, take the part of the wordsfile after
  // the last slash.
  if (textIndexName.empty() && !wordsfile.empty()) {
    textIndexName = ad_utility::getLastPartOfString(wordsfile, '/');
  }

  // If no index name was specified, take the part of the input file name after
  // the last slash.
  if (kbIndexName.empty()) {
    kbIndexName = "no index name specified";
  }

  LOG(INFO) << EMPH_ON << "QLever IndexBuilder, compiled on "
            << qlever::version::DatetimeOfCompilation << " using git hash "
            << qlever::version::GitShortHash << EMPH_OFF << std::endl;

  try {
    size_t posOfLastSlash = baseName.rfind('/');
    string location = baseName.substr(0, posOfLastSlash + 1);
    string tail = baseName.substr(posOfLastSlash + 1);
    LOG(TRACE) << "done." << std::endl;

    index.setKbName(kbIndexName);
    index.setTextName(textIndexName);
    index.usePatterns() = !noPatterns;
    index.setOnDiskBase(baseName);
    index.setKeepTempFiles(keepTemporaryFiles);
    index.setSettingsFile(settingsFile);
    index.loadAllPermutations() = !onlyPsoAndPos;

    // Convert the parameters for the filenames, file types, and default graphs
    // into a `vector<InputFileSpecification>`.
    auto getFileSpecifications = [&]() {
      checkNumParameterValues(filetype, inputFile.size(), "--file-format, -F");
      checkNumParameterValues(defaultGraphs, inputFile.size(),
                              "--default-graph, -g");
      checkNumParameterValues(parseParallel, parseParallel.size(),
                              "--parse-parallel, p");

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

    if (!onlyAddTextIndex) {
      auto fileSpecifications = getFileSpecifications();
      AD_CONTRACT_CHECK(!fileSpecifications.empty());
      index.createFromFiles(fileSpecifications);
    }
    bool wordsAndDocsFileSpecified = !(wordsfile.empty() || docsfile.empty());

    if (!(wordsAndDocsFileSpecified ||
          (wordsfile.empty() && docsfile.empty()))) {
      throw std::runtime_error(absl::StrCat(
          "Only specified ", wordsfile.empty() ? "docsfile" : "wordsfile",
          ". Both or none of docsfile and wordsfile have to be given to build "
          "text index. If none are given the option to add words from literals "
          "has to be true. For details see --help."));
    }
    if (wordsAndDocsFileSpecified || addWordsFromLiterals) {
      index.buildTextIndexFile(
          wordsAndDocsFileSpecified
              ? std::optional{std::pair{wordsfile, docsfile}}
              : std::nullopt,
          addWordsFromLiterals, getTextScoringMetricFromString(scoringMetric),
          {bScoringParam, kScoringParam});
    }

    if (!docsfile.empty()) {
      index.buildDocsDB(docsfile);
    }
  } catch (std::exception& e) {
    LOG(ERROR) << e.what() << std::endl;
    return 2;
  }
  return 0;
}
