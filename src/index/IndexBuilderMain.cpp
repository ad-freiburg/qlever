// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <boost/program_options.hpp>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include "CompilationInfo.h"
#include "global/Constants.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"
#include "parser/Tokenizer.h"
#include "parser/TurtleParser.h"
#include "util/File.h"
#include "util/ProgramOptionsHelpers.h"
#include "util/ReadableNumberFact.h"

using std::cerr;
using std::cout;
using std::endl;
using std::flush;
using std::string;

#define EMPH_ON "\033[1m"
#define EMPH_OFF "\033[22m"

namespace po = boost::program_options;

string getStxxlConfigFileName(const string& location) {
  return absl::StrCat(location, ".stxxl");
}

string getStxxlDiskFileName(const string& location, const string& tail) {
  return absl::StrCat(location, tail, ".stxxl-disk");
}

// Write a .stxxl config-file.
// All we want is sufficient space somewhere with enough space.
// We can use the location of input files and use a constant size for now.
// The required size can only be estimated anyway, since index size
// depends on the structure of words files rather than their size only,
// because of the "multiplications" performed.
void writeStxxlConfigFile(const string& location, const string& tail) {
  string stxxlConfigFileName = getStxxlConfigFileName(location);
  ad_utility::File stxxlConfig(stxxlConfigFileName, "w");
  auto configFile = ad_utility::makeOfstream(stxxlConfigFileName);
  // Inform stxxl about .stxxl location
  setenv("STXXLCFG", stxxlConfigFileName.c_str(), true);
  configFile << "disk=" << getStxxlDiskFileName(location, tail) << ","
             << STXXL_DISK_SIZE_INDEX_BUILDER << ",syscall\n";
}

// Main function.
int main(int argc, char** argv) {
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
  string filetype;
  string inputFile;
  bool noPrefixCompression = false;
  bool onDiskLiterals = false;
  bool noPatterns = false;
  bool onlyAddTextIndex = false;
  bool keepTemporaryFiles = false;
  bool onlyPsoAndPos = false;
  bool addWordsFromLiterals = false;
  std::optional<ad_utility::NonNegative> stxxlMemoryGB;
  optind = 1;

  Index index{ad_utility::makeUnlimitedAllocator<Id>()};

  boost::program_options::options_description boostOptions(
      "Options for IndexBuilderMain");
  auto add = [&boostOptions]<typename... Args>(Args&&... args) {
    boostOptions.add_options()(std::forward<Args>(args)...);
  };
  add("help,h", "Produce this help message.");
  add("index-basename,i", po::value(&baseName)->required(),
      "The basename of the output files (required).");
  add("kg-input-file,f", po::value(&inputFile),
      "The file with the knowledge graph data to be parsed from. If omitted, "
      "will read from stdin.");
  add("file-format,F", po::value(&filetype),
      "The format of the input file with the knowledge graph data. Must be one "
      "of [tsv|nt|ttl]. If not set, QLever will try to deduce it from the "
      "filename suffix.");
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

  // Options for the knowledge graph index.
  add("externalize-literals,l", po::bool_switch(&onDiskLiterals),
      "An unused and deprecated option that will be removed from a future "
      "version of qlever");
  add("settings-file,s", po::value(&settingsFile),
      "A JSON file, where various settings can be specified (see the QLever "
      "documentation).");
  add("no-patterns", po::bool_switch(&noPatterns),
      "Disable the precomputation for `ql:has-predicate`.");
  add("no-compressed-vocabulary,N", po::bool_switch(&noPrefixCompression),
      "Do not apply prefix compression to the vocabulary (default: do apply).");
  add("only-pos-and-pso-permutations,o", po::bool_switch(&onlyPsoAndPos),
      "Only build the PSO and POS permutations. This is faster, but then "
      "queries with predicate variables are not supported");

  // Options for the index building process.
  add("stxxl-memory-gb,m", po::value(&stxxlMemoryGB),
      "The amount of memory in GB to use for sorting during the index build. "
      "Decrease if the index builder runs out of memory.");
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
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line argument: " << e.what() << '\n';
    std::cerr << boostOptions << '\n';
    return EXIT_FAILURE;
  }
  if (stxxlMemoryGB.has_value()) {
    index.stxxlMemoryInBytes() =
        1024ul * 1024ul * 1024ul * stxxlMemoryGB.value();
  }

  if (onDiskLiterals) {
    LOG(WARN) << EMPH_ON
              << "Warning, the -l command line option has no effect anymore "
                 "and will be removed from a future version of QLever"
              << EMPH_OFF << std::endl;
  }

  // If no text index name was specified, take the part of the wordsfile after
  // the last slash.
  if (textIndexName.empty() && !wordsfile.empty()) {
    textIndexName = ad_utility::getLastPartOfString(wordsfile, '/');
  }

  // If no index name was specified, take the part of the input file name after
  // the last slash.
  if (kbIndexName.empty() && !inputFile.empty()) {
    kbIndexName = ad_utility::getLastPartOfString(inputFile, '/');
  }

  LOG(INFO) << EMPH_ON << "QLever IndexBuilder, compiled on "
            << qlever::version::DatetimeOfCompilation << " using git hash "
            << qlever::version::GitShortHash() << EMPH_OFF << std::endl;

  try {
    LOG(TRACE) << "Configuring STXXL..." << std::endl;
    size_t posOfLastSlash = baseName.rfind('/');
    string location = baseName.substr(0, posOfLastSlash + 1);
    string tail = baseName.substr(posOfLastSlash + 1);
    writeStxxlConfigFile(location, tail);
    string stxxlFileName = getStxxlDiskFileName(location, tail);
    LOG(TRACE) << "done." << std::endl;

    index.setKbName(kbIndexName);
    index.setTextName(textIndexName);
    index.setUsePatterns(!noPatterns);
    index.setOnDiskBase(baseName);
    index.setKeepTempFiles(keepTemporaryFiles);
    index.setSettingsFile(settingsFile);
    index.setPrefixCompression(!noPrefixCompression);
    index.setLoadAllPermutations(!onlyPsoAndPos);
    // NOTE: If `onlyAddTextIndex` is true, we do not want to construct an
    // index, but we assume that it already exists. In particular, we then need
    // the vocabulary from the KB index for building the text index.
    if (!onlyAddTextIndex) {
      if (inputFile.empty() || inputFile == "-") {
        inputFile = "/dev/stdin";
      }

      if (!filetype.empty()) {
        LOG(INFO) << "You specified the input format: "
                  << ad_utility::getUppercase(filetype) << std::endl;
      } else {
        bool filetypeDeduced = false;
        if (inputFile.ends_with(".tsv")) {
          filetype = "tsv";
          filetypeDeduced = true;
        } else if (inputFile.ends_with(".nt")) {
          filetype = "nt";
          filetypeDeduced = true;
        } else if (inputFile.ends_with(".ttl")) {
          filetype = "ttl";
          filetypeDeduced = true;
        } else {
          LOG(INFO) << "Unknown or missing extension of input file, assuming: "
                       "TTL"
                    << std::endl;
        }
        if (filetypeDeduced) {
          LOG(INFO) << "Format of input file deduced from extension: "
                    << ad_utility::getUppercase(filetype) << std::endl;
        }
        LOG(INFO) << "If this is not correct, start again using the option "
                     "--file-format (-F)"
                  << std::endl;
      }

      if (filetype == "ttl") {
        LOG(DEBUG) << "Parsing uncompressed TTL from: " << inputFile
                   << std::endl;
        index.createFromFile<TurtleParserAuto>(inputFile);
      } else if (filetype == "nt") {
        LOG(DEBUG) << "Parsing uncompressed N-Triples from: " << inputFile
                   << " (using the Turtle parser)" << std::endl;
        index.createFromFile<TurtleParserAuto>(inputFile);
      } else if (filetype == "mmap") {
        LOG(DEBUG) << "Parsing uncompressed TTL from from: " << inputFile
                   << " (using mmap, which only works for files, not for "
                   << "streams)" << std::endl;
        index.createFromFile<TurtleMmapParser<Tokenizer>>(inputFile);
      } else {
        LOG(ERROR) << "File format must be one of: nt ttl mmap" << std::endl;
        std::cerr << boostOptions << std::endl;
        exit(1);
      }
    }

    if (!wordsfile.empty() || addWordsFromLiterals) {
      index.addTextFromContextFile(wordsfile, addWordsFromLiterals);
    }

    if (!docsfile.empty()) {
      index.buildDocsDB(docsfile);
    }
    ad_utility::deleteFile(stxxlFileName);
  } catch (std::exception& e) {
    LOG(ERROR) << e.what() << std::endl;
    exit(2);
  }
  return 0;
}
