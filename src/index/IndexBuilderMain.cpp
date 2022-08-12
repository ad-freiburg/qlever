// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <boost/program_options.hpp>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "../global/Constants.h"
#include "../util/File.h"
#include "../util/ReadableNumberFact.h"
#include "./ConstantsIndexBuilding.h"
#include "./Index.h"
#include "util/ProgramOptionsHelpers.h"

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
  return absl::StrCat(location, tail, "-stxxl.disk");
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

  Index index;

  boost::program_options::options_description boostOptions(
      "Options for IndexBuilderMain");
  auto add = [&boostOptions]<typename... Args>(Args && ... args) {
    boostOptions.add_options()(std::forward<Args>(args)...);
  };
  add("help,h", "Produce this help message.");
  add("index-basename,i", po::value<std::string>(&baseName)->required(),
      "The basename of the index files (required).");
  add("docs-by-context,d", po::value<std::string>(&docsfile),
      "docs-file to build text index from");
  add("file-format,F", po::value<std::string>(&filetype),
      "Specify format of the input file. Must be one of "
      "[tsv|nt|ttl|mmap]. If not set, we will try to deduce from the filename. "
      "`mmap` assumes an on-disk turtle file that can be mmapped to memory)");
  add("knowledge-base-input-file,f", po::value(&inputFile),
      "The file to be parsed from. If omitted, we will read from stdin");
  add("kb-index-name, K", po::value(&kbIndexName),
      "Assign a name to be displayed in the UI (default: name of nt-file)");
  add("on-disk-literals,l", po::bool_switch(&onDiskLiterals),
      "Externalize parts of the KB vocab.");
  add("no-patterns", po::bool_switch(&noPatterns),
      "Disable the use of patterns. This disables ql:has-predicate.");
  add("text-index-name,T", po::value(&textIndexName),
      "Assign a name to be displayed in the UI "
      "(default: name of words-file).");
  add("words-by-contexts,w", po::value(&wordsfile),
      "words-file to build text index from");
  add("words-from-literals,W", po::bool_switch(&addWordsFromLiterals),
      "Consider all literals from the internal vocabulary as text records");
  add("add-text-index, A", po::bool_switch(&onlyAddTextIndex),
      "Add text index to already existing kb-index");
  add("keep-temporary-files,k", po::bool_switch(&keepTemporaryFiles),
      "Keep Temporary Files from IndexCreation (normally only for debugging)");
  add("settings-file,s", po::value(&settingsFile),
      "Specify a input settings file where prefixes that are to be "
      "externalized etc. can be specified");
  add("no-compressed-vocabulary,N", po::bool_switch(&noPrefixCompression),
      "Do NOT use prefix compression on the vocabulary (default is to "
      "compress).");
  add("only-pos-and-pso-permutations,o", po::bool_switch(&onlyPsoAndPos),
      "Only load PSO and POS permutations");
  add("m, stxxl-memory-gb", po::value(&stxxlMemoryGB),
      "The amount of memory to use for sorting during the index build. "
      "Decrease if the index builder runs out of memory.");

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
    std::cerr << "Error in command-line Argument: " << e.what() << '\n';
    std::cerr << boostOptions << '\n';
    return EXIT_FAILURE;
  }
  if (stxxlMemoryGB.has_value()) {
    index.stxxlMemoryInBytes() =
        1024ul * 1024ul * 1024ul * stxxlMemoryGB.value();
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

  LOG(INFO) << EMPH_ON << "QLever IndexBuilder, compiled on " << __DATE__ << " "
            << __TIME__ << EMPH_OFF << std::endl;

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
    index.setOnDiskLiterals(onDiskLiterals);
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
