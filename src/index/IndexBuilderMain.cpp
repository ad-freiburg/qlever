// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <getopt.h>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include <unordered_set>
#include "../global/Constants.h"
#include "../util/File.h"
#include "../util/ReadableNumberFact.h"
#include "../util/StringUtils.h"
#include "./ConstantsIndexCreation.h"
#include "./Index.h"

using std::cerr;
using std::cout;
using std::endl;
using std::flush;
using std::string;

#define EMPH_ON "\033[1m"
#define EMPH_OFF "\033[22m"

// Available options.
struct option options[] = {
    {"docs-by-contexts", required_argument, NULL, 'd'},
    {"help", no_argument, NULL, 'h'},
    {"index-basename", required_argument, NULL, 'i'},
    {"file-format", required_argument, NULL, 'F'},
    {"knowledge-base-input-file", required_argument, NULL, 'f'},
    {"kb-index-name", required_argument, NULL, 'K'},
    {"on-disk-literals", no_argument, NULL, 'l'},
    {"no-patterns", no_argument, NULL, 'P'},
    {"text-index-name", required_argument, NULL, 'T'},
    {"words-by-contexts", required_argument, NULL, 'w'},
    {"add-text-index", no_argument, NULL, 'A'},
    {"keep-temporary-files", no_argument, NULL, 'k'},
    {"settings-file", required_argument, NULL, 's'},
    {"no-compressed-vocabulary", no_argument, NULL, 'N'},
    {NULL, 0, NULL, 0}};

string getStxxlDiskFileName(const string& location, const string& tail) {
  std::ostringstream os;
  os << location << tail << "-stxxl.disk";
  return os.str();
}

// Write a .stxxl config-file.
// All we want is sufficient space somewhere with enough space.
// We can use the location of input files and use a constant size for now.
// The required size can only ben estimation anyway, since index size
// depends on the structure of words files rather than their size only,
// because of the "multiplications" performed.
void writeStxxlConfigFile(const string& location, const string& tail) {
  ad_utility::File stxxlConfig(".stxxl", "w");
  std::ostringstream config;
  config << "disk=" << getStxxlDiskFileName(location, tail) << ","
         << STXXL_DISK_SIZE_INDEX_BUILDER << ",syscall";
  stxxlConfig.writeLine(config.str());
}

void printUsage(char* execName) {
  std::ios coutState(nullptr);
  coutState.copyfmt(cout);
  cout << std::setfill(' ') << std::left;

  cout << "Usage: " << execName << " -i <index> [OPTIONS]" << endl << endl;
  cout << "Options" << endl;
  cout << "  " << std::setw(20) << "d, docs-by-contexts" << std::setw(1)
       << "    "
       << "docs-file to build text index from." << endl;
  cout << "  " << std::setw(20) << "h, help" << std::setw(1) << "    "
       << "Print this help and exit." << endl;
  cout << "  " << std::setw(20) << "i, index-basename" << std::setw(1) << "    "
       << "(designated) name and path of the index to build." << endl;

  cout << "  " << std::setw(20) << "F, file-format" << std::setw(1) << "    "
       << " Specify format of the input file. Must be one of "
          "[tsv|nt|ttl|mmap]."
       << " " << std::setw(36)
       << "If not set, we will try to deduce from the filename" << endl
       << " " << std::setw(36)
       << "(mmap assumes an on-disk turtle file that can be mmapped to memory)"
       << endl;
  cout << "  " << std::setw(20) << "f, knowledge-base-input-file"
       << std::setw(1) << "    "
       << " The file to be parsed from. If omitted, we will read from stdin"
       << endl;
  cout << "  " << std::setw(20) << "K, kb-index-name" << std::setw(1) << "    "
       << "Assign a name to be displayed in the UI (default: name of nt-file)"
       << endl;
  cout << "  " << std::setw(20) << "l, on-disk-literals" << std::setw(1)
       << "    "
       << "Externalize parts of the KB vocab." << endl;
  cout << "  " << std::setw(20) << "n, ntriples-file" << std::setw(1) << "    "
       << "NT file to build KB index from." << endl;
  cout << "  " << std::setw(20) << "no-patterns" << std::setw(1) << "    "
       << "Disable the use of patterns. This disables ql:has-predicate."
       << endl;
  cout << "  " << std::setw(20) << "T, text-index-name" << std::setw(1)
       << "    "
       << "Assign a name to be displayed in the UI "
          "(default: name of words-file)."
       << endl;
  cout << "  " << std::setw(20) << "w, words-by-contexts" << std::setw(1)
       << "    "
       << "words-file to build text index from." << endl;
  cout << "  " << std::setw(20) << "A, add-text-index" << std::setw(1) << "    "
       << "Add text index to already existing kb-index" << endl;
  cout
      << "  " << std::setw(20) << "k, keep-temporary-files" << std::setw(1)
      << "    "
      << "Keep Temporary Files from IndexCreation (normally only for debugging)"
      << endl;
  cout << "  " << std::setw(20) << "s, settings-file" << std::setw(1) << "    "
       << "Specify a input settings file where prefixes that are to be "
          "externalized etc can be specified"
       << endl;
  cout << "  " << std::setw(20) << "N, no-compressed-vocabulary" << std::setw(1)
       << "    "
       << "Do NOT use prefix compression on the vocabulary (default is to "
          "compress)."
       << endl;
  cout.copyfmt(coutState);
}

// Main function.
int main(int argc, char** argv) {
  char* locale = setlocale(LC_CTYPE, "");

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
  bool useCompression = true;
  bool onDiskLiterals = false;
  bool usePatterns = true;
  bool onlyAddTextIndex = false;
  bool keepTemporaryFiles = false;
  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "F:f:i:w:d:lT:K:hAks:N", options, nullptr);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'h':
        printUsage(argv[0]);
        return 0;
        break;
      case 'i':
        baseName = optarg;
        break;
      case 'w':
        wordsfile = optarg;
        break;
      case 'd':
        docsfile = optarg;
        break;
      case 'l':
        onDiskLiterals = true;
        break;
      case 'T':
        textIndexName = optarg;
        break;
      case 'K':
        kbIndexName = optarg;
        break;
      case 'P':
        usePatterns = false;
        break;
      case 'A':
        onlyAddTextIndex = true;
        break;
      case 'k':
        keepTemporaryFiles = true;
        break;
      case 's':
        settingsFile = optarg;
        break;
      case 'F':
        filetype = optarg;
        break;
      case 'f':
        inputFile = optarg;
        break;
      case 'N':
        useCompression = false;
        break;
      default:
        cout << endl
             << "! ERROR in processing options (getopt returned '" << c
             << "' = 0x" << std::setbase(16) << c << ")" << endl
             << "Corresponding ascii option : -" << std::string(1, c) << endl
             << "This is either an unsupported option or there was an error"
             << endl;
        exit(1);
    }
  }

  if (textIndexName.size() == 0 && wordsfile.size() > 0) {
    textIndexName = ad_utility::getLastPartOfString(wordsfile, '/');
  }

  if (kbIndexName.size() == 0) {
    if (!inputFile.empty()) {
      kbIndexName = ad_utility::getLastPartOfString(inputFile, '/');
    }
  }

  if (baseName.size() == 0) {
    cout << "Missing required argument --index-basename (-i)..." << endl;
    printUsage(argv[0]);
    exit(1);
  }

  std::cout << std::endl
            << EMPH_ON << "IndexBuilderMain, version " << __DATE__ << " "
            << __TIME__ << EMPH_OFF << std::endl
            << std::endl;
  cout << "Set locale LC_CTYPE to: " << locale << endl;
  LOG(DEBUG) << "Configuring STXXL..." << std::endl;
  string stxxlFileName;
  size_t posOfLastSlash = baseName.rfind('/');
  string location = baseName.substr(0, posOfLastSlash + 1);
  string tail = baseName.substr(posOfLastSlash + 1);
  writeStxxlConfigFile(location, tail);
  stxxlFileName = getStxxlDiskFileName(location, tail);
  LOG(DEBUG) << "done." << std::endl;

  if (inputFile.empty()) {
    LOG(INFO) << "No input knowledge-base-file specified. Parsing from stdin\n";
    inputFile = "/dev/stdin";
  } else if (inputFile == "-") {
    LOG(INFO) << "Parsing from stdin.\n";
    inputFile = "/dev/stdin";
  }

  if (filetype.empty()) {
    bool filetypeDeduced = false;
    if (ad_utility::endsWith(inputFile, ".tsv")) {
      filetype = "tsv";
      filetypeDeduced = true;
    } else if (ad_utility::endsWith(inputFile, ".nt")) {
      filetype = "nt";
      filetypeDeduced = true;
    } else if (ad_utility::endsWith(inputFile, ".ttl")) {
      filetype = "ttl";
      filetypeDeduced = true;
    } else {
      LOG(WARN)
          << " Could not deduce the type of the input knowledge-base-file by "
             "its extension. Assuming the input to be turtle. Please specify "
             "--file-format (-F)\n";
      LOG(WARN) << "In case this is not correct \n";
    }
    if (filetypeDeduced) {
      LOG(INFO) << "Assuming input file format to be " << filetype
                << " due to the input file's extension.\n";
      LOG(INFO)
          << "If this is wrong, please manually specify the --file-format "
             "(-F) flag.\n";
    }
  }

  try {
    Index index;
    index.setKbName(kbIndexName);
    index.setTextName(textIndexName);
    index.setUsePatterns(usePatterns);
    index.setOnDiskLiterals(onDiskLiterals);
    index.setOnDiskBase(baseName);
    index.setKeepTempFiles(keepTemporaryFiles);
    index.setSettingsFile(settingsFile);
    index.setPrefixCompression(useCompression);
    if (!onlyAddTextIndex) {
      // if onlyAddTextIndex is true, we do not want to construct an index,
      // but assume that it  already exists (especially we need a valid
      // vocabulary for  text  index  creation)

      if (filetype == "ttl") {
        LOG(INFO) << "Reading from uncompressed turtle file or stream "
                  << inputFile << std::endl;
        index.createFromFile<TurtleStreamParser>(inputFile);
      } else if (filetype == "tsv") {
        LOG(INFO) << "Reading from uncompressed tsv file" << inputFile
                  << std::endl;
        index.createFromFile<TsvParser>(inputFile);
      } else if (filetype == "nt") {
        LOG(INFO) << "Reading from uncompressed NTriples file" << inputFile
                  << std::endl;
        index.createFromFile<NTriplesParser>(inputFile);
      } else if (filetype == "mmap") {
        LOG(INFO) << "Reading from uncompressed turtle file" << inputFile
                  << std::endl;
        LOG(WARN) << "We will load this file using mmap. This will only work "
                     "for files on hard disk and fail for streams.\n";
        LOG(WARN) << "For Turtle streams please use --file-format ttl\n";
        index.createFromFile<TurtleMmapParser>(inputFile);
      } else {
        LOG(ERROR)
            << "Argument to --file-format (-F) must be one of [tsv|nt|ttl|mmap]"
               "(mmap assumes an on-disk turtle file that can be completely "
               "mmaped to memory. \n";
        printUsage(argv[0]);
        exit(1);
      }
    }

    if (wordsfile.size() > 0) {
      index.addTextFromContextFile(wordsfile);
    }

    if (docsfile.size() > 0) {
      index.buildDocsDB(docsfile);
    }
  } catch (ad_semsearch::Exception& e) {
    LOG(ERROR) << e.getFullErrorMessage() << std::endl;
  }
  std::remove(stxxlFileName.c_str());
  return 0;
}
