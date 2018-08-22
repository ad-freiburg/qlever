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

#include "../global/Constants.h"
#include "../util/File.h"
#include "../util/ReadableNumberFact.h"
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
struct option options[] = {{"all-permutations", no_argument, NULL, 'a'},
                           {"docs-by-contexts", required_argument, NULL, 'd'},
                           {"help", no_argument, NULL, 'h'},
                           {"index-basename", required_argument, NULL, 'i'},
                           {"kb-index-name", required_argument, NULL, 'K'},
                           {"on-disk-literals", no_argument, NULL, 'l'},
                           {"ntriples-file", required_argument, NULL, 'n'},
                           {"patterns", no_argument, NULL, 'P'},
                           {"tsv-file", required_argument, NULL, 't'},
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
  cout << "  " << std::setw(20) << "a, all-permutations" << std::setw(1)
       << "    "
       << "Build all 6 (not only 2) KB index permutations." << endl;
  cout << "  " << std::setw(20) << "d, docs-by-contexts" << std::setw(1)
       << "    "
       << "docs-file to build text index from." << endl;
  cout << "  " << std::setw(20) << "h, help" << std::setw(1) << "    "
       << "Print this help and exit." << endl;
  cout << "  " << std::setw(20) << "i, index-basename" << std::setw(1) << "    "
       << "(designated) name and path of the index to build." << endl;
  cout << "  " << std::setw(20) << "K, kb-index-name" << std::setw(1) << "    "
       << "Assign a name to be displayed in the UI (default: name of nt-file)"
       << endl;
  cout << "  " << std::setw(20) << "l, on-disk-literals" << std::setw(1)
       << "    "
       << "Externalize parts of the KB vocab." << endl;
  cout << "  " << std::setw(20) << "n, ntriples-file" << std::setw(1) << "    "
       << "NT file to build KB index from." << endl;
  cout << "  " << std::setw(20) << "P, patterns" << std::setw(1) << "    "
       << "Detect and store prediate patterns to enable ql:has-predicate "
          "queries."
       << endl;
  cout << "  " << std::setw(20) << "t, tsv-file" << std::setw(1) << "    "
       << "TSV file to build KB index from." << endl;
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

  string tsvFile;
  string ntFile;
  string baseName;
  string wordsfile;
  string docsfile;
  string textIndexName;
  string kbIndexName;
  string settingsFile = "";
  bool useCompression = true;
  bool allPermutations = false;
  bool onDiskLiterals = false;
  bool usePatterns = false;
  bool onlyAddTextIndex = false;
  bool keepTemporaryFiles = false;
  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "t:n:i:w:d:alT:K:PhAks:N", options, NULL);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 't':
        tsvFile = optarg;
        break;
      case 'n':
        ntFile = optarg;
        break;
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
      case 'a':
        allPermutations = true;
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
        usePatterns = true;
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
      case 'N':
        useCompression = false;
        break;
      default:
        cout << endl
             << "! ERROR in processing options (getopt returned '" << c
             << "' = 0x" << std::setbase(16) << c << ")" << endl
             << endl;
        exit(1);
    }
  }

  if (textIndexName.size() == 0 && wordsfile.size() > 0) {
    textIndexName = ad_utility::getLastPartOfString(wordsfile, '/');
  }

  if (kbIndexName.size() == 0) {
    if (ntFile.size() > 0) {
      kbIndexName = ad_utility::getLastPartOfString(ntFile, '/');
    }
    if (tsvFile.size() > 0) {
      kbIndexName = ad_utility::getLastPartOfString(tsvFile, '/');
    }
  }

  if (baseName.size() == 0) {
    cout << "Missing required argument --index-basename (-i)..." << endl;
    printUsage(argv[0]);
    exit(1);
  }

  if (tsvFile.size() == 0 && ntFile.size() == 0 && wordsfile.size() == 0 &&
      docsfile.size() == 0) {
    cout << "Missing required argument --tsv-file (-t) or "
            "--ntriples-file (-n) or --words-by-contexts (-w) "
            "or --docs-by-contexts (-d) ..."
         << endl;
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

      if (ntFile.size() > 0) {
        index.createFromFile<NTriplesParser>(ntFile, allPermutations);
      } else if (tsvFile.size() > 0) {
        index.createFromFile<TsvParser>(tsvFile, allPermutations);
      } else {
        index.createFromOnDiskIndex(baseName, allPermutations);
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
