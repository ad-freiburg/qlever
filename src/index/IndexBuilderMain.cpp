// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <getopt.h>
#include <cstdlib>
#include <cstdio>
#include <string>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "../util/File.h"
#include "../global/Constants.h"
#include "../util/ReadableNumberFact.h"
#include "./Index.h"


using std::string;
using std::cout;
using std::endl;
using std::flush;
using std::cerr;

#define EMPH_ON  "\033[1m"
#define EMPH_OFF "\033[22m"

// Available options.
struct option options[] = {
    {"tsv-file",          required_argument, NULL, 't'},
    {"ntriples-file",     required_argument, NULL, 'n'},
    {"index-basename",    required_argument, NULL, 'i'},
    {"words-by-contexts", required_argument, NULL, 'w'},
    {"docs-by-contexts",  required_argument, NULL, 'd'},
    {"all-permutations",  no_argument,       NULL, 'a'},
    {"on-disk-literals",  no_argument,       NULL, 'l'},
    {"text-index-name",   required_argument, NULL, 'T'},
    {"kb-index-name",     required_argument, NULL, 'K'},
    {NULL, 0,                                NULL, 0}
};

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

void printOptions() {
  cout << "Available Options:\n"
       << "--tsv-file (-t): TSV file to build KB index from\n"
      << "--ntriples-file (-n): NT file to build KB index from\n"
      << "--index-basename (-i): (designated) name and path of the index to build\n"
      << "--all-permutations (-a): build all 6 (not only 2) KB index permutations\n"
      << "--on-disk-literals (-l): externalize parts of the KB vocab\n"
      << "--words-by-contexts (-w): words-file to build text index from\n"
      << "--docs-by-contexts (-d): docs-file to build text index from\n"
      << "--kb-index-name (-K): assign a name to be displayed in the UI (default: name of nt-file)\n"
      << "--text-index-name (-T): assign a name to be displayed in the UI (default: name of words-file)\n"
      << std::endl;
}

// Main function.
int main(int argc, char** argv) {
  std::cout << std::endl << EMPH_ON
            << "IndexBuilderMain, version " << __DATE__
            << " " << __TIME__ << EMPH_OFF << std::endl << std::endl;

  char* locale = setlocale(LC_CTYPE, "en_US.utf8");
  cout << "Set locale LC_CTYPE to: " << locale << endl;

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
  bool allPermutations = false;
  bool onDiskLiterals = false;
  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "t:n:i:w:d:alT:K:", options, NULL);
    if (c == -1) { break; }
    switch (c) {
      case 't':
        tsvFile = optarg;
        break;
      case 'n':
        ntFile = optarg;
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
      default:
        cout << endl
             << "! ERROR in processing options (getopt returned '" << c
             << "' = 0x" << std::setbase(16) << c << ")"
             << endl << endl;
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
    printOptions();
    exit(1);
  }

  if (tsvFile.size() == 0 && ntFile.size() == 0 && wordsfile.size() == 0
      && docsfile.size() == 0) {
    cout << "Missing required argument --tsv-file (-t) or "
        "--ntriples-file (-n) or --words-by-contexts (-w) "
        "or --docs-by-contexts (-d) ..." << endl;
    printOptions();
    exit(1);
  }

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
    if (ntFile.size() > 0) {
      index.createFromNTriplesFile(ntFile, baseName, allPermutations,
                                   onDiskLiterals);
    } else if (tsvFile.size() > 0) {
      index.createFromTsvFile(tsvFile, baseName, allPermutations,
                              onDiskLiterals);
    } else {
      index.createFromOnDiskIndex(baseName, allPermutations, onDiskLiterals);
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
