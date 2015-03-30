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
#include "../engine/Constants.h"
#include "../util/ReadableNumberFact.h"
#include "./Index.h"


using std::string;
using std::cout;
using std::endl;
using std::flush;
using std::cerr;

#define EMPH_ON  "\033[1m"
#define EMPH_OFF "\033[21m"

// Available options.
struct option options[] = {
    {"tsv-file",       required_argument, NULL, 't'},
    {"ntriples-file",  required_argument, NULL, 'n'},
    {"index-basename", required_argument, NULL, 'b'},
    {NULL, 0,                             NULL, 0}
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
  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "t:b:n:", options, NULL);
    if (c == -1) { break; }
    switch (c) {
      case 't':
        tsvFile = optarg;
        break;
      case 'n':
        ntFile = optarg;
        break;
      case 'b':
        baseName = optarg;
        break;
      default:
        cout << endl
        << "! ERROR in processing options (getopt returned '" << c
        << "' = 0x" << std::setbase(16) << c << ")"
        << endl << endl;
        exit(1);
    }
  }

  if (baseName.size() == 0) {
    cout << "Missing required argument --index-basename (-b)..." << endl;
    exit(1);
  }

  if (tsvFile.size() == 0 && ntFile.size() == 0) {
    cout << "Missing required argument --tsv-file (-t) or "
        "--ntriples-file (-n) ..." << endl;
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

  Index index;
  if (ntFile.size() > 0) {
    index.createFromNTriplesFile(ntFile, baseName);
  } else {
    index.createFromTsvFile(tsvFile, baseName);
  }

  std::remove(stxxlFileName.c_str());
  return 0;
}
