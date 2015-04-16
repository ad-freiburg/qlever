// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <stxxl/algorithm>
#include "./Index.h"
#include "../parser/ContextFileParser.h"

// _____________________________________________________________________________
void Index::addTextFromContextFile(const string& contextFile) {
  string indexFilename = _onDiskBase + ".text.index";
  size_t nofLines = passContextFileForVocabulary(contextFile);
  _vocab.writeToFile(_onDiskBase + ".text.vocabulary");
  TextVec v(nofLines);
  passContextFileIntoVector(contextFile, v);
  LOG(INFO) << "Sorting text index..." << std::endl;
  stxxl::sort(begin(v), end(v), SortText(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;
  createTextIndex(indexFilename, v, _textMeta);
  openFileHandles();
}

// _____________________________________________________________________________
void Index::addTextFromOnDiskIndex() {

}

// _____________________________________________________________________________
size_t Index::passContextFileForVocabulary(string const& contextFile) {
  return 0;
}

// _____________________________________________________________________________
void Index::passContextFileIntoVector(string const& contextFile,
                                      Index::TextVec& vec) {

}

// _____________________________________________________________________________
void Index::createTextIndex(const string& filename, Index::TextVec const& vec,
                            TextMetaData& meta) {

}