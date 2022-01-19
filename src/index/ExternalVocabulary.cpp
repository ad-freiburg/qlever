// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include "./ExternalVocabulary.h"

#include <fstream>

#include "../parser/RdfEscaping.h"
#include "../parser/Tokenizer.h"
#include "../util/BufferedVector.h"
#include "../util/Log.h"
#include "../util/Generator.h"

template <class Comp>
std::optional<OffsetAndSize> ExternalVocabulary<Comp>::getOffsetAndSize(Id id) const {
  IdAndOffset dummy{id, 0};
  auto it = std::lower_bound(idsAndOffsets().begin(), idsAndOffsets().end(), dummy);
  if (it >= idsAndOffsets().end() - 1 || it->_id != id) {
    return std::nullopt;
  }
  auto offset = it ->_offset;
  auto nextOffset = (it + 1) -> _offset;
  return OffsetAndSize{offset, nextOffset - offset};
}

// _____________________________________________________________________________
template <class Comp>
std::optional<string> ExternalVocabulary<Comp>::operator[](Id id) const {

  auto optionalOffsetAndSize = getOffsetAndSize(id);
  if (!optionalOffsetAndSize.has_value()) {
    return std::nullopt;
  }


  string result(optionalOffsetAndSize->_size, '\0');
  _file.read(result.data(), optionalOffsetAndSize->_size, optionalOffsetAndSize->_offset);
  return result;
}

// _____________________________________________________________________________
template <class Comp>
Id ExternalVocabulary<Comp>::binarySearchInVocab(const string& word) const {
  Id lower = 0;
  Id upper = size();
  while (lower < upper) {
    Id i = lower + (upper - lower) / 2;
    string w = std::move((*this)[i].value());
    int cmp = _caseComparator.compare(w, word, Comp::Level::TOTAL);
    if (cmp < 0) {
      lower = i + 1;
    } else if (cmp > 0) {
      upper = i - 1;
    } else if (cmp == 0) {
      return i;
    }
  }
  return lower;
}


// _____________________________________________________________________________
template <class Comp>
template <typename Iterable>
void ExternalVocabulary<Comp>::buildFromIterable(Iterable&& it,
                                               const string& fileName) {
  {
    _file.open(fileName.c_str(), "w");
    ad_utility::MmapVector<IdAndOffset> idsAndOffsets(
        fileName + _offsetSuffix, ad_utility::CreateTag{
        });
    uint64_t currentOffset = 0;
    uint64_t index = 0;
    for (const std::string& word : it) {
      idsAndOffsets.push_back(IdAndOffset{index, currentOffset});
      currentOffset += _file.write(word.data(), word.size());
      index++;
    }

    // End offset of last entry
    idsAndOffsets.push_back(IdAndOffset{index, currentOffset});
    _file.close();
  } // Run destructor of MmapVector to dump everything to disk.
  initFromFile(fileName);
}

// _____________________________________________________________________________
template <class Comp>
void ExternalVocabulary<Comp>::buildFromVector(const vector<string>& v,
                                               const string& fileName) {
  buildFromIterable(v, fileName);
}

// _____________________________________________________________________________
template <class Comp>
void ExternalVocabulary<Comp>::buildFromTextFile(const string& textFileName,
                                                 const string& outFileName) {

  std::ifstream infile(textFileName);
  AD_CHECK(infile.is_open());
  auto lineGenerator = [&infile]() ->cppcoro::generator<string> {
    std::string word;
    while (std::getline(infile, word)) {
      // The temporary file for the to-be-externalized vocabulary strings is
      // line-based, just like the normal vocabulary file. Therefore, \n and \ are
      // escaped there. When we read from this file, we have to unescape these.
      word = RdfEscaping::unescapeNewlinesAndBackslashes(word);
      co_yield word;
  }}();
  buildFromIterable(lineGenerator, outFileName);
}

// _____________________________________________________________________________
template <class Comp>
void ExternalVocabulary<Comp>::initFromFile(const string& file) {
  _file.open(file.c_str(), "r");
  _idsAndOffsets.open(file + _offsetSuffix);
  LOG(INFO) << "Initialized external vocabulary. It contains " << size()
            << " elements." << std::endl;
}

template class ExternalVocabulary<TripleComponentComparator>;
template class ExternalVocabulary<SimpleStringComparator>;
