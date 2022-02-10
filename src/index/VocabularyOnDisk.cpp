// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include "./VocabularyOnDisk.h"

#include <fstream>

#include "../parser/RdfEscaping.h"
#include "../parser/Tokenizer.h"
#include "../util/BufferedVector.h"
#include "../util/Generator.h"
#include "../util/Log.h"

using OffsetAndSize = VocabularyOnDisk::OffsetAndSize;
// ____________________________________________________________________________
std::optional<OffsetAndSize> VocabularyOnDisk::getOffsetAndSize(
    Id id) const {
  IdAndOffset dummy{id, 0};
  auto it =
      std::lower_bound(idsAndOffsets().begin(), idsAndOffsets().end(), dummy);
  if (it >= idsAndOffsets().end() - 1 || it->_id != id) {
    return std::nullopt;
  }
  auto offset = it->_offset;
  auto nextOffset = (it + 1)->_offset;
  return OffsetAndSize{offset, nextOffset - offset};
}

OffsetAndSize VocabularyOnDisk::getOffsetAndSizeForNthElement(
    size_t n) const {
  AD_CHECK(n < size());
  // TODO<joka921> :: This is duplicated code
  const auto offset = _idsAndOffsets[n]._offset;
  const auto nextOffset = _idsAndOffsets[n + 1]._offset;
  return OffsetAndSize{offset, nextOffset - offset};
}

// _____________________________________________________________________________
std::optional<string> VocabularyOnDisk::idToOptionalString(
    Id id) const {
  auto optionalOffsetAndSize = getOffsetAndSize(id);
  if (!optionalOffsetAndSize.has_value()) {
    return std::nullopt;
  }

  string result(optionalOffsetAndSize->_size, '\0');
  _file.read(result.data(), optionalOffsetAndSize->_size,
             optionalOffsetAndSize->_offset);
  return result;
}

// _____________________________________________________________________________
template <typename Iterable>
void VocabularyOnDisk::buildFromIterable(Iterable&& it,
                                                 const string& fileName) {
  {
    _file.open(fileName.c_str(), "w");
    ad_utility::MmapVector<IdAndOffset> idsAndOffsets(fileName + _offsetSuffix,
                                                      ad_utility::CreateTag{});
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
  }  // Run destructor of MmapVector to dump everything to disk.
  initFromFile(fileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::buildFromVector(const vector<string>& v,
                                               const string& fileName) {
  buildFromIterable(v, fileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::buildFromTextFile(const string& textFileName,
                                                 const string& outFileName) {
  std::ifstream infile(textFileName);
  AD_CHECK(infile.is_open());
  auto lineGenerator = [&infile]() -> cppcoro::generator<string> {
    std::string word;
    while (std::getline(infile, word)) {
      // The temporary file for the to-be-externalized vocabulary strings is
      // line-based, just like the normal vocabulary file. Therefore, \n and \
      // are escaped there. When we read from this file, we have to unescape
      // these.
      word = RdfEscaping::unescapeNewlinesAndBackslashes(word);
      co_yield word;
    }
  }();
  buildFromIterable(lineGenerator, outFileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::initFromFile(const string& file) {
  _file.open(file.c_str(), "r");
  _idsAndOffsets.open(file + _offsetSuffix);
  AD_CHECK(_idsAndOffsets.size() > 0);
  _size = _idsAndOffsets.size() - 1;
  if (_size > 0) {
    _highestId = (*(end() - 1))._index;
  }
}

WordAndIndex VocabularyOnDisk::getNthElement(
    size_t n) const {
  AD_CHECK(n < idsAndOffsets().size());
  auto offsetAndSize = getOffsetAndSizeForNthElement(n);

  string result(offsetAndSize._size, '\0');
  _file.read(result.data(), offsetAndSize._size, offsetAndSize._offset);

  // TODO<joka921> we can get the id by a single read above
  auto id = idsAndOffsets()[n]._id;
  return {std::move(result), id};
}