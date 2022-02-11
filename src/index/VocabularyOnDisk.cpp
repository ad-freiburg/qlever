// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "./VocabularyOnDisk.h"

#include <fstream>

#include "../parser/RdfEscaping.h"
#include "../parser/Tokenizer.h"
#include "../util/BufferedVector.h"
#include "../util/Generator.h"
#include "../util/Log.h"

using OffsetAndSize = VocabularyOnDisk::OffsetAndSize;

// ____________________________________________________________________________
std::optional<OffsetAndSize> VocabularyOnDisk::getOffsetAndSize(Id id) const {
  IdAndOffset dummy{id, 0};
  auto it =
      std::lower_bound(_idsAndOffsets.begin(), _idsAndOffsets.end(), dummy);
  if (it >= _idsAndOffsets.end() - 1 || it->_id != id) {
    return std::nullopt;
  }
  return getOffsetAndSizeForNthElement(it - _idsAndOffsets.begin());
}

// ____________________________________________________________________________
VocabularyOnDisk::OffsetSizeId VocabularyOnDisk::getOffsetSizeIdForNthElement(
    size_t n) const {
  AD_CHECK(n < size());
  const auto offset = _idsAndOffsets[n]._offset;
  const auto nextOffset = _idsAndOffsets[n + 1]._offset;
  return OffsetSizeId{offset, nextOffset - offset, _idsAndOffsets[n]._id};
}

// _____________________________________________________________________________
std::optional<string> VocabularyOnDisk::operator[](Id id) const {
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
    std::optional<uint64_t> lastId = std::nullopt;
    for (const auto& [word, id] : it) {
      AD_CHECK(!lastId.has_value() || lastId.value() < id);
      idsAndOffsets.push_back(IdAndOffset{id, currentOffset});
      currentOffset += _file.write(word.data(), word.size());
      lastId = id;
    }

    // End offset of last entry, also consistent with the empty vocabulary.
    if (lastId.has_value()) {
      idsAndOffsets.push_back(IdAndOffset{lastId.value() + 1, currentOffset});
    } else {
      idsAndOffsets.push_back(IdAndOffset{_highestId + 1, currentOffset});
    }
    _file.close();
  }  // Run destructor of MmapVector to dump everything to disk.
  readFromFile(fileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::buildFromVector(const vector<string>& v,
                                       const string& fileName) {
  // Note: Using a reference-capture for `v` will segfault in GCC11.
  // TODO<joka921> This is a bug in the compiler, report it if still unknown or
  // post reference link here.
  auto generator = [](const auto& words)
      -> cppcoro::generator<std::pair<std::string_view, uint64_t>> {
    uint64_t index = 0;
    for (const auto& word : (words)) {
      // Yielding the temporary directly would segfault in GCC, this is a bug
      // in the compiler, see similar places in the `streamable_generator`
      // class.
      std::pair<std::string, uint64_t> tmp{word, index};
      co_yield tmp;
      index++;
    }
  }(v);
  buildFromIterable(std::move(generator), fileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::buildFromStringsAndIds(
    const vector<std::pair<std::string, uint64_t>>& v, const string& fileName) {
  return buildFromIterable(v, fileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::buildFromTextFile(const string& textFileName,
                                         const string& outFileName) {
  std::ifstream infile(textFileName);
  AD_CHECK(infile.is_open());
  auto lineGenerator =
      [&infile]() -> cppcoro::generator<std::pair<std::string_view, uint64_t>> {
    std::string word;
    uint64_t index = 0;
    while (std::getline(infile, word)) {
      // The temporary file for the to-be-externalized vocabulary strings is
      // line-based, just like the normal vocabulary file. Therefore, \n and \
      // are escaped there. When we read from this file, we have to unescape
      // these.
      word = RdfEscaping::unescapeNewlinesAndBackslashes(word);
      co_yield std::pair{std::string_view{word}, index};
      index++;
    }
  }();
  buildFromIterable(std::move(lineGenerator), outFileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::readFromFile(const string& file) {
  _file.open(file.c_str(), "r");
  _idsAndOffsets.open(file + _offsetSuffix);
  AD_CHECK(_idsAndOffsets.size() > 0);
  _size = _idsAndOffsets.size() - 1;
  if (_size > 0) {
    _highestId = (*(end() - 1))._index;
  }
}

WordAndIndex VocabularyOnDisk::getNthElement(size_t n) const {
  AD_CHECK(n < _idsAndOffsets.size());
  auto offsetSizeId = getOffsetSizeIdForNthElement(n);

  string result(offsetSizeId._size, '\0');
  _file.read(result.data(), offsetSizeId._size, offsetSizeId._offset);

  return {std::move(result), offsetSizeId._id};
}