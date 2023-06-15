//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/Permutation.h"

#include "absl/strings/str_cat.h"
#include "util/StringUtils.h"

// _____________________________________________________________________
Permutation::Permutation(Enum permutation)
    : _readableName(toString(permutation)),
      _fileSuffix(
          absl::StrCat(".", ad_utility::getLowercaseUtf8(_readableName))),
      _keyOrder(toKeyOrder(permutation)) {}

// _____________________________________________________________________
void Permutation::loadFromDisk(const std::string& onDiskBase) {
  if constexpr (MetaData::_isMmapBased) {
    _meta.setup(onDiskBase + ".index" + _fileSuffix + MMAP_FILE_SUFFIX,
                ad_utility::ReuseTag(), ad_utility::AccessPattern::Random);
  }
  auto filename = string(onDiskBase + ".index" + _fileSuffix);
  try {
    _file.open(filename, "r");
  } catch (const std::runtime_error& e) {
    AD_THROW("Could not open the index file " + filename +
             " for reading. Please check that you have read access to "
             "this file. If it does not exist, your index is broken.");
  }
  _meta.readFromFile(&_file);
  LOG(INFO) << "Registered " << _readableName
            << " permutation: " << _meta.statistics() << std::endl;
  _isLoaded = true;
}

// _____________________________________________________________________
void Permutation::scan(Id col0Id, IdTable* result,
                       ad_utility::SharedConcurrentTimeoutTimer timer) const {
  if (!_isLoaded) {
    throw std::runtime_error("This query requires the permutation " +
                             _readableName + ", which was not loaded");
  }
  if (!_meta.col0IdExists(col0Id)) {
    return;
  }
  const auto& metaData = _meta.getMetaData(col0Id);
  return _reader.scan(metaData, _meta.blockData(), _file, result,
                      std::move(timer));
}

// _____________________________________________________________________
void Permutation::scan(Id col0Id, Id col1Id, IdTable* result,
                       ad_utility::SharedConcurrentTimeoutTimer timer) const {
  if (!_meta.col0IdExists(col0Id)) {
    return;
  }
  const auto& metaData = _meta.getMetaData(col0Id);

  return _reader.scan(metaData, col1Id, _meta.blockData(), _file, result,
                      timer);
}

// _____________________________________________________________________
std::array<size_t, 3> Permutation::toKeyOrder(Permutation::Enum permutation) {
  using enum Permutation::Enum;
  switch (permutation) {
    case POS:
      return {1, 2, 0};
    case PSO:
      return {1, 0, 2};
    case SOP:
      return {0, 2, 1};
    case SPO:
      return {0, 1, 2};
    case OPS:
      return {2, 1, 0};
    case OSP:
      return {2, 0, 1};
  }
  AD_FAIL();
}

// _____________________________________________________________________
std::string_view Permutation::toString(Permutation::Enum permutation) {
  using enum Permutation::Enum;
  switch (permutation) {
    case POS:
      return "POS";
    case PSO:
      return "PSO";
    case SOP:
      return "SOP";
    case SPO:
      return "SPO";
    case OPS:
      return "OPS";
    case OSP:
      return "OSP";
  }
  AD_FAIL();
}
