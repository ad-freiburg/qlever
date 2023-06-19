// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
#pragma once

#include <array>
#include <string>

#include "global/Constants.h"
#include "index/IndexMetaData.h"
#include "util/File.h"
#include "util/Log.h"

// Helper class to store static properties of the different permutations to
// avoid code duplication. The first template parameter is a search functor for
// STXXL.
class Permutation {
 public:
  /// Identifiers for the six possible permutations.
  enum struct Enum { PSO, POS, SPO, SOP, OPS, OSP };
  // Unfortunately there is a bug in GCC that doesn't allow use to simply use
  // `using enum`.
  static constexpr auto PSO = Enum::PSO;
  static constexpr auto POS = Enum::POS;
  static constexpr auto SPO = Enum::SPO;
  static constexpr auto SOP = Enum::SOP;
  static constexpr auto OPS = Enum::OPS;
  static constexpr auto OSP = Enum::OSP;
  using MetaData = IndexMetaDataMmapView;
  using Allocator = ad_utility::AllocatorWithLimit<Id>;
  Permutation(string name, string suffix, array<unsigned short, 3> order,
              Allocator allocator)
      : _readableName(std::move(name)),
        _fileSuffix(std::move(suffix)),
        _keyOrder(order),
        _reader(std::move(allocator)) {}

  // everything that has to be done when reading an index from disk
  void loadFromDisk(const std::string& onDiskBase) {
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

  /// For a given ID for the first column, retrieve all IDs of the second and
  /// third column, and store them in `result`. This is just a thin wrapper
  /// around `CompressedRelationMetaData::scan`.
  template <typename IdTableImpl>
  void scan(Id col0Id, IdTableImpl* result,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const {
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
  /// For given IDs for the first and second column, retrieve all IDs of the
  /// third column, and store them in `result`. This is just a thin wrapper
  /// around `CompressedRelationMetaData::scan`.
  template <typename IdTableImpl>
  void scan(Id col0Id, Id col1Id, IdTableImpl* result,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const {
    if (!_meta.col0IdExists(col0Id)) {
      return;
    }
    const auto& metaData = _meta.getMetaData(col0Id);

    return _reader.scan(metaData, col1Id, _meta.blockData(), _file, result,
                        timer);
  }

  // _______________________________________________________
  void setKbName(const string& name) { _meta.setName(name); }

  // for Log output, e.g. "POS"
  const std::string _readableName;
  // e.g. ".pos"
  const std::string _fileSuffix;
  // order of the 3 keys S(0), P(1), and O(2) for which this permutation is
  // sorted. Needed for the createPermutation function in the Index class
  // e.g. {1, 0, 2} for PsO
  const array<unsigned short, 3> _keyOrder;

  const MetaData& metaData() const { return _meta; }
  MetaData _meta;

  mutable ad_utility::File _file;

  CompressedRelationReader _reader;

  bool _isLoaded = false;
};
