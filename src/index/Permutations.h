// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
#pragma once

#include <array>
#include <string>

#include "../global/Constants.h"
#include "../util/File.h"
#include "../util/Log.h"
#include "./IndexMetaData.h"
#include "./StxxlSortFunctors.h"

namespace Permutation {
using std::array;
using std::string;

// Helper class to store static properties of the different permutations to
// avoid code duplication. The first template parameter is a search functor for
// STXXL.
template <class Comparator, class MetaDataT>
class PermutationImpl {
 public:
  using MetaData = MetaDataT;
  PermutationImpl(const Comparator& comp, string name, string suffix,
                  array<unsigned short, 3> order)
      : _comp(comp),
        _readableName(std::move(name)),
        _fileSuffix(std::move(suffix)),
        _keyOrder(order) {}

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
      AD_THROW(ad_semsearch::Exception::BAD_INPUT,
               "Could not open the index file " + filename +
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
    return CompressedRelationMetadata::scan(metaData, _meta.blockData(),
                                            _readableName, _file, result,
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

    return CompressedRelationMetadata::scan(metaData, col1Id, _meta.blockData(),
                                            _file, result, timer);
  }

  // _______________________________________________________
  void setKbName(const string& name) { _meta.setName(name); }

  // stxxl comparison functor
  const Comparator _comp;
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

  bool _isLoaded = false;
};

// Type aliases for the 6 permutations used by QLever
using POS_T = PermutationImpl<SortByPOS, IndexMetaDataHmap>;
using PSO_T = PermutationImpl<SortByPSO, IndexMetaDataHmap>;
using SOP_T = Permutation::PermutationImpl<SortBySOP, IndexMetaDataMmapView>;
using SPO_T = PermutationImpl<SortBySPO, IndexMetaDataMmapView>;
using OPS_T = PermutationImpl<SortByOPS, IndexMetaDataMmapView>;
using OSP_T = PermutationImpl<SortByOSP, IndexMetaDataMmapView>;

}  // namespace Permutation
