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

// Forward declaration of `IdTable`
class IdTable;

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
  Permutation(string name, string suffix, array<unsigned short, 3> order);

  // everything that has to be done when reading an index from disk
  void loadFromDisk(const std::string& onDiskBase);

  /// For a given ID for the first column, retrieve all IDs of the second and
  /// third column, and store them in `result`. This is just a thin wrapper
  /// around `CompressedRelationMetaData::scan`.
  void scan(Id col0Id, IdTable* result,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const;

  /// For given IDs for the first and second column, retrieve all IDs of the
  /// third column, and store them in `result`. This is just a thin wrapper
  /// around `CompressedRelationMetaData::scan`.
  void scan(Id col0Id, Id col1Id, IdTable* result,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const;

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
