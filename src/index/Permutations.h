// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
#pragma once

#include <array>
#include <string>
#include "./StxxlSortFunctors.h"

namespace Permutation {
using std::array;
using std::string;

// helper class to store static properties of the different permutations
// to avoid code duplication
// The template Parameter is a STXXL search functor
template <class Comparator>
class PermutationImpl {
 public:
  PermutationImpl(const Comparator& comp, string name, string suffix,
                  array<unsigned short, 3> order)
      : _comp(comp),
        _readableName(std::move(name)),
        _fileSuffix(std::move(suffix)),
        _keyOrder(order) {}

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
  };

  // instantiations for the 6 Permutations used in QLever
  // They simplify the creation of permutations in the index class
  const PermutationImpl<SortByPOS> Pos(SortByPOS(), "POS", ".pos", {1, 2, 0});
  const PermutationImpl<SortByPSO> Pso(SortByPSO(), "PSO", ".pso", {1, 0, 2});
  const PermutationImpl<SortBySOP> Sop(SortBySOP(), "SOP", ".sop", {0, 2, 1});
  const PermutationImpl<SortBySPO> Spo(SortBySPO(), "SPO", ".spo", {0, 1, 2});
  const PermutationImpl<SortByOPS> Ops(SortByOPS(), "OPS", ".ops", {2, 1, 0});
  const PermutationImpl<SortByOSP> Osp(SortByOSP(), "OSP", ".osp", {2, 0, 1});
}

