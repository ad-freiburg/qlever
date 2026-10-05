// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_GLOBAL_GLOBAL_DATATYPE_H
#define QLEVER_SRC_GLOBAL_GLOBAL_DATATYPE_H

#include <array>
#include <string_view>

#include "backports/keywords.h"
#include "util/Algorithm.h"
#include "util/Exception.h"

// The different Datatypes that a `Id` (see below) can encode.
// Note: If you add a datatype, make sure to update the `MaxValue` if necessary,
// and check whether you have to add it to the `isDatatypeTrivial` function
// directly below.
enum struct Datatype {
  Undefined = 0,
  Bool,
  Int,
  Double,
  VocabIndex,
  LocalVocabIndex,
  // See `index/vocabulary/SecondaryVocabulary.h`. NOTE: The position of this
  // datatype is not arbitrary. It has to be greater than `VocabIndex` (the
  // words of a secondary vocabulary are all sorted after the words of the main
  // vocabulary), and it has to be directly adjacent to `VocabIndex` and
  // `LocalVocabIndex`, which makes the comparison of an `Id` of type
  // `LocalVocabIndex` with an `Id` of an unrelated datatype cheap, see
  // `Id::compareThreeWay`.
  SecondaryVocabIndex,
  TextRecordIndex,
  Date,
  GeoPoint,
  WordVocabIndex,
  BlankNodeIndex,
  EncodedVal,
  MaxValue = EncodedVal
  // Note: Unfortunately, we cannot easily get the size of an enum.
  // If members are added to this enum, then the `MaxValue`
  // alias must always be equal to the last member,
  // else other code breaks with out-of-bounds accesses.
};

// Return true iff the `datatype` is a trivial datatype. This means that IDs
// with this datatype directly encode the value they represent and do not point
// to an external resource. In other words: These IDs can safely be shared
// across different QLever indices without having to rewrite them. Note:
// `BlankNodeIndex` is deliberately NOT considered trivial, as blank nodes
// depend on the context, in particular they have to be remapped when results
// from different  RDF sources are merged. Same goes for `EncodedVal` which
// depends on the (configurable!) prefixes for the encoding.
constexpr bool isDatatypeTrivial(Datatype datatype) {
  using enum Datatype;
  constexpr std::array trivialDatatypes{Undefined, Bool, Int,
                                        Double,    Date, GeoPoint};
  return ad_utility::contains(trivialDatatypes, datatype);
}

// Convert the `Datatype` enum to the corresponding string
inline QL_CONSTEXPR std::string_view toString(Datatype type) {
  switch (type) {
    case Datatype::Undefined:
      return "Undefined";
    case Datatype::Bool:
      return "Bool";
    case Datatype::Double:
      return "Double";
    case Datatype::Int:
      return "Int";
    case Datatype::EncodedVal:
      return "EncodedIri";
    case Datatype::VocabIndex:
      return "VocabIndex";
    case Datatype::LocalVocabIndex:
      return "LocalVocabIndex";
    case Datatype::TextRecordIndex:
      return "TextRecordIndex";
    case Datatype::WordVocabIndex:
      return "WordVocabIndex";
    case Datatype::Date:
      return "Date";
    case Datatype::GeoPoint:
      return "GeoPoint";
    case Datatype::BlankNodeIndex:
      return "BlankNodeIndex";
    case Datatype::SecondaryVocabIndex:
      return "SecondaryVocabIndex";
  }
  // This line is reachable if we cast an arbitrary invalid int to this enum
  AD_FAIL();
}

#endif  // QLEVER_SRC_GLOBAL_GLOBAL_DATATYPE_H
