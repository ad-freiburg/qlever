// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_GLOBAL_DATATYPE_H
#define QLEVER_SRC_GLOBAL_DATATYPE_H

#include <array>
#include <string_view>

#include "backports/keywords.h"
#include "util/Algorithm.h"
#include "util/Exception.h"

// The different datatypes that an `Id` (see `ValueId.h`) can encode.
//
// NOTE: If you add a datatype, keep `MaxValue` equal to the last member and
// check whether the new datatype belongs into `isDatatypeTrivial` below.
enum struct Datatype {
  Undefined = 0,
  Bool,
  Int,
  Double,
  VocabIndex,
  LocalVocabIndex,
  // See `index/vocabulary/SecondaryVocabulary.h`.
  //
  // NOTE: The position of this datatype is not arbitrary. It has to be greater
  // than `VocabIndex` (the words of a secondary vocabulary are all sorted after
  // the words of the main vocabulary), and it has to be directly adjacent to
  // `VocabIndex` and `LocalVocabIndex`, which makes the comparison of an `Id`
  // of type `LocalVocabIndex` with an `Id` of an unrelated datatype cheap, see
  // `Id::compareThreeWay`.
  SecondaryVocabIndex,
  TextRecordIndex,
  Date,
  GeoPoint,
  WordVocabIndex,
  BlankNodeIndex,
  EncodedVal,
  // Always the last member (there is no easy way to get the number of members
  // of an enum, and other code sizes its arrays by `MaxValue`).
  MaxValue = EncodedVal
};

// Return true iff `datatype` is trivial, that is, IDs with this datatype
// directly encode the value they represent and do not point to an external
// resource. Such IDs can be shared across different QLever indices without
// being rewritten.
//
// NOTE: `BlankNodeIndex` is deliberately NOT trivial, as blank nodes depend on
// the context (they have to be remapped when results from different RDF
// sources are merged). The same holds for `EncodedVal`, which depends on the
// configurable prefixes of the encoding.
constexpr bool isDatatypeTrivial(Datatype datatype) {
  using enum Datatype;
  constexpr std::array trivialDatatypes{Undefined, Bool, Int,
                                        Double,    Date, GeoPoint};
  return ad_utility::contains(trivialDatatypes, datatype);
}

// Convert the `Datatype` enum to the corresponding string.
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

#endif  // QLEVER_SRC_GLOBAL_DATATYPE_H
