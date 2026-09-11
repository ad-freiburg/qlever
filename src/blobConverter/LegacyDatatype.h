// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BLOBCONVERTER_LEGACYDATATYPE_H
#define QLEVER_SRC_BLOBCONVERTER_LEGACYDATATYPE_H

#include <cstdint>

#include "global/ValueId.h"

// The layout of an `Id` in the legacy blob format that is written by the
// `demo-v1-c++17_unimodel` branch of the `qlever-bmw` fork. An `Id` consists of
// four datatype bits followed by 60 data bits, exactly as in the current
// format, but the `Datatype` enum of the legacy format lacks the
// `SecondaryVocabIndex` that the current format has inserted after
// `LocalVocabIndex`, so all datatypes from `TextRecordIndex` on have a value
// that is one smaller than in the current format. This is the same `Datatype`
// enum as that of the previous index format, so the conversion of the datatype
// bits is shared with `index/IndexFormatConverter.h`.
namespace qlever::blobConverter {

// The `Datatype` enum of the legacy format.
enum struct LegacyDatatype : uint64_t {
  Undefined = 0,
  Bool,
  Int,
  Double,
  VocabIndex,
  LocalVocabIndex,
  TextRecordIndex,
  Date,
  GeoPoint,
  WordVocabIndex,
  BlankNodeIndex,
  EncodedVal,
  MaxValue = EncodedVal
};

// The number of legacy datatypes.
inline constexpr size_t numLegacyDatatypes =
    static_cast<size_t>(LegacyDatatype::MaxValue) + 1;

// Extract the datatype bits of the `legacyBits` of a legacy `Id`. NOTE: The
// result may be out of range of the `LegacyDatatype` enum if the input is
// corrupt, so it is returned as a plain integer.
inline uint64_t legacyDatatypeBits(uint64_t legacyBits) {
  return legacyBits >> ValueId::numDataBits;
}

// Extract the 60 data bits of the `legacyBits` of a legacy `Id`.
inline uint64_t legacyDataBits(uint64_t legacyBits) {
  return legacyBits & ValueId::maxIndex;
}

// Combine a `LegacyDatatype` and 60 data bits into the bits of a legacy `Id`.
inline uint64_t makeLegacyBits(LegacyDatatype datatype, uint64_t dataBits) {
  return (static_cast<uint64_t>(datatype) << ValueId::numDataBits) | dataBits;
}

// Return the name of a `LegacyDatatype`, for the statistics of a conversion.
inline const char* toString(LegacyDatatype datatype) {
  switch (datatype) {
    case LegacyDatatype::Undefined:
      return "Undefined";
    case LegacyDatatype::Bool:
      return "Bool";
    case LegacyDatatype::Int:
      return "Int";
    case LegacyDatatype::Double:
      return "Double";
    case LegacyDatatype::VocabIndex:
      return "VocabIndex";
    case LegacyDatatype::LocalVocabIndex:
      return "LocalVocabIndex";
    case LegacyDatatype::TextRecordIndex:
      return "TextRecordIndex";
    case LegacyDatatype::Date:
      return "Date";
    case LegacyDatatype::GeoPoint:
      return "GeoPoint";
    case LegacyDatatype::WordVocabIndex:
      return "WordVocabIndex";
    case LegacyDatatype::BlankNodeIndex:
      return "BlankNodeIndex";
    case LegacyDatatype::EncodedVal:
      return "EncodedVal";
  }
  return "Unknown";
}

}  // namespace qlever::blobConverter

#endif  // QLEVER_SRC_BLOBCONVERTER_LEGACYDATATYPE_H
