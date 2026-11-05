// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "engine/binaryExport/BinaryFormat.h"

#include <array>

#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/http/MediaTypes.h"

namespace qlever::binary_export {

// The format transfers the bits of `Id`s directly, so a change of their
// representation requires increasing `ad_utility::binaryQleverExportVersion`.
// The following assertions only detect a change of the datatype tags.
static_assert(ad_utility::binaryQleverExportVersion == 1);
static_assert(Id::numDatatypeBits == 4);
static constexpr auto datatypesOfVersion1 = []() {
  using enum Datatype;
  return std::to_array({Undefined, Bool, Int, Double, VocabIndex,
                        LocalVocabIndex, SecondaryVocabIndex, TextRecordIndex,
                        Date, GeoPoint, WordVocabIndex, BlankNodeIndex,
                        EncodedVal});
}();
static_assert(isListOfAllDatatypesInOrder(datatypesOfVersion1));

// _____________________________________________________________________________
std::string Header::serialize() const {
  ad_utility::serialization::ByteBufferWriteSerializer serializer;
  nlohmann::json patterns;
  detail::patternsToJson(patterns, patterns_,
                         EncodedIriManager::NumBitsEncoding);
  serializer << std::string{magicBytes};
  serializer << ad_utility::binaryQleverExportVersion;
  serializer << geoPointEncoding_;
  serializer << patterns.dump();
  serializer << variableNames_;
  serializer << unboundVariableNames_;
  return std::string{serializer.data().data(), serializer.data().size()};
}

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
