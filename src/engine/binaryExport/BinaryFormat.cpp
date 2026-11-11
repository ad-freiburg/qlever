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
#include <type_traits>

#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/http/MediaTypes.h"

namespace qlever::binary_export {

// The format transfers the bits of `Id`s directly, so a change of their
// representation requires increasing `ad_utility::binaryQleverExportVersion`.
// Version 1 transfers the `Id`s of the legacy layout (`Id::T` is a `uint64_t`)
// and version 1337 the ones of the split layout. The first assertion ties the
// version to the actual type of `Id::T`, so that a version that does not match
// the layout does not compile. The remaining assertions only detect a change
// of the datatype tags.
static_assert(ad_utility::binaryQleverExportVersion ==
              (std::is_same_v<Id::T, uint64_t> ? 1 : 1337));
#ifndef QLEVER_USE_SPLIT_LAYOUT
static_assert(Id::numDatatypeBits == 4);
#endif
static constexpr auto datatypesOfVersion1 = []() {
  using enum Datatype;
  return std::to_array({Undefined, Bool, Int, Double, VocabIndex,
                        LocalVocabIndex, SecondaryVocabIndex, TextRecordIndex,
                        Date, GeoPoint, WordVocabIndex, BlankNodeIndex,
                        EncodedVal});
}();
// The prototype adds a local-only datatype. Version 1's existing tags remain
// unchanged; triple terms must not be sent in that format until a dictionary
// transfer protocol exists (StringMapping rejects them explicitly).
static_assert([] {
  for (size_t i = 0; i < datatypesOfVersion1.size(); ++i) {
    if (static_cast<size_t>(datatypesOfVersion1[i]) != i) {
      return false;
    }
  }
  return static_cast<size_t>(Datatype::TripleTermIndex) ==
             datatypesOfVersion1.size() &&
         Datatype::MaxValue == Datatype::TripleTermIndex;
}());

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
