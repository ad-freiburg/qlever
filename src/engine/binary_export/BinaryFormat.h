// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BINARY_EXPORT_BINARYFORMAT_H
#define QLEVER_SRC_ENGINE_BINARY_EXPORT_BINARYFORMAT_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <absl/strings/str_cat.h>

#include <string>
#include <string_view>
#include <vector>

#include "global/Id.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "rdfTypes/GeoPoint.h"
#include "util/Serializer/AllocationLimitedReadSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/http/MediaTypes.h"

// The parts of QLever's binary export format (see `exportAsQLeverBinary` in
// `BinaryExport.h`) that both the exporting and the importing side need.
namespace qlever::binary_export {

static constexpr std::string_view magicBytes = "QLEVER.EXPORT";

// The marker that precedes each batch of strings (see `StringMapping`) in the
// stream of IDs. It is an undefined `Id` with additional bits set, which never
// occurs as an actual value (all undefined values are `Id::makeUndefined()`).
// The reader reads the first `Id` of each row and compares it to this marker
// to decide whether a row or a batch of strings follows. This is subtle: it
// only works because the marker is never the first `Id` of an actual row.
static constexpr Id::T vocabMarker = Id::makeUndefined().getBits() + 0b10101010;
static_assert(Id::fromBits(vocabMarker).getDatatype() == Datatype::Undefined);

// The header of the format. Its serialization starts with the `magicBytes` and
// the version, such that the reader can check the format and its version.
struct Header {
  // The encoding of the `GeoPoint`s of the exporting instance, see
  // `GeoPoint::encoding()`.
  GeoPointEncodingEnum geoPointEncoding_ = GeoPointEncodingEnum::ZOrder;
  // The patterns of the `EncodedIriManager` of the exporting instance, which
  // are transferred as JSON.
  std::vector<encodedIri::Pattern> patterns_;
  // The names of the variables, one per column.
  std::vector<std::string> variableNames_;
  // The names of the selected variables that are not bound in the result, and
  // therefore have no column. The reader could treat every missing variable
  // as unbound without them, but with them it can check that the result
  // contains exactly the variables it expects.
  std::vector<std::string> unboundVariableNames_;

  // Return the serialized header.
  std::string serialize() const;

  // Read a header that was written by `serialize` from the `serializer`. Each
  // of its strings, as well as each of its vectors of strings, may take up at
  // most `maxNumBytes`, which protects against allocating huge amounts of
  // memory for an invalid response.
  template <typename Serializer>
  static Header read(Serializer& serializer, size_t maxNumBytes) {
    namespace s = ad_utility::serialization;
    // The magic bytes are serialized as a string. Its size is checked first,
    // such that no memory is allocated for the size of an invalid response.
    std::string magic(magicBytes.size(), '\0');
    if (s::read<size_t>(serializer) == magic.size()) {
      serializer.serializeBytes(magic.data(), magic.size());
    }
    if (magic != magicBytes) {
      throw std::runtime_error{
          "The result is not in QLever's binary export format"};
    }
    auto version = s::read<uint16_t>(serializer);
    if (version != ad_utility::binaryQleverExportVersion) {
      throw std::runtime_error{
          absl::StrCat("The result is in version ", version,
                       " of QLever's binary export format, but only version ",
                       ad_utility::binaryQleverExportVersion, " is supported")};
    }
    Header header;
    header.geoPointEncoding_ = s::read<GeoPointEncodingEnum>(serializer);
    if (header.geoPointEncoding_ != GeoPointEncodingEnum::ZOrder &&
        header.geoPointEncoding_ != GeoPointEncodingEnum::LatMajor) {
      throw std::runtime_error{
          "The result in QLever's binary export format has an invalid "
          "encoding of its geographic points"};
    }
    header.patterns_ = detail::patternsFromJson(
        nlohmann::json::parse(
            s::readWithAllocationLimit<std::string>(serializer, maxNumBytes)),
        EncodedIriManager::NumBitsEncoding, EncodedIriManager::maxNumPrefixes_);
    using Strings = std::vector<std::string>;
    header.variableNames_ =
        s::readWithAllocationLimit<Strings>(serializer, maxNumBytes);
    header.unboundVariableNames_ =
        s::readWithAllocationLimit<Strings>(serializer, maxNumBytes);
    return header;
  }
};

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_BINARY_EXPORT_BINARYFORMAT_H
