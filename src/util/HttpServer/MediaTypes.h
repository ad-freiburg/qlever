//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

/// Several types and functions for the managing of MediaTypes like
/// `application/json`.

#ifndef QLEVER_MEDIATYPES_H
#define QLEVER_MEDIATYPES_H

#include <absl/types/optional.h>

#include <compare>
#include <string>
#include <vector>

#include "../HashMap.h"
#include "../HashSet.h"

namespace ad_utility {

/// A (far from complete) enum for different mime types.
enum class MediaType {
  defaultType,
  html,
  css,
  textPlain,
  javascript,
  json,
  sparqlJson,
  qleverJson,
  xml,
  flash,
  flv,
  png,
  jpeg,
  gif,
  bmp,
  ico,
  tiff,
  svg,
  tsv,
  csv,
  textApplication,
  turtle,
  octetStream
};

struct MediaTypeWithQuality {
  struct Wildcard {};  // Represents the total wildcard */*

  // Represents "image/*" (subtype wildcard)
  struct TypeWithWildcard {
    std::string _type;
  };

  using Variant = absl::variant<Wildcard, TypeWithWildcard, MediaType>;
  friend std::weak_ordering operator<=>(const Variant& a, const Variant& b) {
    return a.index() <=> b.index();
  }

  float _qualityValue;
  Variant _mediaType;

  // Order first by the qualities, and then by the specifity of the type.
  std::partial_ordering operator<=>(const MediaTypeWithQuality& rhs) const {
    if (auto cmp = _qualityValue <=> rhs._qualityValue; cmp != 0) {
      return cmp;
    }
    return _mediaType <=> rhs._mediaType;
  }
};

namespace detail {

// Connect a `MediaType` (enum entry) with its string representation and several
// filetypes that imply this media type.
struct MediaTypeImpl {
  MediaTypeImpl(MediaType mediaType, std::string type, std::string subtype,
                std::vector<std::string> fileSuffixes)
      : _mediaType{mediaType},
        _type{std::move(type)},
        _subtype{std::move(subtype)},
        _asString{_type + '/' + _subtype},
        _fileSuffixes{std::move(fileSuffixes)} {}
  MediaType _mediaType;
  std::string _type;
  std::string _subtype;
  std::string _asString;
  std::vector<std::string> _fileSuffixes;
};

// Return a static vector of all possible media types and their associated
// filenames. Modify this function if you need to add or change several of the
// media types.
const ad_utility::HashMap<MediaType, MediaTypeImpl>& getAllMediaTypes();

// Return a static map from file suffixes (e.g. ".json") to media types
// (`MediaType::json`).
const auto& getSuffixToMediaTypeStringMap();

// Return a map from strings like "application/json" to MediaTypes.
const auto& getStringToMediaTypeMap();

}  // namespace detail

// For a given filename (e.g. "index.html") compute the corresponding media type
// ("text/html"). Unknown file suffixes will result in the media type
// "application/text".
const std::string& mediaTypeForFilename(std::string_view filename);

/// Convert a `MediaType` to the corresponding media type string.
const std::string& toString(MediaType t);

/// Convert a  `MediaType` to the corresponding "basic" type. For example
/// `MediaType::json` represents "application/json" so this function will return
/// "application".
const std::string& getType(MediaType t);

/// Convert a string like "application/json" to the appropriate media type.
/// If no corresponding `MediaType` exists, `absl::nullopt` is returned;
/// The comparison is case insensitive: "Application/JSON" would also
/// match the json media type.
[[nodiscard]] absl::optional<MediaType> toMediaType(std::string_view s);

/// Parse the value of an `HTTP Accept` header field. Currently does not support
/// wildcards, quality parameters and other parameters. The media types are
/// already sorted (highest quality first, more specific types first if the
/// quality is the same). Throws on error.
std::vector<MediaTypeWithQuality> parseAcceptHeader(
    std::string_view acceptHeader, std::vector<MediaType> supportedMediaTypes);

/// Parse `acceptHeader`, and determine which of the `supportedMediaTypes`
/// has the highest priority, and return this type. If several mediaTypes have
/// the same priority (e.g. because of a wildcard in `acceptHeader`) then
/// media types that appear earlier in the `supportedMediaTypes`. If none of the
/// `supportedMediaTypes` is accepted by `acceptHeader`, then `absl::nullopt`
/// is returned.
absl::optional<MediaType> getMediaTypeFromAcceptHeader(
    std::string_view acceptHeader,
    const std::vector<MediaType>& supportedMediaTypes);

/// Return an error message, which reports that only the `supportedMediaTypes`
/// are supported.
std::string getErrorMessageForSupportedMediaTypes(
    const std::vector<MediaType>& supportedMediaTypes);
}  // namespace ad_utility

#endif  // QLEVER_MEDIATYPES_H
