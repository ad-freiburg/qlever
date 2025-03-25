//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

/// Several types and functions for the managing of MediaTypes like
/// `application/json`.

#ifndef QLEVER_MEDIATYPES_H
#define QLEVER_MEDIATYPES_H

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "../HashMap.h"
#include "../HashSet.h"

namespace ad_utility {

/// A minimal enum for different mime types.
enum class MediaType {
  textPlain,
  json,
  sparqlJson,
  sparqlXml,
  qleverJson,
  tsv,
  csv,
  turtle,
  ntriples,
  octetStream
};

struct MediaTypeWithQuality {
  struct Wildcard {};  // Represents the total wildcard */*

  // Represents "image/*" (subtype wildcard)
  struct TypeWithWildcard {
    std::string _type;
  };

  using Variant = std::variant<Wildcard, TypeWithWildcard, MediaType>;
  friend std::weak_ordering operator<=>(const Variant& a, const Variant& b) {
    return a.index() <=> b.index();
  }

  float _qualityValue;
  Variant _mediaType;

  // Order first by the qualities, and then by the specificity of the type.
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

// Return a map from strings like "application/json" to MediaTypes.
const auto& getStringToMediaTypeMap();

}  // namespace detail

/// Convert a `MediaType` to the corresponding media type string.
const std::string& toString(MediaType t);

/// Convert a  `MediaType` to the corresponding "basic" type. For example
/// `MediaType::json` represents "application/json" so this function will return
/// "application".
const std::string& getType(MediaType t);

/// Convert a string like "application/json" to the appropriate media type.
/// If no corresponding `MediaType` exists, `std::nullopt` is returned;
/// The comparison is case insensitive: "Application/JSON" would also
/// match the json media type.
[[nodiscard]] std::optional<MediaType> toMediaType(std::string_view s);

/// Parse the value of an `HTTP Accept` header field. Currently does not support
/// wildcards, quality parameters and other parameters. The media types are
/// already sorted (highest quality first, more specific types first if the
/// quality is the same). Throws on error.
std::vector<MediaTypeWithQuality> parseAcceptHeader(
    std::string_view acceptHeader);

/// Parse `acceptHeader`, and determine which of the `SUPPORTED_MEDIA_TYPES`
/// has the highest priority, and return this type. If several mediaTypes have
/// the same priority (e.g. because of a wildcard in `acceptHeader`) then
/// media types that appear earlier in the `SUPPORTED_MEDIA_TYPES`. If none of
/// the `SUPPORTED_MEDIA_TYPES` is accepted by `acceptHeader`, then
/// `std::nullopt` is returned.
// TODO: This function never returns `nullopt`, because an exception is thrown
// if no supported media type is found. Update the docstring and make the return
// type just `MediaType`.
std::optional<MediaType> getMediaTypeFromAcceptHeader(
    std::string_view acceptHeader);

/// Return an error message, which reports that only the `SUPPORTED_MEDIA_TYPES`
/// are supported.
std::string getErrorMessageForSupportedMediaTypes();
}  // namespace ad_utility

#endif  // QLEVER_MEDIATYPES_H
