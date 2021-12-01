//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

/// Several types and functions for the managing of MediaTypes like
/// `application/json`.

#ifndef QLEVER_MEDIATYPES_H
#define QLEVER_MEDIATYPES_H

#include <string>
#include <vector>

#include "../HashMap.h"

namespace ad_utility {

/// A (far from complete) enum for different mime types.
enum class MediaType {
  defaultType,
  html,
  css,
  textPlain,
  javascript,
  json,
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
  textApplication
};

namespace detail {

// Connect a `MediaType` (enum entry) with its string representation and several
// filetypes that imply this media type.
struct MediaTypeImpl {
  MediaTypeImpl(MediaType mediaType, std::string asString,
                std::vector<std::string> fileSuffixes)
      : _mediaType{mediaType},
        _asString{std::move(asString)},
        _fileSuffixes{std::move(fileSuffixes)} {}
  MediaType _mediaType;
  std::string _asString;
  std::vector<std::string> _fileSuffixes;
};

// Return a static vector of all possible media types and their associated
// filenames. Modify this function if you need to add or change several of the
// media types.
const std::vector<MediaTypeImpl>& getAllMediaTypes();

// Return a static map from file suffixes (e.g. ".json") to media types
// (`MediaType::json`).
const auto& getSuffixToMediaTypeStringMap();

// Return a map from `MediaType`s to the corresponding media type string.
const auto& getMediaTypeToStringMap();

}  // namespace detail

// For a given filename (e.g. "index.html") compute the corresponding media type
// ("text/html"). Unknown file suffixes will result in the media type
// "application/text".
const std::string& mediaTypeForFilename(std::string_view filename);

/// Convert a `MediaType` to the corresponding media type string.
const std::string& toString(MediaType t);

}  // namespace ad_utility

#endif  // QLEVER_MEDIATYPES_H
