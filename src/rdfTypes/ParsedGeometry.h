// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_RDFTYPES_PARSEDGEOMETRY_H
#define QLEVER_SRC_RDFTYPES_PARSEDGEOMETRY_H

#include <spatialjoin/GeometryStore.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "util/serializer/Serializer.h"

namespace ad_utility {

// The version of the stored form of a `ParsedGeometry` (see
// `GeoVocabulary`). It must be increased when the serialization of `Part`
// changes, or when `libspatialjoin` changes the format of the geometries in
// its `sj::GeometryStore`. The parsed geometries of an index with another
// version are not used (see `GeoVocabulary::open`).
constexpr uint64_t PARSED_GEOMETRY_VERSION = 2;

// A WKT literal that was parsed and preprocessed by `libspatialjoin` at index
// build time, so that a spatial join can add it to its `sj::GeometryStore`
// without parsing it at query time. The `GeoVocabulary` stores this for the WKT
// literals of at least a configurable length (see the index builder option
// `--parsed-geometries-min-length`), because parsing and preprocessing a huge
// geometry (a country boundary with millions of points) takes seconds, while
// the same for an ordinary geometry takes microseconds.
//
// A literal yields one part per geometry of a `MULTI...` or
// `GEOMETRYCOLLECTION` and one part otherwise, exactly as
// `sj::GeometryStore::add` hands geometries to the store. A part is what
// `sj::GeometryStore::add` produces for a geometry (an `sj::WriteCand`): the
// metadata from which `libspatialjoin` creates the two sweep events, the sub
// id, and the serialized geometry for the store. One thing in a part depends
// on the query and is set by `addToBatch`: the id of the geometry (its row in
// the input of the spatial join), which `libspatialjoin` serializes in front
// of the geometry.
//
// NOTE: The bounding boxes of a part are not padded. `libspatialjoin` pads
// them for a `WITHIN_DIST` join when it creates the sweep events.
class ParsedGeometry {
 public:
  struct Part {
    // The bounding box and the diagonal bounding box of the geometry and its
    // number of anchor points (see `sj::PreparedGeomMeta`).
    ::util::geo::I32Box boundingBox_;
    ::util::geo::I32Box diagonalBoundingBox_;
    size_t numAnchors_;
    sj::GeomType type_;
    ::util::geo::I32Point rightPoint_;
    size_t subid_;
    // The serialized geometry (see `sj::WriteCand::raw`) without the id of the
    // geometry at its beginning.
    std::string rawWithoutId_;

    AD_SERIALIZE_FRIEND_FUNCTION(Part) {
      ad_utility::serialization::triviallySerialize(serializer,
                                                    arg.boundingBox_);
      ad_utility::serialization::triviallySerialize(serializer,
                                                    arg.diagonalBoundingBox_);
      serializer | arg.numAnchors_;
      ad_utility::serialization::triviallySerialize(serializer, arg.type_);
      ad_utility::serialization::triviallySerialize(serializer,
                                                    arg.rightPoint_);
      serializer | arg.subid_;
      serializer | arg.rawWithoutId_;
    }
  };

 private:
  std::vector<Part> parts_;

  // True iff the literal has several geometries. `libspatialjoin` keeps them
  // together as the members of a multi geometry (see `sj::MultiWriteCand`).
  bool isMulti_ = false;

  // The id with which the geometry is added to the store when parsing. It is
  // replaced by the actual id in `addToBatch`. It is longer than
  // `sj::FOLD_MAX_LEN`, because for shorter ids `sj::GeometryStore::add` folds
  // the id of a small geometry into its sweep events ("folded" geometry types)
  // instead of serializing the geometry.
  static constexpr std::string_view placeholderId = "999999999";
  static_assert(placeholderId.size() > sj::FOLD_MAX_LEN);

 public:
  // Parse `wktLiteral` (with quotes and datatype, as stored in the vocabulary)
  // and preprocess it with `store`, which must be configured with
  // `spatialJoinConfig`. Return `std::nullopt` if `libspatialjoin` yields no
  // geometry for the literal (in particular, for invalid WKT), or if it has a
  // line with two points (which `libspatialjoin` stores without its points).
  //
  // NOTE: Nothing is added to `store`. It only provides the preprocessing.
  static std::optional<ParsedGeometry> fromWktLiteral(
      std::string_view wktLiteral, sj::GeometryStore& store);

  // The `libspatialjoin` configuration for `fromWktLiteral`, without callbacks
  // and for a single thread. A spatial join that wants to use the parsed
  // geometries must use the same geometry options (box ids, diagonal boxes),
  // because the parts depend on them.
  static sj::SpatialJoinCfg spatialJoinConfig();

  // Add the geometry to `batch`, as the geometry with the given `row` of the
  // input of the spatial join, like `sj::GeometryStore::add` does for a
  // geometry parsed at query time (the batch is then handed to
  // `sj::GeometryStore::addBatch`). Return the bounding box of the geometry,
  // padded for a `WITHIN_DIST` join according to the configuration of `store`.
  //
  // NOTE: Unlike `sj::GeometryStore::add`, this ignores the filter box of
  // `store`. The filter box only saves work, it is not needed for correctness.
  ::util::geo::I32Box addToBatch(const sj::GeometryStore& store, size_t row,
                                 sj::WriteBatch& batch) const;

  const std::vector<Part>& parts() const { return parts_; }
  bool isMulti() const { return isMulti_; }

  // Serialization to and from a byte string (for storing on disk).
  std::string toBytes() const;
  static ParsedGeometry fromBytes(std::string_view bytes);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_RDFTYPES_PARSEDGEOMETRY_H
