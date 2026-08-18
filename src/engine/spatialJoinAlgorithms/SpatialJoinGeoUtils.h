// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025        Patrick Brosi <brosi@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINGEOUTILS_H
#define QLEVER_SRC_ENGINE_SPATIALJOINGEOUTILS_H

#include <util/geo/Geo.h>

#include <optional>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/Index.h"
#include "rdfTypes/GeoSparqlHelpers.h"

// Forward declaration, only used as a return type below.
class S2Polyline;

// Free functions used to prepare geometries for a spatial join. Unlike the
// classes in `SpatialJoinAlgorithmBase.h`, these don't need any of the state of
// a concrete spatial join algorithm (input tables, join columns, etc.), and
// several of them are needed by more than one algorithm, or by code that
// isn't part of any algorithm at all (parsing the index, building an `S2`
// index for a materialized view). Helpers used by only a single algorithm
// live as static methods on that algorithm's class instead (e.g.
// `LibspatialjoinAlgorithm::sweeperConfig`).
namespace ad_utility::detail::spatialjoin {

// Helper for `LibspatialjoinAlgorithm::parse` to check the
// bounding box (only if available from a `GeoVocabulary`) of a given
// vocabulary entry against the `prefilterLatLngBox`. Returns `true` if the
// geometry can be discarded just by the bounding box. If the bounding box is
// already loaded (for example from a materialized view), it can prefilter in
// memory. Otherwise on-disk `GeometryInfo` will be used. Then this should
// only be applied if the index is known to be built on a `GeoVocabulary`.
bool prefilterGeoByBoundingBox(
    const std::optional<::util::geo::DBox>& prefilterLatLngBox,
    const Index& index, VocabIndex vocabIndex,
    const std::optional<ad_utility::BoundingBox>& precomputedBoundingBox);

// Returns a GeoPoint if the element of the given table represents a GeoPoint.
// Used by the S2-based algorithms as well as `RtreeEntryAlgorithm`.
std::optional<GeoPoint> getPoint(const IdTableView<0>* restable, size_t row,
                                 ColumnIndex col);

// Retrieves and parses a line string from the given cell of an `IdTable` and
// converts it to an `S2Polyline`. Used when building the `S2` index for a
// materialized view (see `SpatialJoinCachedIndex.cpp`).
std::optional<S2Polyline> getPolyline(const IdTableView<0>& restable,
                                      size_t row, ColumnIndex col,
                                      const Index& index);

}  // namespace ad_utility::detail::spatialjoin

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINGEOUTILS_H
