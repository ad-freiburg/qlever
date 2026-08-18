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

#include <spatialjoin/Sweeper.h>
#include <util/geo/Geo.h>

#include <optional>

#include "engine/SpatialJoin.h"
#include "global/Id.h"
#include "index/Index.h"
#include "rdfTypes/GeoSparqlHelpers.h"
#include "util/MemorySize/MemorySize.h"

// Forward declaration, only used as a return type below.
class S2Polyline;

// Free functions used to prepare geometries for a spatial join. Unlike the
// classes in `SpatialJoinAlgorithms.h`, these don't need any of the state of
// a concrete spatial join algorithm (input tables, join columns, etc.) and
// are therefore also used independently of them, for example while parsing
// the index or when building an `S2` index for a materialized view.
namespace ad_utility::detail::spatialjoin {

// Helper for `libspatialjoinParse` (see `LibspatialjoinAlgorithm.h`) to check
// the bounding box (only if available from a `GeoVocabulary`) of a given
// vocabulary entry against the `prefilterLatLngBox`. Returns `true` if the
// geometry can be discarded just by the bounding box. If the bounding box is
// already loaded (for example from a materialized view), it can prefilter in
// memory. Otherwise on-disk `GeometryInfo` will be used. Then this should
// only be applied if the index is known to be built on a `GeoVocabulary`.
bool prefilterGeoByBoundingBox(
    const std::optional<::util::geo::DBox>& prefilterLatLngBox,
    const Index& index, VocabIndex vocabIndex,
    const std::optional<ad_utility::BoundingBox>& precomputedBoundingBox);

// Helper for `libspatialjoinParse` to get the bounding box from an `IdTable`
// if available.
std::optional<ad_utility::BoundingBox> getBoundingBoxFromIdTable(
    const IdTableView<0>* idTable,
    const SpatialJoinBoundingBoxColumns& boundingBoxes, size_t row);

// Retrieve the number of threads to be used for `libspatialjoinParse` and
// `LibspatialjoinAlgorithm::run`.
size_t getNumThreads();

// Returns a GeoPoint if the element of the given table represents a GeoPoint.
std::optional<GeoPoint> getPoint(const IdTableView<0>* restable, size_t row,
                                 ColumnIndex col);

// Retrieves and parses a line string from the given cell of an `IdTable` and
// converts it to an `S2Polyline`.
std::optional<S2Polyline> getPolyline(const IdTableView<0>& restable,
                                      size_t row, ColumnIndex col,
                                      const Index& index);

// Prepare a libspatialjoin `SweeperCfg`. The result doesn't have any of its
// callbacks set yet. Before feeding the configuration to a `Sweeper` you
// usually want to set `writeRelCb` and `sweepCancellationCb`. Also
// `withinDist` should be set if a proximity search is intended.
sj::SweeperCfg libspatialjoinSweeperConfig(
    size_t threads, ad_utility::MemorySize totalAllowedMemory = 8_GB);

}  // namespace ad_utility::detail::spatialjoin

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINGEOUTILS_H
