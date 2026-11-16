// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_RASTER_RASTERFILE_H
#define QLEVER_SRC_ENGINE_RASTER_RASTERFILE_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "backports/span.h"
#include "rdfTypes/GeoPoint.h"
#include "util/ContainersWithAllocator.h"

namespace qlever::raster {

// The value of one pixel: `int64_t` for integer bands, `double` for floating
// point bands.
using PixelValue = std::variant<int64_t, double>;

// The values of one band, one per pixel. Empty where the band is NoData.
using BandValues = qlm::vector<std::optional<PixelValue>>;

// Pixels with their centers and their values in several bands, stored column
// by column. Entry `i` of every vector belongs to pixel `i`.
// The vectors use the given allocator, so they count towards its memory limit.
struct PixelTable {
  qlm::vector<GeoPoint> centers_;
  // One entry per requested band, in the order of the request.
  std::vector<BandValues> valuesPerBand_;

  // An empty table for `numBands` bands.
  PixelTable(size_t numBands, const qlm::Allocator<char>& allocator);

  // A table from complete columns, which must all have the same length.
  PixelTable(qlm::vector<GeoPoint> centers,
             std::vector<BandValues> valuesPerBand);

  size_t numPixels() const { return centers_.size(); }
};

// A raster file (for example a GeoTIFF), opened for lookups with geometries in
// CRS84 (longitude first, as in WKT). A `RasterFile` must not be used by two
// threads at the same time.
class RasterFile {
 private:
  // Holds the GDAL objects, so that this header does not include GDAL.
  struct Impl;
  std::unique_ptr<Impl> impl_;

 public:
  // Throws if the file cannot be opened, has no CRS, or has a band that is
  // neither integer nor floating point.
  explicit RasterFile(const std::string& path);
  ~RasterFile();
  RasterFile(RasterFile&&) noexcept;
  RasterFile& operator=(RasterFile&&) noexcept;

  // The number of bands. Bands are numbered from 1, as in GDAL.
  int numBands() const;

  // All lookups return one column of values per entry of `bands`, in the same
  // order. A pixel is left out if it is NoData in all of `bands`.

  // The pixel under `point`, or nothing if the point is outside the grid.
  PixelTable lookupPoint(const GeoPoint& point, ql::span<const int> bands,
                         const qlm::Allocator<char>& allocator);

  // The pixels with their center inside the circle of `radius` metres around
  // `center`.
  PixelTable lookupCircle(const GeoPoint& center, ql::span<const int> bands,
                          double radius, const qlm::Allocator<char>& allocator);

  // The pixels under a point, on a line, or with their center inside a polygon
  // of `wktLiteral`, without duplicates. With a `radius` in metres, the
  // geometry is first widened by that radius.
  PixelTable lookupGeometry(std::string_view wktLiteral,
                            ql::span<const int> bands,
                            std::optional<double> radius,
                            const qlm::Allocator<char>& allocator);
};

// The path of the raster file for `datasetName`, which is the name of a `.tif`
// file in the `rasters/` folder of the source tree, without `.tif`. Throws if
// there is no such file, so that a query can never choose a path itself. The
// folder is read once, so changes to it need a restart of the server.
std::string rasterFilePath(std::string_view datasetName);

}  // namespace qlever::raster

#endif  // QLEVER_SRC_ENGINE_RASTER_RASTERFILE_H
