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

#include "rdfTypes/GeoPoint.h"
#include "util/ContainersWithAllocator.h"
#include "util/Timer.h"

namespace qlever::raster {

// The value of one pixel: `int64_t` for integer bands, `double` for floating
// point bands.
using PixelValue = std::variant<int64_t, double>;

// The values of one band, one per pixel. Empty where the band is NoData.
using BandValues = qlm::vector<std::optional<PixelValue>>;

// What a lookup returns, the same for every geometry of a query.
struct LookupOptions {
  // The bands to return, numbered from 1 as in GDAL, in the order of the
  // result columns.
  std::vector<int> bands_;
  // With a radius in metres, the geometry is first widened by that radius.
  std::optional<double> radius_;
  // Whether to return the center of each pixel. Computing the centers is
  // expensive, so a query without `raster:center` skips them.
  bool wantCenters_ = true;
};

// Pixels with their values in several bands and, if requested, their centers,
// stored column by column. Entry `i` of every vector belongs to pixel `i`.
// The vectors use the given allocator, so they count towards its memory limit.
struct PixelTable {
  // The number of pixels, which is the length of every column.
  size_t numPixels_ = 0;
  // Empty if the lookup did not ask for centers.
  qlm::vector<GeoPoint> centers_;
  // One entry per requested band, in the order of the request.
  std::vector<BandValues> valuesPerBand_;

  // An empty table for `numBands` bands.
  PixelTable(size_t numBands, const qlm::Allocator<char>& allocator);

  // A table from complete columns. Every band must have `numPixels` values,
  // and `centers` must have `numPixels` entries or none.
  PixelTable(size_t numPixels, qlm::vector<GeoPoint> centers,
             std::vector<BandValues> valuesPerBand);
};

// How much work the lookups of one `RasterFile` did, summed over all lookups.
// The times are measured once per geometry, never once per pixel.
struct LookupStatistics {
  // A point lookup reads one pixel directly, a shape lookup draws a mask
  // first.
  size_t numPointLookups_ = 0;
  size_t numShapeLookups_ = 0;
  // `numWindowPixels_` counts the pixels of all windows that were read, and
  // `numPixelsInsideGeometry_` those of them that lie inside the geometry.
  size_t numWindowPixels_ = 0;
  size_t numPixelsInsideGeometry_ = 0;
  // The time of all point lookups, each measured as a whole.
  ad_utility::Timer::Duration timePointLookups_{};
  // Each time is the sum of one step of the shape lookups over all lookups.
  ad_utility::Timer::Duration timeParsingGeometries_{};
  ad_utility::Timer::Duration timeProjectingGeometries_{};
  ad_utility::Timer::Duration timeGrowingGeometries_{};
  ad_utility::Timer::Duration timeDrawingMasks_{};
  ad_utility::Timer::Duration timeReadingPixels_{};
  ad_utility::Timer::Duration timeCollectingPixels_{};
  ad_utility::Timer::Duration timeConvertingCenters_{};

  // Add the statistics of `other`, for example of another file in the same
  // query.
  LookupStatistics& operator+=(const LookupStatistics& other) {
    numPointLookups_ += other.numPointLookups_;
    numShapeLookups_ += other.numShapeLookups_;
    numWindowPixels_ += other.numWindowPixels_;
    numPixelsInsideGeometry_ += other.numPixelsInsideGeometry_;
    timePointLookups_ += other.timePointLookups_;
    timeParsingGeometries_ += other.timeParsingGeometries_;
    timeProjectingGeometries_ += other.timeProjectingGeometries_;
    timeGrowingGeometries_ += other.timeGrowingGeometries_;
    timeDrawingMasks_ += other.timeDrawingMasks_;
    timeReadingPixels_ += other.timeReadingPixels_;
    timeCollectingPixels_ += other.timeCollectingPixels_;
    timeConvertingCenters_ += other.timeConvertingCenters_;
    return *this;
  }
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

  // Return the statistics of all lookups since this file was opened.
  const LookupStatistics& statistics() const;

  // All lookups return one column of values per entry of `options.bands_`, in
  // the same order. A pixel is left out if it is NoData in all of these bands.

  // The pixel under `point`, or nothing if the point is outside the grid. With
  // a radius, the pixels with their center inside the circle of that radius
  // around `point`.
  PixelTable lookupPoint(const GeoPoint& point, const LookupOptions& options,
                         const qlm::Allocator<char>& allocator);

  // The pixels under a point, on a line, or with their center inside a polygon
  // of `wktLiteral`, without duplicates. With a radius, the geometry is first
  // widened by that radius.
  PixelTable lookupGeometry(std::string_view wktLiteral,
                            const LookupOptions& options,
                            const qlm::Allocator<char>& allocator);
};

// The path of the raster file for `datasetName`, which is the name of a `.tif`
// file in the `rasters/` folder of the source tree, without `.tif`. Throws if
// there is no such file, so that a query can never choose a path itself. The
// folder is read once, so changes to it need a restart of the server.
std::string rasterFilePath(std::string_view datasetName);

}  // namespace qlever::raster

#endif  // QLEVER_SRC_ENGINE_RASTER_RASTERFILE_H
