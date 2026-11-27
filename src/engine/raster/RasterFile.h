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

#include "backports/filesystem.h"
#include "backports/span.h"
#include "rdfTypes/GeoPoint.h"
#include "util/ContainersWithAllocator.h"

namespace qlever::raster {

// The value of one pixel. Integer bands give an `int64_t`, floating point
// bands a `double`.
using PixelValue = std::variant<int64_t, double>;

// The values of one band, one per pixel. A value is empty where the band is
// NoData.
using BandValues = qlm::vector<std::optional<PixelValue>>;

// A table of pixels, stored column by column: their centers, and their values
// in each requested band. Entry `i` of every column belongs to pixel `i`. The
// columns use the given allocator, so they count towards its memory limit.
struct PixelTable {
  qlm::vector<GeoPoint> centers_;
  // There is one column per requested band, in the order of the request.
  std::vector<BandValues> valuesPerBand_;

  // Create an empty table for `numBands` bands.
  PixelTable(size_t numBands, const qlm::Allocator<char>& allocator);

  // Create a table from complete columns, which must all have the same length.
  PixelTable(qlm::vector<GeoPoint> centers,
             std::vector<BandValues> valuesPerBand);

  size_t numPixels() const { return centers_.size(); }
};

// A `RasterFile` is a raster file (e.g. a GeoTIFF) that is opened with GDAL. It
// must not be used by two threads at the same time.
class RasterFile {
 private:
  // The GDAL objects are kept here, so that this header does not include GDAL.
  struct Impl;
  std::unique_ptr<Impl> impl_;

 public:
  // Open the raster file at `path`. Throw if it cannot be opened.
  explicit RasterFile(const std::string& path);
  ~RasterFile();
  RasterFile(RasterFile&&) noexcept;
  RasterFile& operator=(RasterFile&&) noexcept;

  // Return the number of bands. Bands are numbered from 1, as in GDAL.
  [[nodiscard]] int numBands() const;

  // All lookups return one column of values per entry of `bands`, in the same
  // order. A pixel is left out if it is NoData in all of `bands`.

  // Return the pixel under `point`. The table is empty if the point is outside
  // the grid.
  PixelTable lookupPoint(const GeoPoint& point, ql::span<const int> bands,
                         const qlm::Allocator<char>& allocator);
};

// Return the path of the file `<name>.tif` in `folder`. Throw if there is no
// such file. Only files that are in `folder` can be returned, so a query that
// passes `name` can never reach a file outside of it.
std::string findRasterFile(const ql::filesystem::path& folder,
                           std::string_view name);

}  // namespace qlever::raster

#endif  // QLEVER_SRC_ENGINE_RASTER_RASTERFILE_H
