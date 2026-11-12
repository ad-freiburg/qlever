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

namespace qlever::raster {

// The value of one pixel: `int64_t` for integer bands, `double` for floating
// point bands.
using PixelValue = std::variant<int64_t, double>;

// One pixel of a lookup result: its center and its value.
struct Pixel {
  GeoPoint center_;
  PixelValue value_;
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

  // The pixel under `point` in `band`, or nothing if the point is outside the
  // grid or the pixel is NoData.
  std::optional<Pixel> lookupPoint(const GeoPoint& point, int band);

  // The pixels in `band` under a point, on a line, or with their center inside
  // a polygon of `wktLiteral`, without NoData pixels and duplicates.
  std::vector<Pixel> lookupGeometry(std::string_view wktLiteral, int band);
};

// The path of the raster file for `datasetName`, which is the name of a `.tif`
// file in the `rasters/` folder of the source tree, without `.tif`. Throws if
// there is no such file, so that a query can never choose a path itself. The
// folder is read once, so changes to it need a restart of the server.
std::string rasterFilePath(std::string_view datasetName);

}  // namespace qlever::raster

#endif  // QLEVER_SRC_ENGINE_RASTER_RASTERFILE_H
