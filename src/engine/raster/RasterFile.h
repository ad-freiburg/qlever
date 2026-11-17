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

#include <memory>
#include <string>

namespace qlever::raster {

// A raster file (e.g. a GeoTIFF), opened with GDAL.
class RasterFile {
 private:
  // Holds the GDAL objects, so that this header does not include GDAL.
  struct Impl;
  std::unique_ptr<Impl> impl_;

 public:
  // Throws if the file cannot be opened.
  explicit RasterFile(const std::string& path);
  ~RasterFile();
  RasterFile(RasterFile&&) noexcept;
  RasterFile& operator=(RasterFile&&) noexcept;

  // The number of bands. Bands are numbered from 1, as in GDAL.
  int numBands() const;
};

}  // namespace qlever::raster

#endif  // QLEVER_SRC_ENGINE_RASTER_RASTERFILE_H
