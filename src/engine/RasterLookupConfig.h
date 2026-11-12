// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_RASTERLOOKUPCONFIG_H
#define QLEVER_SRC_ENGINE_RASTERLOOKUPCONFIG_H

#include <optional>
#include <string>

#include "rdfTypes/Variable.h"

// What a `RasterLookup` needs to know, filled from the SERVICE by the parser.
// In its own header, so that the parser can use it without including the
// `RasterLookup` operation.
struct RasterLookupConfiguration {
  // The name of the raster, see `qlever::raster::rasterFilePath`.
  std::string dataset_;
  // Numbered from 1, as in GDAL. The parser sets the default.
  int band_;
  // In metres. If set, each geometry is first widened by this radius.
  std::optional<double> radius_;
  // The child's column with the geometries.
  Variable geometry_;
  // The new columns: the pixel value and, if wanted, the pixel center.
  Variable value_;
  std::optional<Variable> center_;
};

#endif  // QLEVER_SRC_ENGINE_RASTERLOOKUPCONFIG_H
