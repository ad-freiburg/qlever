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
#include <vector>

#include "rdfTypes/Variable.h"

// What a `RasterLookup` needs to know, filled from the SERVICE by the parser.
// In its own header, so that the parser can use it without including the
// `RasterLookup` operation.
struct RasterLookupConfiguration {
  // A band of the raster and the variable that is bound to its pixel values.
  struct Band {
    // Numbered from 1, as in GDAL.
    int number_;
    Variable variable_;
  };

  // The name of the raster, see `qlever::raster::rasterFilePath`. The query
  // gives it as a string literal or as an IRI, whose namespace the parser
  // removes (see `parsedQuery::extractRasterDatasetName`).
  std::string dataset_;
  // At least one, in the order of the query.
  std::vector<Band> bands_;
  // In metres. If set, each geometry is first widened by this radius.
  std::optional<double> radius_;
  // The child's column with the geometries.
  Variable geometry_;
  // The variable that is bound to the pixel centers, if wanted.
  std::optional<Variable> center_;
};

#endif  // QLEVER_SRC_ENGINE_RASTERLOOKUPCONFIG_H
