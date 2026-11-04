// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_RASTERQUERY_H
#define QLEVER_SRC_PARSER_RASTERQUERY_H

#include <optional>
#include <string>
#include <string_view>

#include "engine/RasterLookupConfig.h"
#include "parser/MagicServiceQuery.h"

class SparqlTriple;

namespace parsedQuery {
class RasterException : public std::runtime_error {
  // Constructors have to be explicitly inherited.
  using std::runtime_error::runtime_error;
};

// A raster lookup, specified via the SERVICE
// <https://qlever.cs.uni-freiburg.de/raster/>:
// SELECT * {
//   SERVICE raster: {
//     [] raster:dataset "vorratskarte" ;
//        raster:band 1 ;
//        raster:geometry ?geometry ;
//        raster:value ?value ;
//        raster:center ?center .
//     { ?plot geo:hasGeometry/geo:asWKT ?geometry }
//   }
// }
// The band (default 1) and the center are optional.
struct RasterQuery : MagicServiceQuery {
  // Empty until set by a configuration triple.
  std::optional<std::string> dataset_;
  std::optional<int> band_;
  std::optional<Variable> geometry_;
  std::optional<Variable> value_;
  std::optional<Variable> center_;

  // See MagicServiceQuery - processes configuration triples.
  void addParameter(const SparqlTriple& triple) override;

  // Throw if a required parameter or the group graph pattern is missing.
  void validate() const override;

  // The configuration for `RasterLookup`, with the default band. Requires a
  // successful `validate()`.
  RasterLookupConfiguration toRasterLookupConfiguration() const;

  std::string_view name() const override { return "raster lookup"; }
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_RASTERQUERY_H
