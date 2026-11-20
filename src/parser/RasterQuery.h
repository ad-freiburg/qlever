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
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "engine/RasterLookupConfig.h"
#include "parser/MagicServiceQuery.h"
#include "parser/TripleComponent.h"

class SparqlTriple;

namespace parsedQuery {
class RasterException : public std::runtime_error {
  // Constructors have to be explicitly inherited.
  using std::runtime_error::runtime_error;
};

// Return the name of the raster dataset that `iri` identifies, which is the
// part of `iri` after `RASTER_DATASET_IRI_WITHOUT_BRACKETS`. Throw if `iri` is
// not in that namespace or the name is empty.
std::string extractRasterDatasetName(const TripleComponent::Iri& iri);

// A raster lookup, specified via the SERVICE
// <https://qlever.cs.uni-freiburg.de/raster/>:
// SELECT * {
//   SERVICE raster: {
//     [] raster:dataset rds:vorratskarte ;
//        raster:band1 ?species ;
//        raster:band2 ?height ;
//        raster:radius 500 ;
//        raster:geometry ?geometry ;
//        raster:center ?center .
//     { ?plot geo:hasGeometry/geo:asWKT ?geometry }
//   }
// }
// At least one band is required. The radius (in metres) and the center are
// optional. The prefix `rds:` stands for the namespace
// <https://qlever.cs.uni-freiburg.de/raster/dataset/>. The dataset can also be
// given as a string literal with its name, for example "vorratskarte".
struct RasterQuery : MagicServiceQuery {
  // Empty until set by a configuration triple.
  std::optional<std::string> dataset_;
  // In the order of the configuration triples.
  std::vector<RasterLookupConfiguration::Band> bands_;
  std::optional<double> radius_;
  std::optional<Variable> geometry_;
  std::optional<Variable> center_;

  // See MagicServiceQuery - processes configuration triples.
  void addParameter(const SparqlTriple& triple) override;

  // Throw if a required parameter or the group graph pattern is missing.
  void validate() const override;

  // The configuration for `RasterLookup`. Requires a successful `validate()`.
  RasterLookupConfiguration toRasterLookupConfiguration() const;

  std::string_view name() const override { return "raster lookup"; }
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_RASTERQUERY_H
