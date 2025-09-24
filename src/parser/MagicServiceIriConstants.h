// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_MAGICSERVICEIRICONSTANTS_H
#define QLEVER_SRC_PARSER_MAGICSERVICEIRICONSTANTS_H

#include <ctre-unicode.hpp>
#include <string>
#include <string_view>

// Constants for the various magic services - they are invoked using these
// federated querying IRIs but actually never contact these and activate special
// query features locally

constexpr inline std::string_view PATH_SEARCH_IRI =
    "<https://qlever.cs.uni-freiburg.de/pathSearch/>";

constexpr inline std::string_view SPATIAL_SEARCH_IRI =
    "<https://qlever.cs.uni-freiburg.de/spatialSearch/>";

constexpr inline std::string_view TEXT_SEARCH_IRI =
    "<https://qlever.cs.uni-freiburg.de/textSearch/>";

// For backward compatibility: invocation of SpatialJoin via special predicates.
static const std::string MAX_DIST_IN_METERS = "<max-distance-in-meters:";
static const std::string NEAREST_NEIGHBORS = "<nearest-neighbors:";
static constexpr auto MAX_DIST_IN_METERS_REGEX =
    ctll::fixed_string{"<max-distance-in-meters:(?<dist>[0-9]+)>"};
static constexpr auto NEAREST_NEIGHBORS_REGEX = ctll::fixed_string{
    "<nearest-neighbors:(?<results>[0-9]+)(:(?<dist>[0-9]+))?>"};

#endif  // QLEVER_SRC_PARSER_MAGICSERVICEIRICONSTANTS_H
