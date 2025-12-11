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

namespace string_constants::detail {
constexpr inline std::string_view OPENING_BRACKET = "<";
constexpr inline std::string_view CLOSING_BRACKET = ">";
}  // namespace string_constants::detail

constexpr inline std::string_view MATERIALIZED_VIEW_IRI_WITHOUT_BRACKETS =
    "https://qlever.cs.uni-freiburg.de/materializedView/";
constexpr inline std::string_view
    MATERIALIZED_VIEW_IRI_WITHOUT_CLOSING_BRACKET =
        ad_utility::constexprStrCat<string_constants::detail::OPENING_BRACKET,
                                    MATERIALIZED_VIEW_IRI_WITHOUT_BRACKETS>();
constexpr inline std::string_view MATERIALIZED_VIEW_IRI =
    ad_utility::constexprStrCat<MATERIALIZED_VIEW_IRI_WITHOUT_CLOSING_BRACKET,
                                string_constants::detail::CLOSING_BRACKET>();

// For backward compatibility: invocation of SpatialJoin via special predicates.
static const std::string MAX_DIST_IN_METERS = "<max-distance-in-meters:";
static const std::string NEAREST_NEIGHBORS = "<nearest-neighbors:";
static constexpr auto MAX_DIST_IN_METERS_REGEX =
    ctll::fixed_string{"<max-distance-in-meters:(?<dist>[0-9]+)>"};
static constexpr auto NEAREST_NEIGHBORS_REGEX = ctll::fixed_string{
    "<nearest-neighbors:(?<results>[0-9]+)(:(?<dist>[0-9]+))?>"};

#endif  // QLEVER_SRC_PARSER_MAGICSERVICEIRICONSTANTS_H
