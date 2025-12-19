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

namespace string_constants::detail {
constexpr inline std::string_view OPENING_BRACKET = "<";
constexpr inline std::string_view CLOSING_BRACKET = ">";

constexpr inline std::string_view MAGIC_IRI_BASE = "qlever://";

constexpr inline std::string_view PATH_SEARCH_SUFFIX = "pathSearch/";
constexpr inline std::string_view SPATIAL_SEARCH_SUFFIX = "spatialSearch/";
constexpr inline std::string_view TEXT_SEARCH_SUFFIX = "textSearch/";
constexpr inline std::string_view MATERIALIZED_VIEW_SUFFIX =
    "materializedView/";

}  // namespace string_constants::detail

constexpr inline std::string_view PATH_SEARCH_IRI =
    ad_utility::constexprStrCat<string_constants::detail::OPENING_BRACKET,
                                string_constants::detail::MAGIC_IRI_BASE,
                                string_constants::detail::PATH_SEARCH_SUFFIX,
                                string_constants::detail::CLOSING_BRACKET>();

constexpr inline std::string_view SPATIAL_SEARCH_IRI =
    ad_utility::constexprStrCat<string_constants::detail::OPENING_BRACKET,
                                string_constants::detail::MAGIC_IRI_BASE,
                                string_constants::detail::SPATIAL_SEARCH_SUFFIX,
                                string_constants::detail::CLOSING_BRACKET>();

constexpr inline std::string_view TEXT_SEARCH_IRI =
    ad_utility::constexprStrCat<string_constants::detail::OPENING_BRACKET,
                                string_constants::detail::MAGIC_IRI_BASE,
                                string_constants::detail::TEXT_SEARCH_SUFFIX,
                                string_constants::detail::CLOSING_BRACKET>();

constexpr inline std::string_view MATERIALIZED_VIEW_IRI_WITHOUT_BRACKETS =
    ad_utility::constexprStrCat<
        string_constants::detail::MAGIC_IRI_BASE,
        string_constants::detail::MATERIALIZED_VIEW_SUFFIX>();
constexpr inline std::string_view
    MATERIALIZED_VIEW_IRI_WITHOUT_CLOSING_BRACKET =
        ad_utility::constexprStrCat<string_constants::detail::OPENING_BRACKET,
                                    MATERIALIZED_VIEW_IRI_WITHOUT_BRACKETS>();
constexpr inline std::string_view MATERIALIZED_VIEW_IRI =
    ad_utility::constexprStrCat<MATERIALIZED_VIEW_IRI_WITHOUT_CLOSING_BRACKET,
                                string_constants::detail::CLOSING_BRACKET>();

// Magic service IRIs which are no longer supported in QLever, but should throw
// an explanatory error instructing users to switch to the respective new IRI.
namespace string_constants::legacy_magic_service_iris {
using namespace string_constants::detail;

constexpr inline std::string_view MAGIC_IRI_BASE_LEGACY =
    "https://qlever.cs.uni-freiburg.de/";

constexpr inline std::string_view
    MATERIALIZED_VIEW_LEGACY_IRI_WITHOUT_BRACKETS =
        ad_utility::constexprStrCat<MAGIC_IRI_BASE_LEGACY,
                                    MATERIALIZED_VIEW_SUFFIX>();

constexpr inline std::string_view PATH_SEARCH_LEGACY_IRI =
    ad_utility::constexprStrCat<OPENING_BRACKET, MAGIC_IRI_BASE_LEGACY,
                                PATH_SEARCH_SUFFIX, CLOSING_BRACKET>();

constexpr inline std::string_view SPATIAL_SEARCH_LEGACY_IRI =
    ad_utility::constexprStrCat<OPENING_BRACKET, MAGIC_IRI_BASE_LEGACY,
                                SPATIAL_SEARCH_SUFFIX, CLOSING_BRACKET>();

constexpr inline std::string_view TEXT_SEARCH_LEGACY_IRI =
    ad_utility::constexprStrCat<OPENING_BRACKET, MAGIC_IRI_BASE_LEGACY,
                                TEXT_SEARCH_SUFFIX, CLOSING_BRACKET>();

}  // namespace string_constants::legacy_magic_service_iris

// For backward compatibility: invocation of SpatialJoin via special predicates.
static const std::string MAX_DIST_IN_METERS = "<max-distance-in-meters:";
static const std::string NEAREST_NEIGHBORS = "<nearest-neighbors:";
static constexpr auto MAX_DIST_IN_METERS_REGEX =
    ctll::fixed_string{"<max-distance-in-meters:(?<dist>[0-9]+)>"};
static constexpr auto NEAREST_NEIGHBORS_REGEX = ctll::fixed_string{
    "<nearest-neighbors:(?<results>[0-9]+)(:(?<dist>[0-9]+))?>"};

#endif  // QLEVER_SRC_PARSER_MAGICSERVICEIRICONSTANTS_H
