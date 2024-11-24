// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once

// Constants for the various magic services - they are invoked using these
// federated querying IRIs but actually never contact these and activate special
// query features locally

constexpr inline std::string_view PATH_SEARCH_IRI =
    "<https://qlever.cs.uni-freiburg.de/pathSearch/>";

constexpr inline std::string_view SPATIAL_SEARCH_IRI =
    "<https://qlever.cs.uni-freiburg.de/spatialSearch/>";
