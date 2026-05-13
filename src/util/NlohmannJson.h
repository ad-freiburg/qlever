//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Thin wrapper around nlohmann/json.hpp that must be included (directly or
// via util/json.h) before any bare #include <nlohmann/json.hpp>. It sets
// JSON_USE_IMPLICIT_CONVERSIONS=0 before nlohmann reads it, so that implicit
// conversions from json to other types (notably std::string) are disabled.
//
// This header is also the canonical PCH entry for nlohmann: putting it in
// the PCH ensures the flag is baked in correctly. Without this wrapper the
// PCH would include nlohmann without the flag, enabling implicit conversions
// silently for all PCH-compiled TUs.

#pragma once

#ifndef JSON_USE_IMPLICIT_CONVERSIONS
#define JSON_USE_IMPLICIT_CONVERSIONS 0
#endif

#include <nlohmann/json.hpp>
