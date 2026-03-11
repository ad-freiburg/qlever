// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim maps DuckDB's D_ASSERT to QLever's AD_CORRECTNESS_CHECK.

#pragma once

#include "util/Exception.h"

#define D_ASSERT(x) AD_CORRECTNESS_CHECK(x)
