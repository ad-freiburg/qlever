// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ParseableDuration.h"

// Explicit instantiations for the chrono types used in this project. These
// satisfy the `extern template` declarations in the header, so each including
// TU avoids re-instantiating the expensive CTRE automaton.
#ifdef QLEVER_CHEAPER_COMPILATION
template class ad_utility::ParseableDuration<std::chrono::seconds>;
template class ad_utility::ParseableDuration<std::chrono::milliseconds>;
#endif
