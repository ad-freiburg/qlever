//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_FUNCTIONAL_H
#define QLEVER_FUNCTIONAL_H

#include "../Forward.h"

namespace ad_std {

inline auto identityImpl = [](auto&& arg) noexcept { return AD_FWD(arg); };
using identity = decltype(identityImpl);

}  // namespace ad_std

#endif  // QLEVER_FUNCTIONAL_H
