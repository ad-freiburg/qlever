//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SPECIALIDS_H
#define QLEVER_SPECIALIDS_H

#include "global/Constants.h"
#include "global/Id.h"
#include "util/HashMap.h"

namespace qlever {

// TODO<joka921> Comment and add sanity checks (mapped Ids are unique and all
// have the special `undefined` type. Implement this via a immediately invoked
// lambda
static const inline ad_utility::HashMap<std::string, Id> specialIds{
    {HAS_PREDICATE_PREDICATE, Id::fromBits(21)},
    {HAS_PATTERN_PREDICATE, Id::fromBits(22)}};
}  // namespace qlever

#endif  // QLEVER_SPECIALIDS_H
