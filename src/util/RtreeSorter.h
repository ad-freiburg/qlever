//
// Created by nockn on 11/17/23.
//

#ifndef QLEVER_RTREESORTER_H
#define QLEVER_RTREESORTER_H

#include "./Rtree.h"

OrderedBoxes SortInput(const std::string& onDiskBase,
                       const std::string& fileSuffix, size_t M,
                       uintmax_t maxBuildingRamUsage, bool workInRam);

#endif  // QLEVER_RTREESORTER_H
