//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#ifndef QLEVER_RTREESORTER_H
#define QLEVER_RTREESORTER_H

#include "./Rtree.h"

// ___________________________________________________________________________
// Sort the input and create the first set of OrderedBoxes
// Files should be binary files, only containing the boundingbox and id like created by ConvertWordToRtreeEntry
// onDiskBase + fileSuffix + ".tmp" is the absolute path to the file
// M is the desired branching factor of the R-tree
OrderedBoxes SortInput(const std::string& onDiskBase,
                       const std::string& fileSuffix, size_t M,
                       uintmax_t maxBuildingRamUsage, bool workInRam);

#endif  // QLEVER_RTREESORTER_H
