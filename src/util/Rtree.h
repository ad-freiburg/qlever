//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#ifndef QLEVER_RTREE_H
#define QLEVER_RTREE_H

#ifndef EOF
#define EOF std::char_traits<char>::eof()
#endif
#include <boost/serialization/version.hpp>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
#include <variant>

#include "./RtreeBasicGeometry.h"

// ___________________________________________________________________________
// Forward declaration
struct RTreeValue;
struct RTreeValueWithOrderIndex;
using multiBoxGeo = std::vector<RTreeValue>;
using multiBoxWithOrderIndex = std::vector<RTreeValueWithOrderIndex>;
struct SplitResult;
struct SplitBuffers;

// ___________________________________________________________________________
// Data type to store all the information of the rectangles (in ram or on disk)
// + the small lists for one dimension
struct RectanglesForOrderedBoxes {
  std::variant<multiBoxWithOrderIndex, std::filesystem::path> rectangles;
  multiBoxWithOrderIndex rectanglesSmall;

  RectanglesForOrderedBoxes() {
    rectangles = {};
    rectanglesSmall = multiBoxWithOrderIndex();
  }

  void Clear() {
    rectanglesSmall = multiBoxWithOrderIndex();
    if (std::holds_alternative<multiBoxWithOrderIndex>(rectangles)) {
      rectangles = multiBoxWithOrderIndex();
    }
  }
};

// ___________________________________________________________________________
// A Rtree based on bounding boxes and ids
class Rtree {
 private:
  uintmax_t maxBuildingRamUsage_;

 public:
  // ___________________________________________________________________________
  // Build the whole Rtree with the raw data in onDiskBase + fileSuffix +
  // ".tmp", M as branching factor and folder as Rtree destination
  uint64_t BuildTree(const std::string& onDiskBase, const std::string& fileSuffix,
                 size_t M, const std::string& folder) const;
  // ___________________________________________________________________________
  // Search for an intersection of query with any elements of the Rtree
  static multiBoxGeo SearchTree(BasicGeometry::BoundingBox query,
                                const std::string& folder);
  explicit Rtree(uintmax_t maxBuildingRamUsage);
};

// ___________________________________________________________________________
// Data structure handling the datapoints of the Rtree sorted in x and y
// direction (either on ram or on disk)
class OrderedBoxes {
 public: // TODO
  bool workInRam_{};
  uint64_t size_{};
  BasicGeometry::BoundingBox boundingBox_{};
  RectanglesForOrderedBoxes
      rectsD0_;  // the rectangles (datapoints) sorted in x direction
  RectanglesForOrderedBoxes rectsD1_;  // the rectangles sorted in y direction
  // ___________________________________________________________________________
  // Initiate the splitting of the rectangles in the best position (rectangles
  // are stored in ram)
  std::pair<OrderedBoxes, OrderedBoxes> SplitAtBestInRam(size_t S, size_t M);
  // ___________________________________________________________________________
  // Initiate the splitting of the rectangles in the best position (rectangles
  // are stored on disk)
  std::pair<OrderedBoxes, OrderedBoxes> SplitAtBestOnDisk(
      const std::string& filePath, size_t S, size_t M,
      uint64_t maxBuildingRamUsage);
  // ___________________________________________________________________________
  // Get the position and dimension of the best split possible to maximize the
  // quality of the Rtree
  SplitResult GetBestSplit();
  // ___________________________________________________________________________
  // Actually splitting the rectangles at the given split by splitResult
  std::pair<BasicGeometry::BoundingBox, BasicGeometry::BoundingBox>
  PerformSplit(SplitResult splitResult, SplitBuffers& splitBuffers, size_t M,
               size_t S, uint64_t maxBuildingRamUsage = 0);

 public:
  [[nodiscard]] bool WorkInRam() const;
  // ___________________________________________________________________________
  // Set up the OrderedBoxes with the rectangles given as vectors stored in ram
  // and set workInRam to true
  void SetOrderedBoxesToRam(RectanglesForOrderedBoxes rectanglesD0,
                            RectanglesForOrderedBoxes rectanglesD1,
                            BasicGeometry::BoundingBox box);
  // ___________________________________________________________________________
  // Set up the OrderedBoxes with the rectangles given as files stored on disk
  // and set workInRam to false
  void SetOrderedBoxesToDisk(RectanglesForOrderedBoxes rectanglesD0,
                             RectanglesForOrderedBoxes rectanglesD1,
                             uint64_t size, BasicGeometry::BoundingBox box);
  BasicGeometry::BoundingBox GetBoundingBox();
  [[nodiscard]] uint64_t GetSize() const;
  // ___________________________________________________________________________
  // Wrapper function to perform the whole process of splitting the rectangles
  // for either ram or disk case
  std::pair<OrderedBoxes, OrderedBoxes> SplitAtBest(
      const std::filesystem::path& filePath, size_t S, size_t M,
      uint64_t maxBuildingRamUsage);
  // ___________________________________________________________________________
  // return the rectangles of the x sorting for the case where they are stored
  // in ram
  multiBoxWithOrderIndex GetRectanglesInRam();
  // ___________________________________________________________________________
  // return the rectangles of the x sorting for the case where they are stored
  // on disk
  std::filesystem::path GetRectanglesOnDisk();

  // ___________________________________________________________________________
  // Clear all content of the OrderedBoxes
  void Clear();
};

// ___________________________________________________________________________
// Data type containing all the information about the best split found, which
// are needed to actually perform the split.
struct SplitResult {
  double bestCost = -1;
  size_t bestDim = 0;
  uint64_t bestIndex = 0;
  RTreeValueWithOrderIndex bestLastElement;
  RTreeValueWithOrderIndex bestElement;
  RTreeValueWithOrderIndex bestMinElement;
  RTreeValueWithOrderIndex bestMaxElement;
};

struct SplitBuffers {
  RectanglesForOrderedBoxes& rectsD0Split0;
  RectanglesForOrderedBoxes& rectsD1Split0;
  RectanglesForOrderedBoxes& rectsD0Split1;
  RectanglesForOrderedBoxes& rectsD1Split1;
};

#endif  // QLEVER_RTREE_H
