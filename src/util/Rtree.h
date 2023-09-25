//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#ifndef QLEVER_RTREE_H
#define QLEVER_RTREE_H

#include <boost/geometry.hpp>
#include <boost/serialization/split_free.hpp>
#include <boost/serialization/version.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <vector>

namespace bg = boost::geometry;

// ___________________________________________________________________________
// Forward declaration
struct RTreeValue;
struct RTreeValueWithOrderIndex;
using multiBoxGeo = std::vector<RTreeValue>;
using multiBoxWithOrderIndex = std::vector<RTreeValueWithOrderIndex>;
struct SplitResult;
struct SplitBuffersRam;
struct SplitBuffersDisk;

// ___________________________________________________________________________
// Data type to store all the information of the rectangles (in ram or on disk)
// + the small lists for one dimension
struct RectanglesForOrderedBoxes {
  std::shared_ptr<multiBoxWithOrderIndex> rectanglesInRam;
  std::string rectanglesOnDisk;
  std::shared_ptr<multiBoxWithOrderIndex> rectanglesSmall;
};

// ___________________________________________________________________________
// Data structure representing a single node of the Rtree containing the
// boundingbox and its children
class Node {
 protected:
  typedef bg::model::point<double, 2, bg::cs::spherical_equatorial<bg::degree>>
      Point;
  typedef bg::model::box<Point> BoundingBox;
  friend class boost::serialization::access;
  uint64_t id;
  BoundingBox boundingBox{};
  bool isLastInnerNode =
      false;  // when true, this means that the node is the last inner node and
              // all of its children are leafs
  multiBoxGeo children;

  template <class Archive>
  void serialize(Archive& a, [[maybe_unused]] const unsigned int version) {
    a& id;
    a& isLastInnerNode;
    a& boundingBox;
    a& children;
  }

  explicit Node(uint64_t id);

 public:
  Node();
  Node(uint64_t id, BoundingBox boundingBox);
  Node(uint64_t id, BoundingBox boundingBox, multiBoxGeo& children,
       bool isLastInnerNode);
  Node(uint64_t id, double minX, double minY, double maxX, double maxY,
       bool isLastInnerNode);
  [[nodiscard]] uint64_t GetId() const;
  [[nodiscard]] BoundingBox GetBoundingBox() const;
  void AddChild(Node& child);
  void SetIsLastInnerNode(bool isLast);
  [[nodiscard]] bool GetIsLastInnerNode() const;
  multiBoxGeo GetChildren();
};

BOOST_CLASS_VERSION(Node, 1)

// ___________________________________________________________________________
// A Rtree based on bounding boxes and ids
class Rtree {
 private:
  // ___________________________________________________________________________
  // Save the current node in the building process to disk
  static uint64_t SaveNode(Node& node, bool isLastInnerNode,
                           std::ofstream& nodesOfs);
  // ___________________________________________________________________________
  // Load a specific Node to query in its children
  static Node LoadNode(uint64_t id, std::ifstream& lookupIfs,
                       std::ifstream& nodesIfs);
  uintmax_t maxBuildingRamUsage;

 public:
  typedef bg::model::point<double, 2, bg::cs::spherical_equatorial<bg::degree>>
      Point;
  typedef bg::model::box<Point> BoundingBox;
  // ___________________________________________________________________________
  // Build the whole Rtree with the raw data in onDiskBase + ".boundingbox.tmp",
  // M as branching factor and folder as Rtree destination
  void BuildTree(const std::string& onDiskBase, size_t M,
                 const std::string& folder) const;
  // ___________________________________________________________________________
  // Search for an intersection of query with any elements of the Rtree
  static multiBoxGeo SearchTree(BoundingBox query, const std::string& folder);
  // ___________________________________________________________________________
  // Convert a single wkt literal to a datapoint in the format suitable for the
  // Rtree
  static std::optional<BoundingBox> ConvertWordToRtreeEntry(
      const std::string& wkt);
  // ___________________________________________________________________________
  // Save a single datapoint for the Rtree to disk
  static void SaveEntry(BoundingBox boundingBox, uint64_t index,
                        std::ofstream& convertOfs);
  // ___________________________________________________________________________
  // Save a single datapoint of the Rtree, together with its position in the x
  // and y sorting to disk
  static void SaveEntryWithOrderIndex(RTreeValueWithOrderIndex treeValue,
                                      std::ofstream& convertOfs);
  // ___________________________________________________________________________
  // Load all datapoints of the Rtree in file into ram
  static multiBoxGeo LoadEntries(const std::string& file);
  // ___________________________________________________________________________
  // Load all datapoints of the Rtree, together with its x and y sorting into
  // ram
  static multiBoxWithOrderIndex LoadEntriesWithOrderIndex(
      const std::string& file);
  // ___________________________________________________________________________
  // Create a bounding box, based on the corner coordinates
  static BoundingBox createBoundingBox(double pointOneX, double pointOneY,
                                       double pointTwoX, double pointTwoY);
  // ___________________________________________________________________________
  // Take two bounding boxes and combine them into one bounding box containing
  // both
  static BoundingBox combineBoundingBoxes(Rtree::BoundingBox b1,
                                          Rtree::BoundingBox b2);
  explicit Rtree(uintmax_t maxBuildingRamUsage);
};

// ___________________________________________________________________________
// Data structure handling the datapoints of the Rtree sorted in x and y
// direction (either on ram or on disk)
class OrderedBoxes {
 private:
  bool workInRam;
  uint64_t size;
  Rtree::BoundingBox boundingBox;
  RectanglesForOrderedBoxes
      rectsD0;  // the rectangles (datapoints) sorted in x direction
  RectanglesForOrderedBoxes rectsD1;  // the rectangles sorted in y direction
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
  // (rectangles are stored in ram)
  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> PerformSplit(
      SplitResult splitResult, SplitBuffersRam& splitBuffersRam, size_t M,
      size_t S);
  // ___________________________________________________________________________
  // Actually splitting the rectangles at the given split by splitResult
  // (rectangles are stored on disk)
  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> PerformSplit(
      SplitResult splitResult, SplitBuffersDisk& splitBuffers, size_t M,
      size_t S, uint64_t maxBuildingRamUsage);

 public:
  [[nodiscard]] bool WorkInRam() const;
  // ___________________________________________________________________________
  // Set up the OrderedBoxes with the rectangles given as vectors stored in ram
  // and set workInRam to true
  void SetOrderedBoxesToRam(RectanglesForOrderedBoxes& rectanglesD0,
                            RectanglesForOrderedBoxes& rectanglesD1,
                            Rtree::BoundingBox box);
  // ___________________________________________________________________________
  // Set up the OrderedBoxes with the rectangles given as files stored on disk
  // and set workInRam to false
  void SetOrderedBoxesToDisk(RectanglesForOrderedBoxes& rectanglesD0,
                             RectanglesForOrderedBoxes& rectanglesD1,
                             uint64_t size, Rtree::BoundingBox box);
  Rtree::BoundingBox GetBoundingBox();
  [[nodiscard]] uint64_t GetSize() const;
  // ___________________________________________________________________________
  // Wrapper function to perform the whole process of splitting the rectangles
  // for either ram or disk case
  std::pair<OrderedBoxes, OrderedBoxes> SplitAtBest(
      const std::string& filePath, size_t S, size_t M,
      uint64_t maxBuildingRamUsage);
  // ___________________________________________________________________________
  // return the rectangles of the x sorting for the case where they are stored
  // in ram
  std::shared_ptr<multiBoxWithOrderIndex> GetRectanglesInRam();
  // ___________________________________________________________________________
  // return the rectangles of the x sorting for the case where they are stored
  // on disk
  std::string GetRectanglesOnDisk();
};

// ___________________________________________________________________________
// Subclass of the Node only needed while constructing the Rtree (it keeps track
// of the remaining OrderedBoxes of the subtree)
class ConstructionNode : public Node {
 private:
  OrderedBoxes orderedBoxes;

 public:
  ConstructionNode(uint64_t id, OrderedBoxes orderedBoxes);
  OrderedBoxes GetOrderedBoxes();
  void AddChildrenToItem();
};

namespace boost::serialization {
template <class Archive>
void save(Archive& a, const Rtree::BoundingBox& b,
          [[maybe_unused]] unsigned int version) {
  a << b.min_corner().get<0>();
  a << b.min_corner().get<1>();
  a << b.max_corner().get<0>();
  a << b.max_corner().get<1>();
}
template <class Archive>
void load(Archive& a, Rtree::BoundingBox& b,
          [[maybe_unused]] unsigned int version) {
  double minX = 0;
  a >> minX;
  double minY = 0;
  a >> minY;
  double maxX = 0;
  a >> maxX;
  double maxY = 0;
  a >> maxY;
  b = Rtree::BoundingBox(Rtree::Point(minX, minY), Rtree::Point(maxX, maxY));
}
}  // namespace boost::serialization
BOOST_SERIALIZATION_SPLIT_FREE(Rtree::BoundingBox);

// ___________________________________________________________________________
// Data type for a value of the Rtree, which contains the id of the object and
// its bounding box.
struct RTreeValue {
  Rtree::BoundingBox box{};
  uint64_t id = 0;
  double MinX() const { return box.min_corner().get<0>(); }
  double MaxX() const { return box.max_corner().get<0>(); }
  double MinY() const { return box.min_corner().get<1>(); }
  double MaxY() const { return box.max_corner().get<1>(); }

  template <class Archive>
  void serialize(Archive& a, [[maybe_unused]] const unsigned int version) {
    a& box;
    a& id;
  }
};

// ___________________________________________________________________________
// Data type for a value of the Rtree (id and boundingbox), with the addtional
// information of its position in the x- and y-sorting. This is only used to
// create the Rtree in a more efficient way
struct RTreeValueWithOrderIndex : RTreeValue {
  uint64_t orderX = 0;
  uint64_t orderY = 0;
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

// ___________________________________________________________________________
// A Buffer data structure, containing vectors for the result of a split, while
// doing it in ram
struct SplitBuffersRam {
  std::shared_ptr<multiBoxWithOrderIndex> s0Dim0 =
      std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s0Dim1 =
      std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s1Dim0 =
      std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s1Dim1 =
      std::make_shared<multiBoxWithOrderIndex>();

  std::shared_ptr<multiBoxWithOrderIndex> s0SmallDim0 =
      std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s0SmallDim1 =
      std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s1SmallDim0 =
      std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s1SmallDim1 =
      std::make_shared<multiBoxWithOrderIndex>();
};

// ___________________________________________________________________________
// A Buffer data structure, containing the write streams for the result of a
// split, while doing it on disk
struct SplitBuffersDisk {
  SplitBuffersRam splitBuffersRam;
  std::optional<std::ofstream> split0Dim0File;
  std::optional<std::ofstream> split0Dim1File;
  std::optional<std::ofstream> split1Dim0File;
  std::optional<std::ofstream> split1Dim1File;
};

template <size_t dimension>
struct SortRuleLambda {
  // comparison function
  bool operator()(const RTreeValue& b1, const RTreeValue& b2) const {
    double center1 = dimension == 0 ? std::midpoint(b1.MinX(), b1.MaxX())
                                    : std::midpoint(b1.MinY(), b1.MaxY());
    double center2 = dimension == 0 ? std::midpoint(b2.MinX(), b2.MaxX())
                                    : std::midpoint(b2.MinY(), b2.MaxY());
    return center1 < center2;
  }

  // Value that is strictly smaller than any input element.
  static RTreeValue min_value() {
    return {Rtree::createBoundingBox(-std::numeric_limits<double>::max(),
                                     -std::numeric_limits<double>::max(),
                                     -std::numeric_limits<double>::max(),
                                     -std::numeric_limits<double>::max()),
            0};
  }

  // Value that is strictly larger than any input element.
  static RTreeValue max_value() {
    return {Rtree::createBoundingBox(std::numeric_limits<double>::max(),
                                     std::numeric_limits<double>::max(),
                                     std::numeric_limits<double>::max(),
                                     std::numeric_limits<double>::max()),
            0};
  }
};

template <size_t dimension>
struct SortRuleLambdaWithIndex {
  uint64_t RTreeValueWithOrderIndex::*orderSelected =
      dimension == 0 ? &RTreeValueWithOrderIndex::orderX
                     : &RTreeValueWithOrderIndex::orderY;

  // comparison function
  bool operator()(const RTreeValueWithOrderIndex& b1,
                  const RTreeValueWithOrderIndex& b2) const {
    double center1 = dimension == 0 ? std::midpoint(b1.MinX(), b1.MaxX())
                                    : std::midpoint(b1.MinY(), b1.MaxY());
    double center2 = dimension == 0 ? std::midpoint(b2.MinX(), b2.MaxX())
                                    : std::midpoint(b2.MinY(), b2.MaxY());

    if (b1.*orderSelected == b2.*orderSelected) return center1 < center2;
    return b1.*orderSelected < b2.*orderSelected;
  }

  // Value that is strictly smaller than any input element.
  static RTreeValueWithOrderIndex min_value() {
    return {{Rtree::createBoundingBox(-std::numeric_limits<double>::max(),
                                      -std::numeric_limits<double>::max(),
                                      -std::numeric_limits<double>::max(),
                                      -std::numeric_limits<double>::max()),
             0},
            0,
            0};
  }

  // Value that is strictly larger than any input element.
  static RTreeValueWithOrderIndex max_value() {
    return {{Rtree::createBoundingBox(std::numeric_limits<double>::max(),
                                      std::numeric_limits<double>::max(),
                                      std::numeric_limits<double>::max(),
                                      std::numeric_limits<double>::max()),
             0},
            LLONG_MAX,
            LLONG_MAX};
  }
};

#endif  // QLEVER_RTREE_H
