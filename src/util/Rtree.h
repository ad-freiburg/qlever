//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#ifndef QLEVER_RTREE_H
#define QLEVER_RTREE_H

#include <vector>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <optional>
#include <limits>
#include <boost/geometry.hpp>
#include <boost/serialization/version.hpp>
#include <boost/serialization/split_free.hpp>

namespace bg = boost::geometry;

/* Forward declaration */
struct RTreeValue;
struct RTreeValueWithOrderIndex;
using multiBoxGeo = std::vector<RTreeValue>;
using multiBoxWithOrderIndex = std::vector<RTreeValueWithOrderIndex>;
struct SplitResult;
struct SplitBuffersRam;
struct SplitBuffersDisk;

/**
 * Data type to store all the information of the rectangles (in ram or on disk) + the small lists for one dimension
 */
struct RectanglesForOrderedBoxes {
  std::shared_ptr<multiBoxWithOrderIndex> rectanglesInRam;
  std::string rectanglesOnDisk;
  std::shared_ptr<multiBoxWithOrderIndex> rectanglesSmall;
};

class Node {
 protected:
  typedef bg::model::point<double, 2, bg::cs::spherical_equatorial<bg::degree>> Point;
  typedef bg::model::box<Point> BoundingBox;
  friend class boost::serialization::access;
  uint64_t id;
  BoundingBox boundingBox{};
  bool isLastInnerNode = false;
  multiBoxGeo children;

  template<class Archive>
  void serialize(Archive & a, [[maybe_unused]]const unsigned int version) {
    a & id;
    a & isLastInnerNode;
    a & boundingBox;
    a & children;
  }

  explicit Node(uint64_t id);

 public:
  Node();
  Node(uint64_t id, BoundingBox boundingBox);
  Node(uint64_t id, BoundingBox boundingBox, multiBoxGeo &children, bool isLastInnerNode);
  Node(uint64_t id, double minX, double minY, double maxX, double maxY, bool isLastInnerNode);
  [[nodiscard]] uint64_t GetId() const;
  [[nodiscard]] BoundingBox GetBoundingBox() const;
  void AddChild(Node& child);
  void SetIsLastInnerNode(bool isLast);
  [[nodiscard]] bool GetIsLastInnerNode() const;
  multiBoxGeo GetChildren();
};

BOOST_CLASS_VERSION(Node, 1)

class Rtree {
 private:
  static uint64_t SaveNode(Node &node, bool isLastInnerNode, std::ofstream& nodesOfs);
  static Node LoadNode(uint64_t id, std::ifstream& lookupIfs, std::ifstream& nodesIfs);
  uintmax_t maxBuildingRamUsage;
 public:
  typedef bg::model::point<double, 2, bg::cs::spherical_equatorial<bg::degree>> Point;
  typedef bg::model::box<Point> BoundingBox;
  void BuildTree(const std::string& onDiskBase, size_t M, const std::string& folder) const;
  static multiBoxGeo SearchTree(BoundingBox query, const std::string& folder);
  static std::optional<BoundingBox> ConvertWordToRtreeEntry(const std::string& wkt);
  static void SaveEntry(BoundingBox boundingBox, uint64_t index, std::ofstream& convertOfs);
  static void SaveEntryWithOrderIndex(RTreeValueWithOrderIndex treeValue, std::ofstream& convertOfs);
  static multiBoxGeo LoadEntries(const std::string& file);
  static multiBoxWithOrderIndex LoadEntriesWithOrderIndex(const std::string& file);
  static BoundingBox createBoundingBox(double pointOneX, double pointOneY, double pointTwoX, double pointTwoY);
  explicit Rtree(uintmax_t maxBuildingRamUsage);
};

class OrderedBoxes {
 private:
  bool workInRam;
  uint64_t size;
  Rtree::BoundingBox boundingBox;
  RectanglesForOrderedBoxes rectsD0;
  RectanglesForOrderedBoxes rectsD1;
  std::pair<OrderedBoxes, OrderedBoxes> SplitAtBestInRam(size_t S, size_t M);
  std::pair<OrderedBoxes, OrderedBoxes> SplitAtBestOnDisk(const std::string& filePath, size_t S, size_t M, uint64_t maxBuildingRamUsage);
  SplitResult GetBestSplit();
  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> PerformSplit(SplitResult splitResult, SplitBuffersRam& splitBuffersRam, size_t M, size_t S);
  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> PerformSplit(SplitResult splitResult, SplitBuffersDisk& splitBuffers, size_t M, size_t S, uint64_t maxBuildingRamUsage);
 public:
  [[nodiscard]] bool WorkInRam() const;
  void CreateOrderedBoxesInRam(RectanglesForOrderedBoxes& rectanglesD0, RectanglesForOrderedBoxes& rectanglesD1, Rtree::BoundingBox box); // workInRam = true
  void CreateOrderedBoxesOnDisk(RectanglesForOrderedBoxes& rectanglesD0, RectanglesForOrderedBoxes& rectanglesD1, uint64_t size, Rtree::BoundingBox box); // workInRam = false
  Rtree::BoundingBox GetBoundingBox();
  [[nodiscard]] uint64_t GetSize() const;
  std::pair<OrderedBoxes, OrderedBoxes> SplitAtBest(const std::string& filePath, size_t S, size_t M, uint64_t maxBuildingRamUsage);
  std::shared_ptr<multiBoxWithOrderIndex> GetRectanglesInRam();
  std::string GetRectanglesOnDisk();
};

class ConstructionNode: public Node {
 private:
  OrderedBoxes orderedBoxes;

 public:
  ConstructionNode(uint64_t id, OrderedBoxes orderedBoxes);
  OrderedBoxes GetOrderedBoxes();
  void AddChildrenToItem();
};

namespace boost::serialization {
template<class Archive>
void save(Archive & a, const Rtree::BoundingBox & b, [[maybe_unused]]unsigned int version)
{
  a << b.min_corner().get<0>();
  a << b.min_corner().get<1>();
  a << b.max_corner().get<0>();
  a << b.max_corner().get<1>();
}
template<class Archive>
void load(Archive & a, Rtree::BoundingBox & b, [[maybe_unused]]unsigned int version)
{
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
}
BOOST_SERIALIZATION_SPLIT_FREE(Rtree::BoundingBox);

/**
 * Data type for a value of the Rtree, which contains the id of the object and its bounding box.
 */
struct RTreeValue {
  Rtree::BoundingBox box{};
  uint64_t id = 0;

  template<class Archive>
  void serialize(Archive & a, [[maybe_unused]]const unsigned int version) {
    a & box;
    a & id;
  }
};

/**
 * Data type for a value of the Rtree (id and boundingbox), with the addtional information
 * of its position in the x- and y-sorting. This is only used to create the Rtree in a more efficient way
 */
struct RTreeValueWithOrderIndex {
  Rtree::BoundingBox box{};
  uint64_t id = 0;
  uint64_t orderX = 0;
  uint64_t orderY = 0;
};

/**
 * Data type containing all the information about the best split found, which are needed
 * to actually perform the split.
 */
struct SplitResult {
  double bestCost = -1;
  size_t bestDim = 0;
  uint64_t bestIndex = 0;
  RTreeValueWithOrderIndex bestLastElement;
  RTreeValueWithOrderIndex bestElement;
  RTreeValueWithOrderIndex bestMinElement;
  RTreeValueWithOrderIndex bestMaxElement;
};

/**
 * A Buffer data structure, containing vectors for the result of a split, while doing it in ram
 */
struct SplitBuffersRam {
  std::shared_ptr<multiBoxWithOrderIndex> s0Dim0 = std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s0Dim1 = std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s1Dim0 = std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s1Dim1 = std::make_shared<multiBoxWithOrderIndex>();

  std::shared_ptr<multiBoxWithOrderIndex> s0SmallDim0 = std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s0SmallDim1 = std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s1SmallDim0 = std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> s1SmallDim1 = std::make_shared<multiBoxWithOrderIndex>();
};

/**
 * A Buffer data structure, containing the write streams for the result of a split, while doing it on disk
 */
struct SplitBuffersDisk {
  SplitBuffersRam splitBuffersRam;
  std::optional<std::ofstream> split0Dim0File;
  std::optional<std::ofstream> split0Dim1File;
  std::optional<std::ofstream> split1Dim0File;
  std::optional<std::ofstream> split1Dim1File;
};

struct SortRuleLambdaX {
  // comparison function
  bool operator()(const RTreeValue& b1, const RTreeValue& b2) const {
    double center1 = (b1.box.min_corner().get<0>() + b1.box.max_corner().get<0>()) / 2;
    double center2 = (b2.box.min_corner().get<0>() + b2.box.max_corner().get<0>()) / 2;
    return center1 < center2;
  }

  // Value that is strictly smaller than any input element.
  static RTreeValue min_value() { return {Rtree::createBoundingBox(-DBL_MAX, -DBL_MAX, -DBL_MAX, -DBL_MAX), 0}; }

  // Value that is strictly larger than any input element.
  static RTreeValue max_value() { return {Rtree::createBoundingBox(DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX), 0}; }
};


struct SortRuleLambdaXWithIndex {
  // comparison function
  bool operator()(const RTreeValueWithOrderIndex& b1, const RTreeValueWithOrderIndex& b2) const {
    double center1 = (b1.box.min_corner().get<0>() + b1.box.max_corner().get<0>()) / 2;
    double center2 = (b2.box.min_corner().get<0>() + b2.box.max_corner().get<0>()) / 2;

    if (b1.orderX == b2.orderX)
      return center1 < center2;
    return b1.orderX < b2.orderX;
  }

  // Value that is strictly smaller than any input element.
  static RTreeValueWithOrderIndex min_value() { return {Rtree::createBoundingBox(-DBL_MAX, -DBL_MAX, -DBL_MAX, -DBL_MAX), 0, 0, 0}; }

  // Value that is strictly larger than any input element.
  static RTreeValueWithOrderIndex max_value() { return {Rtree::createBoundingBox(DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX), 0, LLONG_MAX, LLONG_MAX}; }
};

struct SortRuleLambdaYWithIndex {
  // comparison function
  bool operator()(const RTreeValueWithOrderIndex& b1, const RTreeValueWithOrderIndex& b2) const {
    double center1 = (b1.box.min_corner().get<1>() + b1.box.max_corner().get<1>()) / 2;
    double center2 = (b2.box.min_corner().get<1>() + b2.box.max_corner().get<1>()) / 2;

    if (b1.orderY == b2.orderY)
      return center1 < center2;
    return b1.orderY < b2.orderY;
  }

  // Value that is strictly smaller than any input element.
  static RTreeValueWithOrderIndex min_value() { return {Rtree::createBoundingBox(-DBL_MAX, -DBL_MAX, -DBL_MAX, -DBL_MAX), 0, 0, 0}; }

  // Value that is strictly larger than any input element.
  static RTreeValueWithOrderIndex max_value() { return {Rtree::createBoundingBox(DBL_MAX, DBL_MAX, DBL_MAX, DBL_MAX), 0, LLONG_MAX, LLONG_MAX}; }
};

#endif //QLEVER_RTREE_H
