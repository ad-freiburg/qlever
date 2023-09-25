//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include <util/Rtree.h>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/vector.hpp>

bool intersects(const Rtree::BoundingBox& b1, const Rtree::BoundingBox& b2) {
  /**
   * Determine whether two bounding boxes intersect
   */
  bool notIntersecting = b1.min_corner().get<0>() > b2.max_corner().get<0>() ||
                         b2.min_corner().get<0>() > b1.max_corner().get<0>() ||
                         b1.min_corner().get<1>() > b2.max_corner().get<1>() ||
                         b2.min_corner().get<1>() > b1.max_corner().get<1>();

  return !notIntersecting;
}

Rtree::BoundingBox Rtree::createBoundingBox(double pointOneX, double pointOneY,
                                            double pointTwoX,
                                            double pointTwoY) {
  return {{pointOneX, pointOneY}, {pointTwoX, pointTwoY}};
}

Rtree::BoundingBox Rtree::combineBoundingBoxes(Rtree::BoundingBox b1,
                                               Rtree::BoundingBox b2) {
  if (b1.min_corner().get<0>() == 0 && b1.min_corner().get<1>() == 0 &&
      b1.max_corner().get<0>() == 0 && b1.max_corner().get<1>() == 0) {
    return b2;
  }
  if (b2.min_corner().get<0>() == 0 && b2.min_corner().get<1>() == 0 &&
      b2.max_corner().get<0>() == 0 && b2.max_corner().get<1>() == 0) {
    return b1;
  }
  auto minX = [](Rtree::BoundingBox b) -> double {
    return b.min_corner().get<0>();
  };
  auto minY = [](Rtree::BoundingBox b) -> double {
    return b.min_corner().get<1>();
  };
  auto maxX = [](Rtree::BoundingBox b) -> double {
    return b.max_corner().get<0>();
  };
  auto maxY = [](Rtree::BoundingBox b) -> double {
    return b.max_corner().get<1>();
  };

  double globalMinX = minX(b1) < minX(b2) ? minX(b1) : minX(b2);
  double globalMinY = minY(b1) < minY(b2) ? minY(b1) : minY(b2);
  double globalMaxX = maxX(b1) > maxX(b2) ? maxX(b1) : maxX(b2);
  double globalMaxY = maxY(b1) > maxY(b2) ? maxY(b1) : maxY(b2);

  return {{globalMinX, globalMinY}, {globalMaxX, globalMaxY}};
}

multiBoxGeo Rtree::SearchTree(Rtree::BoundingBox query,
                              const std::string& folder) {
  std::ifstream lookupIfs =
      std::ifstream(folder + "/lookup.bin", std::ios::binary);
  std::ifstream nodesIfs =
      std::ifstream(folder + "/nodes.bin", std::ios::binary);

  Node rootNode = LoadNode(0, lookupIfs, nodesIfs);
  multiBoxGeo results;
  std::stack<Node> nodes;
  nodes.push(rootNode);

  while (!nodes.empty()) {
    Node currentNode = nodes.top();
    nodes.pop();

    for (RTreeValue child : currentNode.GetChildren()) {
      if (intersects(query, child.box)) {
        if (currentNode.GetIsLastInnerNode()) {
          results.push_back(child);
        } else {
          Node newNode = LoadNode(child.id, lookupIfs, nodesIfs);
          nodes.push(newNode);
        }
      }
    }
  }

  lookupIfs.close();
  nodesIfs.close();
  return results;
}

uint64_t Node::GetId() const { return this->id; }

Node::Node(uint64_t id, Rtree::BoundingBox boundingbox) {
  this->id = id;
  this->boundingBox = boundingbox;
}

Node::Node(uint64_t id) { this->id = id; }

Node::Node() {}

Node::Node(uint64_t id, Rtree::BoundingBox boundingBox, multiBoxGeo& children,
           bool isLastInnerNode) {
  this->id = id;
  this->boundingBox = boundingBox;
  this->children = children;
  this->isLastInnerNode = isLastInnerNode;
}

Node::Node(uint64_t id, double minX, double minY, double maxX, double maxY,
           bool isLastInnerNode) {
  this->id = id;
  this->boundingBox = Rtree::createBoundingBox(minX, minY, maxX, maxY);
  this->isLastInnerNode = isLastInnerNode;
}

bool Node::GetIsLastInnerNode() const { return this->isLastInnerNode; }

multiBoxGeo Node::GetChildren() { return this->children; }

Node Rtree::LoadNode(uint64_t id, std::ifstream& lookupIfs,
                     std::ifstream& nodesIfs) {
  Node newNode;

  uint64_t offset = id * (uint64_t)sizeof(uint64_t);
  lookupIfs.seekg(offset, std::ios::beg);

  uint64_t nodePtr;
  lookupIfs.read(reinterpret_cast<char*>(&nodePtr), sizeof(uint64_t));

  nodesIfs.seekg(nodePtr);
  boost::archive::binary_iarchive ia(nodesIfs);
  ia >> newNode;

  return newNode;
}

Rtree::Rtree(uintmax_t maxBuildingRamUsage) {
  this->maxBuildingRamUsage = maxBuildingRamUsage;
}
