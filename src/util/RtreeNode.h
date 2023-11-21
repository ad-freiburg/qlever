//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#ifndef QLEVER_NODE_H
#define QLEVER_NODE_H

#include "./Rtree.h"

// ___________________________________________________________________________
// Data structure representing a single node of the Rtree containing the
// boundingbox and its children
class RtreeNode {
 protected:
  friend class boost::serialization::access;
  uint64_t id_{};
  BasicGeometry::BoundingBox boundingBox_{};
  bool isLastInnerNode_ =
      false;  // when true, this means that the node is the last inner node and
  // all of its children are leafs
  multiBoxGeo children_;

  template <class Archive>
  void serialize(Archive& a, [[maybe_unused]] const unsigned int version) {
    a& id_;
    a& isLastInnerNode_;
    a& boundingBox_;
    a& children_;
  }

 public:
  RtreeNode();
  explicit RtreeNode(uint64_t id, BasicGeometry::BoundingBox boundingBox = {},
                     bool isLastInnerNode = false, multiBoxGeo children = {});
  [[nodiscard]] uint64_t GetId() const;
  [[nodiscard]] BasicGeometry::BoundingBox GetBoundingBox() const;
  void AddChild(RtreeNode& child);
  void SetIsLastInnerNode(bool isLast);
  [[nodiscard]] bool GetIsLastInnerNode() const;
  multiBoxGeo GetChildren();

  bool operator==(const RtreeNode& other) const
  {
    if (id_ != other.id_) return false;
    if (!BasicGeometry::BoundingBoxesAreEqual(boundingBox_, other.boundingBox_)) return false;
    if (isLastInnerNode_ != other.isLastInnerNode_) return false;
    if (children_ != other.children_) return false;
    return true;
  }
};

BOOST_CLASS_VERSION(RtreeNode, 1)

// ___________________________________________________________________________
// Subclass of the RtreeNode only needed while constructing the Rtree (it keeps
// track of the remaining OrderedBoxes of the subtree)
class ConstructionNode : public RtreeNode {
 private:
  OrderedBoxes orderedBoxes_;

 public:
  ConstructionNode(uint64_t id, OrderedBoxes orderedBoxes);
  OrderedBoxes& GetOrderedBoxes();
  void AddChildrenToItem();
};

#endif  // QLEVER_NODE_H
