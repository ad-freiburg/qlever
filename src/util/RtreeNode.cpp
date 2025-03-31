//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include "./RtreeNode.h"
#include "./RtreeFileReader.h"

// ___________________________________________________________________________
ConstructionNode::ConstructionNode(uint64_t id, OrderedBoxes orderedBoxes)
    : RtreeNode{id, orderedBoxes.GetBoundingBox()}, orderedBoxes_{std::move(orderedBoxes)} { }

// ___________________________________________________________________________
void ConstructionNode::AddChildrenToItem() {
  /**
     * Add all children of a certain node at once.
     * This is used when a leaf node is reached.
   */
  if (orderedBoxes().WorkInRam()) {
    for (RTreeValueWithOrderIndex box :
         orderedBoxes().GetRectanglesInRam()) {
      RtreeNode leafNode = RtreeNode(box.id, box.box);
      AddChild(leafNode);
    }
  } else {
    for (const RTreeValueWithOrderIndex& element :
         FileReader(orderedBoxes().GetRectanglesOnDisk())) {
      RtreeNode leafNode = RtreeNode(element.id, element.box);
      AddChild(leafNode);
    }
  }
}

// ___________________________________________________________________________
OrderedBoxes& ConstructionNode::orderedBoxes() {
  return orderedBoxes_;
}

// ___________________________________________________________________________
void RtreeNode::AddChild(const RtreeNode& child) {
  if (!isSearchNode_) {
    RTreeValue entry = {child.GetBoundingBox(), child.GetId()};
    children_.push_back(entry);
  } else {
    childNodes_.push_back(child);
  }
}


// ___________________________________________________________________________
const BasicGeometry::BoundingBox& RtreeNode::GetBoundingBox() const {
  return boundingBox_;
}

// ___________________________________________________________________________
void RtreeNode::SetIsLastInnerNode(bool isLast) {
  isLastInnerNode_ = isLast;
}

// ___________________________________________________________________________
uint64_t RtreeNode::GetId() const { return id_; }

// ___________________________________________________________________________
RtreeNode::RtreeNode() = default;

// ___________________________________________________________________________
RtreeNode::RtreeNode(uint64_t id, BasicGeometry::BoundingBox boundingBox,
                     bool isLastInnerNode, multiBoxGeo children)
    : id_{id}, boundingBox_{std::move(boundingBox)}, isLastInnerNode_{isLastInnerNode},
      children_{std::move(children)}, childNodes_{std::vector<RtreeNode>()}, isSearchNode_{false}{ }

// ___________________________________________________________________________
void RtreeNode::SetIsSearchNode(bool isSearchNode) {
  isSearchNode_ = isSearchNode;
}

// ___________________________________________________________________________
void RtreeNode::ClearUnusedChildren() {
  if (isSearchNode_) {
    children_ = {};
  } else {
    childNodes_ = {};
  }
}

// ___________________________________________________________________________
std::vector<RtreeNode> RtreeNode::GetSearchChildren() {
  return childNodes_;
}

// ___________________________________________________________________________
bool RtreeNode::GetIsLastInnerNode() const { return isLastInnerNode_; }

// ___________________________________________________________________________
bool RtreeNode::GetIsSearchNode() const { return isSearchNode_; }

// ___________________________________________________________________________
multiBoxGeo RtreeNode::GetChildren() { return children_; }
