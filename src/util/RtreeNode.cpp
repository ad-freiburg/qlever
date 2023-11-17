//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include "./RtreeNode.h"

#include "./RtreeFileReader.h"

ConstructionNode::ConstructionNode(uint64_t id, OrderedBoxes orderedBoxes)
    : RtreeNode{id} {
  this->orderedBoxes_ = orderedBoxes;
  // calculate the boundingBoxes
  this->boundingBox_ = orderedBoxes.GetBoundingBox();
}

void ConstructionNode::AddChildrenToItem() {
  /**
   * Add all children of a certain node at once.
   * This is used when a leaf node is reached.
   */
  if (this->GetOrderedBoxes().WorkInRam()) {
    for (RTreeValueWithOrderIndex box :
         this->GetOrderedBoxes().GetRectanglesInRam()) {
      RtreeNode leafNode = RtreeNode(box.id, box.box);
      this->AddChild(leafNode);
    }
  } else {
    for (const RTreeValueWithOrderIndex& element :
         FileReader(this->GetOrderedBoxes().GetRectanglesOnDisk())) {
      RtreeNode leafNode = RtreeNode(element.id, element.box);
      this->AddChild(leafNode);
    }
  }
}

OrderedBoxes& ConstructionNode::GetOrderedBoxes() {
  return this->orderedBoxes_;
}

void RtreeNode::AddChild(RtreeNode& child) {
  BasicGeometry::BoundingBox box = child.GetBoundingBox();
  uint64_t entryId = child.GetId();
  RTreeValue entry = {box, entryId};
  this->children_.push_back(entry);
}

BasicGeometry::BoundingBox RtreeNode::GetBoundingBox() const {
  return this->boundingBox_;
}

void RtreeNode::SetIsLastInnerNode(bool isLast) {
  this->isLastInnerNode_ = isLast;
}

uint64_t RtreeNode::GetId() const { return this->id_; }

RtreeNode::RtreeNode() = default;

RtreeNode::RtreeNode(uint64_t id, BasicGeometry::BoundingBox boundingBox,
                     bool isLastInnerNode, multiBoxGeo children) {
  this->id_ = id;
  this->boundingBox_ = boundingBox;
  this->children_ = std::move(children);
  this->isLastInnerNode_ = isLastInnerNode;
}

bool RtreeNode::GetIsLastInnerNode() const { return this->isLastInnerNode_; }

multiBoxGeo RtreeNode::GetChildren() { return this->children_; }
