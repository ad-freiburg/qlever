//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include "./Rtree.h"

#include "./RtreeFileReader.h"
#include "./RtreeNode.h"

multiBoxGeo Rtree::SearchTree(BasicGeometry::BoundingBox query,
                              const std::string& folder) {
  std::ifstream lookupIfs =
      std::ifstream(folder + "/lookup.bin", std::ios::binary);
  std::ifstream nodesIfs =
      std::ifstream(folder + "/nodes.bin", std::ios::binary);

  RtreeNode rootNode = FileReader::LoadNode(0, lookupIfs, nodesIfs);
  multiBoxGeo results;
  std::stack<RtreeNode> nodes;
  nodes.push(rootNode);

  while (!nodes.empty()) {
    RtreeNode currentNode = nodes.top();
    nodes.pop();

    for (RTreeValue child : currentNode.GetChildren()) {
      if (intersects(query, child.box)) {
        if (currentNode.GetIsLastInnerNode()) {
          results.push_back(child);
        } else {
          RtreeNode newNode =
              FileReader::LoadNode(child.id, lookupIfs, nodesIfs);
          nodes.push(newNode);
        }
      }
    }
  }

  lookupIfs.close();
  nodesIfs.close();
  return results;
}

Rtree::Rtree(uintmax_t maxBuildingRamUsage) {
  this->maxBuildingRamUsage_ = maxBuildingRamUsage;
}
