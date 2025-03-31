//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include "./Rtree.h"

#include "./RtreeFileReader.h"
#include "./RtreeNode.h"

#include <future>
#include <mutex>
#include <atomic>
#include <utility>

void searchDFS(BasicGeometry::BoundingBox query, const std::string& folder, std::stack<RtreeNode>& nodes,
               multiBoxGeo& results, std::mutex& stackMutex, std::mutex& resultMutex,
               std::atomic<int>& threadsRemaining) {
  std::ifstream lookupIfs =
      std::ifstream(folder + "/lookup.bin", std::ios::binary);
  std::ifstream nodesIfs =
      std::ifstream(folder + "/nodes.bin", std::ios::binary);

  bool currentlyActive = true;

  while (true) {
    RtreeNode currentNode;
    {
      std::lock_guard<std::mutex> lock(stackMutex);
      if (nodes.empty() && threadsRemaining.load() > 0) {
        if (currentlyActive) {
          threadsRemaining--;
          currentlyActive = false;
        }
        continue;
      } else if (nodes.empty() && threadsRemaining.load() == 0) {
        break;
      }
      if (!currentlyActive) {
        currentlyActive = true;
        threadsRemaining++;
      }
      currentNode = nodes.top();
      nodes.pop();
    }

    if (currentNode.GetIsSearchNode()) {
      for (const RtreeNode& child : currentNode.GetSearchChildren()) {
        if (intersects(query, child.GetBoundingBox())) {
          // in this case we are never at the last inner node due to the construction of the cache
          std::lock_guard<std::mutex> lock(stackMutex);
          nodes.push(child);
        }
      }
      continue;
    }

    for (RTreeValue child : currentNode.GetChildren()) {
      if (intersects(query, child.box)) {
        if (currentNode.GetIsLastInnerNode()) {
          std::lock_guard<std::mutex> lock(resultMutex);
          results.push_back(child);
        } else {
          RtreeNode newNode;
          {
            newNode = FileReader::LoadNode(child.id, lookupIfs, nodesIfs);
          }
          std::lock_guard<std::mutex> lock(stackMutex);
          nodes.push(newNode);
        }
      }
    }
  }

  lookupIfs.close();
  nodesIfs.close();
}

multiBoxGeo Rtree::SearchTree(BasicGeometry::BoundingBox query, const std::string& folder) {
  maxBuildingRamUsage_ = 0;
  SetupForSearch(folder);

  return SearchTree(query);
}

multiBoxGeo Rtree::SearchTree(BasicGeometry::BoundingBox query) {
  if (searchFolder_.empty()) {
    std::cout << "Error. Call SetupForSearch before calling SearchTree or call SearchTree(query, searchFolder)" << std::endl;
    return {};
  }

  multiBoxGeo results;
  std::stack<RtreeNode> nodes;
  nodes.push(*rootNode_);

  const int numThreads = (int) std::thread::hardware_concurrency();

  std::mutex stackMutex;
  std::mutex resultMutex;
  std::atomic<int> threadsRemaining;
  threadsRemaining.store(numThreads);

  std::vector<std::thread> threads;

  threads.reserve(numThreads);
  for (int i = 0; i < numThreads; ++i) {
    threads.emplace_back(searchDFS, query, searchFolder_, std::ref(nodes), std::ref(results),
                         std::ref(stackMutex), std::ref(resultMutex), std::ref(threadsRemaining));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return results;
}

void Rtree::SetupForSearch(std::string folder) {
  searchFolder_ = std::move(folder);
  std::queue<RtreeNode> nodesQueue = std::queue<RtreeNode>();

  std::ifstream lookupIfs =
      std::ifstream(searchFolder_ + "/lookup.bin", std::ios::binary);
  std::ifstream nodesIfs =
      std::ifstream(searchFolder_ + "/nodes.bin", std::ios::binary);

  RtreeNode rootNode = FileReader::LoadNode(0, lookupIfs, nodesIfs);
  rootNode_ = std::make_unique<RtreeNode>(rootNode);
  nodesQueue.push(*rootNode_);

  int64_t memoryEstimateOfNode = sizeof(uint64_t) + sizeof(BasicGeometry::BoundingBox) + 2 * sizeof(bool) + sizeof(multiBoxGeo) +
                                 sizeof(std::vector<RtreeNode>) + rootNode_->GetChildren().size() * sizeof(RTreeValue);

  int64_t totalMemoryEstimate = memoryEstimateOfNode;
  int64_t memoryAllowed = maxBuildingRamUsage_ - totalMemoryEstimate;

  while (!nodesQueue.empty()) {
    RtreeNode currentNode = nodesQueue.front();
    nodesQueue.pop();
    if (currentNode.GetIsLastInnerNode()) {
      continue;
    }
    memoryAllowed -= currentNode.GetChildren().size() * (memoryEstimateOfNode - sizeof(RTreeValue));
    if (memoryAllowed <= 0) {
      break;
    }

    currentNode.SetIsSearchNode(true);

    for (RTreeValue child : currentNode.GetChildren()) {
      RtreeNode newNode = FileReader::LoadNode(child.id, lookupIfs, nodesIfs);
      nodesQueue.push(newNode);
      currentNode.AddChild(newNode);
      totalMemoryEstimate += memoryEstimateOfNode;
      totalMemoryEstimate -= sizeof(RTreeValue);
    }
    currentNode.ClearUnusedChildren();
  }

  lookupIfs.close();
  nodesIfs.close();
}

Rtree::Rtree(uintmax_t maxBuildingRamUsage)
    : maxBuildingRamUsage_{maxBuildingRamUsage} {
}
