//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>
#include <util/BackgroundStxxlSorter.h>
#include <util/Rtree.h>
#include <util/RtreeFileReader.h>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>

#include "ctre/ctre.h"

static bool isBorderOfSplitCandidate(uint64_t current, uint64_t splitSize,
                                     uint64_t M) {
  if (((current + 1) % splitSize == 0 && (current + 1) / splitSize < M) ||
      (current % splitSize == 0 && current / splitSize >= 1))
    return true;
  return false;
}

static void centerOrdering(multiBoxGeo& boxes, size_t dim) {
  if (dim == 0) {
    // order by centerX
    std::sort(boxes.begin(), boxes.end(), SortRuleLambda<0>{});
  } else {
    // order by centerY
    std::sort(boxes.begin(), boxes.end(), SortRuleLambda<1>{});
  }
}

static void centerOrdering(multiBoxWithOrderIndex& boxes, size_t dim) {
  if (dim == 0) {
    // order by centerX
    std::sort(boxes.begin(), boxes.end(), SortRuleLambdaWithIndex<0>{});
  } else {
    // order by centerY
    std::sort(boxes.begin(), boxes.end(), SortRuleLambdaWithIndex<1>{});
  }
}

OrderedBoxes SortInput(const std::string& onDiskBase, size_t M,
                       uintmax_t maxBuildingRamUsage, bool workInRam) {
  OrderedBoxes orderedInputRectangles;

  auto maxRamForSorter =
      std::ceil((maxBuildingRamUsage < 9999999999.0 ? maxBuildingRamUsage
                                                    : 9999999999.0) /
                3.0);
  ad_utility::BackgroundStxxlSorter<RTreeValue, SortRuleLambda<0>>
      sorterRectsD0Basic =
          ad_utility::BackgroundStxxlSorter<RTreeValue, SortRuleLambda<0>>(
              maxRamForSorter);
  multiBoxGeo rectsD0Basic;

  if (workInRam) {
    rectsD0Basic = Rtree::LoadEntries(onDiskBase + ".boundingbox.tmp");
    centerOrdering(rectsD0Basic, 0);
  } else {
    for (const RTreeValue& rectD0Element :
         FileReaderWithoutIndex(onDiskBase + ".boundingbox.tmp")) {
      sorterRectsD0Basic.push(rectD0Element);
    }
  }

  uint64_t xSize = 0;
  Rtree::BoundingBox boundingBox = Rtree::createBoundingBox(0, 0, 0, 0);

  ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex,
                                    SortRuleLambdaWithIndex<1>>
      sorterRectsD1 =
          ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex,
                                            SortRuleLambdaWithIndex<1>>(
              maxRamForSorter);
  std::shared_ptr<multiBoxWithOrderIndex> RectanglesD1WithOrder =
      std::make_shared<multiBoxWithOrderIndex>();

  if (workInRam) {
    for (RTreeValue element : rectsD0Basic) {
      RTreeValueWithOrderIndex entry = {{element.box, element.id}, xSize, 0};
      RectanglesD1WithOrder->push_back(entry);
      xSize++;

      boundingBox = Rtree::combineBoundingBoxes(boundingBox, element.box);
    }
    centerOrdering(*RectanglesD1WithOrder, 1);
  } else {
    for (RTreeValue element : sorterRectsD0Basic.sortedView()) {
      RTreeValueWithOrderIndex entry = {{element.box, element.id}, xSize, 0};
      sorterRectsD1.push(entry);
      xSize++;

      boundingBox = Rtree::combineBoundingBoxes(boundingBox, element.box);
    }
  }
  sorterRectsD0Basic.clear();

  size_t currentS = std::ceil(((float)xSize) / ((float)M));

  uint64_t ySize = 0;
  std::ofstream r1File =
      std::ofstream(onDiskBase + ".boundingbox.d1.tmp", std::ios::binary);
  ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex,
                                    SortRuleLambdaWithIndex<0>>
      sorterRectsD0 =
          ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex,
                                            SortRuleLambdaWithIndex<0>>(
              maxRamForSorter);
  std::shared_ptr<multiBoxWithOrderIndex> RectanglesD0WithOrder =
      std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> r1Small =
      std::make_shared<multiBoxWithOrderIndex>();
  // placeholder
  r1Small->push_back(RTreeValueWithOrderIndex());
  r1Small->push_back(RTreeValueWithOrderIndex());
  RTreeValueWithOrderIndex minD1;
  RTreeValueWithOrderIndex maxD1;

  auto processD1Element = [&ySize, currentS, M, &r1Small, &minD1,
                           &maxD1](RTreeValueWithOrderIndex& element) {
    element.orderY = ySize;

    if (isBorderOfSplitCandidate(ySize, currentS, M)) {
      // index i * S - 1 or i * S
      r1Small->push_back(element);
    }

    if (ySize == 0) {
      minD1 = element;
    }

    maxD1 = element;

    ySize++;
  };

  if (workInRam) {
    for (RTreeValueWithOrderIndex element : *RectanglesD1WithOrder) {
      processD1Element(element);

      RectanglesD0WithOrder->push_back(element);
    }
    centerOrdering(*RectanglesD0WithOrder, 0);
  } else {
    for (RTreeValueWithOrderIndex element : sorterRectsD1.sortedView()) {
      processD1Element(element);

      Rtree::SaveEntryWithOrderIndex(element, r1File);
      sorterRectsD0.push(element);
    }
  }

  r1File.close();
  sorterRectsD1.clear();

  // replace the placeholder
  (*r1Small)[0] = minD1;
  (*r1Small)[1] = maxD1;

  uint64_t currentX = 0;
  std::ofstream r0File =
      std::ofstream(onDiskBase + ".boundingbox.d0.tmp", std::ios::binary);
  std::shared_ptr<multiBoxWithOrderIndex> r0Small =
      std::make_shared<multiBoxWithOrderIndex>();
  // placeholder
  r0Small->push_back(RTreeValueWithOrderIndex());
  r0Small->push_back(RTreeValueWithOrderIndex());
  RTreeValueWithOrderIndex minD0;
  RTreeValueWithOrderIndex maxD0;

  auto processD0Element = [&currentX, currentS, M, &r0Small, &minD0,
                           &maxD0](RTreeValueWithOrderIndex& element) {
    if (isBorderOfSplitCandidate(currentX, currentS, M)) {
      // index i * S - 1 or i * S
      r0Small->push_back(element);
    }

    if (currentX == 0) {
      minD0 = element;
    }
    maxD0 = element;

    currentX++;
  };

  if (workInRam) {
    for (RTreeValueWithOrderIndex element : *RectanglesD0WithOrder) {
      processD0Element(element);
    }
  } else {
    for (RTreeValueWithOrderIndex element : sorterRectsD0.sortedView()) {
      Rtree::SaveEntryWithOrderIndex(element, r0File);

      processD0Element(element);
    }
  }

  r0File.close();
  sorterRectsD0.clear();

  // replace the placeholder
  (*r0Small)[0] = minD0;
  (*r0Small)[1] = maxD0;

  RectanglesForOrderedBoxes rectsD0;
  RectanglesForOrderedBoxes rectsD1;
  rectsD0.rectanglesSmall = r0Small;
  rectsD1.rectanglesSmall = r1Small;
  if (workInRam) {
    rectsD0.rectanglesInRam = RectanglesD0WithOrder;
    rectsD1.rectanglesInRam = RectanglesD1WithOrder;
    orderedInputRectangles.SetOrderedBoxesToRam(rectsD0, rectsD1, boundingBox);
  } else {
    rectsD0.rectanglesOnDisk = onDiskBase + ".boundingbox.d0";
    rectsD1.rectanglesOnDisk = onDiskBase + ".boundingbox.d1";
    orderedInputRectangles.SetOrderedBoxesToDisk(rectsD0, rectsD1, xSize,
                                                 boundingBox);
  }
  return orderedInputRectangles;
}

/*OrderedBoxes SortInput(const std::string& onDiskBase, size_t M, uintmax_t
maxBuildingRamUsage, bool workInRam) { if (workInRam) { return
InternalSort(onDiskBase, M); } else { return ExternalSort(onDiskBase, M,
maxBuildingRamUsage);
  }
}*/

static double costFunctionTGS(Rtree::BoundingBox& b0, Rtree::BoundingBox& b1,
                              size_t dim) {
  /**
   * The cost function determines the quality of a split. The lower the cost,
   * the better the split. Each split gets represented by the resulting bounding
   * boxes of the split pieces.
   */
  double cost;

  // The cost represents the overlap of the two boxes
  if (dim == 0) {
    cost = b0.max_corner().get<0>() - b1.min_corner().get<0>();
    cost = cost < 0 ? 0 : cost;
  } else {
    cost = b0.max_corner().get<1>() - b1.min_corner().get<1>();
    cost = cost < 0 ? 0 : cost;
  }

  return cost;
}

static std::vector<OrderedBoxes> TGSRecursive(
    const std::string& filePath, OrderedBoxes orderedInputRectangles, size_t M,
    size_t S, uint64_t maxBuildingRamUsage) {
  /**
   * This function recursively constructs one layer of children for a certain
   * root node. The input rectangles must be sorted in both x- and y-direction.
   * The algorithm is based on this paper
   * https://dl.acm.org/doi/pdf/10.1145/288692.288723
   */

  uint64_t n = orderedInputRectangles.GetSize();

  if (n <= S || n <= M) {
    // stop condition
    return std::vector<OrderedBoxes>{orderedInputRectangles};
  }
  // split the rectangles at the best split
  std::pair<OrderedBoxes, OrderedBoxes> split =
      orderedInputRectangles.SplitAtBest(filePath, S, M, maxBuildingRamUsage);

  // recursion
  std::vector<OrderedBoxes> result0 =
      TGSRecursive(filePath + ".0", split.first, M, S, maxBuildingRamUsage);
  std::vector<OrderedBoxes> result1 =
      TGSRecursive(filePath + ".1", split.second, M, S, maxBuildingRamUsage);

  std::vector<OrderedBoxes> result;
  result.insert(result.begin(), result0.begin(), result0.end());
  result.insert(result.end(), result1.begin(), result1.end());

  return result;
}

void Rtree::BuildTree(const std::string& onDiskBase, size_t M,
                      const std::string& folder) const {
  const std::string file = onDiskBase + ".boundingbox.tmp";

  // prepare the files
  std::filesystem::create_directory(folder);
  std::ofstream nodesOfs =
      std::ofstream(folder + "/nodes.bin", std::ios::binary);
  std::map<uint64_t, uint64_t> lookup;

  // sort the rectangles
  uint64_t fileLines =
      std::ceil(std::filesystem::file_size(file) /
                (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t)));
  bool workInRam =
      (std::filesystem::file_size(file) + fileLines * 2 * sizeof(uint64_t)) *
          4 <
      this->maxBuildingRamUsage;

  OrderedBoxes orderedInputRectangles =
      SortInput(onDiskBase, M, maxBuildingRamUsage, workInRam);

  // build the tree in a depth first approach
  std::stack<ConstructionNode> layerStack;

  uint64_t newId = 1;  // start from 1, because 0 is the root item
  ConstructionNode rootItem = ConstructionNode(0, orderedInputRectangles);
  layerStack.push(rootItem);
  size_t layer = 0;

  while (!layerStack.empty()) {
    ConstructionNode currentItem = layerStack.top();
    layerStack.pop();

    if (currentItem.GetOrderedBoxes().GetSize() <= M) {
      // reached a leaf
      currentItem.AddChildrenToItem();
      uint64_t nodePtr = SaveNode(currentItem, true, nodesOfs);
      lookup[currentItem.GetId()] = nodePtr;
    } else {
      std::vector<OrderedBoxes> tgsResult = TGSRecursive(
          onDiskBase + ".boundingbox." + std::to_string(layer),
          currentItem.GetOrderedBoxes(), M,
          std::ceil(((float)currentItem.GetOrderedBoxes().GetSize()) /
                    ((float)M)),
          this->maxBuildingRamUsage);
      for (OrderedBoxes& currentOrderedRectangles : tgsResult) {
        ConstructionNode newItem =
            ConstructionNode(newId, currentOrderedRectangles);
        layerStack.push(newItem);

        currentItem.AddChild(newItem);

        newId++;
      }

      uint64_t nodePtr = SaveNode(currentItem, false, nodesOfs);
      lookup[currentItem.GetId()] = nodePtr;
    }
    layer++;
  }
  nodesOfs.close();

  std::ofstream lookupOfs(folder + "/lookup.bin", std::ios::binary);
  for (unsigned int i = 0; i < newId; i++) {
    uint64_t nodePtr = lookup[i];
    lookupOfs.write(reinterpret_cast<const char*>(&nodePtr), sizeof(uint64_t));
  }
  lookupOfs.close();
}

ConstructionNode::ConstructionNode(uint64_t id, OrderedBoxes orderedBoxes)
    : Node{id} {
  this->orderedBoxes = orderedBoxes;

  // calculate the boundingBoxes
  this->boundingBox = orderedBoxes.GetBoundingBox();
}

void ConstructionNode::AddChildrenToItem() {
  /**
   * Add all children of a certain node at once.
   * This is used when a leaf node is reached.
   */
  if (this->GetOrderedBoxes().WorkInRam()) {
    for (RTreeValueWithOrderIndex box :
         *this->GetOrderedBoxes().GetRectanglesInRam()) {
      Node leafNode = Node(box.id, box.box);
      this->AddChild(leafNode);
    }
  } else {
    for (const RTreeValueWithOrderIndex& element :
         FileReader(this->GetOrderedBoxes().GetRectanglesOnDisk())) {
      Node leafNode = Node(element.id, element.box);
      this->AddChild(leafNode);
    }
  }
}

OrderedBoxes ConstructionNode::GetOrderedBoxes() { return this->orderedBoxes; }

void Node::AddChild(Node& child) {
  Rtree::BoundingBox box = child.GetBoundingBox();
  uint64_t entryId = child.GetId();
  RTreeValue entry = {box, entryId};
  this->children.push_back(entry);
}

Rtree::BoundingBox Node::GetBoundingBox() const { return this->boundingBox; }

void Node::SetIsLastInnerNode(bool isLast) { this->isLastInnerNode = isLast; }

uint64_t Rtree::SaveNode(Node& node, bool isLastInnerNode,
                         std::ofstream& nodesOfs) {
  node.SetIsLastInnerNode(isLastInnerNode);

  uint64_t pos = static_cast<uint64_t>(nodesOfs.tellp());
  boost::archive::binary_oarchive archive(nodesOfs);
  archive << node;
  nodesOfs.write(" ", 1);

  return pos;
}

std::optional<Rtree::BoundingBox> GetBoundingBoxFromWKT(
    const std::string& wkt) {
  /**
   * Parse the wkt literal in a way, that only the relevant data for the rtree
   * gets read in.
   */
  double maxDouble = std::numeric_limits<double>::max();

  double minX = maxDouble;
  double maxX = -maxDouble;
  double minY = maxDouble;
  double maxY = -maxDouble;

  // Iterate over matches and capture x and y coordinates
  for (auto match :
       ctre::range<R"(( *([\-|\+]?[0-9]+.[0-9]+) +([\-|\+]?[0-9]+.[0-9]+)))">(
           wkt)) {
    double x = std::stod(std::string(match.get<1>()));
    double y = std::stod(std::string(match.get<2>()));

    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }

  return {Rtree::createBoundingBox(minX, minY, maxX, maxY)};
}

std::optional<Rtree::BoundingBox> Rtree::ConvertWordToRtreeEntry(
    const std::string& wkt) {
  /*
   * Convert a single wkt literal to a boundingbox.
   * Get the bounding box(es) of either a multipolygon, polygon or a linestring
   */

  if (wkt.starts_with("\"MULTIPOLYGON") || wkt.starts_with("\"POLYGON") ||
      wkt.starts_with("\"LINESTRING")) {
    return GetBoundingBoxFromWKT(wkt);
  }

  return {};
}

void Rtree::SaveEntry(Rtree::BoundingBox boundingBox, uint64_t index,
                      std::ofstream& convertOfs) {
  /**
   * Save a single entry (which was e.g. converted by ConvertWordToRtreeEntry)
   * to the disk
   */
  double minX = boundingBox.min_corner().get<0>();
  double minY = boundingBox.min_corner().get<1>();
  double maxX = boundingBox.max_corner().get<0>();
  double maxY = boundingBox.max_corner().get<1>();

  convertOfs.write(reinterpret_cast<const char*>(&minX), sizeof(double));
  convertOfs.write(reinterpret_cast<const char*>(&minY), sizeof(double));
  convertOfs.write(reinterpret_cast<const char*>(&maxX), sizeof(double));
  convertOfs.write(reinterpret_cast<const char*>(&maxY), sizeof(double));
  convertOfs.write(reinterpret_cast<const char*>(&index), sizeof(uint64_t));
}

void Rtree::SaveEntryWithOrderIndex(RTreeValueWithOrderIndex treeValue,
                                    std::ofstream& convertOfs) {
  /**
   * Save a single entry, containing its postion in the x- and y-sorting
   */
  double minX = treeValue.MinX();
  double minY = treeValue.MinY();
  double maxX = treeValue.MaxX();
  double maxY = treeValue.MaxY();

  convertOfs.write(reinterpret_cast<const char*>(&minX), sizeof(double));
  convertOfs.write(reinterpret_cast<const char*>(&minY), sizeof(double));
  convertOfs.write(reinterpret_cast<const char*>(&maxX), sizeof(double));
  convertOfs.write(reinterpret_cast<const char*>(&maxY), sizeof(double));
  convertOfs.write(reinterpret_cast<const char*>(&treeValue.id),
                   sizeof(uint64_t));
  convertOfs.write(reinterpret_cast<const char*>(&treeValue.orderX),
                   sizeof(uint64_t));
  convertOfs.write(reinterpret_cast<const char*>(&treeValue.orderY),
                   sizeof(uint64_t));
}

multiBoxGeo Rtree::LoadEntries(const std::string& file) {
  multiBoxGeo boxes;

  for (const RTreeValue& element : FileReaderWithoutIndex(file)) {
    boxes.push_back(element);
  }

  return boxes;
}

multiBoxWithOrderIndex Rtree::LoadEntriesWithOrderIndex(
    const std::string& file) {
  multiBoxWithOrderIndex boxes;

  for (const RTreeValueWithOrderIndex& element : FileReader(file)) {
    boxes.push_back(element);
  }

  return boxes;
}

bool OrderedBoxes::WorkInRam() const { return this->workInRam; }

void OrderedBoxes::SetOrderedBoxesToRam(RectanglesForOrderedBoxes& rectanglesD0,
                                        RectanglesForOrderedBoxes& rectanglesD1,
                                        Rtree::BoundingBox box) {
  SetOrderedBoxesToDisk(rectanglesD0, rectanglesD1,
                        (*rectsD0.rectanglesInRam).size(), box);
}

void OrderedBoxes::SetOrderedBoxesToDisk(
    RectanglesForOrderedBoxes& rectanglesD0,
    RectanglesForOrderedBoxes& rectanglesD1, uint64_t size,
    Rtree::BoundingBox box) {
  this->workInRam = false;
  this->rectsD0 = rectanglesD0;
  this->rectsD1 = rectanglesD1;
  this->size = size;
  this->boundingBox = box;
}

Rtree::BoundingBox OrderedBoxes::GetBoundingBox() { return this->boundingBox; }

uint64_t OrderedBoxes::GetSize() const { return this->size; }

std::shared_ptr<multiBoxWithOrderIndex> OrderedBoxes::GetRectanglesInRam() {
  return this->rectsD0.rectanglesInRam;
}

std::string OrderedBoxes::GetRectanglesOnDisk() {
  return this->rectsD0.rectanglesOnDisk;
}

SplitResult OrderedBoxes::GetBestSplit() {
  /**
   * Determine based on the "small-lists", which split is the best for the
   * rtree.
   */
  struct SplitResult splitResult;

  RTreeValueWithOrderIndex minElement;
  RTreeValueWithOrderIndex maxElement;
  RTreeValueWithOrderIndex currentLastElement;
  RTreeValueWithOrderIndex currentElement;

  // This bool is used, since we need every other element as our element "S * i"
  // (described in the algorithm) To perform the split better, the element
  // before it (S * i - 1) is saved as well
  bool currentlyAtSTimesI = false;

  for (size_t dim = 0; dim < 2; dim++) {
    for (uint64_t i = 0; i < this->rectsD0.rectanglesSmall->size(); i++) {
      currentElement = dim == 0 ? (*this->rectsD0.rectanglesSmall)[i]
                                : (*this->rectsD1.rectanglesSmall)[i];

      if (i == 0) {
        // this is the min element
        minElement = currentElement;
        continue;
      }

      if (i == 1) {
        // this is the max element
        maxElement = currentElement;
        continue;
      }

      if (!currentlyAtSTimesI) {
        currentLastElement = currentElement;
        currentlyAtSTimesI = true;
        continue;
      }

      if (!currentlyAtSTimesI || currentElement.id != maxElement.id) {
        break;
      }

      currentlyAtSTimesI = false;

      // the current element is a possible split position.
      double minXB0 = minElement.MinX();
      double maxXB0 = currentLastElement.MaxX();
      double minXB1 = currentElement.MinX();
      double maxXB1 = maxElement.MaxX();

      double minYB0 = minElement.MinY();
      double maxYB0 = currentLastElement.MaxY();
      double minYB1 = currentElement.MinY();
      double maxYB1 = maxElement.MaxY();

      Rtree::BoundingBox b0 =
          Rtree::createBoundingBox(minXB0, minYB0, maxXB0, maxYB0);
      Rtree::BoundingBox b1 =
          Rtree::createBoundingBox(minXB1, minYB1, maxXB1, maxYB1);

      double cost = costFunctionTGS(b0, b1, dim);

      if (splitResult.bestCost == -1 || cost < splitResult.bestCost) {
        splitResult.bestCost = cost;
        splitResult.bestDim = dim;
        splitResult.bestLastElement = currentLastElement;
        splitResult.bestElement = currentElement;
        splitResult.bestMinElement = minElement;
        splitResult.bestMaxElement = maxElement;
        splitResult.bestIndex = i;
      }
    }
    currentlyAtSTimesI = false;
  }

  return splitResult;
}

std::pair<OrderedBoxes, OrderedBoxes> OrderedBoxes::SplitAtBest(
    const std::string& filePath, size_t S, size_t M,
    uint64_t maxBuildingRamUsage) {
  if (this->workInRam) {
    return this->SplitAtBestInRam(S, M);
  } else {
    return this->SplitAtBestOnDisk(filePath, S, M, maxBuildingRamUsage);
  }
}

std::pair<OrderedBoxes, OrderedBoxes> OrderedBoxes::SplitAtBestInRam(size_t S,
                                                                     size_t M) {
  /**
   * Split the ordered boxes in ram. First determine the best split and then
   * perform it
   */

  struct SplitResult splitResult = this->GetBestSplit();

  OrderedBoxes split0;
  OrderedBoxes split1;

  struct SplitBuffersRam splitBuffers;

  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> boundingBoxes =
      PerformSplit(splitResult, splitBuffers, M, S);

  RectanglesForOrderedBoxes rectsD0Split0 = {splitBuffers.s0Dim0, "",
                                             splitBuffers.s0SmallDim0};
  RectanglesForOrderedBoxes rectsD1Split0 = {splitBuffers.s0Dim1, "",
                                             splitBuffers.s0SmallDim1};
  RectanglesForOrderedBoxes rectsD0Split1 = {splitBuffers.s1Dim0, "",
                                             splitBuffers.s1SmallDim0};
  RectanglesForOrderedBoxes rectsD1Split1 = {splitBuffers.s1Dim1, "",
                                             splitBuffers.s1SmallDim1};
  split0.SetOrderedBoxesToRam(rectsD0Split0, rectsD1Split0,
                              boundingBoxes.first);
  split1.SetOrderedBoxesToRam(rectsD0Split1, rectsD1Split1,
                              boundingBoxes.second);

  return std::make_pair(split0, split1);
}

std::pair<OrderedBoxes, OrderedBoxes> OrderedBoxes::SplitAtBestOnDisk(
    const std::string& filePath, size_t S, size_t M,
    uint64_t maxBuildingRamUsage) {
  /**
   * Split the ordered boxes on disk. First determine the best split and then
   * perform it
   */

  OrderedBoxes split0;
  OrderedBoxes split1;

  struct SplitResult splitResult = this->GetBestSplit();

  struct SplitBuffersDisk splitBuffers;
  struct SplitBuffersRam splitBuffersRam;

  // perfrom the split
  uint64_t sizeLeft = std::ceil((splitResult.bestIndex - 2) / 2.0) * S;
  uint64_t sizeRight = this->size - sizeLeft;
  uint64_t split0ByteSize =
      sizeLeft * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t));
  uint64_t split1ByteSize = sizeRight * (4 * sizeof(double) + sizeof(uint64_t) +
                                         2 * sizeof(uint64_t));
  bool split0InRam = split0ByteSize * 4 < maxBuildingRamUsage;
  bool split1InRam = split1ByteSize * 4 < maxBuildingRamUsage;

  if (!split0InRam) {
    splitBuffers.split0Dim0File = {
        std::ofstream(filePath + ".0.dim0.tmp", std::ios::binary)};
    splitBuffers.split0Dim1File = {
        std::ofstream(filePath + ".0.dim1.tmp", std::ios::binary)};
  }

  if (!split1InRam) {
    splitBuffers.split1Dim0File = {
        std::ofstream(filePath + ".1.dim0.tmp", std::ios::binary)};
    splitBuffers.split1Dim1File = {
        std::ofstream(filePath + ".1.dim1.tmp", std::ios::binary)};
  }

  splitBuffers.splitBuffersRam = splitBuffersRam;

  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> boundingBoxes =
      PerformSplit(splitResult, splitBuffers, M, S, maxBuildingRamUsage);

  RectanglesForOrderedBoxes rectsD0Split0;
  RectanglesForOrderedBoxes rectsD1Split0;
  RectanglesForOrderedBoxes rectsD0Split1;
  RectanglesForOrderedBoxes rectsD1Split1;
  rectsD0Split0.rectanglesInRam = splitBuffers.splitBuffersRam.s0SmallDim0;
  rectsD1Split0.rectanglesInRam = splitBuffers.splitBuffersRam.s0SmallDim1;
  rectsD0Split1.rectanglesInRam = splitBuffers.splitBuffersRam.s1SmallDim0;
  rectsD1Split1.rectanglesInRam = splitBuffers.splitBuffersRam.s1SmallDim1;

  if (!split0InRam) {
    splitBuffers.split0Dim0File.value().close();
    splitBuffers.split0Dim1File.value().close();

    rectsD0Split0.rectanglesOnDisk = filePath + ".0.dim0";
    rectsD1Split0.rectanglesOnDisk = filePath + ".0.dim1";

    split0.SetOrderedBoxesToDisk(rectsD0Split0, rectsD1Split0, sizeLeft,
                                 boundingBoxes.first);
  } else {
    rectsD0Split0.rectanglesInRam = splitBuffers.splitBuffersRam.s0Dim0;
    rectsD1Split0.rectanglesInRam = splitBuffers.splitBuffersRam.s0Dim1;
    split0.SetOrderedBoxesToRam(rectsD0Split0, rectsD1Split0,
                                boundingBoxes.first);
  }

  if (!split1InRam) {
    splitBuffers.split1Dim0File.value().close();
    splitBuffers.split1Dim1File.value().close();

    rectsD0Split1.rectanglesOnDisk = filePath + ".1.dim0";
    rectsD1Split1.rectanglesOnDisk = filePath + ".1.dim1";

    split1.SetOrderedBoxesToDisk(rectsD0Split1, rectsD1Split1, sizeRight,
                                 boundingBoxes.second);
  } else {
    rectsD0Split1.rectanglesInRam = splitBuffers.splitBuffersRam.s1Dim0;
    rectsD1Split1.rectanglesInRam = splitBuffers.splitBuffersRam.s1Dim1;
    split1.SetOrderedBoxesToRam(rectsD0Split1, rectsD1Split1,
                                boundingBoxes.second);
  }

  std::remove(this->rectsD0.rectanglesOnDisk.c_str());
  std::remove(this->rectsD1.rectanglesOnDisk.c_str());

  return std::make_pair(split0, split1);
}

std::pair<Rtree::BoundingBox, Rtree::BoundingBox> OrderedBoxes::PerformSplit(
    SplitResult splitResult, SplitBuffersRam& splitBuffersRam, size_t M,
    size_t S) {
  /**
   * Perform the best split on the current ordered boxes in the ram case
   */

  struct SplitBuffersDisk splitBuffersDisk;

  splitBuffersDisk.splitBuffersRam = splitBuffersRam;
  splitBuffersDisk.split0Dim0File = {};
  splitBuffersDisk.split0Dim1File = {};
  splitBuffersDisk.split1Dim0File = {};
  splitBuffersDisk.split1Dim1File = {};

  // reuse the PerfromSplit of the Disk case.
  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> boundingBoxes =
      PerformSplit(splitResult, splitBuffersDisk, M, S, 0);

  splitBuffersRam = splitBuffersDisk.splitBuffersRam;

  return boundingBoxes;
}

std::pair<Rtree::BoundingBox, Rtree::BoundingBox> OrderedBoxes::PerformSplit(
    SplitResult splitResult, SplitBuffersDisk& splitBuffers, size_t M, size_t S,
    uint64_t maxBuildingRamUsage) {
  /**
   * Perform the best split on the current ordered boxes in the disk case
   */

  uint64_t sizeLeft = std::ceil((splitResult.bestIndex - 2) / 2.0) * S;
  uint64_t sizeRight = this->size - sizeLeft;
  size_t SSplit0 = sizeLeft <= S ? std::ceil(sizeLeft / (double)M) : S;
  size_t SSplit1 = sizeRight <= S ? std::ceil(sizeRight / (double)M) : S;
  uint64_t split0ByteSize =
      sizeLeft * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t));
  uint64_t split1ByteSize = sizeRight * (4 * sizeof(double) + sizeof(uint64_t) +
                                         2 * sizeof(uint64_t));
  bool split0InRam =
      maxBuildingRamUsage == 0 || split0ByteSize * 4 < maxBuildingRamUsage;
  bool split1InRam =
      maxBuildingRamUsage == 0 || split1ByteSize * 4 < maxBuildingRamUsage;

  Rtree::BoundingBox boxSplit0 = Rtree::createBoundingBox(0, 0, 0, 0);
  Rtree::BoundingBox boxSplit1 = Rtree::createBoundingBox(0, 0, 0, 0);

  RTreeValueWithOrderIndex minSplit0OtherDim;
  RTreeValueWithOrderIndex maxSplit0OtherDim;
  RTreeValueWithOrderIndex minSplit1OtherDim;
  RTreeValueWithOrderIndex maxSplit1OtherDim;

  struct OtherDimension {
    std::shared_ptr<multiBoxWithOrderIndex> smallSplit0;
    std::shared_ptr<multiBoxWithOrderIndex> smallSplit1;
  } otherDimension;

  if (splitResult.bestDim == 0) {
    splitBuffers.splitBuffersRam.s0SmallDim0->push_back(
        splitResult.bestMinElement);
    splitBuffers.splitBuffersRam.s0SmallDim0->push_back(
        splitResult.bestLastElement);
    splitBuffers.splitBuffersRam.s1SmallDim0->push_back(
        splitResult.bestElement);
    splitBuffers.splitBuffersRam.s1SmallDim0->push_back(
        splitResult.bestMaxElement);

    // placeholder, since we need the min and max element of the split in the
    // first two spots
    otherDimension.smallSplit0 = splitBuffers.splitBuffersRam.s0SmallDim1;
    otherDimension.smallSplit1 = splitBuffers.splitBuffersRam.s1SmallDim1;

    otherDimension.smallSplit0->push_back(RTreeValueWithOrderIndex());
    otherDimension.smallSplit0->push_back(RTreeValueWithOrderIndex());
    otherDimension.smallSplit1->push_back(RTreeValueWithOrderIndex());
    otherDimension.smallSplit1->push_back(RTreeValueWithOrderIndex());
  } else {
    splitBuffers.splitBuffersRam.s0SmallDim1->push_back(
        splitResult.bestMinElement);
    splitBuffers.splitBuffersRam.s0SmallDim1->push_back(
        splitResult.bestLastElement);
    splitBuffers.splitBuffersRam.s1SmallDim1->push_back(
        splitResult.bestElement);
    splitBuffers.splitBuffersRam.s1SmallDim1->push_back(
        splitResult.bestMaxElement);

    // placeholder
    otherDimension.smallSplit0 = splitBuffers.splitBuffersRam.s0SmallDim0;
    otherDimension.smallSplit1 = splitBuffers.splitBuffersRam.s1SmallDim0;

    otherDimension.smallSplit0->push_back(RTreeValueWithOrderIndex());
    otherDimension.smallSplit0->push_back(RTreeValueWithOrderIndex());
    otherDimension.smallSplit1->push_back(RTreeValueWithOrderIndex());
    otherDimension.smallSplit1->push_back(RTreeValueWithOrderIndex());
  }

  std::optional<RTreeValueWithOrderIndex> elementOpt;
  std::optional<FileReader> fileReaderDim0;
  std::optional<FileReader> fileReaderDim1;
  if (!this->workInRam) {
    fileReaderDim0 = {FileReader(this->rectsD0.rectanglesOnDisk)};
    fileReaderDim1 = {FileReader(this->rectsD1.rectanglesOnDisk)};
  }
  FileReader::iterator fileReaderDim0Iterator =
      fileReaderDim0 ? fileReaderDim0.value().begin() : FileReader::iterator();
  FileReader::iterator fileReaderDim1Iterator =
      fileReaderDim1 ? fileReaderDim1.value().begin() : FileReader::iterator();
  uint64_t currentXSplit0 = 0;
  uint64_t currentXSplit1 = 0;
  uint64_t currentYSplit0 = 0;
  uint64_t currentYSplit1 = 0;

  auto performCertainSplit =
      [M, &splitBuffers, &splitResult](
          size_t dim, size_t split, uint64_t& current,
          uint64_t& currentSplitSize, RTreeValueWithOrderIndex& minElement,
          RTreeValueWithOrderIndex& maxElement, bool currentSplitInRam,
          bool workInRam, RTreeValueWithOrderIndex& element,
          Rtree::BoundingBox& box) {
        std::shared_ptr<multiBoxWithOrderIndex> currentList;
        std::shared_ptr<multiBoxWithOrderIndex> currentSmallList;
        std::ofstream* currentFile;

        if (split == 0) {
          if (dim == 0) {
            currentList = splitBuffers.splitBuffersRam.s0Dim0;
            currentSmallList = splitBuffers.splitBuffersRam.s0SmallDim0;
            currentFile = &splitBuffers.split0Dim0File.value();
          } else {
            currentList = splitBuffers.splitBuffersRam.s0Dim1;
            currentSmallList = splitBuffers.splitBuffersRam.s0SmallDim1;
            currentFile = &splitBuffers.split0Dim1File.value();
          }
        } else {
          if (dim == 0) {
            currentList = splitBuffers.splitBuffersRam.s1Dim0;
            currentSmallList = splitBuffers.splitBuffersRam.s1SmallDim0;
            currentFile = &splitBuffers.split1Dim0File.value();
          } else {
            currentList = splitBuffers.splitBuffersRam.s1Dim1;
            currentSmallList = splitBuffers.splitBuffersRam.s1SmallDim1;
            currentFile = &splitBuffers.split1Dim1File.value();
          }
        }

        // add the element to the current split dimension 0/1 vector / file
        if (currentSplitInRam || workInRam) {
          currentList->push_back(element);
        } else {
          Rtree::SaveEntryWithOrderIndex(element, *currentFile);
        }

        // check if the element is at the position i * S (described in the
        // algorithm) or one before it. In this case it is a future possible
        // split position and needs to be saved to the "small list"
        if (isBorderOfSplitCandidate(current, currentSplitSize, M)) {
          // index i * S - 1 or i * S
          currentSmallList->push_back(element);
        }

        // update the boundingbox to get the whole boundingbox of the split
        if (dim == 0) box = Rtree::combineBoundingBoxes(box, element.box);

        // keep track of the min and max element of the split, to later
        // replace the placeholder in the "small lists"
        if (splitResult.bestDim == 1 - dim) {
          if (current == 0) {
            minElement = element;
          }
          // max element gets updated each time, because the elements are sorted
          // in an ascending way
          maxElement = element;
        }

        current++;
      };

  for (size_t dim = 0; dim < 2; dim++) {
    // start performing the actual split
    uint64_t i = 0;

    if (!this->workInRam &&
        fileReaderDim0Iterator != fileReaderDim0.value().end() &&
        fileReaderDim1Iterator != fileReaderDim1.value().end()) {
      if (dim == 0) elementOpt = *fileReaderDim0Iterator;
      if (dim == 1) elementOpt = *fileReaderDim1Iterator;
    }

    while ((this->workInRam && i < this->size) ||
           (!this->workInRam && elementOpt)) {
      RTreeValueWithOrderIndex element;

      // get the current element, either from disk or from ram
      if (this->workInRam) {
        element = dim == 0 ? (*this->rectsD0.rectanglesInRam)[i]
                           : (*this->rectsD1.rectanglesInRam)[i];
      } else {
        element = elementOpt.value();
      }

      if ((splitResult.bestDim == 0 &&
           element.orderX < splitResult.bestElement.orderX) ||
          (splitResult.bestDim == 1 &&
           element.orderY < splitResult.bestElement.orderY)) {
        // the element belongs to split 0

        if (dim == 0) {
          performCertainSplit(0, 0, currentXSplit0, SSplit0, minSplit0OtherDim,
                              maxSplit0OtherDim, split0InRam, this->workInRam,
                              element, boxSplit0);
        } else {
          performCertainSplit(1, 0, currentYSplit0, SSplit0, minSplit0OtherDim,
                              maxSplit0OtherDim, split0InRam, this->workInRam,
                              element, boxSplit0);
        }
      } else {
        // the element belongs to split 1

        if (dim == 0) {
          performCertainSplit(0, 1, currentXSplit1, SSplit1, minSplit1OtherDim,
                              maxSplit1OtherDim, split1InRam, this->workInRam,
                              element, boxSplit1);
        } else {
          performCertainSplit(1, 1, currentYSplit1, SSplit1, minSplit1OtherDim,
                              maxSplit1OtherDim, split1InRam, this->workInRam,
                              element, boxSplit1);
        }
      }
      i++;

      if (!this->workInRam &&
          fileReaderDim0Iterator != fileReaderDim0.value().end() &&
          fileReaderDim1Iterator != fileReaderDim1.value().end()) {
        if (dim == 0) {
          ++fileReaderDim0Iterator;
          elementOpt = *fileReaderDim0Iterator;
        }
        if (dim == 1) {
          ++fileReaderDim1Iterator;
          elementOpt = *fileReaderDim1Iterator;
        }
      }
    }
  }

  // replace the placeholder
  (*otherDimension.smallSplit0)[0] = minSplit0OtherDim;
  (*otherDimension.smallSplit0)[1] = maxSplit0OtherDim;
  (*otherDimension.smallSplit1)[0] = minSplit1OtherDim;
  (*otherDimension.smallSplit1)[1] = maxSplit1OtherDim;

  (*this->rectsD0.rectanglesInRam).clear();
  (*this->rectsD1.rectanglesInRam).clear();
  (*this->rectsD0.rectanglesSmall).clear();
  (*this->rectsD1.rectanglesSmall).clear();
  (*this->rectsD0.rectanglesInRam).shrink_to_fit();
  (*this->rectsD1.rectanglesInRam).shrink_to_fit();
  (*this->rectsD0.rectanglesSmall).shrink_to_fit();
  (*this->rectsD1.rectanglesSmall).shrink_to_fit();

  return std::make_pair(boxSplit0, boxSplit1);
}
