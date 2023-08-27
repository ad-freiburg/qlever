//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>
#include <util/Rtree.h>
#include <util/RtreeFileReader.h>
#include <util/BackgroundStxxlSorter.h>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>

static void centerOrdering(multiBoxGeo& boxes, size_t dim) {
  if (dim == 0) {
    // order by centerX
    SortRuleLambdaX comp;

    std::sort(boxes.begin(), boxes.end(), comp);
  } else {
    // order by centerY
    auto sortRuleLambda = [](RTreeValue b1, RTreeValue b2) -> bool {
      double center1 = std::midpoint(b1.box.min_corner().get<1>(), b1.box.max_corner().get<1>());
      double center2 = std::midpoint(b2.box.min_corner().get<1>(), b2.box.max_corner().get<1>());
      return center1 < center2;
    };

    std::sort(boxes.begin(), boxes.end(), sortRuleLambda);
  }
}

static void centerOrdering(multiBoxWithOrderIndex& boxes, size_t dim) {
  if (dim == 0) {
    // order by centerX
    SortRuleLambdaXWithIndex comp;

    std::sort(boxes.begin(), boxes.end(), comp);
  } else {
    // order by centerY
    SortRuleLambdaYWithIndex comp;

    std::sort(boxes.begin(), boxes.end(), comp);
  }
}

OrderedBoxes SortInput(const std::string& onDiskBase, size_t M, uintmax_t maxBuildingRamUsage, bool workInRam) {
  OrderedBoxes orderedInputRectangles;

  ad_utility::BackgroundStxxlSorter<RTreeValue, SortRuleLambdaX> sorterRectsD0Basic = ad_utility::BackgroundStxxlSorter<RTreeValue, SortRuleLambdaX>(std::ceil((maxBuildingRamUsage < 9999999999 ? maxBuildingRamUsage : 9999999999) / 3.0));
  multiBoxGeo rectsD0Basic;

  if (workInRam) {
    rectsD0Basic = Rtree::LoadEntries(onDiskBase + ".boundingbox.tmp");
    centerOrdering(rectsD0Basic, 0);
  } else {
    FileReaderWithoutIndex fileReaderRectsD0 = FileReaderWithoutIndex(onDiskBase + ".boundingbox.tmp");
    std::optional<RTreeValue> rectD0Element = fileReaderRectsD0.GetNextElement();
    while (rectD0Element) {
      sorterRectsD0Basic.push(rectD0Element.value());
      rectD0Element = fileReaderRectsD0.GetNextElement();
    }
    fileReaderRectsD0.Close();
  }

  uint64_t xSize = 0;
  double globalMinX = -1;
  double globalMinY = -1;
  double globalMaxX = -1;
  double globalMaxY = -1;

  ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex, SortRuleLambdaYWithIndex> sorterRectsD1 = ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex, SortRuleLambdaYWithIndex>(std::ceil((maxBuildingRamUsage < 9999999999 ? maxBuildingRamUsage : 9999999999) / 3.0));
  std::shared_ptr<multiBoxWithOrderIndex> RectanglesD1WithOrder = std::make_shared<multiBoxWithOrderIndex>();

  if (workInRam) {
    for (RTreeValue element : rectsD0Basic) {
      RTreeValueWithOrderIndex entry = {element.box, element.id, xSize, 0};
      RectanglesD1WithOrder->push_back(entry);
      xSize++;

      if (globalMinX == -1 || element.box.min_corner().get<0>() < globalMinX) {
        globalMinX = element.box.min_corner().get<0>();
      }
      if (globalMinY == -1 || element.box.min_corner().get<1>() < globalMinY) {
        globalMinY = element.box.min_corner().get<1>();
      }
      if (element.box.max_corner().get<0>() > globalMaxX) {
        globalMaxX = element.box.max_corner().get<0>();
      }
      if (element.box.max_corner().get<1>() > globalMaxY) {
        globalMaxY = element.box.max_corner().get<1>();
      }
    }
    centerOrdering(*RectanglesD1WithOrder, 1);
  } else {
    for (RTreeValue element : sorterRectsD0Basic.sortedView()) {
      RTreeValueWithOrderIndex entry = {element.box, element.id, xSize, 0};
      sorterRectsD1.push(entry);
      xSize++;

      if (globalMinX == -1 || element.box.min_corner().get<0>() < globalMinX) {
        globalMinX = element.box.min_corner().get<0>();
      }
      if (globalMinY == -1 || element.box.min_corner().get<1>() < globalMinY) {
        globalMinY = element.box.min_corner().get<1>();
      }
      if (element.box.max_corner().get<0>() > globalMaxX) {
        globalMaxX = element.box.max_corner().get<0>();
      }
      if (element.box.max_corner().get<1>() > globalMaxY) {
        globalMaxY = element.box.max_corner().get<1>();
      }
    }
  }
  sorterRectsD0Basic.clear();

  size_t currentS = std::ceil(((float) xSize) / ((float) M));

  uint64_t ySize = 0;
  std::ofstream r1File = std::ofstream(onDiskBase + ".boundingbox.d1.tmp", std::ios::binary);
  ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex, SortRuleLambdaXWithIndex> sorterRectsD0 = ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex, SortRuleLambdaXWithIndex>(std::ceil((maxBuildingRamUsage < 9999999999 ? maxBuildingRamUsage : 9999999999) / 3.0));
  std::shared_ptr<multiBoxWithOrderIndex> RectanglesD0WithOrder = std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> r1Small = std::make_shared<multiBoxWithOrderIndex>();
  // placeholder
  r1Small->push_back(RTreeValueWithOrderIndex());
  r1Small->push_back(RTreeValueWithOrderIndex());
  RTreeValueWithOrderIndex minD1;
  RTreeValueWithOrderIndex maxD1;

  if (workInRam) {
    for (RTreeValueWithOrderIndex element : *RectanglesD1WithOrder) {
      element.orderY = ySize;
      RectanglesD0WithOrder->push_back(element);

      if (((ySize + 1) % currentS == 0 && (ySize + 1) / currentS >= 1 && (ySize + 1) / currentS < M)
          || (ySize % currentS == 0 && ySize / currentS >= 1 && ySize / currentS < M)) {
        // index i * S - 1 or i * S
        r1Small->push_back(element);
      }

      if (ySize == 0) {
        minD1 = element;
        maxD1 = element;
      }
      if (element.orderY > maxD1.orderY) {
        maxD1 = element;
      }

      ySize++;
    }
    centerOrdering(*RectanglesD0WithOrder, 0);
  } else {
    for (RTreeValueWithOrderIndex element : sorterRectsD1.sortedView()) {
      element.orderY = ySize;
      Rtree::SaveEntryWithOrderIndex(element, r1File);
      sorterRectsD0.push(element);

      if (((ySize + 1) % currentS == 0 && (ySize + 1) / currentS >= 1 && (ySize + 1) / currentS < M)
          || (ySize % currentS == 0 && ySize / currentS >= 1 && ySize / currentS < M)) {
        // index i * S - 1 or i * S
        r1Small->push_back(element);
      }

      if (ySize == 0) {
        minD1 = element;
        maxD1 = element;
      }
      if (element.orderY > maxD1.orderY) {
        maxD1 = element;
      }

      ySize++;
    }
  }

  r1File.close();
  sorterRectsD1.clear();

  // replace the placeholder
  (*r1Small)[0] = minD1;
  (*r1Small)[1] = maxD1;

  uint64_t currentX = 0;
  std::ofstream r0File = std::ofstream(onDiskBase + ".boundingbox.d0.tmp", std::ios::binary);
  std::shared_ptr<multiBoxWithOrderIndex> r0Small = std::make_shared<multiBoxWithOrderIndex>();
  // placeholder
  r0Small->push_back(RTreeValueWithOrderIndex());
  r0Small->push_back(RTreeValueWithOrderIndex());
  RTreeValueWithOrderIndex minD0;
  RTreeValueWithOrderIndex maxD0;

  if (workInRam) {
    for (RTreeValueWithOrderIndex element : *RectanglesD0WithOrder) {
      if (((currentX + 1) % currentS == 0 && (currentX + 1) / currentS >= 1 && (currentX + 1) / currentS < M)
          || (currentX % currentS == 0 && currentX / currentS >= 1 && currentX / currentS < M)) {
        // index i * S - 1 or i * S
        r0Small->push_back(element);
      }

      if (currentX == 0) {
        minD0 = element;
        maxD0 = element;
      }
      if (element.orderX > maxD0.orderX) {
        maxD0 = element;
      }

      currentX++;
    }
  } else {
    for (RTreeValueWithOrderIndex element : sorterRectsD0.sortedView()) {
      Rtree::SaveEntryWithOrderIndex(element, r0File);

      if (((currentX + 1) % currentS == 0 && (currentX + 1) / currentS >= 1 && (currentX + 1) / currentS < M)
          || (currentX % currentS == 0 && currentX / currentS >= 1 && currentX / currentS < M)) {
        // index i * S - 1 or i * S
        r0Small->push_back(element);
      }

      if (currentX == 0) {
        minD0 = element;
        maxD0 = element;
      }
      if (element.orderX > maxD0.orderX) {
        maxD0 = element;
      }

      currentX++;
    }
  }

  r0File.close();
  sorterRectsD0.clear();

  // replace the placeholder
  (*r0Small)[0] = minD0;
  (*r0Small)[1] = maxD0;

  Rtree::BoundingBox boundingBox = Rtree::createBoundingBox(globalMinX, globalMinY, globalMaxX, globalMaxY);
  RectanglesForOrderedBoxes rectsD0;
  RectanglesForOrderedBoxes rectsD1;
  rectsD0.rectanglesSmall = r0Small;
  rectsD1.rectanglesSmall = r1Small;
  if (workInRam) {
    rectsD0.rectanglesInRam = RectanglesD0WithOrder;
    rectsD1.rectanglesInRam = RectanglesD1WithOrder;
    orderedInputRectangles.CreateOrderedBoxesInRam(rectsD0, rectsD1, boundingBox);
  } else {
    rectsD0.rectanglesOnDisk = onDiskBase + ".boundingbox.d0";
    rectsD1.rectanglesOnDisk = onDiskBase + ".boundingbox.d1";
    orderedInputRectangles.CreateOrderedBoxesOnDisk(rectsD0, rectsD1, xSize, boundingBox);
  }
  return orderedInputRectangles;
}

/*OrderedBoxes SortInput(const std::string& onDiskBase, size_t M, uintmax_t maxBuildingRamUsage, bool workInRam) {
  if (workInRam) {
    return InternalSort(onDiskBase, M);
  } else {
    return ExternalSort(onDiskBase, M, maxBuildingRamUsage);
  }
}*/

static double costFunctionTGS(Rtree::BoundingBox& b0, Rtree::BoundingBox& b1, size_t dim) {
  /**
     * The cost function determines the quality of a split. The lower the cost, the better the split.
     * Each split gets represented by the resulting bounding boxes of the split pieces.
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

static std::vector<OrderedBoxes> TGSRecursive(const std::string& filePath, OrderedBoxes orderedInputRectangles, size_t M, size_t S, uint64_t maxBuildingRamUsage) {
  /**
     * This function recursively constructs one layer of children for a certain root node.
     * The input rectangles must be sorted in both x- and y-direction.
     * The algorithm is based on this paper https://dl.acm.org/doi/pdf/10.1145/288692.288723
   */

  uint64_t n = orderedInputRectangles.GetSize();

  if (n <= S || n <= M) {
    // stop condition
    return std::vector<OrderedBoxes> { orderedInputRectangles };
  }
  // split the rectangles at the best split
  std::pair<OrderedBoxes, OrderedBoxes> split = orderedInputRectangles.SplitAtBest(filePath, S, M, maxBuildingRamUsage);

  // recursion
  std::vector<OrderedBoxes> result0 = TGSRecursive(filePath + ".0", split.first, M, S, maxBuildingRamUsage);
  std::vector<OrderedBoxes> result1 = TGSRecursive(filePath + ".1", split.second, M, S, maxBuildingRamUsage);

  std::vector<OrderedBoxes> result;
  result.insert(result.begin(), result0.begin(), result0.end());
  result.insert(result.end(), result1.begin(), result1.end());

  return result;
}

void Rtree::BuildTree(const std::string& onDiskBase, size_t M, const std::string& folder) const {
  const std::string file = onDiskBase + ".boundingbox.tmp";

  // prepare the files
  std::filesystem::create_directory(folder);
  std::ofstream nodesOfs = std::ofstream(folder + "/nodes.bin", std::ios::binary);
  std::map<uint64_t, uint64_t> lookup;

  // sort the rectangles
  uint64_t fileLines = std::ceil(std::filesystem::file_size(file) / (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t)));
  bool workInRam = (std::filesystem::file_size(file) + fileLines * 2 * sizeof(uint64_t)) * 4 < this->maxBuildingRamUsage;

  OrderedBoxes orderedInputRectangles = SortInput(onDiskBase, M, maxBuildingRamUsage, workInRam);

  // build the tree in a depth first approach
  std::stack<ConstructionNode> layerStack;

  uint64_t newId = 1; // start from 1, because 0 is the root item
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
      std::vector<OrderedBoxes> tgsResult = TGSRecursive(onDiskBase + ".boundingbox." + std::to_string(layer), currentItem.GetOrderedBoxes(), M, std::ceil(((float) currentItem.GetOrderedBoxes().GetSize()) / ((float) M)), this->maxBuildingRamUsage);
      for (OrderedBoxes& currentOrderedRectangles : tgsResult) {
        ConstructionNode newItem = ConstructionNode(newId, currentOrderedRectangles);
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
    lookupOfs.write(reinterpret_cast<const char *>(&nodePtr), sizeof(uint64_t));
  }
  lookupOfs.close();
}

ConstructionNode::ConstructionNode(uint64_t id, OrderedBoxes orderedBoxes)
    : Node{id}
{
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
    for(RTreeValueWithOrderIndex box : *this->GetOrderedBoxes().GetRectanglesInRam()) {
      Node leafNode = Node(box.id, box.box);
      this->AddChild(leafNode);
    }
  } else {
    FileReader fileReader = FileReader(this->GetOrderedBoxes().GetRectanglesOnDisk());

    std::optional<RTreeValueWithOrderIndex> element = fileReader.GetNextElement();
    while(element) {
      Node leafNode = Node(element.value().id, element.value().box);
      this->AddChild(leafNode);
      element = fileReader.GetNextElement();
    }

    fileReader.Close();
  }
}

OrderedBoxes ConstructionNode::GetOrderedBoxes() {
  return this->orderedBoxes;
}

void Node::AddChild(Node& child) {
  Rtree::BoundingBox box = child.GetBoundingBox();
  uint64_t entryId = child.GetId();
  RTreeValue entry = {box, entryId};
  this->children.push_back(entry);
}

Rtree::BoundingBox Node::GetBoundingBox() const {
  return this->boundingBox;
}

void Node::SetIsLastInnerNode(bool isLast) {
  this->isLastInnerNode = isLast;
}

uint64_t Rtree::SaveNode(Node &node, bool isLastInnerNode, std::ofstream& nodesOfs) {
  node.SetIsLastInnerNode(isLastInnerNode);

  uint64_t pos = static_cast<uint64_t>(nodesOfs.tellp());
  boost::archive::binary_oarchive archive(nodesOfs);
  archive << node;
  nodesOfs.write(" ", 1);

  return pos;
}

std::optional<Rtree::BoundingBox> GetBoundingBoxFromWKT(const std::string& wkt) {
  /**
     * Parse the wkt literal in a way, that only the relevant data for the rtree gets read in.
   */
  bool lookingForX = true;
  bool readingDouble = false;
  std::string currentDouble;

  double minX = -1;
  double maxX = -1;
  double minY = -1;
  double maxY = -1;

  for (char c : wkt) {
    if (isdigit(c)) {
      readingDouble = true;
      currentDouble += c;
    } else if (c == '.') {
      readingDouble = true;
      currentDouble += '.';
    } else if (c == ' ') {
      if (readingDouble && lookingForX) {
        // x is completely read in
        readingDouble = false;
        lookingForX = false;
        double x;
        try {
          x = std::stod(currentDouble);
        } catch(...) {
          return { };
        }
        currentDouble = "";
        if (x < minX || minX == -1) {
          minX = x;
        }

        if (x > maxX) {
          maxX = x;
        }
      }
    } else {
      if (readingDouble && !lookingForX) {
        // y is completely read in
        readingDouble = false;
        lookingForX = true;
        double y;
        try {
          y = std::stod(currentDouble);
        } catch(...) {
          return { };
        }
        currentDouble = "";
        if (y < minY || minY == -1) {
          minY = y;
        }

        if (y > maxY) {
          maxY = y;
        }
      }
    }
  }

  return { Rtree::createBoundingBox(minX, minY, maxX, maxY) };
}

std::optional<Rtree::BoundingBox> Rtree::ConvertWordToRtreeEntry(const std::string& wkt) {
  /**
     * Convert a single wkt literal to a boundingbox.
   */
  std::optional<Rtree::BoundingBox> boundingBox;

  /* Get the bounding box(es) of either a multipolygon, polygon or a linestring */
  std::size_t posWKTStart = wkt.find("MULTIPOLYGON(((") + 14;
  std::size_t posWKTEnd = wkt.find(")))", posWKTStart);
  if (posWKTStart != std::string::npos && posWKTEnd != std::string::npos) {
    std::string newWkt = wkt.substr(posWKTStart, posWKTEnd - posWKTStart + 1);
    boundingBox = GetBoundingBoxFromWKT(newWkt);
  } else {
    posWKTStart = wkt.find("POLYGON((") + 8;
    posWKTEnd = wkt.find("))", posWKTStart);
    if (posWKTStart != std::string::npos && posWKTEnd != std::string::npos) {
      std::string newWkt = wkt.substr(posWKTStart, posWKTEnd - posWKTStart + 1);
      boundingBox = GetBoundingBoxFromWKT(newWkt);
    } else {
      posWKTStart = wkt.find("LINESTRING(") + 10;
      posWKTEnd = wkt.find(')', posWKTStart);
      if (posWKTStart != std::string::npos && posWKTEnd != std::string::npos) {
        std::string newWkt = wkt.substr(posWKTStart, posWKTEnd - posWKTStart + 1);
        boundingBox = GetBoundingBoxFromWKT(newWkt);
      } else {
        return { };
      }
    }
  }

  return boundingBox;
}

void Rtree::SaveEntry(Rtree::BoundingBox boundingBox, uint64_t index, std::ofstream& convertOfs) {
  /**
     * Save a single entry (which was e.g. converted by ConvertWordToRtreeEntry) to the disk
   */
  double minX = boundingBox.min_corner().get<0>();
  double minY = boundingBox.min_corner().get<1>();
  double maxX = boundingBox.max_corner().get<0>();
  double maxY = boundingBox.max_corner().get<1>();

  convertOfs.write(reinterpret_cast<const char *>(&minX), sizeof(double));
  convertOfs.write(reinterpret_cast<const char *>(&minY), sizeof(double));
  convertOfs.write(reinterpret_cast<const char *>(&maxX), sizeof(double));
  convertOfs.write(reinterpret_cast<const char *>(&maxY), sizeof(double));
  convertOfs.write(reinterpret_cast<const char *>(&index), sizeof(uint64_t));
}

void Rtree::SaveEntryWithOrderIndex(RTreeValueWithOrderIndex treeValue, std::ofstream& convertOfs) {
  /**
     * Save a single entry, containing its postion in the x- and y-sorting
   */
  double minX = treeValue.box.min_corner().get<0>();
  double minY = treeValue.box.min_corner().get<1>();
  double maxX = treeValue.box.max_corner().get<0>();
  double maxY = treeValue.box.max_corner().get<1>();

  convertOfs.write(reinterpret_cast<const char *>(&minX), sizeof(double));
  convertOfs.write(reinterpret_cast<const char *>(&minY), sizeof(double));
  convertOfs.write(reinterpret_cast<const char *>(&maxX), sizeof(double));
  convertOfs.write(reinterpret_cast<const char *>(&maxY), sizeof(double));
  convertOfs.write(reinterpret_cast<const char *>(&treeValue.id), sizeof(uint64_t));
  convertOfs.write(reinterpret_cast<const char *>(&treeValue.orderX), sizeof(uint64_t));
  convertOfs.write(reinterpret_cast<const char *>(&treeValue.orderY), sizeof(uint64_t));
}

multiBoxGeo Rtree::LoadEntries(const std::string& file) {
  multiBoxGeo boxes;

  FileReaderWithoutIndex fileReader = FileReaderWithoutIndex(file);

  std::optional<RTreeValue> element = fileReader.GetNextElement();
  while (element) {
    boxes.push_back(element.value());
    element = fileReader.GetNextElement();
  }

  fileReader.Close();

  return boxes;
}

multiBoxWithOrderIndex Rtree::LoadEntriesWithOrderIndex(const std::string& file) {
  multiBoxWithOrderIndex boxes;
  FileReader fileReader = FileReader(file);

  std::optional<RTreeValueWithOrderIndex> element = fileReader.GetNextElement();
  while (element) {
    boxes.push_back(element.value());
    element = fileReader.GetNextElement();
  }

  fileReader.Close();

  return boxes;
}

bool OrderedBoxes::WorkInRam() const{
  return this->workInRam;
}

void OrderedBoxes::CreateOrderedBoxesInRam(RectanglesForOrderedBoxes& rectanglesD0, RectanglesForOrderedBoxes& rectanglesD1, Rtree::BoundingBox box) {
  this->workInRam = true;
  this->rectsD0 = rectanglesD0;
  this->rectsD1 = rectanglesD1;
  this->size = (*rectsD0.rectanglesInRam).size();
  this->boundingBox = box;
}

void OrderedBoxes::CreateOrderedBoxesOnDisk(RectanglesForOrderedBoxes& rectanglesD0, RectanglesForOrderedBoxes& rectanglesD1, uint64_t size, Rtree::BoundingBox box) {
  this->workInRam = false;
  this->rectsD0 = rectanglesD0;
  this->rectsD1 = rectanglesD1;
  this->size = size;
  this->boundingBox = box;
}

Rtree::BoundingBox OrderedBoxes::GetBoundingBox() {
  return this->boundingBox;
}

uint64_t OrderedBoxes::GetSize() const {
  return this->size;
}

std::shared_ptr<multiBoxWithOrderIndex> OrderedBoxes::GetRectanglesInRam() {
  return this->rectsD0.rectanglesInRam;
}

std::string OrderedBoxes::GetRectanglesOnDisk() {
  return this->rectsD0.rectanglesOnDisk;
}

SplitResult OrderedBoxes::GetBestSplit() {
  /**
     * Determine based on the "small-lists", which split is the best for the rtree.
   */
  struct SplitResult splitResult;

  RTreeValueWithOrderIndex minElement;
  RTreeValueWithOrderIndex maxElement;
  RTreeValueWithOrderIndex currentLastElement;
  RTreeValueWithOrderIndex currentElement;

  // This bool is used, since we need every other element as our element "S * i" (described in the algorithm)
  // To perform the split better, the element before it (S * i - 1) is saved as well
  bool currentlyAtSTimesI = false;

  for (size_t dim = 0; dim < 2; dim++) {
    for (uint64_t i = 0; i < this->rectsD0.rectanglesSmall->size(); i++) {
      currentElement = dim == 0 ? (*this->rectsD0.rectanglesSmall)[i] : (*this->rectsD1.rectanglesSmall)[i];

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

      double minXB0 = 0;
      double maxXB0 = 1;
      double minXB1 = 0;
      double maxXB1 = 1;
      double minYB0 = 0;
      double maxYB0 = 1;
      double minYB1 = 0;
      double maxYB1 = 1;

      if (currentlyAtSTimesI && currentElement.id != maxElement.id) {
        // the current element is a possible split position.
        if (dim == 0) {
          minXB0 = std::midpoint(minElement.box.min_corner().get<0>(), minElement.box.max_corner().get<0>());
          maxXB0 = std::midpoint(currentLastElement.box.min_corner().get<0>(), currentLastElement.box.max_corner().get<0>());

          minXB1 = std::midpoint(currentElement.box.min_corner().get<0>(), currentElement.box.max_corner().get<0>());
          maxXB1 = std::midpoint(maxElement.box.min_corner().get<0>(), maxElement.box.max_corner().get<0>());
        } else {
          minYB0 = std::midpoint(minElement.box.min_corner().get<1>(), minElement.box.max_corner().get<1>());
          maxYB0 = std::midpoint(currentLastElement.box.min_corner().get<1>(), currentLastElement.box.max_corner().get<1>());

          minYB1 = std::midpoint(currentElement.box.min_corner().get<1>(), currentElement.box.max_corner().get<1>());
          maxYB1 = std::midpoint(maxElement.box.min_corner().get<1>(), maxElement.box.max_corner().get<1>());
        }

        currentlyAtSTimesI = false;
      } else {
        break;
      }

      Rtree::BoundingBox b0 = Rtree::createBoundingBox(minXB0, minYB0, maxXB0, maxYB0);
      Rtree::BoundingBox b1 = Rtree::createBoundingBox(minXB1, minYB1, maxXB1, maxYB1);


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

std::pair<OrderedBoxes, OrderedBoxes> OrderedBoxes::SplitAtBest(const std::string& filePath, size_t S, size_t M, uint64_t maxBuildingRamUsage) {
  if (this->workInRam) {
    return this->SplitAtBestInRam(S, M);
  } else {
    return this->SplitAtBestOnDisk(filePath, S, M, maxBuildingRamUsage);
  }
}

std::pair<OrderedBoxes, OrderedBoxes> OrderedBoxes::SplitAtBestInRam(size_t S, size_t M) {
  /**
     * Split the ordered boxes in ram. First determine the best split and then perform it
   */

  struct SplitResult splitResult = this->GetBestSplit();

  OrderedBoxes split0;
  OrderedBoxes split1;

  struct SplitBuffersRam splitBuffers;

  splitBuffers.s0Dim0 = std::make_shared<multiBoxWithOrderIndex>();
  splitBuffers.s0Dim1 = std::make_shared<multiBoxWithOrderIndex>();
  splitBuffers.s1Dim0 = std::make_shared<multiBoxWithOrderIndex>();
  splitBuffers.s1Dim1 = std::make_shared<multiBoxWithOrderIndex>();

  splitBuffers.s0SmallDim0 = std::make_shared<multiBoxWithOrderIndex>();
  splitBuffers.s0SmallDim1 = std::make_shared<multiBoxWithOrderIndex>();
  splitBuffers.s1SmallDim0 = std::make_shared<multiBoxWithOrderIndex>();
  splitBuffers.s1SmallDim1 = std::make_shared<multiBoxWithOrderIndex>();

  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> boundingBoxes = PerformSplit(splitResult, splitBuffers, M, S);

  RectanglesForOrderedBoxes rectsD0Split0 = {splitBuffers.s0Dim0, "", splitBuffers.s0SmallDim0};
  RectanglesForOrderedBoxes rectsD1Split0 = {splitBuffers.s0Dim1, "", splitBuffers.s0SmallDim1};
  RectanglesForOrderedBoxes rectsD0Split1 = {splitBuffers.s1Dim0, "", splitBuffers.s1SmallDim0};
  RectanglesForOrderedBoxes rectsD1Split1 = {splitBuffers.s1Dim1, "", splitBuffers.s1SmallDim1};
  split0.CreateOrderedBoxesInRam(rectsD0Split0, rectsD1Split0, boundingBoxes.first);
  split1.CreateOrderedBoxesInRam(rectsD0Split1, rectsD1Split1, boundingBoxes.second);

  (*this->rectsD0.rectanglesInRam).clear();
  (*this->rectsD1.rectanglesInRam).clear();
  (*this->rectsD0.rectanglesSmall).clear();
  (*this->rectsD1.rectanglesSmall).clear();
  (*this->rectsD0.rectanglesInRam).shrink_to_fit();
  (*this->rectsD1.rectanglesInRam).shrink_to_fit();
  (*this->rectsD0.rectanglesSmall).shrink_to_fit();
  (*this->rectsD1.rectanglesSmall).shrink_to_fit();

  return std::make_pair(split0, split1);
}

std::pair<OrderedBoxes, OrderedBoxes> OrderedBoxes::SplitAtBestOnDisk(const std::string& filePath, size_t S, size_t M, uint64_t maxBuildingRamUsage) {
  /**
     * Split the ordered boxes on disk. First determine the best split and then perform it
   */

  OrderedBoxes split0;
  OrderedBoxes split1;

  struct SplitResult splitResult = this->GetBestSplit();

  struct SplitBuffersDisk splitBuffers;
  struct SplitBuffersRam splitBuffersRam;

  // perfrom the split
  uint64_t sizeLeft = std::ceil((splitResult.bestIndex - 2) / 2.0) * S;
  uint64_t sizeRight = this->size - sizeLeft;
  uint64_t split0ByteSize = sizeLeft * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t));
  uint64_t split1ByteSize = sizeRight * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t));
  bool split0InRam = split0ByteSize * 4 < maxBuildingRamUsage;
  bool split1InRam = split1ByteSize * 4 < maxBuildingRamUsage;

  splitBuffersRam.s0SmallDim0 = std::make_shared<multiBoxWithOrderIndex>();
  splitBuffersRam.s0SmallDim1 = std::make_shared<multiBoxWithOrderIndex>();
  splitBuffersRam.s1SmallDim0 = std::make_shared<multiBoxWithOrderIndex>();
  splitBuffersRam.s1SmallDim1 = std::make_shared<multiBoxWithOrderIndex>();

  if (!split0InRam) {
    splitBuffers.split0Dim0File = { std::ofstream(filePath + ".0.dim0.tmp", std::ios::binary) };
    splitBuffers.split0Dim1File = { std::ofstream(filePath + ".0.dim1.tmp", std::ios::binary) };
  } else {
    splitBuffersRam.s0Dim0 = std::make_shared<multiBoxWithOrderIndex>();
    splitBuffersRam.s0Dim1 = std::make_shared<multiBoxWithOrderIndex>();
  }

  if (!split1InRam) {
    splitBuffers.split1Dim0File = { std::ofstream(filePath + ".1.dim0.tmp", std::ios::binary) };
    splitBuffers.split1Dim1File = { std::ofstream(filePath + ".1.dim1.tmp", std::ios::binary) };
  } else {
    splitBuffersRam.s1Dim0 = std::make_shared<multiBoxWithOrderIndex>();
    splitBuffersRam.s1Dim1 = std::make_shared<multiBoxWithOrderIndex>();
  }

  splitBuffers.splitBuffersRam = splitBuffersRam;

  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> boundingBoxes = PerformSplit(splitResult, splitBuffers, M, S, maxBuildingRamUsage);

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

    split0.CreateOrderedBoxesOnDisk(rectsD0Split0, rectsD1Split0, sizeLeft, boundingBoxes.first);
  } else {
    rectsD0Split0.rectanglesInRam = splitBuffers.splitBuffersRam.s0Dim0;
    rectsD1Split0.rectanglesInRam = splitBuffers.splitBuffersRam.s0Dim1;
    split0.CreateOrderedBoxesInRam(rectsD0Split0, rectsD1Split0, boundingBoxes.first);
  }

  if (!split1InRam) {
    splitBuffers.split1Dim0File.value().close();
    splitBuffers.split1Dim1File.value().close();

    rectsD0Split1.rectanglesOnDisk = filePath + ".1.dim0";
    rectsD1Split1.rectanglesOnDisk = filePath + ".1.dim1";

    split1.CreateOrderedBoxesOnDisk(rectsD0Split1, rectsD1Split1, sizeRight, boundingBoxes.second);
  } else {
    rectsD0Split1.rectanglesInRam = splitBuffers.splitBuffersRam.s1Dim0;
    rectsD1Split1.rectanglesInRam = splitBuffers.splitBuffersRam.s1Dim1;
    split1.CreateOrderedBoxesInRam(rectsD0Split1, rectsD1Split1, boundingBoxes.second);
  }

  std::remove(this->rectsD0.rectanglesOnDisk.c_str());
  std::remove(this->rectsD1.rectanglesOnDisk.c_str());

  return std::make_pair(split0, split1);
}

std::pair<Rtree::BoundingBox, Rtree::BoundingBox> OrderedBoxes::PerformSplit(SplitResult splitResult, SplitBuffersRam& splitBuffersRam, size_t M, size_t S) {
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
  std::pair<Rtree::BoundingBox, Rtree::BoundingBox> boundingBoxes = PerformSplit(splitResult, splitBuffersDisk, M, S, 0);

  splitBuffersRam = splitBuffersDisk.splitBuffersRam;

  return boundingBoxes;
}

std::pair<Rtree::BoundingBox, Rtree::BoundingBox> OrderedBoxes::PerformSplit(SplitResult splitResult, SplitBuffersDisk& splitBuffers, size_t M, size_t S, uint64_t maxBuildingRamUsage) {
  /**
     * Perform the best split on the current ordered boxes in the disk case
   */

  uint64_t sizeLeft = std::ceil((splitResult.bestIndex - 2) / 2.0) * S;
  uint64_t sizeRight = this->size - sizeLeft;
  size_t SSplit0 = sizeLeft <= S ? std::ceil(sizeLeft / (double) M) : S;
  size_t SSplit1 = sizeRight <= S ? std::ceil(sizeRight / (double) M) : S;
  uint64_t split0ByteSize = sizeLeft * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t));
  uint64_t split1ByteSize = sizeRight * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t));
  bool split0InRam = maxBuildingRamUsage == 0 || split0ByteSize * 4 < maxBuildingRamUsage;
  bool split1InRam = maxBuildingRamUsage == 0 || split1ByteSize * 4 < maxBuildingRamUsage;

  double globalMinXS0 = -1;
  double globalMinYS0 = -1;
  double globalMaxXS0 = -1;
  double globalMaxYS0 = -1;

  double globalMinXS1 = -1;
  double globalMinYS1 = -1;
  double globalMaxXS1 = -1;
  double globalMaxYS1 = -1;

  RTreeValueWithOrderIndex minSplit0OtherDim;
  RTreeValueWithOrderIndex maxSplit0OtherDim;
  RTreeValueWithOrderIndex minSplit1OtherDim;
  RTreeValueWithOrderIndex maxSplit1OtherDim;

  struct OtherDimension {
    std::shared_ptr<multiBoxWithOrderIndex> smallSplit0;
    std::shared_ptr<multiBoxWithOrderIndex> smallSplit1;
  } otherDimension;

  if (splitResult.bestDim == 0) {
    splitBuffers.splitBuffersRam.s0SmallDim0->push_back(splitResult.bestMinElement);
    splitBuffers.splitBuffersRam.s0SmallDim0->push_back(splitResult.bestLastElement);
    splitBuffers.splitBuffersRam.s1SmallDim0->push_back(splitResult.bestElement);
    splitBuffers.splitBuffersRam.s1SmallDim0->push_back(splitResult.bestMaxElement);

    // placeholder, since we need the min and max element of the split in the first two spots
    otherDimension.smallSplit0 = splitBuffers.splitBuffersRam.s0SmallDim1;
    otherDimension.smallSplit1 = splitBuffers.splitBuffersRam.s1SmallDim1;

    otherDimension.smallSplit0->push_back(RTreeValueWithOrderIndex());
    otherDimension.smallSplit0->push_back(RTreeValueWithOrderIndex());
    otherDimension.smallSplit1->push_back(RTreeValueWithOrderIndex());
    otherDimension.smallSplit1->push_back(RTreeValueWithOrderIndex());
  } else {
    splitBuffers.splitBuffersRam.s0SmallDim1->push_back(splitResult.bestMinElement);
    splitBuffers.splitBuffersRam.s0SmallDim1->push_back(splitResult.bestLastElement);
    splitBuffers.splitBuffersRam.s1SmallDim1->push_back(splitResult.bestElement);
    splitBuffers.splitBuffersRam.s1SmallDim1->push_back(splitResult.bestMaxElement);

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
    fileReaderDim0 = { FileReader(this->rectsD0.rectanglesOnDisk) };
    fileReaderDim1 = { FileReader(this->rectsD1.rectanglesOnDisk) };
  }
  uint64_t currentXSplit0 = 0;
  uint64_t currentXSplit1 = 0;
  uint64_t currentYSplit0 = 0;
  uint64_t currentYSplit1 = 0;
  for (size_t dim = 0; dim < 2; dim++) {
    // start performing the actual split
    uint64_t i = 0;

    if (!this->workInRam) {
      if (dim == 0)
        elementOpt = fileReaderDim0.value().GetNextElement();
      if (dim == 1)
        elementOpt = fileReaderDim1.value().GetNextElement();
    }

    while ((this->workInRam && i < this->size) || (!this->workInRam && elementOpt)) {
      RTreeValueWithOrderIndex element;

      // get the current element, either from disk or from ram
      if (this->workInRam) {
        element = dim == 0 ? (*this->rectsD0.rectanglesInRam)[i] : (*this->rectsD1.rectanglesInRam)[i];
      } else {
        element = elementOpt.value();
      }

      if ((splitResult.bestDim == 0 && element.orderX < splitResult.bestElement.orderX)
          || (splitResult.bestDim == 1 && element.orderY < splitResult.bestElement.orderY)) {
        // the element belongs to split 0

        if (dim == 0) {
          // add the element to the split 0 dimension 0 vector / file
          if (split0InRam || this->workInRam) {
            splitBuffers.splitBuffersRam.s0Dim0->push_back(element);
          } else {
            Rtree::SaveEntryWithOrderIndex(element, splitBuffers.split0Dim0File.value());
          }

          // check if the element is at the position i * S (described in the algorithm) or one before it.
          // In this case it is a future possible split position and needs to be saved to the "small list"
          if (((currentXSplit0 + 1) % SSplit0 == 0 && (currentXSplit0 + 1) / SSplit0 >= 1 && (currentXSplit0 + 1) / SSplit0 < M)
              || (currentXSplit0 % SSplit0 == 0 && currentXSplit0 / SSplit0 >= 1 && currentXSplit0 / SSplit0 < M)) {
            // index i * S - 1 or i * S
            splitBuffers.splitBuffersRam.s0SmallDim0->push_back(element);
          }

          // keep track of the min and max values to construct the bounding box of the split later
          if (globalMinXS0 == -1 || element.box.min_corner().get<0>() < globalMinXS0) {
            globalMinXS0 = element.box.min_corner().get<0>();
          }
          if (globalMinYS0 == -1 || element.box.min_corner().get<1>() < globalMinYS0) {
            globalMinYS0 = element.box.min_corner().get<1>();
          }
          if (element.box.max_corner().get<0>() > globalMaxXS0) {
            globalMaxXS0 = element.box.max_corner().get<0>();
          }
          if (element.box.max_corner().get<1>() > globalMaxYS0) {
            globalMaxYS0 = element.box.max_corner().get<1>();
          }

          // keep track of the min and max element of the split, to later replace the placeholder in the "small lists"
          if (splitResult.bestDim == 1) {
            if (currentXSplit0 == 0) {
              minSplit0OtherDim = element;
              maxSplit0OtherDim = element;
            }
            if (element.orderX > maxSplit0OtherDim.orderX) {
              maxSplit0OtherDim = element;
            }
          }

          currentXSplit0++;
        } else {
          if (split0InRam || this->workInRam) {
            splitBuffers.splitBuffersRam.s0Dim1->push_back(element);
          } else {
            Rtree::SaveEntryWithOrderIndex(element, splitBuffers.split0Dim1File.value());
          }

          if (((currentYSplit0 + 1) % SSplit0 == 0 && (currentYSplit0 + 1) / SSplit0 >= 1 && (currentYSplit0 + 1) / SSplit0 < M)
              || (currentYSplit0 % SSplit0 == 0 && currentYSplit0 / SSplit0 >= 1 && currentYSplit0 / SSplit0 < M)) {
            // index i * S - 1 or i * S
            splitBuffers.splitBuffersRam.s0SmallDim1->push_back(element);
          }

          if (splitResult.bestDim == 0) {
            if (currentYSplit0 == 0) {
              minSplit0OtherDim = element;
              maxSplit0OtherDim = element;
            }
            if (element.orderX > maxSplit0OtherDim.orderX) {
              maxSplit0OtherDim = element;
            }
          }

          currentYSplit0++;
        }
      } else {
        // the element belongs to split 1

        if (dim == 0) {
          if (split1InRam || this->workInRam) {
            splitBuffers.splitBuffersRam.s1Dim0->push_back(element);
          } else {
            Rtree::SaveEntryWithOrderIndex(element, splitBuffers.split1Dim0File.value());
          }
          if (((currentXSplit1 + 1) % SSplit1 == 0 && (currentXSplit1 + 1) / SSplit1 >= 1 && (currentXSplit1 + 1) / SSplit1 < M)
              || (currentXSplit1 % SSplit1 == 0 && currentXSplit1 / SSplit1 >= 1 && currentXSplit1 / SSplit1 < M)) {
            // index i * S - 1 or i * S
            splitBuffers.splitBuffersRam.s1SmallDim0->push_back(element);
          }

          if (globalMinXS1 == -1 || element.box.min_corner().get<0>() < globalMinXS1) {
            globalMinXS1 = element.box.min_corner().get<0>();
          }
          if (globalMinYS1 == -1 || element.box.min_corner().get<1>() < globalMinYS1) {
            globalMinYS1 = element.box.min_corner().get<1>();
          }
          if (element.box.max_corner().get<0>() > globalMaxXS1) {
            globalMaxXS1 = element.box.max_corner().get<0>();
          }
          if (element.box.max_corner().get<1>() > globalMaxYS1) {
            globalMaxYS1 = element.box.max_corner().get<1>();
          }

          if (splitResult.bestDim == 1) {
            if (currentXSplit1 == 0) {
              minSplit1OtherDim = element;
              maxSplit1OtherDim = element;
            }
            if (element.orderX > maxSplit1OtherDim.orderX) {
              maxSplit1OtherDim = element;
            }
          }

          currentXSplit1++;
        } else {
          if (split1InRam || this->workInRam) {
            splitBuffers.splitBuffersRam.s1Dim1->push_back(element);
          } else {
            Rtree::SaveEntryWithOrderIndex(element, splitBuffers.split1Dim1File.value());
          }
          if (((currentYSplit1 + 1) % SSplit1 == 0 && (currentYSplit1 + 1) / SSplit1 >= 1 && (currentYSplit1 + 1) / SSplit1 < M)
              || (currentYSplit1 % SSplit1 == 0 && currentYSplit1 / SSplit1 >= 1 && currentYSplit1 / SSplit1 < M)) {
            // index i * S - 1 or i * S
            splitBuffers.splitBuffersRam.s1SmallDim1->push_back(element);
          }

          if (splitResult.bestDim == 0) {
            if (currentYSplit1 == 0) {
              minSplit1OtherDim = element;
              maxSplit1OtherDim = element;
            }
            if (element.orderX > maxSplit1OtherDim.orderX) {
              maxSplit1OtherDim = element;
            }
          }

          currentYSplit1++;
        }
      }
      i++;

      if (!this->workInRam) {
        if (dim == 0)
          elementOpt = fileReaderDim0.value().GetNextElement();
        if (dim == 1)
          elementOpt = fileReaderDim1.value().GetNextElement();
      }
    }
  }

  if (!this->workInRam) {
    fileReaderDim0.value().Close();
    fileReaderDim1.value().Close();
  }

  // replace the placeholder
  (*otherDimension.smallSplit0)[0] = minSplit0OtherDim;
  (*otherDimension.smallSplit0)[1] = maxSplit0OtherDim;
  (*otherDimension.smallSplit1)[0] = minSplit1OtherDim;
  (*otherDimension.smallSplit1)[1] = maxSplit1OtherDim;

  Rtree::BoundingBox boxSplit0 = Rtree::createBoundingBox(globalMinXS0, globalMinYS0, globalMaxXS0, globalMaxYS0);
  Rtree::BoundingBox boxSplit1 = Rtree::createBoundingBox(globalMinXS1, globalMinYS1, globalMaxXS1, globalMaxYS1);

  return std::make_pair(boxSplit0, boxSplit1);
}

