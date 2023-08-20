//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include <util/Rtree.h>
#include <util/BackgroundStxxlSorter.h>

static void centerOrdering(multiBoxGeo& boxes, size_t dim) {
  if (dim == 0) {
    // order by centerX
    sortRuleLambdaX comp;

    std::sort(boxes.begin(), boxes.end(), comp);
  } else {
    // order by centerY
    auto sortRuleLambda = [](rTreeValue b1, rTreeValue b2) -> bool {
      double center1 = (b1.box.min_corner().get<1>() + b1.box.max_corner().get<1>()) / 2;
      double center2 = (b2.box.min_corner().get<1>() + b2.box.max_corner().get<1>()) / 2;
      return center1 < center2;
    };

    std::sort(boxes.begin(), boxes.end(), sortRuleLambda);
  }
}

static void centerOrdering(multiBoxWithOrderIndex& boxes, size_t dim) {
  if (dim == 0) {
    // order by centerX
    sortRuleLambdaXWithIndex comp;

    std::sort(boxes.begin(), boxes.end(), comp);
  } else {
    // order by centerY
    sortRuleLambdaYWithIndex comp;

    std::sort(boxes.begin(), boxes.end(), comp);
  }
}

OrderedBoxes SortInput(const std::string& onDiskBase, size_t M, uintmax_t maxBuildingRamUsage, bool workInRam) {
  OrderedBoxes orderedInputRectangles;

  ad_utility::BackgroundStxxlSorter<rTreeValue, sortRuleLambdaX> sorterRectsD0Basic = ad_utility::BackgroundStxxlSorter<rTreeValue, sortRuleLambdaX>(std::ceil(maxBuildingRamUsage / 3.0));
  multiBoxGeo rectsD0Basic;

  if (workInRam) {
    rectsD0Basic = Rtree::LoadEntries(onDiskBase + ".boundingbox.tmp");
    centerOrdering(rectsD0Basic, 0);
  } else {
    FileReaderWithoutIndex fileReaderRectsD0 = FileReaderWithoutIndex(onDiskBase + ".boundingbox.tmp");
    std::optional<rTreeValue> rectD0Element = fileReaderRectsD0.GetNextElement();
    while (rectD0Element) {
      sorterRectsD0Basic.push(rectD0Element.value());
      rectD0Element = fileReaderRectsD0.GetNextElement();
    }
    fileReaderRectsD0.Close();
  }

  long long xSize = 0;
  double globalMinX = -1;
  double globalMinY = -1;
  double globalMaxX = -1;
  double globalMaxY = -1;

  ad_utility::BackgroundStxxlSorter<rTreeValueWithOrderIndex, sortRuleLambdaYWithIndex> sorterRectsD1 = ad_utility::BackgroundStxxlSorter<rTreeValueWithOrderIndex, sortRuleLambdaYWithIndex>(std::ceil(maxBuildingRamUsage / 3.0));
  std::shared_ptr<multiBoxWithOrderIndex> RectanglesD1WithOrder = std::make_shared<multiBoxWithOrderIndex>();

  if (workInRam) {
    for (rTreeValue element : rectsD0Basic) {
      rTreeValueWithOrderIndex entry = rTreeValueWithOrderIndex(element.box, element.id, xSize, 0);
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
    for (rTreeValue element : sorterRectsD0Basic.sortedView()) {
      rTreeValueWithOrderIndex entry = rTreeValueWithOrderIndex(element.box, element.id, xSize, 0);
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

  long long ySize = 0;
  std::ofstream r1File = std::ofstream(onDiskBase + ".boundingbox.d1.tmp", std::ios::binary);
  ad_utility::BackgroundStxxlSorter<rTreeValueWithOrderIndex, sortRuleLambdaXWithIndex> sorterRectsD0 = ad_utility::BackgroundStxxlSorter<rTreeValueWithOrderIndex, sortRuleLambdaXWithIndex>(std::ceil(maxBuildingRamUsage / 3.0));
  std::shared_ptr<multiBoxWithOrderIndex> RectanglesD0WithOrder = std::make_shared<multiBoxWithOrderIndex>();
  std::shared_ptr<multiBoxWithOrderIndex> r1Small = std::make_shared<multiBoxWithOrderIndex>();
  // placeholder
  r1Small->push_back(rTreeValueWithOrderIndex());
  r1Small->push_back(rTreeValueWithOrderIndex());
  rTreeValueWithOrderIndex minD1;
  rTreeValueWithOrderIndex maxD1;

  if (workInRam) {
    for (rTreeValueWithOrderIndex element : *RectanglesD1WithOrder) {
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
    for (rTreeValueWithOrderIndex element : sorterRectsD1.sortedView()) {
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

  long long currentX = 0;
  std::ofstream r0File = std::ofstream(onDiskBase + ".boundingbox.d0.tmp", std::ios::binary);
  std::shared_ptr<multiBoxWithOrderIndex> r0Small = std::make_shared<multiBoxWithOrderIndex>();
  // placeholder
  r0Small->push_back(rTreeValueWithOrderIndex());
  r0Small->push_back(rTreeValueWithOrderIndex());
  rTreeValueWithOrderIndex minD0;
  rTreeValueWithOrderIndex maxD0;

  if (workInRam) {
    for (rTreeValueWithOrderIndex element : *RectanglesD0WithOrder) {
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
    for (rTreeValueWithOrderIndex element : sorterRectsD0.sortedView()) {
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

  boxGeo boundingBox = Rtree::createBoundingBox(globalMinX, globalMinY, globalMaxX, globalMaxY);
  if (workInRam) {
      orderedInputRectangles.CreateOrderedBoxesInRam(RectanglesD0WithOrder, RectanglesD1WithOrder, r0Small, r1Small, boundingBox);
  } else {
      orderedInputRectangles.CreateOrderedBoxesOnDisk(onDiskBase + ".boundingbox.d0", onDiskBase + ".boundingbox.d1", r0Small, r1Small, xSize, boundingBox);
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

bool intersects(const boxGeo &b1, const boxGeo &b2) {
  /**
     * Determine whether two bounding boxes intersect
   */
  bool notIntersecting = b1.min_corner().get<0>() > b2.max_corner().get<0>() ||
                         b2.min_corner().get<0>() > b1.max_corner().get<0>() ||
                         b1.min_corner().get<1>() > b2.max_corner().get<1>() ||
                         b2.min_corner().get<1>() > b1.max_corner().get<1>();

  return !notIntersecting;
}

static double costFunctionTGS(boxGeo& b0, boxGeo& b1, size_t dim) {
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

boxGeo Rtree::createBoundingBox(double pointOneX, double pointOneY, double pointTwoX, double pointTwoY) {
  return make<boxGeo>(make<pointGeo>(pointOneX, pointOneY), make<pointGeo>(pointTwoX, pointTwoY));
}

static std::vector<OrderedBoxes> TGSRecursive(const std::string& filePath, OrderedBoxes orderedInputRectangles, size_t M, size_t S, long long maxBuildingRamUsage) {
  /**
     * This function recursively constructs one layer of children for a certain root node.
     * The input rectangles must be sorted in both x- and y-direction.
     * The algorithm is based on this paper https://dl.acm.org/doi/pdf/10.1145/288692.288723
   */

  unsigned long long n = orderedInputRectangles.GetSize();

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
  std::map<long long, long long> lookup;

  // sort the rectangles
  long long fileLines = std::ceil(std::filesystem::file_size(file) / (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(long long)));
  bool workInRam = (std::filesystem::file_size(file) + fileLines * 2 * sizeof(long long)) * 4 < this->maxBuildingRamUsage;

  if (workInRam) {
    std::cout << "Building in ram" << std::endl;
  } else {
    std::cout << "Building on disk" << std::endl;
  }
  OrderedBoxes orderedInputRectangles = SortInput(onDiskBase, M, maxBuildingRamUsage, workInRam);
  std::cout << "Finished intital sorting" << std::endl;

  // build the tree in a depth first approach
  std::stack<ConstructionNode> layerStack;

  long long newId = 1; // start from 1, because 0 is the root item
  ConstructionNode rootItem = ConstructionNode(0, orderedInputRectangles);
  layerStack.push(rootItem);
  size_t layer = 0;

  while (!layerStack.empty()) {
    ConstructionNode currentItem = layerStack.top();
    layerStack.pop();

    if (currentItem.GetOrderedBoxes().GetSize() <= M) {
      // reached a leaf
      currentItem.AddChildrenToItem();
      long long nodePtr = SaveNode(currentItem, true, nodesOfs);
      lookup[currentItem.GetId()] = nodePtr;
    } else {
      std::vector<OrderedBoxes> tgsResult = TGSRecursive(onDiskBase + ".boundingbox." + std::to_string(layer), currentItem.GetOrderedBoxes(), M, std::ceil(((float) currentItem.GetOrderedBoxes().GetSize()) / ((float) M)), this->maxBuildingRamUsage);
      for (OrderedBoxes& currentOrderedRectangles : tgsResult) {
        ConstructionNode newItem = ConstructionNode(newId, currentOrderedRectangles);
        layerStack.push(newItem);

        currentItem.AddChild(newItem);

        newId++;
      }

      long long nodePtr = SaveNode(currentItem, false, nodesOfs);
      lookup[currentItem.GetId()] = nodePtr;
    }
    layer++;
  }
  nodesOfs.close();

  std::ofstream lookupOfs(folder + "/lookup.bin", std::ios::binary);
  for (unsigned int i = 0; i < newId; i++) {
    long long nodePtr = lookup[i];
    lookupOfs.write(reinterpret_cast<const char *>(&nodePtr), sizeof(long long));
  }
  lookupOfs.close();
}

multiBoxGeo Rtree::SearchTree(boxGeo query, const std::string &folder) {
  std::ifstream lookupIfs = std::ifstream(folder + "/lookup.bin", std::ios::binary);
  std::ifstream nodesIfs = std::ifstream(folder + "/nodes.bin", std::ios::binary);

  Node rootNode = LoadNode(0, lookupIfs, nodesIfs);
  multiBoxGeo results;
  std::stack<Node> nodes;
  nodes.push(rootNode);

  while(!nodes.empty()) {
    Node currentNode = nodes.top();
    nodes.pop();

    for (rTreeValue child : currentNode.GetChildren()) {
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

ConstructionNode::ConstructionNode(long long id, OrderedBoxes orderedBoxes)
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
    for(rTreeValueWithOrderIndex box : *this->GetOrderedBoxes().GetRectanglesInRam()) {
      Node leafNode = Node(box.id, box.box);
      this->AddChild(leafNode);
    }
  } else {
    FileReader fileReader = FileReader(this->GetOrderedBoxes().GetRectanglesOnDisk());

    std::optional<rTreeValueWithOrderIndex> element = fileReader.GetNextElement();
    while(element) {
      Node leafNode = Node(element.value().id, element.value().box);
      this->AddChild(leafNode);
      element = fileReader.GetNextElement();
    }

    fileReader.Close();
  }
}

long long Node::GetId() const {
  return this->id;
}

OrderedBoxes ConstructionNode::GetOrderedBoxes() {
  return this->orderedBoxes;
}

Node::Node(long long id, boxGeo boundingbox) {
  this->id = id;
  this->boundingBox = boundingbox;
}

Node::Node(long long id) {
  this->id = id;
}

Node::Node() {}

Node::Node(long long id, boxGeo boundingBox, multiBoxGeo &children, bool isLastInnerNode) {
  this->id = id;
  this->boundingBox = boundingBox;
  this->children = children;
  this->isLastInnerNode = isLastInnerNode;
}

Node::Node(long long id, double minX, double minY, double maxX, double maxY, bool isLastInnerNode) {
  this->id = id;
  this->boundingBox = Rtree::createBoundingBox(minX, minY, maxX, maxY);
  this->isLastInnerNode = isLastInnerNode;
}

void Node::AddChild(Node& child) {
  boxGeo box = child.GetBoundingBox();
  unsigned long long entryId = child.GetId();
  rTreeValue entry = rTreeValue(box, entryId);
  this->children.push_back(entry);
}

boxGeo Node::GetBoundingBox() const {
  return this->boundingBox;
}

void Node::SetIsLastInnerNode(bool _isLastInnerNode) {
  this->isLastInnerNode = _isLastInnerNode;
}

bool Node::GetIsLastInnerNode() const {
  return this->isLastInnerNode;
}

multiBoxGeo Node::GetChildren() {
  return this->children;
}

long long Rtree::SaveNode(Node &node, bool isLastInnerNode, std::ofstream& nodesOfs) {
  node.SetIsLastInnerNode(isLastInnerNode);

  long long pos = static_cast<long long>(nodesOfs.tellp());
  boost::archive::binary_oarchive archive(nodesOfs);
  archive << node;
  nodesOfs.write(" ", 1);

  return pos;
}

Node Rtree::LoadNode(long long id, std::ifstream& lookupIfs, std::ifstream& nodesIfs) {
  Node newNode;

  long long offset = id * (long long)sizeof(long long);
  lookupIfs.seekg(offset, std::ios::beg);

  long long nodePtr;
  lookupIfs.read(reinterpret_cast<char*>(&nodePtr), sizeof(long long));

  nodesIfs.seekg(nodePtr);
  boost::archive::binary_iarchive ia(nodesIfs);
  ia >> newNode;

  return newNode;
}

std::optional<boxGeo> GetBoundingBoxFromWKT(const std::string& wkt) {
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

std::optional<boxGeo> Rtree::ConvertWordToRtreeEntry(const std::string& wkt) {
  /**
     * Convert a single wkt literal to a boundingbox.
   */
  std::optional<boxGeo> boundingBox;

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

void Rtree::SaveEntry(boxGeo boundingBox, uint64_t index, std::ofstream& convertOfs) {
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

void Rtree::SaveEntryWithOrderIndex(rTreeValueWithOrderIndex treeValue, std::ofstream& convertOfs) {
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
  convertOfs.write(reinterpret_cast<const char *>(&treeValue.orderX), sizeof(long long));
  convertOfs.write(reinterpret_cast<const char *>(&treeValue.orderY), sizeof(long long));
}

multiBoxGeo Rtree::LoadEntries(const std::string& file) {
  multiBoxGeo boxes;

  FileReaderWithoutIndex fileReader = FileReaderWithoutIndex(file);

  std::optional<rTreeValue> element = fileReader.GetNextElement();
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

  std::optional<rTreeValueWithOrderIndex> element = fileReader.GetNextElement();
  while (element) {
    boxes.push_back(element.value());
    element = fileReader.GetNextElement();
  }

  fileReader.Close();

  return boxes;
}

Rtree::Rtree(uintmax_t maxBuildingRamUsage) {
  this->maxBuildingRamUsage = maxBuildingRamUsage;
}

bool OrderedBoxes::WorkInRam() const{
  return this->workInRam;
}

void OrderedBoxes::CreateOrderedBoxesInRam(const std::shared_ptr<multiBoxWithOrderIndex>& rectanglesD0, const std::shared_ptr<multiBoxWithOrderIndex>& rectanglesD1, const std::shared_ptr<multiBoxWithOrderIndex>& rectanglesSmallD0, const std::shared_ptr<multiBoxWithOrderIndex>& rectanglesSmallD1, boxGeo box) {
  this->workInRam = true;
  this->rectanglesD0InRam = rectanglesD0;
  this->rectanglesD1InRam = rectanglesD1;
  this->rectanglesD0Small = rectanglesSmallD0;
  this->rectanglesD1Small = rectanglesSmallD1;
  this->size = (*rectanglesD0).size();
  this->boundingBox = box;
}

void OrderedBoxes::CreateOrderedBoxesOnDisk(const std::string& rectanglesD0, const std::string& rectanglesD1, const std::shared_ptr<multiBoxWithOrderIndex>& rectanglesSmallD0, const std::shared_ptr<multiBoxWithOrderIndex>& rectanglesSmallD1, long long size, boxGeo box) {
  this->workInRam = false;
  this->rectanglesD0OnDisk = rectanglesD0 + ".tmp";
  this->rectanglesD1OnDisk = rectanglesD1 + ".tmp";
  this->rectanglesD0Small = rectanglesSmallD0;
  this->rectanglesD1Small = rectanglesSmallD1;
  this->size = size;
  this->boundingBox = box;
}

boxGeo OrderedBoxes::GetBoundingBox() {
  return this->boundingBox;
}

long long OrderedBoxes::GetSize() const {
  return this->size;
}

std::shared_ptr<multiBoxWithOrderIndex> OrderedBoxes::GetRectanglesInRam() {
  return this->rectanglesD0InRam;
}

std::string OrderedBoxes::GetRectanglesOnDisk() {
  return this->rectanglesD0OnDisk;
}

SplitResult OrderedBoxes::GetBestSplit() {
  /**
     * Determine based on the "small-lists", which split is the best for the rtree.
   */
  struct SplitResult splitResult;

  rTreeValueWithOrderIndex minElement;
  rTreeValueWithOrderIndex maxElement;
  rTreeValueWithOrderIndex currentLastElement;
  rTreeValueWithOrderIndex currentElement;

  // This bool is used, since we need every other element as our element "S * i" (described in the algorithm)
  // To perform the split better, the element before it (S * i - 1) is saved as well
  bool currentlyAtSTimesI = false;

  for (size_t dim = 0; dim < 2; dim++) {
    for (long long i = 0; i < this->rectanglesD0Small->size(); i++) {
      currentElement = dim == 0 ? (*this->rectanglesD0Small)[i] : (*this->rectanglesD1Small)[i];

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
          minXB0 = (minElement.box.min_corner().get<0>() + minElement.box.max_corner().get<0>()) / 2;
          maxXB0 = (currentLastElement.box.min_corner().get<0>() + currentLastElement.box.max_corner().get<0>()) / 2;

          minXB1 = (currentElement.box.min_corner().get<0>() + currentElement.box.max_corner().get<0>()) / 2;
          maxXB1 = (maxElement.box.min_corner().get<0>() + maxElement.box.max_corner().get<0>()) / 2;
        } else {
          minYB0 = (minElement.box.min_corner().get<1>() + minElement.box.max_corner().get<1>()) / 2;
          maxYB0 = (currentLastElement.box.min_corner().get<1>() + currentLastElement.box.max_corner().get<1>()) / 2;

          minYB1 = (currentElement.box.min_corner().get<1>() + currentElement.box.max_corner().get<1>()) / 2;
          maxYB1 = (maxElement.box.min_corner().get<1>() + maxElement.box.max_corner().get<1>()) / 2;
        }

        currentlyAtSTimesI = false;
      } else {
        break;
      }

      boxGeo b0 = Rtree::createBoundingBox(minXB0, minYB0, maxXB0, maxYB0);
      boxGeo b1 = Rtree::createBoundingBox(minXB1, minYB1, maxXB1, maxYB1);


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

std::pair<OrderedBoxes, OrderedBoxes> OrderedBoxes::SplitAtBest(const std::string& filePath, size_t S, size_t M, long long maxBuildingRamUsage) {
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

  std::pair<boxGeo, boxGeo> boundingBoxes = PerformSplit(splitResult, splitBuffers, M, S);

  split0.CreateOrderedBoxesInRam(splitBuffers.s0Dim0, splitBuffers.s0Dim1, splitBuffers.s0SmallDim0, splitBuffers.s0SmallDim1, boundingBoxes.first);
  split1.CreateOrderedBoxesInRam(splitBuffers.s1Dim0, splitBuffers.s1Dim1, splitBuffers.s1SmallDim0, splitBuffers.s1SmallDim1, boundingBoxes.second);

  (*this->rectanglesD0InRam).clear();
  (*this->rectanglesD1InRam).clear();
  (*this->rectanglesD0Small).clear();
  (*this->rectanglesD1Small).clear();
  (*this->rectanglesD0InRam).shrink_to_fit();
  (*this->rectanglesD1InRam).shrink_to_fit();
  (*this->rectanglesD0Small).shrink_to_fit();
  (*this->rectanglesD1Small).shrink_to_fit();

  return std::make_pair(split0, split1);
}

std::pair<OrderedBoxes, OrderedBoxes> OrderedBoxes::SplitAtBestOnDisk(const std::string& filePath, size_t S, size_t M, long long maxBuildingRamUsage) {
  /**
     * Split the ordered boxes on disk. First determine the best split and then perform it
   */

  OrderedBoxes split0;
  OrderedBoxes split1;

  struct SplitResult splitResult = this->GetBestSplit();

  struct SplitBuffersDisk splitBuffers;
  struct SplitBuffersRam splitBuffersRam;

  // perfrom the split
  long long sizeLeft = std::ceil((splitResult.bestIndex - 2) / 2.0) * S;
  long long sizeRight = this->size - sizeLeft;
  long long split0ByteSize = sizeLeft * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(long long));
  long long split1ByteSize = sizeRight * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(long long));
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

  std::pair<boxGeo, boxGeo> boundingBoxes = PerformSplit(splitResult, splitBuffers, M, S, maxBuildingRamUsage);

  if (!split0InRam) {
    splitBuffers.split0Dim0File.value().close();
    splitBuffers.split0Dim1File.value().close();

    split0.CreateOrderedBoxesOnDisk(filePath + ".0.dim0", filePath + ".0.dim1", splitBuffers.splitBuffersRam.s0SmallDim0, splitBuffers.splitBuffersRam.s0SmallDim1, sizeLeft, boundingBoxes.first);
  } else {
    split0.CreateOrderedBoxesInRam(splitBuffers.splitBuffersRam.s0Dim0, splitBuffers.splitBuffersRam.s0Dim1, splitBuffers.splitBuffersRam.s0SmallDim0, splitBuffers.splitBuffersRam.s0SmallDim1, boundingBoxes.first);
  }

  if (!split1InRam) {
    splitBuffers.split1Dim0File.value().close();
    splitBuffers.split1Dim1File.value().close();

    split1.CreateOrderedBoxesOnDisk(filePath + ".1.dim0", filePath + ".1.dim1", splitBuffers.splitBuffersRam.s1SmallDim0, splitBuffers.splitBuffersRam.s1SmallDim1, sizeRight, boundingBoxes.second);
  } else {
    split1.CreateOrderedBoxesInRam(splitBuffers.splitBuffersRam.s1Dim0, splitBuffers.splitBuffersRam.s1Dim1, splitBuffers.splitBuffersRam.s1SmallDim0, splitBuffers.splitBuffersRam.s1SmallDim1, boundingBoxes.second);
  }

  std::remove(this->rectanglesD0OnDisk.c_str());
  std::remove(this->rectanglesD1OnDisk.c_str());

  return std::make_pair(split0, split1);
}

std::pair<boxGeo, boxGeo> OrderedBoxes::PerformSplit(SplitResult splitResult, SplitBuffersRam& splitBuffersRam, size_t M, size_t S) {
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
  std::pair<boxGeo, boxGeo> boundingBoxes = PerformSplit(splitResult, splitBuffersDisk, M, S, 0);

  splitBuffersRam = splitBuffersDisk.splitBuffersRam;

  return boundingBoxes;
}

std::pair<boxGeo, boxGeo> OrderedBoxes::PerformSplit(SplitResult splitResult, SplitBuffersDisk& splitBuffers, size_t M, size_t S, long long maxBuildingRamUsage) {
  /**
     * Perform the best split on the current ordered boxes in the disk case
   */

  long long sizeLeft = std::ceil((splitResult.bestIndex - 2) / 2.0) * S;
  long long sizeRight = this->size - sizeLeft;
  size_t SSplit0 = sizeLeft <= S ? std::ceil(sizeLeft / (double) M) : S;
  size_t SSplit1 = sizeRight <= S ? std::ceil(sizeRight / (double) M) : S;
  long long split0ByteSize = sizeLeft * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(long long));
  long long split1ByteSize = sizeRight * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(long long));
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

  rTreeValueWithOrderIndex minSplit0OtherDim;
  rTreeValueWithOrderIndex maxSplit0OtherDim;
  rTreeValueWithOrderIndex minSplit1OtherDim;
  rTreeValueWithOrderIndex maxSplit1OtherDim;

  if (splitResult.bestDim == 0) {
    splitBuffers.splitBuffersRam.s0SmallDim0->push_back(splitResult.bestMinElement);
    splitBuffers.splitBuffersRam.s0SmallDim0->push_back(splitResult.bestLastElement);
    splitBuffers.splitBuffersRam.s1SmallDim0->push_back(splitResult.bestElement);
    splitBuffers.splitBuffersRam.s1SmallDim0->push_back(splitResult.bestMaxElement);

    // placeholder, since we need the min and max element of the split in the first two spots
    splitBuffers.splitBuffersRam.s0SmallDim1->push_back(rTreeValueWithOrderIndex());
    splitBuffers.splitBuffersRam.s0SmallDim1->push_back(rTreeValueWithOrderIndex());
    splitBuffers.splitBuffersRam.s1SmallDim1->push_back(rTreeValueWithOrderIndex());
    splitBuffers.splitBuffersRam.s1SmallDim1->push_back(rTreeValueWithOrderIndex());
  } else {
    splitBuffers.splitBuffersRam.s0SmallDim1->push_back(splitResult.bestMinElement);
    splitBuffers.splitBuffersRam.s0SmallDim1->push_back(splitResult.bestLastElement);
    splitBuffers.splitBuffersRam.s1SmallDim1->push_back(splitResult.bestElement);
    splitBuffers.splitBuffersRam.s1SmallDim1->push_back(splitResult.bestMaxElement);

    // placeholder
    splitBuffers.splitBuffersRam.s0SmallDim0->push_back(rTreeValueWithOrderIndex());
    splitBuffers.splitBuffersRam.s0SmallDim0->push_back(rTreeValueWithOrderIndex());
    splitBuffers.splitBuffersRam.s1SmallDim0->push_back(rTreeValueWithOrderIndex());
    splitBuffers.splitBuffersRam.s1SmallDim0->push_back(rTreeValueWithOrderIndex());
  }

  std::optional<rTreeValueWithOrderIndex> elementOpt;
  std::optional<FileReader> fileReaderDim0;
  std::optional<FileReader> fileReaderDim1;
  if (!this->workInRam) {
    fileReaderDim0 = { FileReader(this->rectanglesD0OnDisk) };
    fileReaderDim1 = { FileReader(this->rectanglesD1OnDisk) };
  }
  long long currentXSplit0 = 0;
  long long currentXSplit1 = 0;
  long long currentYSplit0 = 0;
  long long currentYSplit1 = 0;
  for (size_t dim = 0; dim < 2; dim++) {
    // start performing the actual split
    long long i = 0;

    if (!this->workInRam) {
      if (dim == 0)
        elementOpt = fileReaderDim0.value().GetNextElement();
      if (dim == 1)
        elementOpt = fileReaderDim1.value().GetNextElement();
    }

    while ((this->workInRam && i < this->size) || (!this->workInRam && elementOpt)) {
      rTreeValueWithOrderIndex element;

      // get the current element, either from disk or from ram
      if (this->workInRam) {
        element = dim == 0 ? (*this->rectanglesD0InRam)[i] : (*this->rectanglesD1InRam)[i];
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
  if (splitResult.bestDim == 0) {
    (*splitBuffers.splitBuffersRam.s0SmallDim1)[0] = minSplit0OtherDim;
    (*splitBuffers.splitBuffersRam.s0SmallDim1)[1] = maxSplit0OtherDim;
    (*splitBuffers.splitBuffersRam.s1SmallDim1)[0] = minSplit1OtherDim;
    (*splitBuffers.splitBuffersRam.s1SmallDim1)[1] = maxSplit1OtherDim;
  } else {
    (*splitBuffers.splitBuffersRam.s0SmallDim0)[0] = minSplit0OtherDim;
    (*splitBuffers.splitBuffersRam.s0SmallDim0)[1] = maxSplit0OtherDim;
    (*splitBuffers.splitBuffersRam.s1SmallDim0)[0] = minSplit1OtherDim;
    (*splitBuffers.splitBuffersRam.s1SmallDim0)[1] = maxSplit1OtherDim;
  }

  boxGeo boxSplit0 = Rtree::createBoundingBox(globalMinXS0, globalMinYS0, globalMaxXS0, globalMaxYS0);
  boxGeo boxSplit1 = Rtree::createBoundingBox(globalMinXS1, globalMinYS1, globalMaxXS1, globalMaxYS1);

  return std::make_pair(boxSplit0, boxSplit1);
}

FileReader::FileReader(const std::string& filePath) {
  this->filePath = filePath;

  this->file = std::ifstream(this->filePath, std::ios::binary);
  this->file.seekg (0, std::ifstream::end);
  this->fileLength = this->file.tellg();
  this->file.seekg (0, std::ifstream::beg);
}

std::optional<rTreeValueWithOrderIndex> FileReader::GetNextElement() {
  if (this->file.tellg() < this->fileLength) {
    double minX;
    double minY;
    double maxX;
    double maxY;
    uint64_t id;
    long long orderX;
    long long orderY;

    this->file.read(reinterpret_cast<char*>(&minX), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&minY), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&maxX), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&maxY), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&id), sizeof(uint64_t));
    this->file.read(reinterpret_cast<char*>(&orderX), sizeof(long long));
    this->file.read(reinterpret_cast<char*>(&orderY), sizeof(long long));

    boxGeo box = Rtree::createBoundingBox(minX, minY, maxX, maxY);
    rTreeValueWithOrderIndex element = rTreeValueWithOrderIndex(box, id, orderX, orderY);

    return { element };
  } else {
    return {};
  }
}

void FileReader::Close() {
  this->file.close();
}

FileReaderWithoutIndex::FileReaderWithoutIndex(const std::string& filePath) {
  this->filePath = filePath;

  this->file = std::ifstream(this->filePath, std::ios::binary);
  this->file.seekg (0, std::ifstream::end);
  this->fileLength = this->file.tellg();
  this->file.seekg (0, std::ifstream::beg);
}

std::optional<rTreeValue> FileReaderWithoutIndex::GetNextElement() {
  if (this->file.tellg() < this->fileLength) {
    double minX;
    double minY;
    double maxX;
    double maxY;
    uint64_t id;

    this->file.read(reinterpret_cast<char*>(&minX), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&minY), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&maxX), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&maxY), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&id), sizeof(uint64_t));

    boxGeo box = Rtree::createBoundingBox(minX, minY, maxX, maxY);
    rTreeValue boxWithId = rTreeValue(box, id);

    return { boxWithId };
  } else {
    return {};
  }
}

void FileReaderWithoutIndex::Close() {
  this->file.close();
}
