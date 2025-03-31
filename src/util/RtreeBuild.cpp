//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>
#include "./Rtree.h"
#include "./RtreeFileReader.h"
#include "./RtreeSorter.h"

static double costFunctionTGS(BasicGeometry::BoundingBox& b0,
                              BasicGeometry::BoundingBox& b1) {
  /**
     * The cost function determines the quality of a split. The lower the cost,
     * the better the split. Each split gets represented by the resulting bounding
     * boxes of the split pieces.
   */
  double cost;

  // The cost represents the overlap of the two boxes
  double overlapX = BasicGeometry::GetMaxX(b0) - BasicGeometry::GetMinX(b1);
  overlapX = overlapX < 0 ? 0 : overlapX;

  double overlapY = BasicGeometry::GetMaxY(b0) - BasicGeometry::GetMinY(b1);
  overlapY = overlapY < 0 ? 0 : overlapY;

  cost = overlapX * overlapY;

  return cost;
}

static std::vector<OrderedBoxes> TGSRecursive(
    const std::string& filePath, OrderedBoxes* orderedInputRectangles, size_t M,
    size_t S, uint64_t maxBuildingRamUsage) {
  /**
     * This function recursively constructs one layer of children for a certain
     * root node. The input rectangles must be sorted in both x- and y-direction.
     * The algorithm is based on this paper
     * https://dl.acm.org/doi/pdf/10.1145/288692.288723
   */

  uint64_t n = orderedInputRectangles->GetSize();

  if (n <= S) {
    // stop condition
    return std::vector<OrderedBoxes>{*orderedInputRectangles};
  }

  // split the rectangles at the best split
  std::pair<OrderedBoxes, OrderedBoxes> split =
      orderedInputRectangles->SplitAtBest(filePath, S, M, maxBuildingRamUsage);

  // recursion
  std::vector<OrderedBoxes> result0 =
      TGSRecursive(filePath + ".0", &split.first, M, S, maxBuildingRamUsage);
  std::vector<OrderedBoxes> result1 =
      TGSRecursive(filePath + ".1", &split.second, M, S, maxBuildingRamUsage);

  std::vector<OrderedBoxes> result;
  result.insert(result.begin(), result0.begin(), result0.end());
  result.insert(result.end(), result1.begin(), result1.end());

  return result;
}

uint64_t Rtree::BuildTree(const std::string& onDiskBase,
                          const std::string& fileSuffix, size_t M,
                          const std::string& folder) const {
  const std::filesystem::path file = onDiskBase + fileSuffix + ".tmp";

  // sort the rectangles
  uint64_t fileLines =
      std::ceil(std::filesystem::file_size(file) /
                (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t)));
  bool workInRam =
      (std::filesystem::file_size(file) + fileLines * 2 * sizeof(uint64_t)) *
          4 <
      maxBuildingRamUsage_;

  std::cout << "[R-tree] Starting the initial sorting" << (workInRam ? " in ram..." : " on disk...") << std::endl;
  OrderedBoxes orderedInputRectangles =
      SortInput(onDiskBase, fileSuffix, M, maxBuildingRamUsage_, workInRam);
  uint64_t totalSize = orderedInputRectangles.GetSize();
  std::cout << "[R-tree] Finished sorting" << std::endl;

  // prepare the files
  std::filesystem::create_directory(folder);
  std::ofstream nodesOfs =
      std::ofstream(folder + "/nodes.bin", std::ios::binary);
  std::map<uint64_t, uint64_t> lookup;

  // build the tree in a depth first approach
  std::stack<ConstructionNode> layerStack;

  uint64_t newId = 1;  // start from 1, because 0 is the root item
  ConstructionNode rootItem = ConstructionNode(0, orderedInputRectangles);
  layerStack.push(rootItem);
  orderedInputRectangles.Clear();
  rootItem.orderedBoxes().Clear();
  size_t layer = 0;

  while (!layerStack.empty()) {
    ConstructionNode currentItem = layerStack.top();
    layerStack.pop();

    if (currentItem.orderedBoxes().GetSize() <= M) {
      // reached a leaf
      currentItem.AddChildrenToItem();
      currentItem.SetIsLastInnerNode(true);
      uint64_t nodePtr = FileReader::SaveNode(currentItem, nodesOfs);
      lookup[currentItem.GetId()] = nodePtr;
    } else {
      size_t S = std::ceil(((double)currentItem.orderedBoxes().GetSize()) /
                           ((double)M));
      if (currentItem.orderedBoxes().GetSize() <= M * M) {
        // in this case S can be just M
        S = M;
      }
      std::vector<OrderedBoxes> tgsResult = TGSRecursive(
          onDiskBase + fileSuffix + "." + std::to_string(layer),
          &currentItem.orderedBoxes(), M, S, maxBuildingRamUsage_);
      for (OrderedBoxes& currentOrderedRectangles : tgsResult) {
        ConstructionNode newItem =
            ConstructionNode(newId, currentOrderedRectangles);
        layerStack.push(newItem);

        currentItem.AddChild(newItem);

        newId++;
      }

      uint64_t nodePtr = FileReader::SaveNode(currentItem, nodesOfs);
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

  return totalSize;
}

bool OrderedBoxes::WorkInRam() const { return workInRam_; }

void OrderedBoxes::Clear() {
  size_ = 0;
  boundingBox_ = BasicGeometry::CreateBoundingBox(0, 0, 0, 0);
  rectsD0_.Clear();
  rectsD1_.Clear();
}

void OrderedBoxes::SetOrderedBoxesToRam(RectanglesForOrderedBoxes rectanglesD0,
                                        RectanglesForOrderedBoxes rectanglesD1,
                                        BasicGeometry::BoundingBox box) {
  workInRam_ = true;
  rectsD0_ = std::move(rectanglesD0);
  rectsD1_ = std::move(rectanglesD1);
  size_ =
      std::get<multiBoxWithOrderIndex>(rectsD0_.rectangles).size();
  boundingBox_ = box;
}

void OrderedBoxes::SetOrderedBoxesToDisk(RectanglesForOrderedBoxes rectanglesD0,
                                         RectanglesForOrderedBoxes rectanglesD1,
                                         uint64_t size,
                                         BasicGeometry::BoundingBox box) {
  workInRam_ = false;
  rectsD0_ = std::move(rectanglesD0);
  rectsD1_ = std::move(rectanglesD1);
  size_ = size;
  boundingBox_ = box;
}

BasicGeometry::BoundingBox OrderedBoxes::GetBoundingBox() {
  return boundingBox_;
}

uint64_t OrderedBoxes::GetSize() const { return size_; }

multiBoxWithOrderIndex OrderedBoxes::GetRectanglesInRam() {
  return std::get<multiBoxWithOrderIndex>(rectsD0_.rectangles);
}

std::filesystem::path OrderedBoxes::GetRectanglesOnDisk() {
  return std::get<std::filesystem::path>(rectsD0_.rectangles);
}

SplitResult OrderedBoxes::GetBestSplit(size_t M) {
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

  uint64_t S = std::ceil(GetSize() / M);

  for (size_t dim = 0; dim < 2; dim++) {
    for (uint64_t i = 0; i < rectsD0_.rectanglesSmall.size(); i++) {
      currentElement = dim == 0 ? rectsD0_.rectanglesSmall[i]
                                : rectsD1_.rectanglesSmall[i];

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

      BasicGeometry::BoundingBox b0 =
          BasicGeometry::CreateBoundingBox(minXB0, minYB0, maxXB0, maxYB0);
      BasicGeometry::BoundingBox b1 =
          BasicGeometry::CreateBoundingBox(minXB1, minYB1, maxXB1, maxYB1);

      // size of b0 and b1
      uint64_t nB0 = (i / 2) * S;
      uint64_t nB1 = GetSize() - nB0;

      double cost = costFunctionTGS(b0, b1);

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
    const std::filesystem::path& filePath, size_t S, size_t M,
    uint64_t maxBuildingRamUsage) {
  if (workInRam_) {
    return SplitAtBestInRam(S, M);
  } else {
    return SplitAtBestOnDisk(filePath, S, M, maxBuildingRamUsage);
  }
}

std::pair<OrderedBoxes, OrderedBoxes> OrderedBoxes::SplitAtBestInRam(size_t S,
                                                                     size_t M) {
  /**
     * Split the ordered boxes in ram. First determine the best split and then
     * perform it
   */

  struct SplitResult splitResult = GetBestSplit(M);

  OrderedBoxes split0;
  OrderedBoxes split1;

  RectanglesForOrderedBoxes rectsD0Split0;
  RectanglesForOrderedBoxes rectsD1Split0;
  RectanglesForOrderedBoxes rectsD0Split1;
  RectanglesForOrderedBoxes rectsD1Split1;

  struct SplitBuffers splitBuffers = {rectsD0Split0, rectsD1Split0,
                                      rectsD0Split1, rectsD1Split1};

  std::pair<BasicGeometry::BoundingBox, BasicGeometry::BoundingBox>
      boundingBoxes = PerformSplit(splitResult, splitBuffers, M, S);

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

  struct SplitResult splitResult = GetBestSplit(M);

  RectanglesForOrderedBoxes rectsD0Split0;
  RectanglesForOrderedBoxes rectsD1Split0;
  RectanglesForOrderedBoxes rectsD0Split1;
  RectanglesForOrderedBoxes rectsD1Split1;
  struct SplitBuffers splitBuffers = {rectsD0Split0, rectsD1Split0,
                                      rectsD0Split1, rectsD1Split1};

  // perform the split
  auto sizeLeft =
      (uint64_t)(std::ceil(((double)splitResult.bestIndex - 2.0) / 2.0) *
                 (double)S);
  uint64_t sizeRight = size_ - sizeLeft;
  uint64_t split0ByteSize =
      sizeLeft * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t));
  uint64_t split1ByteSize = sizeRight * (4 * sizeof(double) + sizeof(uint64_t) +
                                         2 * sizeof(uint64_t));
  bool split0InRam = split0ByteSize * 4 < maxBuildingRamUsage;
  bool split1InRam = split1ByteSize * 4 < maxBuildingRamUsage;

  auto configureDiskPath = [filePath](RectanglesForOrderedBoxes& rectsD0, RectanglesForOrderedBoxes& rectsD1, const std::string& splitNumber) {
    rectsD0.rectangles = filePath + "." + splitNumber + ".dim0.tmp";
    rectsD1.rectangles = filePath + "." + splitNumber + ".dim1.tmp";
  };

  if (!split0InRam) {
    configureDiskPath(splitBuffers.rectsD0Split0, splitBuffers.rectsD1Split0, "0");
  }

  if (!split1InRam) {
    configureDiskPath(splitBuffers.rectsD0Split1, splitBuffers.rectsD1Split1, "1");
  }

  std::pair<BasicGeometry::BoundingBox, BasicGeometry::BoundingBox>
      boundingBoxes =
          PerformSplit(splitResult, splitBuffers, M, S, maxBuildingRamUsage);

  if (!split0InRam) {
    split0.SetOrderedBoxesToDisk(rectsD0Split0, rectsD1Split0, sizeLeft,
                                 boundingBoxes.first);
  } else {
    split0.SetOrderedBoxesToRam(rectsD0Split0, rectsD1Split0,
                                boundingBoxes.first);
  }

  if (!split1InRam) {
    split1.SetOrderedBoxesToDisk(rectsD0Split1, rectsD1Split1, sizeRight,
                                 boundingBoxes.second);
  } else {
    split1.SetOrderedBoxesToRam(rectsD0Split1, rectsD1Split1,
                                boundingBoxes.second);
  }

  std::remove(
      std::get<std::filesystem::path>(rectsD0_.rectangles).c_str());
  std::remove(
      std::get<std::filesystem::path>(rectsD1_.rectangles).c_str());

  return std::make_pair(split0, split1);
}

std::pair<BasicGeometry::BoundingBox, BasicGeometry::BoundingBox>
OrderedBoxes::PerformSplit(SplitResult splitResult, SplitBuffers& splitBuffers,
                           size_t M, size_t S, uint64_t maxBuildingRamUsage) {
  /**
     * Perform the best split on the current ordered boxes in the disk case
   */

  auto sizeLeft =
      (uint64_t)(std::ceil(((double)splitResult.bestIndex - 2.0) / 2.0) *
                 (double)S);
  uint64_t sizeRight = size_ - sizeLeft;
  size_t SSplit0 =
      sizeLeft <= S ? (size_t)std::ceil((double)sizeLeft / (double)M) : S;
  if (sizeLeft <= S && sizeLeft <= M * M) {
    SSplit0 = M;
  }
  size_t SSplit1 =
      sizeRight <= S ? (size_t)std::ceil((double)sizeRight / (double)M) : S;
  if (sizeRight <= S && sizeRight <= M * M) {
    SSplit1 = M;
  }
  uint64_t split0ByteSize =
      sizeLeft * (4 * sizeof(double) + sizeof(uint64_t) + 2 * sizeof(uint64_t));
  uint64_t split1ByteSize = sizeRight * (4 * sizeof(double) + sizeof(uint64_t) +
                                         2 * sizeof(uint64_t));
  bool split0InRam =
      maxBuildingRamUsage == 0 || split0ByteSize * 4 < maxBuildingRamUsage;
  bool split1InRam =
      maxBuildingRamUsage == 0 || split1ByteSize * 4 < maxBuildingRamUsage;

  BasicGeometry::BoundingBox boxSplit0 =
      BasicGeometry::CreateBoundingBox(0, 0, 0, 0);
  BasicGeometry::BoundingBox boxSplit1 =
      BasicGeometry::CreateBoundingBox(0, 0, 0, 0);

  RTreeValueWithOrderIndex minSplit0OtherDim;
  RTreeValueWithOrderIndex maxSplit0OtherDim;
  RTreeValueWithOrderIndex minSplit1OtherDim;
  RTreeValueWithOrderIndex maxSplit1OtherDim;

  struct OtherDimension {
    multiBoxWithOrderIndex* smallSplit0;
    multiBoxWithOrderIndex* smallSplit1;
  } otherDimension{};

  auto pushSmallBoundaries = [splitResult](
                                 multiBoxWithOrderIndex& smallListS0,
                                 multiBoxWithOrderIndex& smallListS1) {
    smallListS0.push_back(splitResult.bestMinElement);
    smallListS0.push_back(splitResult.bestLastElement);
    smallListS1.push_back(splitResult.bestElement);
    smallListS1.push_back(splitResult.bestMaxElement);
  };

  auto addPlaceHolder = [&otherDimension](RectanglesForOrderedBoxes& rectsSplit0, RectanglesForOrderedBoxes& rectsSplit1) {
    otherDimension.smallSplit0 = &rectsSplit0.rectanglesSmall;
    otherDimension.smallSplit1 = &rectsSplit1.rectanglesSmall;

    // placeholder, since we need the min and max element of the split in the first two spots
    otherDimension.smallSplit0->emplace_back();
    otherDimension.smallSplit0->emplace_back();
    otherDimension.smallSplit1->emplace_back();
    otherDimension.smallSplit1->emplace_back();
  };

  if (splitResult.bestDim == 0) {
    pushSmallBoundaries(splitBuffers.rectsD0Split0.rectanglesSmall,
                        splitBuffers.rectsD0Split1.rectanglesSmall);
    addPlaceHolder(splitBuffers.rectsD1Split0, splitBuffers.rectsD1Split1);
  } else {
    pushSmallBoundaries(splitBuffers.rectsD1Split0.rectanglesSmall,
                        splitBuffers.rectsD1Split1.rectanglesSmall);
    addPlaceHolder(splitBuffers.rectsD0Split0, splitBuffers.rectsD0Split1);
  }

  std::optional<RTreeValueWithOrderIndex> elementOpt;
  std::filesystem::path rectsD0Path =
      !workInRam_
          ? std::get<std::filesystem::path>(rectsD0_.rectangles)
          : "";
  std::filesystem::path rectsD1Path =
      !workInRam_
          ? std::get<std::filesystem::path>(rectsD1_.rectangles)
          : "";
  FileReader fileReaderDim0 = FileReader(rectsD0Path);
  FileReader fileReaderDim1 = FileReader(rectsD1Path);
  FileReader::iterator fileReaderDim0Iterator =
      !rectsD0Path.empty() ? fileReaderDim0.begin() : FileReader::iterator();
  FileReader::iterator fileReaderDim1Iterator =
      !rectsD1Path.empty() ? fileReaderDim1.begin() : FileReader::iterator();
  uint64_t currentXSplit0 = 0;
  uint64_t currentXSplit1 = 0;
  uint64_t currentYSplit0 = 0;
  uint64_t currentYSplit1 = 0;

  std::optional<std::ofstream> rectanglesOnDiskS0D0Stream = {};
  std::optional<std::ofstream> rectanglesOnDiskS0D1Stream = {};
  std::optional<std::ofstream> rectanglesOnDiskS1D0Stream = {};
  std::optional<std::ofstream> rectanglesOnDiskS1D1Stream = {};
  if (!split0InRam && !workInRam_) {
    rectanglesOnDiskS0D0Stream = std::ofstream(
        std::get<std::filesystem::path>(splitBuffers.rectsD0Split0.rectangles),
        std::ios::binary);
    rectanglesOnDiskS0D1Stream = std::ofstream(
        std::get<std::filesystem::path>(splitBuffers.rectsD1Split0.rectangles),
        std::ios::binary);
  }
  if (!split1InRam && !workInRam_) {
    rectanglesOnDiskS1D0Stream = std::ofstream(
        std::get<std::filesystem::path>(splitBuffers.rectsD0Split1.rectangles),
        std::ios::binary);
    rectanglesOnDiskS1D1Stream = std::ofstream(
        std::get<std::filesystem::path>(splitBuffers.rectsD1Split1.rectangles),
        std::ios::binary);
  }

  auto performCertainSplit =
      [M, &splitBuffers, &splitResult, &rectanglesOnDiskS0D0Stream,
       &rectanglesOnDiskS0D1Stream, &rectanglesOnDiskS1D0Stream,
       &rectanglesOnDiskS1D1Stream](
          size_t dim, size_t split, uint64_t& current, size_t& currentSplitSize,
          RTreeValueWithOrderIndex& minElement,
          RTreeValueWithOrderIndex& maxElement, bool currentSplitInRam,
          bool workInRam, RTreeValueWithOrderIndex& element,
          BasicGeometry::BoundingBox& box) {
        multiBoxWithOrderIndex* currentSmallList;
        // current list is either in ram or on disk
        std::variant<multiBoxWithOrderIndex*, std::ofstream*> currentList;

        auto initializeCurrentList = [&currentSmallList, &currentList, &currentSplitInRam, &workInRam](RectanglesForOrderedBoxes& rects, std::optional<std::ofstream>& stream) {
          currentSmallList = &rects.rectanglesSmall;
          if (currentSplitInRam || workInRam) {
            currentList = &std::get<multiBoxWithOrderIndex>(
                rects.rectangles);
          } else {
            currentList = &stream.value();
          }
        };

        if (split == 0) {
          if (dim == 0) {
            initializeCurrentList(splitBuffers.rectsD0Split0, rectanglesOnDiskS0D0Stream);
          } else {
            initializeCurrentList(splitBuffers.rectsD1Split0, rectanglesOnDiskS0D1Stream);
          }
        } else {
          if (dim == 0) {
            initializeCurrentList(splitBuffers.rectsD0Split1, rectanglesOnDiskS1D0Stream);
          } else {
            initializeCurrentList(splitBuffers.rectsD1Split1, rectanglesOnDiskS1D1Stream);
          }
        }

        // add the element to the current split dimension 0/1 vector / file
        if (currentSplitInRam || workInRam) {
          std::get<multiBoxWithOrderIndex*>(currentList)->push_back(element);
        } else {
          FileReader::SaveEntryWithOrderIndex(
              element, *(std::get<std::ofstream*>(currentList)));
        }

        // check if the element is at the position i * S (described in the
        // algorithm) or one before it. In this case it is a future possible
        // split position and needs to be saved to the "small list"
        if (BasicGeometry::IsBorderOfSplitCandidate(current, currentSplitSize,
                                                    M)) {
          // index i * S - 1 or i * S
          currentSmallList->push_back(element);
        }

        // update the boundingbox to get the whole boundingbox of the split
        if (dim == 0) {
          if (current == 0) {
            box = element.box;
          } else {
            box = BasicGeometry::CombineBoundingBoxes(box, element.box);
          }
        }

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

    if (!workInRam_) {
      if (dim == 0 && fileReaderDim0Iterator != FileReader::end())
        elementOpt = *fileReaderDim0Iterator;
      if (dim == 1 && fileReaderDim1Iterator != FileReader::end())
        elementOpt = *fileReaderDim1Iterator;
    }

    while ((workInRam_ && i < size_) ||
           (!workInRam_ && elementOpt)) {
      RTreeValueWithOrderIndex element;

      // get the current element, either from disk or from ram
      if (workInRam_) {
        element =
            dim == 0
                ? std::get<multiBoxWithOrderIndex>(rectsD0_.rectangles)[i]
                : std::get<multiBoxWithOrderIndex>(
                      rectsD1_.rectangles)[i];
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
                              maxSplit0OtherDim, split0InRam, workInRam_,
                              element, boxSplit0);
        } else {
          performCertainSplit(1, 0, currentYSplit0, SSplit0, minSplit0OtherDim,
                              maxSplit0OtherDim, split0InRam, workInRam_,
                              element, boxSplit0);
        }
      } else {
        // the element belongs to split 1

        if (dim == 0) {
          performCertainSplit(0, 1, currentXSplit1, SSplit1, minSplit1OtherDim,
                              maxSplit1OtherDim, split1InRam, workInRam_,
                              element, boxSplit1);
        } else {
          performCertainSplit(1, 1, currentYSplit1, SSplit1, minSplit1OtherDim,
                              maxSplit1OtherDim, split1InRam, workInRam_,
                              element, boxSplit1);
        }
      }
      i++;

      if (!workInRam_) {
        if (dim == 0 && ++fileReaderDim0Iterator != FileReader::end()) {
          elementOpt = *fileReaderDim0Iterator;
          continue;
        }
        if (dim == 1 && ++fileReaderDim1Iterator != FileReader::end()) {
          elementOpt = *fileReaderDim1Iterator;
          continue;
        }
      }
      elementOpt = {};
    }
  }

  // replace the placeholder
  (*otherDimension.smallSplit0)[0] = minSplit0OtherDim;
  (*otherDimension.smallSplit0)[1] = maxSplit0OtherDim;
  (*otherDimension.smallSplit1)[0] = minSplit1OtherDim;
  (*otherDimension.smallSplit1)[1] = maxSplit1OtherDim;

  Clear();

  return std::make_pair(boxSplit0, boxSplit1);
}