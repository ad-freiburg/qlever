//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include <util/BackgroundStxxlSorter.h>

#include "./RtreeFileReader.h"

template <size_t dimension>
struct SortRuleLambda {
  // comparison function
  bool operator()(const RTreeValue& b1, const RTreeValue& b2) const {
    double center1 = dimension == 0 ? std::midpoint(b1.MinX(), b1.MaxX())
                                    : std::midpoint(b1.MinY(), b1.MaxY());
    double center2 = dimension == 0 ? std::midpoint(b2.MinX(), b2.MaxX())
                                    : std::midpoint(b2.MinY(), b2.MaxY());
    return center1 < center2;
  }

  // Value that is strictly smaller than any input element.
  static RTreeValue min_value() {
    return {
        BasicGeometry::CreateBoundingBox(-std::numeric_limits<double>::max(),
                                         -std::numeric_limits<double>::max(),
                                         -std::numeric_limits<double>::max(),
                                         -std::numeric_limits<double>::max()),
        0};
  }

  // Value that is strictly larger than any input element.
  static RTreeValue max_value() {
    return {
        BasicGeometry::CreateBoundingBox(std::numeric_limits<double>::max(),
                                         std::numeric_limits<double>::max(),
                                         std::numeric_limits<double>::max(),
                                         std::numeric_limits<double>::max()),
        0};
  }
};

template <size_t dimension>
struct SortRuleLambdaWithIndex {
  uint64_t RTreeValueWithOrderIndex::*orderSelected =
      dimension == 0 ? &RTreeValueWithOrderIndex::orderX
                     : &RTreeValueWithOrderIndex::orderY;

  // comparison function
  bool operator()(const RTreeValueWithOrderIndex& b1,
                  const RTreeValueWithOrderIndex& b2) const {
    double center1 = dimension == 0 ? std::midpoint(b1.MinX(), b1.MaxX())
                                    : std::midpoint(b1.MinY(), b1.MaxY());
    double center2 = dimension == 0 ? std::midpoint(b2.MinX(), b2.MaxX())
                                    : std::midpoint(b2.MinY(), b2.MaxY());

    if (b1.*orderSelected == b2.*orderSelected) return center1 < center2;
    return b1.*orderSelected < b2.*orderSelected;
  }

  // Value that is strictly smaller than any input element.
  static RTreeValueWithOrderIndex min_value() {
    return {
        {BasicGeometry::CreateBoundingBox(-std::numeric_limits<double>::max(),
                                          -std::numeric_limits<double>::max(),
                                          -std::numeric_limits<double>::max(),
                                          -std::numeric_limits<double>::max()),
         0},
        0,
        0};
  }

  // Value that is strictly larger than any input element.
  static RTreeValueWithOrderIndex max_value() {
    return {
        {BasicGeometry::CreateBoundingBox(std::numeric_limits<double>::max(),
                                          std::numeric_limits<double>::max(),
                                          std::numeric_limits<double>::max(),
                                          std::numeric_limits<double>::max()),
         0},
        std::numeric_limits<long long>::max(),
        std::numeric_limits<long long>::max()};
  }
};

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

RTreeValueWithOrderIndex handleD0ElementsWithoutIndex(RTreeValue& element, uint64_t i, BasicGeometry::BoundingBox& boundingBox) {
  RTreeValueWithOrderIndex entry = {{element.box, element.id}, i, 0};
  if (i == 0) {
    boundingBox = element.box;
  } else {
    boundingBox =
        BasicGeometry::CombineBoundingBoxes(boundingBox, element.box);  // TODO
  }
  return entry;
}

void handleD1Elements(RTreeValueWithOrderIndex& element, uint64_t i, uint64_t S, size_t M, multiBoxWithOrderIndex& r1Small) {
  element.orderY = i;

  if (BasicGeometry::IsBorderOfSplitCandidate(i, S, M)) {
    // index i * S - 1 or i * S
    r1Small.push_back(element);
  }
}

void handleD0Elements(RTreeValueWithOrderIndex& element, uint64_t i, uint64_t S, size_t M, multiBoxWithOrderIndex& r0Small) {
  if (BasicGeometry::IsBorderOfSplitCandidate(i, S, M)) {
    // index i * S - 1 or i * S
    r0Small.push_back(element);
  }
}

OrderedBoxes InternalSort(const std::string& onDiskBase, const std::string& fileSuffix, size_t M) {
  OrderedBoxes orderedInputRectangles;
  multiBoxGeo RectanglesD0 = FileReaderWithoutIndex::LoadEntries(onDiskBase + fileSuffix + ".tmp");
  centerOrdering(RectanglesD0, 0);

  size_t currentS = std::ceil(((float) RectanglesD0.size()) / ((float) M));
  if (RectanglesD0.size() <= M * M) {
    // in this case S can just be M
    currentS = M;
  }

  multiBoxWithOrderIndex R0Small = multiBoxWithOrderIndex();
  multiBoxWithOrderIndex R1Small = multiBoxWithOrderIndex();

  BasicGeometry::BoundingBox boundingBox = BasicGeometry::CreateBoundingBox(0, 0, 0, 0);
  multiBoxWithOrderIndex RectanglesD1WithOrder = multiBoxWithOrderIndex();
  for (uint64_t i = 0; i < RectanglesD0.size(); i++) {
    RTreeValueWithOrderIndex entry = handleD0ElementsWithoutIndex(RectanglesD0[i], i, boundingBox);
    RectanglesD1WithOrder.push_back(entry);
  }

  centerOrdering(RectanglesD1WithOrder, 1);

  R1Small.push_back((RectanglesD1WithOrder)[0]);
  RTreeValueWithOrderIndex maxElementDim1 = (RectanglesD1WithOrder)[RectanglesD1WithOrder.size() - 1];
  maxElementDim1.orderY = RectanglesD1WithOrder.size() - 1;
  R1Small.push_back(maxElementDim1);
  for (uint64_t i = 0; i < RectanglesD1WithOrder.size(); i++) {
    handleD1Elements(RectanglesD1WithOrder[i], i, currentS, M, R1Small);
  }

  multiBoxWithOrderIndex RectanglesD0WithOrder = multiBoxWithOrderIndex(RectanglesD1WithOrder);
  centerOrdering(RectanglesD0WithOrder, 0);

  R0Small.push_back((RectanglesD0WithOrder)[0]);
  RTreeValueWithOrderIndex maxElementDim0 = (RectanglesD0WithOrder)[RectanglesD0WithOrder.size() - 1];
  maxElementDim0.orderY = RectanglesD0WithOrder.size() - 1;
  R0Small.push_back(maxElementDim0);
  for (uint64_t i = 0; i < RectanglesD0WithOrder.size(); i++) {
    handleD0Elements(RectanglesD0WithOrder[i], i, currentS, M, R0Small);
  }

  RectanglesForOrderedBoxes d0WithOrder;
  d0WithOrder.rectangles = RectanglesD0WithOrder;
  d0WithOrder.rectanglesSmall = R0Small;
  RectanglesForOrderedBoxes d1WithOrder;
  d1WithOrder.rectangles = RectanglesD1WithOrder;
  d1WithOrder.rectanglesSmall = R1Small;
  orderedInputRectangles.SetOrderedBoxesToRam(d0WithOrder, d1WithOrder, boundingBox);
  return orderedInputRectangles;
}

OrderedBoxes ExternalSort(const std::string& onDiskBase,
                          const std::string& fileSuffix, size_t M,
                          uintmax_t maxBuildingRamUsage) {
  OrderedBoxes orderedInputRectangles;
  std::filesystem::path file = onDiskBase + fileSuffix + ".tmp";

  auto maxRamForSorter = std::ceil(std::min((double)maxBuildingRamUsage / 3.0, 9999999999.0 / 3.0));
  ad_utility::BackgroundStxxlSorter<RTreeValue, SortRuleLambda<0>> sorterRectsD0Basic =
      ad_utility::BackgroundStxxlSorter<RTreeValue, SortRuleLambda<0>>((size_t)maxRamForSorter);;

  for (const RTreeValue& rectD0Element : FileReaderWithoutIndex(file)) {
    sorterRectsD0Basic.push(rectD0Element);
  }

  uint64_t xSize = 0;
  BasicGeometry::BoundingBox boundingBox =
      BasicGeometry::CreateBoundingBox(0, 0, 0, 0);

  ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex, SortRuleLambdaWithIndex<1>> sorterRectsD1 =
          ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex,
                                        SortRuleLambdaWithIndex<1>>((size_t)maxRamForSorter);

  for (RTreeValue element : sorterRectsD0Basic.sortedView()) {
    RTreeValueWithOrderIndex entry = handleD0ElementsWithoutIndex(element, xSize, boundingBox);
    sorterRectsD1.push(entry);
    xSize++;
  }
  sorterRectsD0Basic.clear();

  size_t currentS = std::ceil(((float)xSize) / ((float)M));
  if (xSize <= M * M) {
    // in this case S can just be M
    currentS = M;
  }

  uint64_t ySize = 0;
  std::ofstream r1File =
      std::ofstream(onDiskBase + fileSuffix + ".d1.tmp", std::ios::binary);
  ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex, SortRuleLambdaWithIndex<0>> sorterRectsD0 =
          ad_utility::BackgroundStxxlSorter<RTreeValueWithOrderIndex,
                                        SortRuleLambdaWithIndex<0>>((size_t)maxRamForSorter);
  multiBoxWithOrderIndex r1Small = multiBoxWithOrderIndex();
  // placeholder
  r1Small.emplace_back();
  r1Small.emplace_back();

  RTreeValueWithOrderIndex minD1;
  RTreeValueWithOrderIndex maxD1;
  for (RTreeValueWithOrderIndex element : sorterRectsD1.sortedView()) {
    handleD1Elements(element, ySize, currentS, M, r1Small);
    FileReader::SaveEntryWithOrderIndex(element, r1File);
    sorterRectsD0.push(element);
    if (ySize == 0) {
      minD1 = element;
    }

    maxD1 = element;
    ySize++;
  }

  r1File.close();
  sorterRectsD1.clear();

  // replace the placeholder
  r1Small[0] = minD1;
  r1Small[1] = maxD1;

  uint64_t currentX = 0;
  std::ofstream r0File =
      std::ofstream(onDiskBase + fileSuffix + ".d0.tmp", std::ios::binary);
  multiBoxWithOrderIndex r0Small = multiBoxWithOrderIndex();
  // placeholder
  r0Small.emplace_back();
  r0Small.emplace_back();

  RTreeValueWithOrderIndex minD0;
  RTreeValueWithOrderIndex maxD0;
  for (RTreeValueWithOrderIndex element : sorterRectsD0.sortedView()) {
    FileReader::SaveEntryWithOrderIndex(element, r0File);
    handleD0Elements(element, currentX, currentS, M, r0Small);
    if (currentX == 0) {
      minD0 = element;
    }
    maxD0 = element;
    currentX++;
  }

  r0File.close();
  sorterRectsD0.clear();

  // replace the placeholder
  r0Small[0] = minD0;
  r0Small[1] = maxD0;

  RectanglesForOrderedBoxes rectsD0;
  RectanglesForOrderedBoxes rectsD1;
  rectsD0.rectanglesSmall = std::move(r0Small);
  rectsD1.rectanglesSmall = std::move(r1Small);
  rectsD0.rectangles = onDiskBase + fileSuffix + ".d0.tmp";
  rectsD1.rectangles = onDiskBase + fileSuffix + ".d1.tmp";
  orderedInputRectangles.SetOrderedBoxesToDisk(rectsD0, rectsD1, xSize,
                                               boundingBox);
  return orderedInputRectangles;
}

OrderedBoxes SortInput(const std::string& onDiskBase,
                       const std::string& fileSuffix, size_t M,
                       uintmax_t maxBuildingRamUsage, bool workInRam) {
  if (workInRam) {
    return InternalSort(onDiskBase, fileSuffix, M);
  }
  return ExternalSort(onDiskBase, fileSuffix, M, maxBuildingRamUsage);
}

/*OrderedBoxes SortInput(const std::filesystem::path& onDiskBase, size_t M,
uintmax_t maxBuildingRamUsage, bool workInRam) { if (workInRam) { return
InternalSort(onDiskBase, M); } else { return ExternalSort(onDiskBase, M,
                        maxBuildingRamUsage);
  }
}*/
