//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include <gtest/gtest.h>
#include <util/Rtree.h>
#include <util/RtreeBasicGeometry.h>
#include <util/RtreeFileReader.h>
#include <util/RtreeSorter.h>

bool boundingBoxesAreEqual(BasicGeometry::BoundingBox b1,
                           BasicGeometry::BoundingBox b2) {
  if (BasicGeometry::GetMinX(b1) != BasicGeometry::GetMinX(b2)) return false;
  if (BasicGeometry::GetMinY(b1) != BasicGeometry::GetMinY(b2)) return false;
  if (BasicGeometry::GetMaxX(b1) != BasicGeometry::GetMaxX(b2)) return false;
  if (BasicGeometry::GetMaxY(b1) != BasicGeometry::GetMaxY(b2)) return false;
  return true;
}

bool multiBoxGeosAreEqual(multiBoxGeo& m1, multiBoxGeo& m2) {
  if (m1.size() != m2.size()) return false;
  for (size_t i = 0; i < m1.size(); i++) {
    RTreeValue r1 = m1[i];
    RTreeValue r2 = m2[i];
    if (r1.id != r2.id) return false;
    if (!boundingBoxesAreEqual(r1.box, r2.box)) return false;
  }
  return true;
}

bool multiBoxGeosWithOrderIndexAreEqual(multiBoxWithOrderIndex& m1,
                                        multiBoxWithOrderIndex& m2) {
  if (m1.size() != m2.size()) return false;
  for (size_t i = 0; i < m1.size(); i++) {
    RTreeValueWithOrderIndex r1 = m1[i];
    RTreeValueWithOrderIndex r2 = m2[i];
    if (r1.id != r2.id) return false;
    if (!boundingBoxesAreEqual(r1.box, r2.box)) return false;
    if (r1.orderX != r2.orderX) return false;
    if (r1.orderY != r2.orderY) return false;
  }
  return true;
}

TEST(Rtree, ConvertWordToRtreeEntry) {
  std::string wkt1 =
      "\"POLYGON((0.0 0.0,0.0 0.0,0.0 0.0,0.0 0.0,0.0 "
      "0.0))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  std::string wkt2 =
      "\"MULTIPOLYGON(((-100 -100,0 0,50 50,75 75,100 100), (10 10,20 20,30 "
      "30)), ((0 0,-10.0 -10,-20 -20), (-5 -5,-7 "
      "-7)))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  std::string wkt3 =
      "\"LINESTRING(-120 -110,0.0 0.0,0.0 0.0,0.0 0.0,120.0 "
      "110.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  std::string wkt4 = "Invalid input";
  std::string wkt5 =
      "\"POLYGON((1 1,2 2,5 5), (1.1 1.1, 2 2, 3 "
      "3))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  std::string wkt6 =
      "\"MULTIPOLYGON(((-100 -100,0 0,50 50,75 75,100 100), (10 10,20 20,30 "
      "30)), ((-150 -140,-10.0 -10,160 170), (-5 -5,-7 "
      "-7)))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  std::optional<BasicGeometry::BoundingBox> result1 =
      BasicGeometry::ConvertWordToRtreeEntry(wkt1);
  std::optional<BasicGeometry::BoundingBox> result2 =
      BasicGeometry::ConvertWordToRtreeEntry(wkt2);
  std::optional<BasicGeometry::BoundingBox> result3 =
      BasicGeometry::ConvertWordToRtreeEntry(wkt3);
  std::optional<BasicGeometry::BoundingBox> result4 =
      BasicGeometry::ConvertWordToRtreeEntry(wkt4);
  std::optional<BasicGeometry::BoundingBox> result5 =
      BasicGeometry::ConvertWordToRtreeEntry(wkt5);
  std::optional<BasicGeometry::BoundingBox> result6 =
      BasicGeometry::ConvertWordToRtreeEntry(wkt6);
  ASSERT_TRUE(result1);
  ASSERT_TRUE(boundingBoxesAreEqual(
      result1.value(), BasicGeometry::CreateBoundingBox(0, 0, 0, 0)));
  ASSERT_TRUE(result2);
  ASSERT_TRUE(boundingBoxesAreEqual(
      result2.value(), BasicGeometry::CreateBoundingBox(-100, -100, 100, 100)));
  ASSERT_TRUE(result3);
  ASSERT_TRUE(boundingBoxesAreEqual(
      result3.value(), BasicGeometry::CreateBoundingBox(-120, -110, 120, 110)));
  ASSERT_TRUE(!result4);
  ASSERT_TRUE(result5);
  ASSERT_TRUE(boundingBoxesAreEqual(
      result5.value(), BasicGeometry::CreateBoundingBox(1, 1, 5, 5)));
  ASSERT_TRUE(result6);
  ASSERT_TRUE(boundingBoxesAreEqual(
      result6.value(), BasicGeometry::CreateBoundingBox(-150, -140, 160, 170)));
}

TEST(Rtree, IsBorderOfSplitCandidate) {
  ASSERT_TRUE(BasicGeometry::IsBorderOfSplitCandidate(16, 16, 16));
  ASSERT_TRUE(BasicGeometry::IsBorderOfSplitCandidate(15, 16, 16));
  ASSERT_TRUE(!BasicGeometry::IsBorderOfSplitCandidate(17, 16, 16));
  ASSERT_TRUE(!BasicGeometry::IsBorderOfSplitCandidate(3185, 200, 16));
  ASSERT_TRUE(!BasicGeometry::IsBorderOfSplitCandidate(3184, 200, 16));
  ASSERT_TRUE(BasicGeometry::IsBorderOfSplitCandidate(3000, 200, 16));
  ASSERT_TRUE(BasicGeometry::IsBorderOfSplitCandidate(2999, 200, 16));
  ASSERT_TRUE(BasicGeometry::IsBorderOfSplitCandidate(200, 200, 16));
  ASSERT_TRUE(BasicGeometry::IsBorderOfSplitCandidate(199, 200, 16));
}

TEST(Rtree, CreateBoundingBox) {
  BasicGeometry::Point p1 = {-1, -2};
  BasicGeometry::Point p2 = {3, 4};
  BasicGeometry::BoundingBox b = {p1, p2};
  ASSERT_TRUE(
      boundingBoxesAreEqual(b, BasicGeometry::CreateBoundingBox(-1, -2, 3, 4)));
}

TEST(Rtree, CombineBoundingBoxes) {
  BasicGeometry::BoundingBox b1 = BasicGeometry::CreateBoundingBox(0, 0, 0, 0);
  BasicGeometry::BoundingBox b2 = BasicGeometry::CreateBoundingBox(1, 2, 3, 4);
  BasicGeometry::BoundingBox b3 =
      BasicGeometry::CreateBoundingBox(-1, -2, -3, -4);
  ASSERT_TRUE(
      boundingBoxesAreEqual(BasicGeometry::CombineBoundingBoxes(b1, b2),
                            BasicGeometry::CreateBoundingBox(0, 0, 3, 4)));
  ASSERT_TRUE(
      boundingBoxesAreEqual(BasicGeometry::CombineBoundingBoxes(b2, b1),
                            BasicGeometry::CreateBoundingBox(0, 0, 3, 4)));
  ASSERT_TRUE(
      boundingBoxesAreEqual(BasicGeometry::CombineBoundingBoxes(b1, b3),
                            BasicGeometry::CreateBoundingBox(-1, -2, 0, 0)));
  ASSERT_TRUE(
      boundingBoxesAreEqual(BasicGeometry::CombineBoundingBoxes(b3, b1),
                            BasicGeometry::CreateBoundingBox(-1, -2, 0, 0)));
  BasicGeometry::BoundingBox b4 =
      BasicGeometry::CreateBoundingBox(-150.0, 30.4, -70.0, 50);
  BasicGeometry::BoundingBox b5 =
      BasicGeometry::CreateBoundingBox(5.0, -30.4, 10.0, 20);
  ASSERT_TRUE(boundingBoxesAreEqual(
      BasicGeometry::CombineBoundingBoxes(b4, b5),
      BasicGeometry::CreateBoundingBox(-150, -30.4, 10, 50)));
}

TEST(Rtree, SaveAndLoadEntry) {
  multiBoxGeo boxes1 = multiBoxGeo();
  boxes1.push_back(RTreeValue(BasicGeometry::CreateBoundingBox(0, 0, 0, 0), 1));
  std::filesystem::path path1 = "RtreeTest_SaveEntry1";
  std::ofstream ofs1 = std::ofstream("RtreeTest_SaveEntry1", std::ios::binary);
  for (RTreeValue element : boxes1) {
    FileReaderWithoutIndex::SaveEntry(element.box, element.id, ofs1);
  }
  ofs1.close();
  multiBoxGeo boxes2 =
      FileReaderWithoutIndex::LoadEntries(std::filesystem::absolute(path1));
  ASSERT_TRUE(multiBoxGeosAreEqual(boxes1, boxes2));
}
