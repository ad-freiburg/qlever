#pragma once

#include "engine/Operation.h"
#include "parser/ParsedQuery.h"
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/foreach.hpp>

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

typedef bg::model::point<double, 2, bg::cs::cartesian> point;
typedef bg::model::box<point> box;
typedef std::pair<point, size_t> value;

// This class is implementing a SpatialJoin operation. This operations joins
// two tables, using their positional column. If the distance of the two
// positions is less than a given maximum distance, the pair will be in the
// result table.
class SpatialJoin : public Operation {
 public:
  // creates a SpatialJoin operation. The triple is needed, to get the
  // variable names. Those are the names of the children, which get added
  // later. In addition to that, the SpatialJoin operation needs a maximum
  // distance, which two objects can be apart, which will still be accepted
  // as a match and therefore be part of the result table. The distance is
  // parsed from the triple.
  SpatialJoin(QueryExecutionContext* qec, SparqlTriple triple,
              std::optional<std::shared_ptr<QueryExecutionTree>> childLeft_,
              std::optional<std::shared_ptr<QueryExecutionTree>> childRight_);
  std::vector<QueryExecutionTree*> getChildren() override;
  string getCacheKeyImpl() const override;
  string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;
  uint64_t getSizeEstimateBeforeLimit() override;

  // this function assumes, that the complete cross product is build and
  // returned. If the SpatialJoin does not have both children yet, it just
  // returns one as a dummy return. As no column gets joined in the
  // SpatialJoin (both point columns stay in the result table in their row)
  // each row can have at most the same number of distinct elements as it had
  // before. If the size increases, the multiplicity increases. The assumption
  // is, that the distinctness doesn't change and only the multiplicity
  // changes.
  float getMultiplicity(size_t col) override;

  bool knownEmptyResult() override;
  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;
  Result computeResult(bool requestLaziness) override;

  // Depending on the amount of children the operation returns a different
  // VariableToColumnMap. If the operation doesn't have both children it needs
  // to aggressively push the queryplanner to add the children, because the
  // operation can't exist without them. If it has both children, it can
  // return the variable to column map, which will be present, after the
  // operation has computed its result
  VariableToColumnMap computeVariableToColumnMap() const override;

  // this method creates a new SpatialJoin object, to which the child gets
  // added. The reason for this behavior is, that the QueryPlanner can then
  // still use the existing SpatialJoin object, to try different orders
  std::shared_ptr<SpatialJoin> addChild(
      std::shared_ptr<QueryExecutionTree> child,
      const Variable& varOfChild) const;

  // if the spatialJoin has both children its construction is done. Then true
  // is returned. This function is needed in the QueryPlanner, so that the
  // QueryPlanner stops trying to add children, after the SpatialJoin is
  // already constructed
  bool isConstructed() const;

  // this function is used to give the maximum distance for testing purposes
  long long getMaxDist() const;

  std::shared_ptr<QueryExecutionTree> onlyForTestingGetLeftChild() const {
    return childLeft_;
  }

  std::shared_ptr<QueryExecutionTree> onlyForTestingGetRightChild() const {
    return childRight_;
  }

  void onlyForTestingSetAddDistToResult(bool addDistToResult) {
    addDistToResult_ = addDistToResult;
  }

  void onlyForTestingSetUseBaselineAlgorithm(bool useBaselineAlgorithm) {
    useBaselineAlgorithm_ = useBaselineAlgorithm;
  }

  const string& getInternalDistanceName() const {
    return nameDistanceInternal_;
  }

  // this function computes the bounding box(es), which represent all points, which
  // are in reach of the starting point with a distance of at most maxDistanceInMeters
  std::vector<box> computeBoundingBox(const point& startPoint);

  // this function returns true, when the given point is contained in any of the
  // bounding boxes
  bool containedInBoundingBoxes(const std::vector<box>& bbox, point point1);

 private:
  // helper function, which parses the max distance triple into a long long
  // distance
  void parseMaxDistance();

  // helper function which gets the coordinates from the coordinates string
  // (usually the object of a triple)
  std::string betweenQuotes(std::string extractFrom) const;

  // helper function, which gets the columnIndex, of a "join" variable
  ColumnIndex getJoinCol(const std::shared_ptr<const QueryExecutionTree>& child,
                       const Variable& childVariable);

  // this helper function calculates the bounding boxes based on a box, where
  // definetly no match can occur. This function gets used, when the usual
  // procedure, would just result in taking a big bounding box, which covers
  // the whole planet (so for large max distances)
  std::vector<box> computeAntiBoundingBox(const point& startPoint);

  // helper function, which gets the point of an id table
  std::optional<GeoPoint> getPoint(const IdTable* restable, size_t row,
                      ColumnIndex col) const;

  // helper function, which computes the distance of two points, where each
  // point comes from a different result table
  Id computeDist(const IdTable* resLeft, const IdTable* resRight,
                 size_t rowLeft, size_t rowRight, ColumnIndex leftPointCol,
                 ColumnIndex rightPointCol) const;

  // Helper function, which adds a row, which belongs to the result to the
  // result table. As inputs it uses a row of the left and a row of the right
  // child result table.
  void addResultTableEntry(IdTable* result, const IdTable* resultLeft,
                           const IdTable* resultRight, size_t rowLeft,
                           size_t rowRight, Id distance) const;

  // the baseline algorithm, which just checks every combination
  Result baselineAlgorithm();

  // the advanced algorithm, which uses bounding boxes
  Result boundingBoxAlgorithm();

  SparqlTriple triple_;
  Variable leftChildVariable_;
  Variable rightChildVariable_;
  std::shared_ptr<QueryExecutionTree> childLeft_ = nullptr;
  std::shared_ptr<QueryExecutionTree> childRight_ = nullptr;
  long long maxDist_ = 0;  // max distance in meters

  // adds an extra column to the result, which contains the actual distance,
  // between the two objects
  bool addDistToResult_ = true;
  const string nameDistanceInternal_ = "?distOfTheTwoObjectsAddedInternally";
  bool useBaselineAlgorithm_ = false;
  // circumference in meters at the equator (max) and the pole (min) (as the earth is not exactly a
  // sphere the circumference is different. Note the factor of 1000 to convert to meters)
  static constexpr double circumferenceMax_ = 40075 * 1000;
  static constexpr double circumferenceMin_ = 40007 * 1000;
  // radius of the earth in meters (as the earth is not exactly a sphere the
  // radius at the equator has been taken)
  static constexpr double radius_ = 6378 * 1000;  // * 1000 to convert to meters
};
