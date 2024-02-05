#include <GraphBLAS.h>
#include <gtest/gtest.h>

#include <cstdio>

#include "engine/GrbMatrix.h"
#include "gmock/gmock.h"

TEST(GrbMatrixTest, constructor) {
  GrbMatrix::initialize();

  GrbMatrix matrix = GrbMatrix(2, 3);
  size_t numRows = matrix.numRows();
  size_t numCols = matrix.numCols();
  size_t nvals = matrix.numNonZero();

  GrbMatrix::finalize();

  EXPECT_EQ(nvals, 0);
  EXPECT_EQ(numRows, 2);
  EXPECT_EQ(numCols, 3);
}

TEST(GrbMatrixTest, copy) {
  GrbMatrix::initialize();

  GrbMatrix matrix1 = GrbMatrix(2, 2);
  matrix1.setElement(0, 0, true);

  GrbMatrix matrix2 = matrix1.clone();

  matrix1.setElement(1, 1, true);

  EXPECT_EQ(matrix2.getElement(0, 0), true);
  EXPECT_EQ(matrix2.getElement(0, 1), false);
  EXPECT_EQ(matrix2.getElement(1, 0), false);
  EXPECT_EQ(matrix2.getElement(1, 1), false);

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, getSetElement) {
  GrbMatrix::initialize();

  GrbMatrix matrix = GrbMatrix(3, 3);
  matrix.setElement(1, 0, true);
  matrix.setElement(0, 2, true);

  bool elemOneZero = matrix.getElement(1, 0);
  bool elemZeroTwo = matrix.getElement(0, 2);
  bool elemOneTwo = matrix.getElement(1, 2);
  size_t nvals = matrix.numNonZero();

  GrbMatrix::finalize();

  EXPECT_EQ(nvals, 2);
  EXPECT_EQ(elemOneZero, true);
  EXPECT_EQ(elemZeroTwo, true);
  EXPECT_EQ(elemOneTwo, false);
}

TEST(GrbMatrixTest, build) {
  GrbMatrix::initialize();

  std::vector<size_t> rowIndices{0, 0, 1};
  std::vector<size_t> colIndices{1, 2, 2};

  GrbMatrix matrix = GrbMatrix::build(rowIndices, colIndices, 3, 3);

  EXPECT_EQ(false, matrix.getElement(0, 0));
  EXPECT_EQ(true, matrix.getElement(0, 1));
  EXPECT_EQ(true, matrix.getElement(0, 2));

  EXPECT_EQ(false, matrix.getElement(1, 0));
  EXPECT_EQ(false, matrix.getElement(1, 1));
  EXPECT_EQ(true, matrix.getElement(1, 2));

  EXPECT_EQ(false, matrix.getElement(2, 0));
  EXPECT_EQ(false, matrix.getElement(2, 1));
  EXPECT_EQ(false, matrix.getElement(2, 2));

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, diag) {
  GrbMatrix::initialize();

  auto matrix = GrbMatrix::diag(3);

  EXPECT_EQ(true, matrix.getElement(0, 0));
  EXPECT_EQ(false, matrix.getElement(0, 1));
  EXPECT_EQ(false, matrix.getElement(0, 2));

  EXPECT_EQ(false, matrix.getElement(1, 0));
  EXPECT_EQ(true, matrix.getElement(1, 1));
  EXPECT_EQ(false, matrix.getElement(1, 2));

  EXPECT_EQ(false, matrix.getElement(2, 0));
  EXPECT_EQ(false, matrix.getElement(2, 1));
  EXPECT_EQ(true, matrix.getElement(2, 2));

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, extractTuples) {
  GrbMatrix::initialize();

  GrbMatrix matrix = GrbMatrix(3, 3);

  matrix.setElement(0, 1, true);
  matrix.setElement(0, 2, true);
  matrix.setElement(1, 2, true);

  auto [rowIndices, colIndices] = matrix.extractTuples();

  GrbMatrix::finalize();

  std::vector<size_t> expectedRowIndices{0, 0, 1};
  std::vector<size_t> expectedColIndices{1, 2, 2};
  auto expected = {expectedRowIndices, expectedColIndices};
  auto got = {rowIndices, colIndices};

  EXPECT_THAT(got, testing::UnorderedElementsAreArray(expected));
}

TEST(GrbMatrixTest, extractColumn) {
  GrbMatrix::initialize();

  GrbMatrix matrix = GrbMatrix(3, 3);

  matrix.setElement(0, 1, true);
  matrix.setElement(2, 1, true);

  std::vector<size_t> colIndices = matrix.extractColumn(1);

  GrbMatrix::finalize();

  std::vector<size_t> expected{0, 2};

  EXPECT_THAT(colIndices, testing::UnorderedElementsAreArray(expected));
}

TEST(GrbMatrixTest, extractRow) {
  GrbMatrix::initialize();

  GrbMatrix matrix = GrbMatrix(3, 3);

  matrix.setElement(1, 0, true);
  matrix.setElement(1, 2, true);

  std::vector<size_t> rowIndices = matrix.extractRow(1);

  GrbMatrix::finalize();

  std::vector<size_t> expected{0, 2};

  EXPECT_THAT(rowIndices, testing::UnorderedElementsAreArray(expected));
}

TEST(GrbMatrixTest, multiplySquareMatrices) {
  GrbMatrix::initialize();

  GrbMatrix matrix1 = GrbMatrix(2, 2);
  matrix1.setElement(0, 0, true);
  matrix1.setElement(1, 1, true);

  GrbMatrix matrix2 = GrbMatrix(2, 2);
  matrix2.setElement(0, 0, true);
  matrix2.setElement(1, 0, true);

  GrbMatrix matrix3 = matrix1.multiply(matrix2);

  EXPECT_EQ(matrix3.getElement(0, 0), true);
  EXPECT_EQ(matrix3.getElement(0, 1), false);
  EXPECT_EQ(matrix3.getElement(1, 0), true);
  EXPECT_EQ(matrix3.getElement(1, 1), false);

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, multiplyShapedMatrices) {
  GrbMatrix::initialize();

  GrbMatrix matrix1 = GrbMatrix(2, 3);
  matrix1.setElement(0, 0, true);
  matrix1.setElement(1, 1, true);

  GrbMatrix matrix2 = GrbMatrix(3, 2);
  matrix2.setElement(0, 0, true);
  matrix2.setElement(1, 0, true);
  matrix2.setElement(2, 0, true);

  GrbMatrix matrix3 = matrix1.multiply(matrix2);

  EXPECT_EQ(matrix3.numRows(), 2);
  EXPECT_EQ(matrix3.numCols(), 2);
  EXPECT_EQ(matrix3.getElement(0, 0), true);
  EXPECT_EQ(matrix3.getElement(0, 1), false);
  EXPECT_EQ(matrix3.getElement(1, 0), true);
  EXPECT_EQ(matrix3.getElement(1, 1), false);

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, transpose) {
  GrbMatrix::initialize();

  auto matrix = GrbMatrix(2, 3);

  matrix.setElement(0, 0, true);
  matrix.setElement(0, 1, true);
  matrix.setElement(0, 2, true);

  GrbMatrix result = matrix.transpose();

  EXPECT_EQ(3, result.numRows());
  EXPECT_EQ(2, result.numCols());

  EXPECT_EQ(true, result.getElement(0, 0));
  EXPECT_EQ(false, result.getElement(0, 1));

  EXPECT_EQ(true, result.getElement(1, 0));
  EXPECT_EQ(false, result.getElement(1, 1));

  EXPECT_EQ(true, result.getElement(2, 0));
  EXPECT_EQ(false, result.getElement(2, 1));

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, accumulateMultiply) {
  GrbMatrix::initialize();

  GrbMatrix matrix1 = GrbMatrix(2, 2);
  matrix1.setElement(0, 0, true);
  matrix1.setElement(1, 1, true);

  GrbMatrix matrix2 = GrbMatrix(2, 2);
  matrix2.setElement(0, 1, true);
  matrix2.setElement(1, 0, true);

  matrix1.accumulateMultiply(matrix2);

  EXPECT_EQ(matrix1.getElement(0, 0), true);
  EXPECT_EQ(matrix1.getElement(0, 1), true);
  EXPECT_EQ(matrix1.getElement(1, 0), true);
  EXPECT_EQ(matrix1.getElement(1, 1), true);

  GrbMatrix::finalize();
}
