// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.roland.herrmann@mars.uni-freiburg.de)

#include <gtest/gtest.h>

#include "engine/GrbMatrix.h"
#include "gmock/gmock.h"

// This helper function checks all important proprties of a matrix.
// One matrix consists of row index, column index and value in this order.
// Entries which do not appear in the entries vector are ignored.
using Entries = std::vector<std::tuple<size_t, size_t, bool>>;
void checkMatrix(GrbMatrix& matrix, size_t numRows, size_t numCols,
                 size_t numNonZero, Entries entries) {
  EXPECT_THAT(matrix.numNonZero(), numNonZero);
  EXPECT_THAT(matrix.numRows(), numRows);
  EXPECT_THAT(matrix.numCols(), numCols);

  for (auto [rowIndex, colIndex, value] : entries) {
    EXPECT_THAT(matrix.getElement(rowIndex, colIndex), value);
  }
}

TEST(GrbMatrixTest, constructor) {
  GrbMatrix::initialize();

  GrbMatrix matrix = GrbMatrix(2, 3);

  checkMatrix(matrix, 2, 3, 0, {});

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, clone) {
  GrbMatrix::initialize();

  GrbMatrix matrix1 = GrbMatrix(2, 2);
  matrix1.setElement(0, 0, true);

  GrbMatrix matrix2 = matrix1.clone();

  matrix1.setElement(1, 1, true);

  checkMatrix(matrix2, 2, 2, 1,
              {{0, 0, true}, {0, 1, false}, {1, 0, false}, {1, 1, false}});

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, getSetElement) {
  GrbMatrix::initialize();

  GrbMatrix matrix = GrbMatrix(3, 3);
  matrix.setElement(1, 0, true);
  matrix.setElement(0, 2, true);

  checkMatrix(matrix, 3, 3, 2, {{1, 0, true}, {0, 2, true}});

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, build) {
  GrbMatrix::initialize();

  std::vector<size_t> rowIndices{0, 0, 1};
  std::vector<size_t> colIndices{1, 2, 2};

  GrbMatrix matrix = GrbMatrix::build(rowIndices, colIndices, 3, 3);

  checkMatrix(matrix, 3, 3, 3, {{0, 1, true}, {0, 2, true}, {1, 2, true}});

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, diag) {
  GrbMatrix::initialize();

  auto matrix = GrbMatrix::diag(3);

  checkMatrix(matrix, 3, 3, 3, {{0, 0, true}, {1, 1, true}, {2, 2, true}});

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

  checkMatrix(matrix3, 2, 2, 2, {{0, 0, true}, {1, 0, true}});

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

  checkMatrix(matrix3, 2, 2, 2, {{0, 0, true}, {1, 0, true}});

  GrbMatrix::finalize();
}

TEST(GrbMatrixTest, transpose) {
  GrbMatrix::initialize();

  auto matrix = GrbMatrix(2, 3);

  matrix.setElement(0, 0, true);
  matrix.setElement(0, 1, true);
  matrix.setElement(0, 2, true);

  GrbMatrix result = matrix.transpose();

  checkMatrix(result, 3, 2, 3, {{0, 0, true}, {1, 0, true}, {2, 0, true}});

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

  checkMatrix(matrix1, 2, 2, 4,
              {{0, 0, true}, {0, 1, true}, {1, 0, true}, {1, 1, true}});

  GrbMatrix::finalize();
}
