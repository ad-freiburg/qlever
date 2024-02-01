// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.roland.herrmann@mars.uni-freiburg.de)

#pragma once

extern "C" {
#include <GraphBLAS.h>
}

#include <memory>
#include <vector>

// This class wraps the functionality of the GraphBLAS object GrB_Matrix.
// Currently only boolean matrices are supported.
class GrbMatrix {
 private:
  std::unique_ptr<GrB_Matrix> matrix_ = std::make_unique<GrB_Matrix>();
  static bool isInitialized_;

 public:
  GrbMatrix(size_t numRows, size_t numCols);

  GrbMatrix() = default;

  // Move constructor
  GrbMatrix(GrbMatrix&& otherMatrix) = default;

  // Disable copy constructor and assignment operator
  GrbMatrix(const GrbMatrix&) = delete;
  GrbMatrix& operator=(const GrbMatrix&) = delete;

  ~GrbMatrix() { GrB_Matrix_free(matrix_.get()); }

  GrbMatrix clone() const;

  void setElement(size_t row, size_t col, bool value);

  bool getElement(size_t row, size_t col) const;

  // Create a matrix from the given lists of indices.
  // For each given pair of indices, the corresponding entry in the result
  // matrix is set to true. All other entries are false (by default).
  static GrbMatrix build(const std::vector<size_t> rowIndices,
                         const std::vector<size_t> colIndices, size_t numRows,
                         size_t numCols);

  // Create a square, diagonal matrix. All entries on the diagonal are set to
  // true, all others to false. The resulting matrix will have nvals rows and
  // columns.
  static GrbMatrix diag(size_t nvals);

  // Extract all true entries from the matrix. The first entry in the pair is
  // the row index, the second entry is the column index.
  std::vector<std::pair<size_t, size_t>> extractTuples() const;

  // Extract a column from the matrix. Returns all row indices where this
  // column's entries are true.
  std::vector<size_t> extractColumn(size_t colIndex) const;

  // Extract a row from the matrix. Returns all column indices where this
  // rows's entries are true.
  std::vector<size_t> extractRow(size_t rowIndex) const;

  // Number of "true" values in the matrix.
  size_t numNonZero() const;

  size_t numRows() const;

  size_t numCols() const;

  GrbMatrix transpose() const;

  // Multiply this matrix with the other matrix and accumulate the result in
  // this matrix. Logical or is used for accumulation.
  void accumulateMultiply(const GrbMatrix& otherMatrix) const;

  // Multiply this matrix with another matrix and write the result to a new
  // matrix.
  GrbMatrix multiply(const GrbMatrix& otherMatrix) const;

  static void initialize();

  static void finalize();

 private:
  GrB_Matrix& matrix() const { return *matrix_; }
  GrB_Matrix* rawMatrix() const;
  static void handleError(GrB_Info info);
};
