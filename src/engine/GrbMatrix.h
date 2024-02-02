// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.roland.herrmann@mars.uni-freiburg.de)

#pragma once

extern "C" {
#include <GraphBLAS.h>
}

#include <memory>
#include <vector>

/**
 * @class GrbMatrix
 * @brief This class wraps the functionality of the GraphBLAS object GrB_Matrix.
 * Currently only boolean matrices are supported.
 */
class GrbMatrix {
 private:
  std::unique_ptr<GrB_Matrix> matrix_ = std::make_unique<GrB_Matrix>();
  static bool isInitialized_;

 public:
  /**
   * @brief Construct a matrix with the given dimensions
   *
   * @param numRows
   * @param numCols
   */
  GrbMatrix(size_t numRows, size_t numCols);

  GrbMatrix() = default;

  // Move constructor
  GrbMatrix(GrbMatrix&& otherMatrix) = default;

  // Disable copy constructor and assignment operator
  GrbMatrix(const GrbMatrix&) = delete;
  GrbMatrix& operator=(const GrbMatrix&) = delete;

  ~GrbMatrix() { GrB_Matrix_free(matrix_.get()); }

  /**
   * @brief Create a matrix and fill it with the data of this matrix.
   *
   * @return GrbMatrix duplicate matrix
   */
  GrbMatrix clone() const;

  /**
   * @brief Set an element in the matrix to a specified value.
   *
   * @param row Row index, must be smaller than numRows()
   * @param col Column index, must be smaller than numCols()
   * @param value Boolean, which value to set
   */
  void setElement(size_t row, size_t col, bool value);

  /**
   * @brief Get an element from the matrix.
   *
   * @param row Row index, must be smaller than numRows()
   * @param col Column index, must be smaller than numCols()
   * @return Boolean value
   */
  bool getElement(size_t row, size_t col) const;

  /**
   * @brief Create a matrix from the given lists of indices. For each given pair
   * of indices, the corresponding entry in the result matrix is set to true.
   * All other entries are false (by default).
   * The vectors rowIndices and colIndices have to be the same length. Their
   * entries have to be smaller than numRows and numCols respectively.
   *
   * @param rowIndices Vector of row indices, entries must be smaller than
   * numRows
   * @param colIndices Vector of column indices, entries must be smaller than
   * numCols
   * @param numRows Number of rows of the result matrix
   * @param numCols Number of columns of the result matrix
   * @return New matrix with given entries set to true
   */
  static GrbMatrix build(const std::vector<size_t>& rowIndices,
                         const std::vector<size_t>& colIndices, size_t numRows,
                         size_t numCols);

  /**
   * @brief Create a square, diagonal matrix. All entries on the diagonal are
   * set to true, all others to false. The resulting matrix will have nvals rows
   * and columns.
   *
   * @param nvals
   * @return
   */
  static GrbMatrix diag(size_t nvals);

  /**
   * @brief Extract all true entries from the matrix. The first entry in the
   * pair is the row index, the second entry is the column index.
   */
  std::vector<std::pair<size_t, size_t>> extractTuples() const;

  /**
   * @brief Extract a column from the matrix. Returns all row indices where this
   * column's entries are true.
   *
   * @param colIndex
   */
  std::vector<size_t> extractColumn(size_t colIndex) const;

  /**
   * @brief Extract a row from the matrix. Returns all column indices where this
   * rows's entries are true.
   *
   * @param rowIndex
   */
  std::vector<size_t> extractRow(size_t rowIndex) const;

  /**
   * @brief Number of "true" values in the matrix.
   *
   * @return
   */
  size_t numNonZero() const;

  /**
   * @brief Number of rows of the matrix.
   *
   * @return
   */
  size_t numRows() const;

  /**
   * @brief Number of columns of the matrix.
   *
   * @return
   */
  size_t numCols() const;

  /**
   * @brief Create a new matrix, which is the transpose of this matrix.
   *
   * @return
   */
  GrbMatrix transpose() const;

  /**
   * @brief Multiply this matrix with the other matrix and accumulate the result
   * in this matrix. Logical or is used for accumulation.
   *
   * @param otherMatrix
   */
  void accumulateMultiply(const GrbMatrix& otherMatrix) const;

  /**
   * @brief Multiply this matrix with another matrix and write the result to a
   * new matrix.
   *
   * @param otherMatrix
   * @return
   */
  GrbMatrix multiply(const GrbMatrix& otherMatrix) const;

  // TODO: Move to singleton class
  static void initialize();
  static void finalize();

 private:
  /**
   * @brief Get a reference to the internal matrix.
   *
   * @return
   */
  GrB_Matrix& matrix() const { return *matrix_; }

  /**
   * @brief Get a raw pointer to the internal matrix. If this pointer is the
   * nullptr, an Exception is thrown.
   *
   * @return
   */
  GrB_Matrix* rawMatrix() const;

  /**
   * @brief Handle the GrB_Info object. GrB_SUCCESS is ignored, all other return
   * valus cause an Exception.
   * See also GraphBLAS userguide, section 5.5
   *
   * @param info
   */
  static void handleError(GrB_Info info);
};
