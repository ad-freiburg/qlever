// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.roland.herrmann@mars.uni-freiburg.de)

#include "GrbMatrix.h"

#include <GraphBLAS.h>

#include <memory>
#include <vector>

#include "util/Exception.h"

// _____________________________________________________________________________
GrbMatrix::GrbMatrix(size_t numRows, size_t numCols) {
  auto info = GrB_Matrix_new(rawMatrix(), GrB_BOOL, numRows, numCols);
  handleError(info);
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::clone() const {
  GrbMatrix matrixCopy = GrbMatrix();
  auto info =
      GrB_Matrix_new(matrixCopy.rawMatrix(), GrB_BOOL, numRows(), numCols());
  handleError(info);
  info = GrB_Matrix_dup(matrixCopy.rawMatrix(), matrix());
  handleError(info);

  return matrixCopy;
}

// _____________________________________________________________________________
void GrbMatrix::setElement(size_t row, size_t col, bool value) {
  auto info = GrB_Matrix_setElement_BOOL(matrix(), value, row, col);
  handleError(info);
}

// _____________________________________________________________________________
bool GrbMatrix::getElement(size_t row, size_t col) const {
  bool result;
  auto info = GrB_Matrix_extractElement_BOOL(&result, matrix(), row, col);
  handleError(info);
  if (info == GrB_NO_VALUE) {
    return false;
  }
  return result;
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::build(const std::vector<size_t>& rowIndices,
                           const std::vector<size_t>& colIndices,
                           size_t numRows, size_t numCols) {
  auto matrix = GrbMatrix(numRows, numCols);
  GrB_Index nvals = rowIndices.size();
  if (nvals == 0) {
    return matrix;
  }

  bool values[nvals];
  for (size_t i = 0; i < nvals; i++) {
    values[i] = true;
  }
  auto info =
      GrB_Matrix_build_BOOL(matrix.matrix(), rowIndices.data(),
                            colIndices.data(), values, nvals, GxB_IGNORE_DUP);
  GrbMatrix::handleError(info);
  return matrix;
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::diag(size_t nvals) {
  auto result = GrbMatrix(nvals, nvals);

  for (size_t i = 0; i < nvals; i++) {
    result.setElement(i, i, true);
  }

  return result;
}

// _____________________________________________________________________________
std::vector<std::pair<size_t, size_t>> GrbMatrix::extractTuples() const {
  size_t n = numNonZero();
  size_t rowIndices[n];
  size_t colIndices[n];
  bool values[n];
  auto info = GrB_Matrix_extractTuples_BOOL(rowIndices, colIndices, values, &n,
                                            matrix());
  GrbMatrix::handleError(info);

  std::vector<std::pair<size_t, size_t>> result;
  for (size_t i = 0; i < n; i++) {
    if (values[i]) {
      result.push_back(std::make_pair(rowIndices[i], colIndices[i]));
    }
  }
  return result;
}

// _____________________________________________________________________________
std::vector<size_t> GrbMatrix::extractColumn(size_t colIndex) const {
  std::unique_ptr<GrB_Vector> columnVector = std::make_unique<GrB_Vector>();
  size_t rows = numRows();
  auto info = GrB_Vector_new(columnVector.get(), GrB_BOOL, rows);
  handleError(info);

  info = GrB_Col_extract(*columnVector, GrB_NULL, GrB_NULL, matrix(), GrB_ALL,
                         rows, colIndex, GrB_NULL);
  handleError(info);

  size_t indices[rows];
  bool values[rows];
  std::unique_ptr<size_t> nvals = std::make_unique<size_t>(rows);
  info = GrB_Vector_extractTuples_BOOL(indices, values, nvals.get(),
                                       *columnVector);
  handleError(info);

  info = GrB_Vector_free(columnVector.get());
  handleError(info);

  std::vector<size_t> vec;
  vec.insert(vec.begin(), indices, indices + *nvals);
  return vec;
}

// _____________________________________________________________________________
std::vector<size_t> GrbMatrix::extractRow(size_t rowIndex) const {
  GrbMatrix transposed = transpose();
  return transposed.extractColumn(rowIndex);
}

// _____________________________________________________________________________
size_t GrbMatrix::numNonZero() const {
  size_t nvals;
  auto info = GrB_Matrix_nvals(&nvals, matrix());
  GrbMatrix::handleError(info);
  return nvals;
}

// _____________________________________________________________________________
size_t GrbMatrix::numRows() const {
  size_t nrows;
  auto info = GrB_Matrix_nrows(&nrows, matrix());
  GrbMatrix::handleError(info);
  return nrows;
}

// _____________________________________________________________________________
size_t GrbMatrix::numCols() const {
  size_t ncols;
  auto info = GrB_Matrix_ncols(&ncols, matrix());
  GrbMatrix::handleError(info);
  return ncols;
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::transpose() const {
  GrbMatrix transposed;
  auto info =
      GrB_Matrix_new(transposed.rawMatrix(), GrB_BOOL, numCols(), numRows());
  handleError(info);
  info = GrB_transpose(transposed.matrix(), GrB_NULL, GrB_NULL, matrix(),
                       GrB_NULL);
  handleError(info);

  return transposed;
}

// _____________________________________________________________________________
void GrbMatrix::accumulateMultiply(const GrbMatrix& otherMatrix) const {
  auto info = GrB_mxm(matrix(), GrB_NULL, GrB_LOR, GrB_LOR_LAND_SEMIRING_BOOL,
                      matrix(), otherMatrix.matrix(), GrB_NULL);
  handleError(info);
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::multiply(const GrbMatrix& otherMatrix) const {
  size_t resultNumRows = numRows();
  size_t resultNumCols = otherMatrix.numCols();
  GrbMatrix result;
  auto info = GrB_Matrix_new(result.rawMatrix(), GrB_BOOL, resultNumRows,
                             resultNumCols);

  info =
      GrB_mxm(result.matrix(), GrB_NULL, GrB_NULL, GrB_LOR_LAND_SEMIRING_BOOL,
              matrix(), otherMatrix.matrix(), GrB_NULL);
  handleError(info);

  return result;
}

// _____________________________________________________________________________
GrB_Matrix* GrbMatrix::rawMatrix() const {
  if (matrix_.get() != nullptr) {
    return matrix_.get();
  }
  AD_THROW("GrbMatrix error: internal GrB_Matrix is null");
}

// _____________________________________________________________________________
void GrbMatrix::handleError(GrB_Info info) {
  switch (info) {
    case GrB_SUCCESS:
      return;
    case GrB_NO_VALUE:
      return;
    case GrB_UNINITIALIZED_OBJECT:
      AD_THROW("GraphBLAS error: object has not been initialized");
    case GrB_NULL_POINTER:
      AD_THROW("GraphBLAS error: input pointer is NULL");
    case GrB_INVALID_VALUE:
      AD_THROW("GraphBLAS error: generic error code; some value is bad");
    case GrB_INVALID_INDEX:
      AD_THROW("GraphBLAS error: a row or column index is out of bounds");
    case GrB_DOMAIN_MISMATCH:
      AD_THROW("GraphBLAS error: object domains are not compatible");
    case GrB_DIMENSION_MISMATCH:
      AD_THROW("GraphBLAS error: matrix dimensions do not match");
    case GrB_OUTPUT_NOT_EMPTY:
      AD_THROW("GraphBLAS error: output matrix already has values in it");
    case GrB_NOT_IMPLEMENTED:
      AD_THROW("GraphBLAS error: not implemented in SuiteSparse:GraphBLAS");
    case GrB_PANIC:
      AD_THROW("GraphBLAS error: unrecoverable error");
    case GrB_OUT_OF_MEMORY:
      AD_THROW("GraphBLAS error: out of memory");
    case GrB_INSUFFICIENT_SPACE:
      AD_THROW("GraphBLAS error: output array not large enough");
    case GrB_INVALID_OBJECT:
      AD_THROW("GraphBLAS error: object is corrupted");
    case GrB_INDEX_OUT_OF_BOUNDS:
      AD_THROW("GraphBLAS error: a row or column is out of bounds");
    case GrB_EMPTY_OBJECT:
      AD_THROW("GraphBLAS error: an input scalar has no entry");
  }
  AD_FAIL();
}

bool GrbMatrix::isInitialized_ = false;

// _____________________________________________________________________________
void GrbMatrix::initialize() {
  if (!GrbMatrix::isInitialized_) {
    GrB_init(GrB_NONBLOCKING);
    GrbMatrix::isInitialized_ = true;
  }
}

// _____________________________________________________________________________
void GrbMatrix::finalize() {
  if (GrbMatrix::isInitialized_) {
    GrB_finalize();
    GrbMatrix::isInitialized_ = false;
  }
}
