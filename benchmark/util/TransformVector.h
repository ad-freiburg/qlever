// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <iterator>
#include <concepts>
#include <algorithm>
#include <utility>
#include <vector>

/*
@brief The implementation of `transformVectorAndAppend` without any constraints
on the template parameter, so that the actual functions can easier impose
constraints with minimal code duplication.

@tparam SourceVectorType, TargetVectorType Should be vectors.
@tparam TranslationFunction Must be a function, that takes one argument of
the type of the entries in `SourceVectorType` and return a value of the type
of the entries in `TargetVectorType`.

@param sourceVector The vector to transform.
@param targetVecctor The vector to append to.
*/
template<typename SourceVectorType, typename TargetVectorType,
typename TranslationFunction>
static void transformVectorAndAppendImpl(
  SourceVectorType& sourceVector,
  TargetVectorType* const targetVector,
  TranslationFunction translationFunction){
  targetVector->reserve(sourceVector.size() + targetVector->size());

  std::ranges::transform(sourceVector, std::back_inserter(*targetVector),
    translationFunction, {});
}

/*
@brief The implementation of `transformVector` without any constraints
on the template parameter, so that the actual functions can easier impose
constraints with minimal code duplication.

@tparam SourceVectorType, TargetVectorType Should be vectors.
@tparam TranslationFunction Must be a function, that takes one argument of
the type of the entries in `SourceVectorType` and return a value of the type
of the entries in `TargetVectorType`.

@param sourceVector The vector to transform.
*/
template<typename SourceVectorType, typename TargetVectorType,
typename TranslationFunction>
static TargetVectorType transformVectorImpl(
  SourceVectorType& sourceVector,
  TranslationFunction translationFunction){
  // We just use `transformVectorAndAppendImpl` on an empty vector and return
  // that.
  TargetVectorType targetVector{};
  transformVectorAndAppendImpl(sourceVector, &targetVector,
    translationFunction);
  return targetVector;
}

/*
Concept, that a function `UnaryFunction` takes one argument of a specified typ
and returns a value of an other specified type.
*/
template<typename UnaryFunction, typename SourceType, typename TargetType>
concept UnaryFunctionWithSpecificTypes = requires(UnaryFunction func, SourceType s){
  {func(s)} -> std::same_as<TargetType>;
};

/*
@brief Transforms the content of a given vector with a given function and
appends it to another given vector.

@tparam SourceVectorContentType, TargetVectorContentType The type of the
objects, that the vectors hold.
@tparam TranslationFunction Must be a function, that takes one argument of
type SourceVectorContentType and return a value of TargetVectorContentType.

@param sourceVector The vector to transform.
@param targetVecctor The vector to append to.
*/
template<typename SourceVectorContentType, typename TargetVectorContentType,
typename TranslationFunction>
requires UnaryFunctionWithSpecificTypes<TranslationFunction,
SourceVectorContentType, TargetVectorContentType>
void transformVectorAndAppend(
  std::vector<SourceVectorContentType>& sourceVector,
  std::vector<TargetVectorContentType>* const targetVector,
  TranslationFunction translationFunction){

  transformVectorAndAppendImpl(sourceVector, targetVector, translationFunction);
}

/*
@brief Transforms the content of a given vector with a given function and
appends it to another given vector.

@tparam SourceVectorContentType, TargetVectorContentType The type of the
objects, that the vectors hold.
@tparam TranslationFunction Must be a function, that takes one argument of
type SourceVectorContentType and return a value of TargetVectorContentType.

@param sourceVector The vector to transform.
@param targetVecctor The vector to append to.
*/
template<typename SourceVectorContentType, typename TargetVectorContentType,
typename TranslationFunction>
requires UnaryFunctionWithSpecificTypes<TranslationFunction,
SourceVectorContentType, TargetVectorContentType>
void transformVectorAndAppend(
  const std::vector<SourceVectorContentType>& sourceVector,
  std::vector<TargetVectorContentType>* const targetVector,
  TranslationFunction translationFunction){

  transformVectorAndAppendImpl(sourceVector, targetVector, translationFunction);
}

/*
@brief Transforms the content of a given vector with a given function and
return a new vector.

@tparam SourceVectorContentType, TargetVectorContentType The type of the
objects, that the vectors hold.
@tparam TranslationFunction Must be a function, that takes one argument of
type SourceVectorContentType and return a value of TargetVectorContentType.

@param sourceVector The vector to transform.
*/
template<typename SourceVectorContentType, typename TargetVectorContentType,
typename TranslationFunction>
requires UnaryFunctionWithSpecificTypes<TranslationFunction,
SourceVectorContentType, TargetVectorContentType>
std::vector<TargetVectorContentType> transformVector(
  std::vector<SourceVectorContentType>& sourceVector,
  TranslationFunction translationFunction){

  return transformVectorImpl<std::vector<SourceVectorContentType>&,
  std::vector<TargetVectorContentType>>(sourceVector, translationFunction);
}

/*
@brief Transforms the content of a given vector with a given function and
return a new vector.

@tparam SourceVectorContentType, TargetVectorContentType The type of the
objects, that the vectors hold.
@tparam TranslationFunction Must be a function, that takes one argument of
type SourceVectorContentType and return a value of TargetVectorContentType.

@param sourceVector The vector to transform.
*/
template<typename SourceVectorContentType, typename TargetVectorContentType,
typename TranslationFunction>
requires UnaryFunctionWithSpecificTypes<TranslationFunction,
SourceVectorContentType, TargetVectorContentType>
std::vector<TargetVectorContentType> transformVector(
  const std::vector<SourceVectorContentType>& sourceVector,
  TranslationFunction translationFunction){

  return transformVectorImpl<const std::vector<SourceVectorContentType>&,
  std::vector<TargetVectorContentType>>(sourceVector, translationFunction);
}