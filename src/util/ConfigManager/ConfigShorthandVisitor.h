// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)
#pragma once

// ANTLR runtime uses this as a variable name.
#ifdef EOF
#undef EOF
#endif
#include <antlr4-runtime.h>

#include "util/ConfigManager/generated/ConfigShorthandParser.h"
#define EOF std::char_traits<char>::eof()
#include "util/json.h"

/*
This visitor will translate the parsed short hand into a `nlohmann::json`
object.
*/
class ToJsonConfigShorthandVisitor final {
 public:
  using Parser = ConfigShorthandParser;

  nlohmann::json::object_t visitShortHandString(
      Parser::ShortHandStringContext* context) const;

  nlohmann::json::object_t visitAssignments(
      const Parser::AssignmentsContext* context) const;

  /*
  @brief A custom exception, the `ConfigShortHandVisitor` runs into a key
  collision.
  */
  class ConfigShortHandVisitorKeyCollisionException : public std::exception {
    // The error message.
    std::string message_;

   public:
    /*
    @param keyName The string representation of the key.
    */
    explicit ConfigShortHandVisitorKeyCollisionException(
        std::string_view keyName);

    const char* what() const throw() override;
  };

  std::pair<std::string, nlohmann::json> visitAssignment(
      Parser::AssignmentContext* context) const;

  nlohmann::json::object_t visitObject(Parser::ObjectContext* context) const;

  nlohmann::json::array_t visitList(const Parser::ListContext* context) const;

  nlohmann::json visitContent(Parser::ContentContext* context) const;
};
