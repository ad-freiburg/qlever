// TODO: we always peek before we parse, so we can throw
// TODO: find out, where to exactly we have to or can skip whitespace
// ______________________________________________________________
std::pair<bool, It> parseStatement(It it) {
  if (peekDirective(it)) {
    auto tmpIt = parseDirective(it);
    if (tmpIt.first) {
      return parseLiteral(it, ".");
    } else {
      return It::Failure;
    }
  } else if (peekTriples(it)) {
    auto tmpIt = parseTriples(it);
    if (tmpIt.first) {
      return parseLiteral(it, ".");
    } else {
      return It::Failure;
    }
    return parseTriples(it)
  }
  return skipWhitespace(it, true);
}

// _____________________________________________________________
std::pair<bool, It> parseDirective(It it) {
  if (peekPrefix(it)) {
    return parsePrefix(it);
  } else if (peekBase(it)) {
    return parseBase(it);
  } else {
    return It::Failure;
  }
}

// _____________________________________________________________
bool peekDirective(It it) { return peekLiteral(it, "@"); }

// _____________________________________________________________
std::pair<bool, It> parsePrefix(It it) {
  it = parseLiteral(it, "@prefix");
  it = skipWhitespace(it, true);
  if (!peekLiteral(it, ":")) {
    it = parsePrefixName(it);
  }
  it = parseLiteral(":");
  it = parseUriref(it);
  return it;
}

// _____________________________________________________________
std::pair<bool, It> parseBase(It it) {
  it = parseLiteral(it, "@base");
  it = skipWhitespace(it, true);
  it = parseUriref(it);
  return it;
}

// _________________________________________________________
It parseTriples(It it) {
  it = parseSubject(it);
  it = parsePredicateObjectList(it);
  return it;
}

// ____________________________________________________________
It parsePredicateObjectList(It it) {
  it = parseVerb(it);
  it = parseObjectList(it);
  while (peekLiteral(it, ";")) {
    it = parseLiteral(it, ";");
    it = parseVerb(it);
    it = parseObjectList(it);
  }
  // TODO: the turtle grammar says, that there might be an additional ';' at the
  // end, however I am not sure about this, since it is not easy to peek and not
  // in the description
}

// _________________________________________________________
It parseObjectList(It it) {
  it = parseObject(it);
  while (peekLiteral(it, ",")) {
    it = parseLiteral(it, ",");
    it = parseObject(it);
  }
}

// _________________________________________________________
It parseVerb(It it) {
  // what is this "a" for?
  // peekToken means that there must be  whitespace after it
  if (peekToken("a")) {
    return parseLiteral("a");
  }
  return parsePredicate(it);
}
