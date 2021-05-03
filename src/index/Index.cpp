// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Index.h"

#include "./IndexImpl.h"

using std::array;

// _____________________________________________________________________________
Index::Index() : _pimpl{std::make_unique<IndexImpl>()} {}

// ____________________________________________________________________________
Index::~Index() = default;

// _____________________________________________________________________________
template <class Parser>
void Index::createFromFile(const string& filename) {
  return _pimpl->template createFromFile<Parser>(filename);
}
// explicit instantiations
template void Index::createFromFile<TsvParser>(const string& filename);
template void Index::createFromFile<NTriplesParser>(const string& filename);
template void Index::createFromFile<TurtleStreamParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleMmapParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleParserDummy>(const string& filename);

// _____________________________________________________________________________
void Index::addPatternsToExistingIndex() {
  _pimpl->addPatternsToExistingIndex();
}

// _____________________________________________________________________________
void Index::createFromOnDiskIndex(const string& onDiskBase) {
  _pimpl->createFromOnDiskIndex(onDiskBase);
}

// _____________________________________________________________________________
const vector<PatternID>& Index::getHasPattern() const {
  return _pimpl->getHasPattern();
}

// _____________________________________________________________________________
const CompactStringVector<Id, Id>& Index::getHasPredicate() const {
  return _pimpl->getHasPredicate();
}

// _____________________________________________________________________________
const CompactStringVector<size_t, Id>& Index::getPatterns() const {
  return _pimpl->getPatterns();
}

// _____________________________________________________________________________
double Index::getHasPredicateMultiplicityEntities() const {
  return _pimpl->getHasPredicateMultiplicityEntities();
}

// _____________________________________________________________________________
double Index::getHasPredicateMultiplicityPredicates() const {
  return _pimpl->getHasPredicateMultiplicityPredicates();
}

// _____________________________________________________________________________
size_t Index::getHasPredicateFullSize() const {
  return _pimpl->getHasPredicateFullSize();
}

// _____________________________________________________________________________
size_t Index::relationCardinality(const string& relationName) const {
  return _pimpl->relationCardinality(relationName);
}

// _____________________________________________________________________________
size_t Index::subjectCardinality(const string& sub) const {
  return _pimpl->subjectCardinality(sub);
}

// _____________________________________________________________________________
size_t Index::objectCardinality(const string& obj) const {
  return _pimpl->objectCardinality(obj);
}

// _____________________________________________________________________________
size_t Index::sizeEstimate(const string& sub, const string& pred,
                           const string& obj) const {
  return _pimpl->sizeEstimate(sub, pred, obj);
}

// _____________________________________________________________________________
void Index::setKbName(const string& name) { return _pimpl->setKbName(name); }

// ____________________________________________________________________________
void Index::setOnDiskLiterals(bool onDiskLiterals) {
  return _pimpl->setOnDiskLiterals(onDiskLiterals);
}

// ____________________________________________________________________________
void Index::setOnDiskBase(const std::string& onDiskBase) {
  return _pimpl->setOnDiskBase(onDiskBase);
}

// ____________________________________________________________________________
void Index::setKeepTempFiles(bool keepTempFiles) {
  return _pimpl->setKeepTempFiles(keepTempFiles);
}

// _____________________________________________________________________________
void Index::setUsePatterns(bool usePatterns) {
  return _pimpl->setUsePatterns(usePatterns);
}

// ____________________________________________________________________________
void Index::setSettingsFile(const std::string& filename) {
  return _pimpl->setSettingsFile(filename);
}

// ____________________________________________________________________________
void Index::setPrefixCompression(bool compressed) {
  return _pimpl->setPrefixCompression(compressed);
}

// ____________________________________________________________________________
void Index::addTextFromContextFile(const string& contextFile) {
  return _pimpl->addTextFromContextFile(contextFile);
}

// ___________________________________________________________
void Index::buildDocsDB(const string& docsFile) {
  return _pimpl->buildDocsDB(docsFile);
}

// ___________________________________________________________________________
void Index::addTextFromOnDiskIndex() {
  return _pimpl->addTextFromOnDiskIndex();
}

// ___________________________________________________________________________
const RdfsVocabulary& Index::getVocab() const { return _pimpl->getVocab(); }

// ____________________________________________________________________________
const TextVocabulary& Index::getTextVocab() const {
  return _pimpl->getTextVocab();
}

// ___________________________________________________________________________
void Index::setTextName(const string& name) {
  return _pimpl->setTextName(name);
}

// ____________________________________________________________________________
std::optional<string> Index::idToOptionalString(Id id) const {
  return _pimpl->idToOptionalString(id);
}

// ____________________________________________________________
const string& Index::getTextName() const { return _pimpl->getTextName(); }

// _____________________________________________________________________________
const string& Index::getKbName() const { return _pimpl->getKbName(); }

// _____________________________________________________________________________
size_t Index::getNofTriples() const { return _pimpl->getNofTriples(); }

// __________________________________________________________-
size_t Index::getNofTextRecords() const { return _pimpl->getNofTextRecords(); }
size_t Index::getNofWordPostings() const {
  return _pimpl->getNofWordPostings();
}
size_t Index::getNofEntityPostings() const {
  return _pimpl->getNofEntityPostings();
}

// _____________________________________________________________________________
size_t Index::getNofSubjects() const { return _pimpl->getNofSubjects(); }
size_t Index::getNofObjects() const { return _pimpl->getNofObjects(); }
size_t Index::getNofPredicates() const { return _pimpl->getNofPredicates(); }

// _____________________________________________________________________________
bool Index::hasAllPermutations() const { return _pimpl->hasAllPermutations(); }

// _____________________________________________________________________________
vector<float> Index::getMultiplicities(const string& key,
                                       const Permutation p) const {
  return _pimpl->getMultiplicities(key, p);
}
// _____________________________________________________________________________

vector<float> Index::getMultiplicities(const Permutation p) const {
  return _pimpl->getMultiplicities(p);
}
// ______________________________________________________________________________
void Index::dumpAsciiLists(const vector<string>& lists,
                           bool decodeGapsFreq) const {
  return _pimpl->dumpAsciiLists(lists, decodeGapsFreq);
}

// _____________________________________________________
const string& Index::wordIdToString(Id id) const {
  return _pimpl->wordIdToString(id);
}

// _______________________________________________________
size_t Index::getSizeEstimate(const string& words) const {
  return _pimpl->getSizeEstimate(words);
}

// ________________________________________________________
void Index::getContextListForWords(const string& words, IdTable* result) const {
  return _pimpl->getContextListForWords(words, result);
}

// ________________________________________________________________________
void Index::getECListForWordsOneVar(const string& words, size_t limit,
                                    IdTable* result) const {
  return _pimpl->getECListForWordsOneVar(words, limit, result);
}

// _____________________________________________________________________________________
void Index::getECListForWords(const string& words, size_t nofVars, size_t limit,
                              IdTable* result) const {
  return _pimpl->getECListForWords(words, nofVars, limit, result);
}

// __________________________________________________________________________
void Index::getFilteredECListForWords(const string& words,
                                      const IdTable& filter,
                                      size_t filterColumn, size_t nofVars,
                                      size_t limit, IdTable* result) const {
  return _pimpl->getFilteredECListForWords(words, filter, filterColumn, nofVars,
                                           limit, result);
}

// ____________________________________________________________________________
void Index::getFilteredECListForWordsWidthOne(const string& words,
                                              const IdTable& filter,
                                              size_t nofVars, size_t limit,
                                              IdTable* result) const {
  return _pimpl->getFilteredECListForWordsWidthOne(words, filter, nofVars,
                                                   limit, result);
}

// _____________________________________________________________________________
void Index::getContextEntityScoreListsForWords(const string& words,
                                               vector<Id>& cids,
                                               vector<Id>& eids,
                                               vector<Score>& scores) const {
  return _pimpl->getContextEntityScoreListsForWords(words, cids, eids, scores);
}

// ____________________________________________________________________________
template <size_t I>
void Index::getECListForWordsAndSingleSub(const string& words,
                                          const vector<array<Id, I>>& subres,
                                          size_t subResMainCol, size_t limit,
                                          vector<array<Id, 3 + I>>& res) const {
  return _pimpl->template getECListForWordsAndSingleSub<I>(
      words, subres, subResMainCol, limit, res);
}

// __________________________________________________________________________
void Index::getECListForWordsAndTwoW1Subs(const string& words,
                                          const vector<array<Id, 1>>& subres1,
                                          const vector<array<Id, 1>>& subres2,
                                          size_t limit,
                                          vector<array<Id, 5>>& res) const {
  return _pimpl->getECListForWordsAndTwoW1Subs(words, subres1, subres2, limit,
                                               res);
}

// ___________________________________________________________________________
void Index::getECListForWordsAndSubtrees(
    const string& words,
    const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>& subResVecs,
    size_t limit, vector<vector<Id>>& res) const {
  return _pimpl->getECListForWordsAndSubtrees(words, subResVecs, limit, res);
}

// ____________________________________________________________________________
void Index::getWordPostingsForTerm(const string& term, vector<Id>& cids,
                                   vector<Score>& scores) const {
  return _pimpl->getWordPostingsForTerm(term, cids, scores);
}

// ____________________________________________________________________________
void Index::getEntityPostingsForTerm(const string& term, vector<Id>& cids,
                                     vector<Id>& eids,
                                     vector<Score>& scores) const {
  return _pimpl->getEntityPostingsForTerm(term, cids, eids, scores);
}

// ____________________________________________________________________________
string Index::getTextExcerpt(Id cid) const {
  return _pimpl->getTextExcerpt(cid);
}

// ____________________________________________________________________________
float Index::getAverageNofEntityContexts() const {
  return _pimpl->getAverageNofEntityContexts();
}

// ____________________________________________________________________________
void Index::scan(Id key, IdTable* result, const Permutation& p,
                 ad_utility::SharedConcurrentTimeoutTimer timer) const {
  return _pimpl->scan(key, result, p, std::move(timer));
}

// ___________________________________________________________________________
void Index::scan(const string& key, IdTable* result, const Permutation& p,
                 ad_utility::SharedConcurrentTimeoutTimer timer) const {
  return _pimpl->scan(key, result, p, std::move(timer));
}

// _____________________________________________________________________________
void Index::scan(const string& keyFirst, const string& keySecond,
                 IdTable* result, const Permutation& p) const {
  return _pimpl->scan(keyFirst, keySecond, result, p);
}
