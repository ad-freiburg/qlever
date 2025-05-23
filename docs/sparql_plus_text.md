# SPARQL+Text support in QLever

QLever allows the combination of SPARQL and full-text search. The text input
consists of text records, where passages of the text are annotated by entities
from the RDF data. The format of the required input files is described below.
In SPARQL+Text queries you can then specify co-occurrence of words from the text
with entities from the RDF data. This is a very powerful concept: it contains
keyword search in literals as a special case, but goes far beyond it.

## SPARQL+Text quickstart

The `qlever` script makes it very easy to build a text index and to start a
server using that text index. Just download the script using `pip install qlever`
or from https://github.com/ad-freiburg/qlever-control and follow the
instruction in `qlever --help`. As a quick summary, there are the following
commands and options related to SPARQL+Text, use `qlever <command> --help` to get
detailed information about the options for a particular command:
```
qlever index --text-index [OPTIONS]
qlever start --use-text-index [OPTIONS]
qlever add-text-index [OPTIONS]
```
## Format of the text input files

When you build a text index soley from the literals in the RDF data (check out
the respective option of `qlever index` or `qlever add-text-index`), you do not
need any additional input files.

However, there is also the option to add text records that are not part of the
RDF data. These text records may contain mentions of entities from the RDF
data. This information can be passed to QLever via a so-called "wordsfile" and
"docsfile". The wordsfile determines the co-occurrences of words and entities.
The docsfile is just the text record that is returned in query results.

The `wordsfile` is a tab-separated file with one line per word occurrence, in
the following format:

    word    is_entity    record_id   score

Here is an example excerpt for the text record `In 1928, Fleming discovered
penicillin`, assuming that the id of the text record is `17`and that the
scientist and the drug are annotated with the IRIs of the corresponding entities
in the RDF data. Note that the IRI can have an arbitrary name that may be
syntactically complete unrelated to the words used to refer to that entity.

    In                  0   17   1
    1928                0   17   1
    <Alexander_Fleming> 1   17   1
    Fleming             0   17   1
    discovered          0   17   1
    penicillin          0   17   1
    <Penicillin>        1   17   1

The `docsfile` is a tab-separated file with one line per text record, in the
following format:

    record_id  text

For example, for the sentence above:

    17   Alexander Fleming discovered penicillin, a drug.

## Example SPARQL+Text queries

Here is an example query on Wikidata, which returns astronauts that are
mentioned in a sentence from the English Wikipedia that also contains the word
"moon" and the prefix "walk", ordered by the number of matching sentences.

```
SELECT ?astronaut ?astronaut_name (COUNT(?text) AS ?count) (SAMPLE(?text) AS ?example_text) WHERE {
  ?astronaut wdt:P106 wd:Q11631 .
  ?astronaut rdfs:label ?astronaut_name .
  FILTER (LANG(?astronaut_name) = "en")
  ?text ql:contains-entity ?astronaut .
  ?text ql:contains-word "moon walk*" .
}
GROUP BY ?astronaut ?astronaut_name
ORDER BY DESC(?count)
```
[Try with QLever](https://qlever.cs.uni-freiburg.de/wikidata/UmCdKa)
