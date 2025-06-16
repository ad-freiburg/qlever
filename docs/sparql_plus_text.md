# SPARQL+Text support in QLever

QLever allows the combination of SPARQL and full-text search in a collection of
text records. The text records can be the literals from the RDF data, or
additional text that is not part of the RDF data, or both.

The additional text can contain mentions of entities from the RDF data. A
SPARQL+Text query can then specify co-occurrence of words from the text with
entities from the RDF data. This is a powerful concept. See below for the input
format and an example query.

## SPARQL+Text quickstart

The `qlever` script makes it very easy to build a text index and to start a
server using that text index. Just download the script using `pip install qlever`
or from https://github.com/ad-freiburg/qlever-control and follow the
instructions from `qlever --help`. As a quick summary, there are the following
commands and options related to SPARQL+Text:
```
qlever index --text-index [OPTIONS] ...
qlever add-text-index [OPTIONS]
qlever start --use-text-index [OPTIONS] ...
```
Note that `qlever index` builds both the RDF index and the text index in one
go, while `qlever add-text-index` adds a text index to an already existing RDF
index. Use `qlever <command> --help` to get detailed information about the
options for each of these commands.

## Format of the text input files

This section describes the format of the input files for additional text that
is not part of the RDF data. These text records may contain mentions of
entities from the RDF data. This information can be passed to QLever via two
files, a so-called "wordsfile" and a so-called "docsfile". The wordsfile
specifies which words and entities occur in which text record. The docsfile is
just the text record that is returned in query results.

The wordsfile is a tab-separated file with one line per word occurrence, in
the following format:
```
word    is_entity    record_id   score
```
Here is an example excerpt for the text record `In 1928, Fleming discovered
penicillin`, assuming that the id of the text record is `17`and that the
scientist and the drug are annotated with the IRIs of the corresponding
entities in the RDF data. Note that the IRI can be syntactically completely
different from the words used to refer to that entity in the text.
```
In                  0   17   1
1928                0   17   1
<Alexander_Fleming> 1   17   1
Fleming             0   17   1
discovered          0   17   1
penicillin          0   17   1
<Penicillin>        1   17   1
```
The docsfile is a tab-separated file with one line per text record, in the
following format:
```

    record_id  text
```
For example, for the sentence above:
```
    17   Alexander Fleming discovered penicillin, a drug.
```

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
