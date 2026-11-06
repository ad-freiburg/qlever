# Validation suite for `sameTerm`

A small, engine-independent test suite for the SPARQL function
[`sameTerm`](https://www.w3.org/TR/sparql11-query/#func-sameTerm), which can be
run with [Apache Jena](https://jena.apache.org/) (as an external reference) and
with QLever, so that the results of the two can be compared.

* `data.ttl`: the test data. Each object is an RDF term of a different kind,
  and the local name of the subject describes that term.
* `queries/*.rq`: the queries. They only return IRIs, booleans, and unbound
  values, so that the results do not depend on how an engine formats literals.
* `expected/*.tsv`: the expected results according to the specifications (see
  below).
* `run.sh`: runs all queries with Jena (`./run.sh jena [<path to sparql>]`) or
  against a SPARQL endpoint (`./run.sh endpoint <URL>`), and compares the
  results with `expected/`.
* `run-qlever.sh`: builds a QLever index for `data.ttl`, starts a QLever server,
  and runs `run.sh` against it (`./run-qlever.sh [<build directory>]`).

## How the expected results were obtained

The expected results were generated with Jena 4.6.1 (`UPDATE=1 ./run-cli-sparql.sh sparql`)
and then checked against the specifications by hand. Two of them were
corrected, because Jena 4.6.1 deviates from the specification of `=` (not of
`sameTerm`), which uses
[`op:numeric-equal`](https://www.w3.org/TR/xpath-functions/#func-numeric-equal)
for numbers:

* `03-same-term-but-not-equal-to-itself`: `NaN = NaN` is false, but Jena
  returns true, so Jena returns no rows.
* `04-equal-but-not-the-same-term`: `0.0E0 = -0.0E0` is true (but they are
  different terms), and `"NaN"^^xsd:double = "NaN"^^xsd:float` is false. Jena
  returns the opposite for both.

So with Jena 4.6.1, these two queries are expected to fail, and all other
queries are expected to pass.

Language tags with different case (like `"1"@en` and `"1"@EN`) are deliberately
not part of the suite: RDF 1.1 compares language tags "character by character",
but also allows implementations to convert them to lower case, so both answers
are correct. (Jena 4.6.1 is not even consistent here: `"1"@EN` from the data is
not the same term as `"1"@en` from the data, but the same term as `"1"@en` from
a query.)

## Known deviations of QLever

QLever stores some literals not as strings, but folds them into an ID that
encodes their value (numbers, booleans, dates, and points), and does not keep
their lexical form or exact datatype. Literals that are folded into the same ID
are indistinguishable, so `sameTerm` considers them to be the same term,
although according to the specification they are not:

* `"1"^^xsd:integer`, `"01"^^xsd:integer`, and `"1"^^xsd:int` (and all other
  integer datatypes with the same value) are all stored as the integer 1.
* `1.0` (`xsd:decimal`) and `1.0E0` (`xsd:double`) are both stored as the double
  1.0, and `"NaN"^^xsd:double` and `"NaN"^^xsd:float` are both stored as NaN.
* `"POINT(1 2)"^^geo:wktLiteral` and `"POINT(1.0 2.0)"^^geo:wktLiteral` are both
  stored as the same point.
* This also holds for terms computed in a query, so for example
  `STRDT("01", xsd:integer)` is the same term as `1`.

As a consequence, `./run-qlever.sh` is expected to fail for
`01-pairs-of-same-terms`, `04-equal-but-not-the-same-term`,
`05-the-same-term-as-a-constant`, and `06-constants-and-computed-terms` (only
`?r06`), and all differences are due to the cases above. In particular, QLever
agrees with the specification (and with Jena) for blank nodes, IRIs (including
encoded IRIs), plain strings and `xsd:string`, language-tagged strings,
`"1"^^xsd:boolean` and `true` (which QLever stores differently), `0.0E0` and
`-0.0E0`, dates, literals with custom datatypes, and unbound arguments.
