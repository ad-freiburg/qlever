# SPARQL + Text Support
## Text Corpus Format

The following two input files are needed for full feature support:

### Wordsfile

A tab-separated file with one line per posting and following the format:

    word    isEntity    recordId   score

For example, for a sentence `Alexander Fleming discovered penicillin, a drug.`,
it could look like this when using contexts as records. In this case the
`penicilin` entity was prepended to the second context by the CSD-IE
preprocessing. In a more simple setting each record could also be a full
sentence.

    Alexander           0   0   1
    <Alexander_Fleming> 1   0   1
    Fleming             0   0   1
    <Alexander_Fleming> 1   0   1
    dicovered           0   0   1
    penicillin          0   0   1
    <Penicillin>        1   0   1
    penicillin          0   1   1
    <Penicillin>        1   1   1
    a                   0   1   1
    drug                0   1   1

Note that some form of entity recognition / linking has been done.
This file is used to build the text index from.

### Docsfile

A tab-separated file with one line per original unit of text and following the format:

    max_record_id  text

For example, for the sentence above:

    1   Alexander Fleming discovered penicillin, a drug.

Note that this file is only used to display proper excerpts as evidence for texttual matches.

## Supported Queries


Typical SPARQL queries can then be augmented. The features are best explained using examples:

A query for plants with edible leaves:

    SELECT ?plant WHERE {
        ?plant <is-a> <Plant> .
        ?t ql:contains-entity ?plant .
        ?t ql:contains-word "'edible' 'leaves'"
    }

The special predicate `ql:contains-entity` requires that results for `?plant` have to occur in a text record `?t`.
In records matching `?t`, there also have to be both words `edible` and `leaves` as specified thorugh the `ql:contains-word` predicate.
Note that the single quotes can also be omitted and will be in further examples.
Single quotes are necessary to mark phrases (which are not supported yet, but may be in the future).

A query for Astronauts who walked on the moon:

    SELECT ?a TEXT(?t) SCORE(?t) WHERE {
        ?a <is-a> <Astronaut> .
        ?t ql:contains-entity ?a .
        ?t ql:contains-word "walk* moon"
    } ORDER BY DESC(SCORE(?t))
    TEXTLIMIT 2

Note the following features:

* A star `*` can be used to search for a prefix as done in the keyword `walk*`. Note that there is a min prefix size depending on settings at index build-time.
* `SCORE` can be used to obtain the score of a text match. This is important to achieve a good ordering in the result. The typical way would be to `ORDER BY DESC(SCORE(?t))`.
* Where `?t` just matches a text record Id, `TEXT(?t)` can be used to extract a snippet.
* `TEXTLIMIT` can be used to control the number of result lines per text match. The default is 1.

An alternative query for astronauts who walked on the moon:

        SELECT ?a TEXT(?t) SCORE(?t) WHERE {
            ?a <is-a> <Astronaut> .
            ?t ql:contains-entity ?a .
            ?t ql:contains-word "walk*" .
            ?t ql:contains-entity <Moon> .
        } ORDER BY DESC(SCORE(?t))
        TEXTLIMIT 2

This query doesn't search for an occurrence of the word moon but played where the entity `<Moon>` has been linked.
For the sake of brevity, it is possible to treat the concrete URI `<Moon>` like word and include it in the `contains-word` triple.
This can be convenient but should be avoided to keep queries readable: `?t ql:contains-word "walk* <Moon>"`


Text / Knowledge-base data can be nested in queries. This allows queries like one for politicians that were friends with a scientist associated with the manhattan project:

    SELECT ?p TEXT(?t) ?s TEXT(?t2) WHERE {
        ?p <is-a> <Politician> .
        ?t ql:contains-entity ?p .
        ?t ql:contains-word friend* .
        ?t ql:contains-entity ?s .
        ?s <is-a> <Scientist> .
        ?t2 ql:contains-entity ?s .
        ?t2 ql:contains-word "manhattan project"
    } ORDER BY DESC(SCORE(?t))


For now, each text-record variable is required to have a triple `ql:contains-word/entity WORD/URI`.
Pure connections to variables (e.g. "Books with a description that mentions a plant.") are planned for the future.

To obtain a list of available predicates and their counts `ql:has-predicate` can be used if the index was build with the `--patterns` option, and the server was started with the `--patterns` option:

    SELECT ?relations (COUNT(?relations) as ?count) WHERE {
      ?s <is-a> <Scientist> .
      ?t2 ql:contains-entity ?s .
      ?t2 ql:contains-word "manhattan project" .
      ?s ql:has-predicate ?relations .
    }
    GROUP BY ?relations
    ORDER BY DESC(?count)

`ql:has-predicate` can also be used as a normal predicate in an arbitrary query.

Group by is supported, its aggregates can be used both for selecting as well as in ORDER BY clauses:

    SELECT ?profession (AVG(?height) as ?avg) WHERE {
      ?a <is-a> ?profession .
      ?a <Height> ?height .
    }
    GROUP BY ?profession
    ORDER BY ?avg
    HAVING (?profession > <H)


    SELECT ?profession  WHERE {
      ?a <is-a> ?profession .
      ?a <Height> ?height .
    }
    ORDER BY (AVG(?height) as ?avg)
    GROUP BY ?profession

Supported aggregates are `MIN, MAX, AVG, GROUP_CONCAT, SAMPLE, COUNT, SUM`. All of the aggreagates support `DISTINCT`, e.g. `(GROUP_CONCAT(DISTINCT ?a) as ?b)`.
Group concat also supports a custom separator: `(GROUP_CONCAT(?a ; separator=" ; ") as ?concat)`. Xsd types float, decimal and integer are recognized as numbers, other types or unbound variables (e.g. no entries for an optional part) in one of the aggregates that need to interpret the variable (e.g. AVG) lead to either no result or nan. MAX with an unbound variable will always return the unbound variable.
A query can only have one GROUP BY and one HAVING clause, but may have several
variables in the GROUP BY and several filters in the HAVING clause (which will
then be concatenated using and)

    SELECT ?profession ?gender (AVG(?height) as ?avg) WHERE {
      ?a <is-a> ?profession .
      ?a <Gender> ?gender .
      ?a <Height> ?height .
    }
    GROUP BY ?profession ?gender
    HAVING (?a > <H) (?gender == <Female>)


QLever has support for both OPTIONAL as well as UNION. When marking part of a query as optional the variable bindings of the optional parts are only added to results that
are created by the non optional part. Furthermore, if the optional part has no result matching the rest of the graph pattern its variables are left unbound, leading
to empty entries in the result table. UNION combines two graph patterns by appending the result of one pattern to the result of the other. Results for Variables with the same name
in both graph patterns will end up in the same column in the result table.


    SELECT ?profession ?gender (AVG(?height) as ?avg) WHERE {
      { ?a <is-a> ?profession . }
      UNION
      {
        ?a <Gender> ?gender .
        OPTIONAL {
          ?a <Height> ?height .
        }
      }
    }

