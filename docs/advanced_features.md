# Advanced features

## SPARQL+Text and SPARQL Autocompletion

On top of the vanilla SPARQL functionality, QLever allows so-called SPARQL+Text
queries on a text corpus linked to a knowledge base via entity recognition.  For
example, the following query finds all mentions of astronauts next to the words
"moon" and "walk*" in the text corpus:

    SELECT ?a TEXT(?t) SCORE(?t) WHERE {
        ?a <is-a> <Astronaut> .
        ?t ql:contains-entity ?a .
        ?t ql:contains-word "walk* moon"
    } ORDER BY DESC(SCORE(?t))

Such queries can be simulated in standard SPARQL, but only with poor
performance, see the CIKM'17 paper above.  Details about the required input data
and the SPARQL+text query syntax and semantics can be found
[here](/docs/sparql_plus_text.md).

QLever also supports efficient SPARQL autocompletion.  For example, the
following query yields a list of all predicates associated with people in the
knowledge base, ordered by the number of people which have that predicate.

    SELECT ?predicate (COUNT(?predicate) as ?count) WHERE {
      ?x <is-a> <Person> .
      ?x ql:has-predicate ?predicate
    }
    GROUP BY ?predicate
    ORDER BY DESC(?count)

Note that this query could also be processed by a standard SPARQL engine simply
by replacing the second triple with `?x ?predicate ?object` and adding
`DISTINCT` inside the `COUNT()`.

However, that query will produce a very large intermediate result (all triples
of all people) with a correspondingly long query time.  In contrast, the query
above takes only about 100 ms on a standard Linux machine (with 16 GB memory)
and a dataset with 360 million triples and 530 million text records.

## Statistics:

You can get statistics for the currently active index in the following way:

    <server>:<port>/?cmd=stats

This query will yield a JSON response that features:

* The name of the KB index
* The number of triples in the KB index
* The number of index permutations build (usually 2 or 6)
* The numbers of distinct subjects, predicates and objects (only available if 6 permutations are built)
* The name of the text index (if one is present)
* The number of text records in the text index (if a text index is present)
* The number of word occurrences/postings in the text index (if a text index is present)
* The number of entity occurrences/postings in the text index (if a text index is present)


The name of an index is the name of the input `.nt` file (and wordsfile for the
text index), but can also be specified manually while building an index.
Therefore, qlever-index takes two optional arguments: `--text-index-name` (`-T`)
and `--kb-index-name` (`-K`).


## Send vs Compute

Currently, QLever does not compute partial results if there is a `LIMIT` modifier.

However, strings (for entities and text excerpts) are only resolved for those
items that that will be transmitted.  Furthermore, a UI usually only requires
a limited amount of rows at a time.

While specifying a `LIMIT` is recommended, some experiments may want
to measure the time to produce the full result.
Therefore an additional HTTP parameter `&send=<x>` can be used to send only
k result rows while still computing the readable result for up to `LIMIT` rows.

**IMPORTANT: Unless you want to measure QLever's performance, using `LIMIT` (+
`OFFSET` for sequential loading) is preferred in all applications. `LIMIT` is
faster and produces the same output as the `send` parameter**
