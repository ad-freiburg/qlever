# Obtaining Knowledge Base + Text Data

A SPARQL engine without data is as useless as a library without books, therefore
we provide a selection of knowledge bases with linked text corpora ready to play
around with.

## The Scientists Collection (small)

These files are of small size (facts about scientists - only one hop from
a scientist in a knowledge graph. Text are Wikipedia articles about scientists.)
Includes a knowledge base as nt file, and a words- and docsfile as tsv.

[scientist-collection.zip](http://qlever.informatik.uni-freiburg.de/data/scientist-collection.zip):

* 78 MB zipped
* 318 MB unzipped
* 350 k facts
* 11.7 m text postings

Here is a sample query to try and check if everything worked for you:

    SELECT ?x SCORE(?t) TEXT(?t) WHERE {
        ?x <is-a> <Scientist> .
        ?t ql:contains-entity ?x .
        ?t ql:contains-word "relati*"
    }
    ORDER BY DESC(SCORE(?t))

Curl-version (ready for copy&paste) of the query:

    SELECT ?x SCORE(?t) TEXT(?t) WHERE \{ ?x <is-a> <Scientist> . ?t ql:contains-entity ?x . ?t ql:contains-word \"relati*\" \} ORDER BY DESC(SCORE(?t))

## Freebase Easy and Wikipedia (medium)


Includes a knowledge base as nt file, and a words- and docsfile as tsv.  Text
and facts are basically equivalent to the
[Broccoli](http://broccoli.cs.uni-freiburg.de) search engine.

[wikipedia-freebase-easy.zip](http://qlever.informatik.uni-freiburg.de/data/wikipedia-freebase-easy.zip):

* 21 GB zipped
* 103 GB unzipped
* 372 million facts
* 2.8 billion text postings

Here is a sample query to try and check if everything worked for you:

    SELECT ?x SCORE(?t) TEXT(?t) WHERE {
        ?x <is-a> <Astronaut> .
        ?t ql:contains-entity ?x .
        ?t ql:contains-word "walk* moon"
    }
    ORDER BY DESC(SCORE(?t))

Curl-version (ready for copy&paste) of the query:

    SELECT ?x SCORE(?t) TEXT(?t) WHERE \{ ?x <is-a> <Astronaut> . ?t ql:contains-entity ?x . ?t ql:contains-word \"walk* moon\" \} ORDER BY DESC(SCORE(?t))

## Wikidata Truthy (large)

QLever is known to work well with the truthy subset of Wikidata **in Turtle
format (.ttl)** which can be downloaded
[here](https://dumps.wikimedia.org/wikidatawiki/entities/).

* Currently no linked text corpus is publicly available but an internal version
  can be experimented with [here](http://qlever.informatik.uni-freiburg.de)
* > 150 GB


A query asking for regions and a mountain in that region is

    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX schema: <http://schema.org/>
    SELECT ?region ?rname (SAMPLE(?mname) AS ?sample) (COUNT(?mountain) AS ?count) (MIN(?height) AS ?minh) (MAX(?height) AS ?maxh) (AVG(?height) AS ?avg) WHERE {
      ?mountain wdt:P31 wd:Q8502 .
      ?mountain wdt:P361 ?region .
      ?mountain wdt:P2044 ?height .
      ?mountain schema:name ?mname .
      ?region schema:name ?rname .
      FILTER langMatches(lang(?mname), "en") .
      FILTER langMatches(lang(?rname), "en")
    }
    GROUP BY ?region ?rname
    ORDER BY ASC(?rname)

## Wikidata Full (larger)
QLever also works for the full Wikidata knowledge base also **in Turtle format
(.ttl)** which is also available [here](https://dumps.wikimedia.org/wikidatawiki/entities/).

* Currently no linked text corpus is publicly available but an internal version
  can be experimented with [here](http://qlever.informatik.uni-freiburg.de)
* > 331 GB

A query asking for all mountains > 8000 m can be written as

    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subj ?label ?coord ?elev WHERE
    {
      ?subj wdt:P31 wd:Q8502 .
      ?subj p:P2044 ?elev_s .
      ?elev_s psn:P2044 ?elev_v .
      ?elev_v wikibase:quantityAmount ?elev .
      ?subj wdt:P625 ?coord .
      ?subj rdfs:label ?label .

      FILTER(?elev > 8000.0) .
      FILTER langMatches(lang(?label), "en") . 
    }
    ORDER BY DESC(?elev)

## The Tiny Sanity Check (tiny, included)

These are tiny and there's nothing meaningful to discover.
They are fine for setting up a working sever within seconds and getting
comfortable with the query language:

    QLever/misc/tiny-example.kb.nt
    QLever/misc/tiny-example.wordsfile.tsv
    QLever/misc/tiny-example.docsfile.tsv

Note that we left out stopwords (unlike in the docsfile) to demonstrate how this
can be done if desired.
If you build an index using these files and ask the query:

    SELECT ?x TEXT(?t) WHERE {
        ?x <is-a> <Scientist> .
        ?t ql:contains-entity ?x .
        ?t ql:contains-word "penicillin"
    }  ORDER BY DESC(SCORE(?t))

You should find `<Alexander_Fleming>` and the textual evidence for that match.

You can also display his awards or find `<Albert_Einstein>` and his awards with
the following query:

    SELECT ?x ?award TEXT(?t) WHERE {
        ?x <is-a> <Scientist> .
        ?t ql:contains-entity ?x .
        ?t ql:contains-word "theory rela*" .
        ?x <Award_Won> ?award
    }  ORDER BY DESC(SCORE(?t))

have a look at the (really tiny) input files to get a feeling for how this works.

Curl-versions (ready for copy&paste) of the queries:

    SELECT ?x TEXT(?t) WHERE \{ ?x <is-a> <Scientist> . ?t ql:contains-entity ?x . ?t ql:contains-word \"penicillin\" \} ORDER BY DESC(SCORE(?t))

    SELECT ?x ?award TEXT(?t) WHERE \{ ?x <is-a> <Scientist> . ?t ql:contains-entity ?x . ?t ql:contains-word \"theory rela*\" . ?x <Award_Won> ?award \}  ORDER BY DESC(SCORE(?t))

Again, there's not much to be done with this data.
For a meaningful index, use the example data below.
