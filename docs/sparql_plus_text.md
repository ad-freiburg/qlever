# SPARQL+Text support in QLever

QLever allows the combination of SPARQL and full-text search. The text input
consists of text records, where passages of the text are annotated by entitites
from the RDF data. The format of the required input files is described below.
In SPARQL+Text queries you can then specify co-occurrence of words from the text
with entities from the RDF data. This is a very powerful concept: it contains
keyword search in literals as a special case, but goes far beyond it.

## SPARQL+Text quickstart

This section shows how to build an SPARQL+text index and start the server for
a small example dataset. It assumes that you are already familiar with the
process without text, as described [here](/docs/quickstart.md), and that you
have set `QLEVER_HOME`, the QLever code resides under
`$QLEVER_HOME/qlever-code`, and the docker image is called `qlever`.

        # Create directory for this example and go there.
        mkdir -p $QLEVER_HOME/qlever-indices/scientists
        cd $QLEVER_HOME/qlever-indices/scientists
        # Get the dataset.
        wget http://qlever.informatik.uni-freiburg.de/data/scientist-collection.zip
        unzip -j scientist-collection.zip
        # Build the index (SPARQL+Text, note the -w and -d option).
        cp $QLEVER_HOME/qlever-code/examples/scientists.settings.json .
        chmod o+w . && docker run -it --rm -v $QLEVER_HOME/qlever-indices/scientists:/index --entrypoint bash qlever -c "cd /index && cat scientists.nt | IndexBuilderMain -F ttl -f - -l -i scientists -w scientists.wordsfile.tsv -d scientists.docsfile.tsv -s scientists.settings.json | tee scientists.index-log.txt"
        # Start the engine on a port of your choice; don't forget the -t option in the end!
        PORT=7001; docker run --rm -v $QLEVER_HOME/qlever-indices/scientists:/index -p $PORT:7001 -e INDEX_PREFIX=scientists --name qlever.scientists qlever -t

You can now try the follow example query (scientists who co-occur with relativity
theory in the text, ordered by the number of occurrences):

        curl -Gs http://localhost:$PORT --data-urlencode 'query=SELECT ?x SCORE(?t) TEXT(?t) WHERE { ?x <is-a> <Scientist> .  ?t ql:contains-entity ?x .  ?t ql:contains-word "relativity" } ORDER BY DESC(SCORE(?t)) LIMIT 10' --data-urlencode "action=tsv_export"

## Format of the text input files

For SPARQL+Text queries, the index builder needs to input files, a so-called
`wordsfile` and a `docsfile`. The wordsfile determines the co-occurrences of
words and entities. The docsfile is just the text record that is returned in
query results.

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

Here are some more example queries on the larger [Fbeasy+Wikipedia](
(http://qlever.informatik.uni-freiburg.de/data/fbeasy-wikipedia.zip) dataset.
In this dataset, the text records are the sentences from a dump of the English
Wikipedia, and the entities are from the [Freebase
Easy](https://freebase-easy.cs.uni-freiburg.de) RDF data.

Plants with edible leaves. Note that the object of `ql:contains-word` can be a
string with several keywords. The keywords are treated as a bag of words, not as
a phrase. That is, they can occur in the text record in any order. They just
have to co-occur with a plant (as specified by the `ql:contains-entity` triple).

    SELECT ?plant WHERE {
      ?plant <is-a> <Plant> .
      ?text ql:contains-entity ?plant .
      ?text ql:contains-word "edible leaves"
    }

Astronauts who walked on the moon. Note the use of `*` for prefix search. The
`TEXT(?text)` returns the respective text record. The `SCORE(?text)` returns the
number of matching records (sums of the score in the `wordsfile`, see above).
The `TEXLIMIT` limits the number of text records shown per entity.

    SELECT ?astronaut TEXT(?text) SCORE(?text) WHERE {
      ?astronaut <is-a> <Astronaut> .
      ?text ql:contains-entity ?astronaut .
      ?text ql:contains-word "walk* moon"
    }
    ORDER BY DESC(SCORE(?t))
    TEXTLIMIT 2

The query could be equivalently formulated as:

    SELECT ?astronaut TEXT(?text) SCORE(?text) WHERE {
      ?astronaut <is-a> <Astronaut> .
      ?text ql:contains-entity ?astronaut .
      ?text ql:contains-word "walk*" .
      ?text ql:contains-word "moon"
    }
    ORDER BY DESC(SCORE(?t))
    TEXTLIMIT 2

Politicians that were friends with a scientist associated with the Manhattan
project. Note that each match for this query involves two text records: One that
establishes that the respective politician was friends with the respective
scientist. And one that established that the respective scientist was involved
with the Manhattan project. Using two text records here is important because
there is no reason why these two facts should be mentioned in one and the same
text records. But both are important for the desired query results.

    SELECT ?politician ?scientist TEXT(?text_1) TEXT(?text_2) WHERE {
      ?politician <is-a> <Politician> .
      ?text_1 ql:contains-entity ?politician .
      ?text_1 ql:contains-word "friend*" .
      ?text_1 ql:contains-entity ?scientist .
      ?scientist <is-a> <Scientist> .
      ?text_2 ql:contains-entity ?scientist .
      ?text_2 ql:contains-word "manhattan project"
    }
    ORDER BY DESC(SCORE(?text_1))

