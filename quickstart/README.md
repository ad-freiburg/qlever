# QLever Quickstart 

This page gives easy step-by-step instructions on how to build a qlever index
for an arbitrary given (https://www.w3.org/TR/n-triples/)[RDF N-Triples] or
(https://www.w3.org/TR/turtle)[RDF Turtle] file.

QLever can build indexes for up to 100 billion triples on a standard PC or
server with 128 GB of RAM (for smaller datasets, less RAM suffices). For an 
AMD Ryzen 9 5900X processor, indexing time is currently around 1 hour / 1
billion triples.

For the following, it is useful to have a single directory for all your QLever
stuff. This can be any directory. For example:

        export QLEVER_HOME=/local/data/qlever
        mkdir $QLEVER_HOME

## Get the QLever code and build it

Building the code will take a few minutes. Don't forget the --recursive, it's
important, since the QLever repository uses a few submodules.

        cd $QLEVER_HOME
        git clone --recursive https://github.com/ad-freiburg/qlever qlever-code
        cd qlever-code
        docker build -t qlever .

## Get the data

Create and go to a directory of your choice, and download or copy an N-Triples
for Turtle file of your choice there. If the file is compressed (as large files
usually are), there is no need to decompress it. If there are multiple files,
there is no need to concatenate them. All this will become clear in the next
step. As an example, let us copy the example file `olympics.ttl.xz` from the
from your local copy of the Qlever repository.

        mkdir -p $QLEVER_HOME/qlever-indices/olympics
        cd $QLEVER_HOME/qlever-indices/olympics
        cp $QLEVER_HOME/qlever-code/quickstart/olympics.nt.xz .
        cp $QLEVER_HOME/qlever-code/quickstart/olympics.settings.json .

As another example, let us download the latest version of the complete Wikidata
(this takes a few hours, only do this if you actually want to build a QLever
index for Wikidata).

        mkdir -p $QLEVER_HOME/qlever-indices/wikidata
        cd $QLEVER_HOME/qlever-indices/wikidata
        wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.ttl.bz2
        wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-lexemes.ttl.bz2
        cp $QLEVER_HOME/qlever-code/quickstart/wikidata.settings.json .

## Build a QLever index

To build an index for the olympics dataset (1.8M triples), the following command
line does the job. It takes about 20 seconds on an AMD Ryzen 9 5900X.  The only
purpose of the final `tee` is to save the log in a file (you can also recover
the log from the docker container as long as it exists).

        chmod o+w . && docker run -it --rm -v $QLEVER_HOME/qlever-indices/olympics:/index --entrypoint bash qlever -c "cd /index && xzcat olympics.nt.xz | IndexBuilderMain -F ttl -f - -l -i olympics -s olympics.settings.json | tee olympics.index-log.txt"

To build an index for the complete Wikidata (12B triples as of 30.09.2021), the
following command line does the job (after obtaining the dataset and the
settings as explained in the previous paragraph). It takes about 20 hours on an
AMD Ryzen 9 5900X. Note that the only difference is the basename (`wikidata`
instead of `olympics`) and how the input files are piped into the
`IndexBuilderMain` executable (using `bzcat` instead of `xzcat` and two files
instead of one).

        chmod o+w . && docker run -it --rm -v $QLEVER_HOME/qlever-indices/wikidata:/index --entrypoint bash qlever -c "cd /index && bzcat latest-all.ttl.bz2 latest-lexemes.ttl.bz2 | IndexBuilderMain -F ttl -f - -l -i wikidata -s wikidata.settings.json | tee wikidata.index-log.txt"

## Start the engine

To start the engine for the olympics dataset, the following command line does
the job. Choose a `PORT` of your choice, the SPARQL endpoint will then be
available on that port on the machine where you have started this. If you want
the container to run in the background, replace `--rm` by `-d`.

        PORT=7018; docker run --rm -v $QLEVER_HOME/qlever-indices/olympics:/index -p $PORT:7001 -e INDEX_PREFIX=olympics --name qlever.olympics qlever

Here is an example query to this SPARQL endpoint, computing the names of the
three athletes with the most gold medals in the Olympics games.

        curl -Gs http://localhost:$PORT --data-urlencode "query=PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX medal: <http://wallscope.co.uk/resource/olympics/medal/> PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/> SELECT ?athlete (COUNT(?medal) as ?count_medal) WHERE { ?medal olympics:medal medal:Gold .  ?medal olympics:athlete/rdfs:label ?athlete .  } GROUP BY ?athlete ORDER BY DESC(?count_medal) LIMIT 10" --data-urlencode "action=tsv_export"


