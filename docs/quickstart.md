# Quickstart
The following instructions build an index from our Scientists Example Knowledge
Base.

## Download and build QLever using `docker`

    git clone --recursive https://github.com/ad-freiburg/QLever.git qlever
    cd qlever
    docker build -t qlever .

## Download the example Knowledge Base

    wget http://filicudi.informatik.uni-freiburg.de/bjoern-data/scientist-collection.zip
    unzip -j scientist-collection.zip -d scientists

## Build a QLever Index

    docker run -it --rm \
        -v "$(pwd)/scientists:/input" \
        -v "$(pwd)/index:/index" --entrypoint "bash" qlever
    qlever@xyz:/app$ IndexBuilderMain -a -l -i /index/scientists \
        -n /input/scientists.nt \
        -w /input/scientists.wordsfile.tsv \
        -d /input/scientists.docsfile.tsv
    qlever@xyz:/app$ exit

## Run QLever

    docker run -it -p 7001:7001 \
        -v "$(pwd)/index:/index" \
        -e INDEX_PREFIX=scientists \
        --name qlever \
        qlever

Then open [http://localhost:7001/](http://localhost:7001/) in your browser.

For example, all female scientists occuring in the text corpus with the regex
"algo.*" can be obtained with the following query

    SELECT ?x TEXT(?t) SCORE(?t) WHERE {
        ?x <is-a> <Scientist> .
        ?x <Gender> <Female> .
        ?t ql:contains-entity ?x .
        ?t ql:contains-word "algo*"
    }
    ORDER BY DESC(SCORE(?t))
