import argparse
import sys
import compare_performance

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--queryfile',
                    type=str,
                    help='One line per (standard) SPARQL query. TS specific transformations are made by the python script.',
                    required=True)

parser.add_argument('--rdf3x-query-outfile', type=str)

parser.add_argument('--my-query-outfile', type=str)

parser.add_argument('--bifc-query-outfile', type=str)

parser.add_argument('--bifc-inc-query-outfile', type=str)


def main():
    args = vars(parser.parse_args())
    queries = args['queryfile']
    if (args['rdf3x_query_outfile']):
        with open(args['rdf3x_query_outfile'], 'w') as outfile:
            for line in open(queries):
                print(compare_performance.rewrite_for_rdf3x(line), file=outfile)
    if (args['my_query_outfile']):
        with open(args['my_query_outfile'], 'w') as outfile:
            for line in open(queries):
                print(compare_performance.expanded_to_my_syntax(line), file=outfile)
    if (args['bifc_query_outfile']):
        with open(args['bifc_query_outfile'], 'w') as outfile:
            for line in open(queries):
                print(compare_performance.rewrite_to_bif_contains(line), file=outfile)
    if (args['bifc_inc_query_outfile']):
        with open(args['bifc_inc_query_outfile'], 'w') as outfile:
            for line in open(queries):
                print(compare_performance.rewrite_to_bif_contains_inc(line), file=outfile)

if __name__ == '__main__':
    main()
