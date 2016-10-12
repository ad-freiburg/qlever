import compare_performance
import argparse
from random import shuffle
import statistics

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--queryfile',
                    type=str,
                    help='One line per (standard) SPARQL query. TS specific transformations are made by the python script.',
                    required=True)

parser.add_argument('--virtuoso-pwd',
                    type=str,
                    help='Password for the (already running) virtuoso instance',
                    required=True)

parser.add_argument('--all', action='store_true', help='should all be executed?',
                    default=False)
parser.add_argument('--bifc-inc', action='store_true', help='should bifc_inc be executed?',
                    default=False)
parser.add_argument('--mine', action='store_true', help='should mine be executed?',
                    default=False)
parser.add_argument('--rdf3x', action='store_true', help='should rdf3x be executed?',
                    default=False)


def print_result_table(headers, query_to_approach_to_times):
    print('\t'.join(headers))
    print('\t'.join(['---'] * len(headers)))
    for q, att in query_to_approach_to_times.items():
        row = [q]
        for approach in headers[2:]:
            row.append(produce_cell(att[approach]))
        print('\t'.join(row))

def produce_cell(times):
    avg = sum(times) / len(times)
    dev = statistics.stdev(times)
    cell = ' '.join([str(t) for t in times])
    cell += ' | avg = ' + '{0:.2f}'.format(avg) + ' | stdev = ' + '{0:.2f}'.format(dev)
    return cell

def main():
    args = vars(parser.parse_args())
    query_file = args['queryfile']
    queries = []
    nof_permutations = 5
    all = args['all']
    bifc_inc = args['bifc_inc']
    rdf3x = args['rdf3x']
    mine = args['mine']

    for line in open(query_file):
        queries.append(line.strip())
    for permutation_num in range(0, nof_permutations):
        shuffle(queries)
        with open(query_file + '_perm' + str(permutation_num), 'w') as f:
            for q in queries:
                print(q, file=f)

    headers = []
    headers.append('id')
    headers.append('query')
    if all or bifc_inc:
        headers.append('bifc_inc')
    if all or rdf3x:
        headers.append('rdf3x')
    if all or mine:
        headers.append('mine')
    query_to_approach_to_times = {}
    for q in queries:
        query_to_approach_to_times[q] = {}
        for approach in headers[2:]:
            query_to_approach_to_times[q][approach] = []
    for permutation_num in range(0, nof_permutations):
        qfile = query_file + '_perm' + str(permutation_num)
        qs = []
        for line in open(qfile):
            qs.append(line.strip())
        if all or bifc_inc:
            times, matches = compare_performance.get_virtuoso_bifc_inc_query_times(qfile, args['virtuoso_pwd'])
            for i, q in enumerate(qs):
                query_to_approach_to_times[q]['bifc_inc'].append(float(times[i].strip().strip('ms').strip()))
        if all or rdf3x:
            times, matches = compare_performance.get_rdf3X_query_times(qfile)
            for i, q in enumerate(qs):
                if (times[i] != '-'):
                    query_to_approach_to_times[q]['rdf3x'].append(float(times[i].strip().strip('ms').strip()))
                else:
                    query_to_approach_to_times[q]['rdf3x'].append(-1.0)
        if all or mine:
            times, matches = compare_performance.get_my_query_times(qfile)
            for i, q in enumerate(qs):
                query_to_approach_to_times[q]['mine'].append(float(times[i].strip().strip('ms').strip()))
    print_result_table(headers, query_to_approach_to_times)


if __name__ == '__main__':
    main()
