import argparse
import sys
import os
import subprocess
from subprocess import Popen

__author__ = 'buchholb'

virtuoso_run_binary = "/home/buchholb/virtuoso/bin/isql"
virtuoso_isql_port = "1111"
virtuso_isql_user = "dba"
rdf3x_run_binary = "/home/buchholb/rdf3x-0.3.8/bin/rdf3xquery"
rdf3x_db = "/local/scratch/bjoern/data/rdf3x/fulldb"
my_binary_old = "/local/scratch/bjoern/work/SparqlEngineMainOld"
my_index_old = "/local/scratch/bjoern/data/testfulltext"
my_index = "/local/scratch/bjoern/data/full-with-text.25jan2016"
my_binary = "/local/scratch/bjoern/work/tmp/SparqlEngineMain"

parser = argparse.ArgumentParser()

parser.add_argument('--queryfile',
                    type=str,
                    help='One line per (standard) SPARQL query. TS specific transformations are made by the python script.',
                    required=True)

parser.add_argument('--virtuoso-pwd',
                    type=str,
                    help='Password for the (already running) virtuoso instance',
                    required=True)

def expanded_to_my_syntax(q):
  before_where, after_where = q.split('WHERE')
  no_mod, mod = after_where.strip().split('}')
  clauses = no_mod.strip('{').split('.')
  new_clauses = []
  context_to_words = {}
  for c in clauses:
    if '<word:' in c:
      s, p, o = c.strip().split(' ')
      if o not in context_to_words:
        context_to_words[o] = []
      context_to_words[o].append(s[6 : -1])
    else:
      new_clauses.append(c)
  for c, ws in context_to_words.iteritems():
    new_clauses.append(' ' + c + ' <in-context> ' + ' '.join(ws) + ' ')
  new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
  return 'WHERE'.join([before_where, new_after_where])

def rewrite_filter_for_rdf3x(q):
  before_where, after_where = q.split('WHERE')
  no_mod, mod = after_where.strip().split('}')
  clauses = no_mod.strip('{').split('.')
  new_clauses = []
  for c in clauses:
    if 'FILTER' in c and '!=' in c:
      e1 = c[c.find('?') : c.find('!=')].strip()
      e2 = c[c.find('!=') + 2 : c.find(')')].strip()
      new_clauses.append(' FILTER(!(' + e1 + '=' + e2 + ')) ')
    else:
      new_clauses.append(c)
  if 'OFFSET' in mod:
    sys.stderr.write('WARNING: IGNORING OFFSET CLAUSE FOR RDF3X!\n')
    tokens = mod.split(' ')
    mod = ''
    i = 0
    while i < len(tokens):
      if tokens[i] == 'OFFSET':
        i += 2
      else:
        mod += tokens[i] + ' '
        i += 1
  new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
  return 'WHERE'.join([before_where, new_after_where])

def get_virtuoso_query_times(query_file, pwd):
  with open('__tmp.virtqueries', 'w') as tmpfile:
    for line in open(query_file):
      tmpfile.write('SPARQL ' + line.strip().split('\t')[1] + ';\n')
  virtout = subprocess.check_output([virtuoso_run_binary, virtuoso_isql_port, virtuso_isql_user, pwd, '__tmp.virtqueries'])
  results = []
  print 'virtuoso output lines: ' + str(len(virtout.split('\n')))
  for line in virtout.split('\n'):
    i = line.find('Rows. -- ')
    if i >= 0:
      j = line.find('msec')
      results.append(line[i + 9 : j + 2])
  #os.remove('__tmp.virtqueries')
  return results

def get_rdf3X_query_times(query_file):
  with open('__tmp.rdf3xqueries', 'w') as tmpfile:
    for line in open(query_file):
      tmpfile.write(rewrite_filter_for_rdf3x(line.strip().split('\t')[1]) + '\n')
  rdf3xout = subprocess.check_output([rdf3x_run_binary, rdf3x_db, '__tmp.rdf3xqueries'])
  print 'rdf3x output lines: ' + str(len(rdf3xout.split('\n')))
  results = []
  for line in rdf3xout.split('\n'):
    i = line.find('Done. Time: ')
    if i >= 0:
      j = line.find('ms')
      results.append(line[i + 12 : j + 2])
  #os.remove('__tmp.rdf3xqueries')
  return results
  
def get_my_query_times_old(query_file):
  with open('__tmp.myqueries', 'w') as tmpfile:
    for line in open(query_file):
      tmpfile.write(expanded_to_my_syntax(line.strip().split('\t')[1]) + '\n')
  myout = subprocess.check_output([my_binary_old, '-i', my_index_old, '-t', '--queryfile', '__tmp.myqueries'])
  print 'my (old) output lines: ' + str(len(myout.split('\n')))
  results = []
  for line in myout.split('\n'):
    i = line.find('Done. Time: ')
    if i >= 0:
      j = line.find('ms')
      results.append(line[i + 12 : j + 2])
  #os.remove('__tmp.myqueries')
  return results

def get_my_query_times(query_file):
  with open('__tmp.myqueries', 'w') as tmpfile:
    for line in open(query_file):
      tmpfile.write(expanded_to_my_syntax(line.strip().split('\t')[1]) + '\n')
  myout = subprocess.check_output([my_binary, '-i', my_index, '-t', '--queryfile', '__tmp.myqueries'])
  print 'my output lines: ' + str(len(myout.split('\n')))
  results = []
  for line in myout.split('\n'):
    i = line.find('Done. Time: ')
    if i >= 0:
      j = line.find('ms')
      results.append(line[i + 12 : j + 2])
  #os.remove('__tmp.myqueries')
  return results

def processQueries(query_file, pwd):
  queries = []
  for line in open(query_file):
    queries.append(line.strip())
  virtuoso_times = get_virtuoso_query_times(query_file, pwd)
  rdf3x_times = get_rdf3X_query_times(query_file)
  my_times = get_my_query_times(query_file)
  my_times_old = get_my_query_times_old(query_file)
  return queries, virtuoso_times, rdf3x_times, my_times, my_times_old


def print_result_table(queries, virtuoso_times, rdf3x_times, my_times, my_times_old):
  assert len(queries) == len(virtuoso_times)
  assert len(queries) == len(rdf3x_times)
  assert len(queries) == len(my_times)
  assert len(queries) == len(my_times_old)
  print "\t".join(['id', 'query', 'virtuoso', 'rdf3x', 'mine', 'mine_old'])
  print "\t".join(['----', '-----', '-----', '-----', '-----', '-----'])
  for i in range(0, len(queries)):
    print "\t".join([queries[i], virtuoso_times[i], rdf3x_times[i], my_times[i], my_times_old[i]])



def main():
  args = vars(parser.parse_args())
  queries = args['queryfile']
  queries, virtuoso_times, rdf3x_times, my_times, my_times_old = processQueries(queries, args['virtuoso_pwd'])
  print_result_table(queries, virtuoso_times, rdf3x_times, my_times, my_times_old)


if __name__ == '__main__':
  main()
