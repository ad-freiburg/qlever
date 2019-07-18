#!/usr/bin/env python3

import sqlite3
import sys
import os

_DBFILE = ".jointestdb"

class Settings:
  def __init__(self):
    self.filename1 = ''
    self.filename2 = ''
    self.resultfile = ''
    self.jc1 = 0
    self.jc2 = 0

class TsvTable:
  def __init__(self):
    self.data = []

  def from_file(self, path):
    with open(path, 'r') as f:
      size = None
      for line in f:
        parts = [s.strip() for s in line.split('\t')]
        if size is None:
          size = len(parts)
        elif len(parts) != size:
          raise Exception("The tsv file has a row of the wrong length " + line)
        self.data.append(parts)

  def to_file(self, path):
    with open(path, 'w') as f:
      for row in self.data:
        print('\t'.join(row), file=f)


class Database:
  def __init__(self, path):
    if os.path.exists(path):
      os.unlink(path)
    self._conn = sqlite3.connect(path)
  
  def add_table(self, name, data):
    """
    data ([[String]]) : A table of strings
    """
    fields = "(" + (', '.join(['f' + str(i) + ' text' for i in range(0, len(data[0]))])) + ')'
    self._conn.execute('CREATE TABLE ' + name + ' ' + fields)
    vals_template = "( " + (', '.join(['?' for s in data[0]])) + ")"
    for row in data:
      self._conn.execute('INSERT INTO ' + name + ' VALUES ' + vals_template, tuple(row))
    self._conn.commit()
  
  def join(self, t1, t2, jc1, jc2):
    c = self._conn.cursor()
    jc1n = 'f' + str(jc1)
    jc2n = 'f' + str(jc2)
    c.execute('SELECT * FROM ' + t1 + ' JOIN ' + t2 + ' ON ' + t1 + '.' + str(jc1n) + ' = ' + t2 + '.' + str(jc2n));
    return c.fetchall()

def print_help():
  print('Usage:', sys.argv[0], '<table1> <table2> <out> <jc1 index> <jc2 index>')
  print('Creates sqlite tables from tsv and joins them')

def parse_cmd():
  s = Settings()
  if len(sys.argv) != 6:
    print('Wrong number of arguments')
    print_help()
    sys.exit(1)
  s.filename1 = sys.argv[1]
  s.filename2 = sys.argv[2]
  s.resultfile = sys.argv[3]
  try:
    s.jc1 = int(sys.argv[4])
    s.jc2 = int(sys.argv[5])
  except ValueError as e:
    print(e)
    print('The join columns have to be indices')
    print_help()
    sys.exit(1)
  return s


if __name__ == '__main__':
  settings = parse_cmd()
  db = Database(_DBFILE)
  t1 = TsvTable()
  t1.from_file(settings.filename1)
  t2 = TsvTable()
  t2.from_file(settings.filename2)
  db.add_table('t1', t1.data)
  db.add_table('t2', t2.data)
  resdata = db.join('t1', 't2', settings.jc1, settings.jc2)
  res = TsvTable()
  res.data = resdata
  print(str(res.data))
  res.to_file(settings.resultfile)

