import sys


def fix(element):
  if element and element[0] == '<':
    new = '<'
    for c in element[1:-1]:
      if c == '<':
        new += '&lt'
      elif c == '>':
        new += '&gt'
      else:
        new += c
    new += '>'
    return new
  else:
    return element

for line in open(sys.argv[1]):
  cols = line.strip().split('\t')
  if len(cols) != 4 or cols[3] != '.':
    sys.err.write('Ignoring malformed line: ' + line)
  else:
    # for now only touch subject for efficiency reasons.
    cols[0] = fix(cols[0])
    print '\t'.join(cols)
