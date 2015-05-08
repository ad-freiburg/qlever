import argparse
import sys

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--docsfile',
    type=str,
    help='Broccoli Docsfile.',
    required=True)

def writeOrigTextFileToStdout(wordsfile):
  lastCid = ''
  currentText = ''
  for line in open(wordsfile):
    cols = line.strip('\n').split('\t')
    cid = cols[0]
    text = cols[3].replace('@@', '')
    if currentText and currentText != text:
      print "\t".join([lastCid, currentText])
    currentText = text
    lastCid = cid


def main():
  args = vars(parser.parse_args())
  docsfile = args['docsfile']
  writeOrigTextFileToStdout(docsfile)


if __name__ == '__main__':
  main()
