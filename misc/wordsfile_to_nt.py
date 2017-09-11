import argparse

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--wordsfile',
                    type=str,
                    help='Wordsfile input.',
                    required=True)

def escapeUriStuff(word):
    new = ""
    for c in word:
        if c == '<':
            new += '&lt'
        elif c == '>':
            new += '&gt'
        else:
            new += c
    return new

def writeNtToStdout(wordsfile):
    for line in open(wordsfile):
        (word, entityFlag, cid, score) = line.strip('\n').split('\t')
        if entityFlag == "1":
            print('\t'.join(["<context:" + cid + ">", "<contains-entity>", word, "."]))
        else:
            print('\t'.join(["<context:" + cid + ">", "<contains-word>", "<word:" +
                escapeUriStuff(word) + ">",'.']))


def main():
    args = vars(parser.parse_args())
    wordsfile = args['wordsfile']
    writeNtToStdout(wordsfile)


if __name__ == '__main__':
    main()
