import argparse

__author__ = 'buchholb'

parser = argparse.ArgumentParser()

parser.add_argument('--wordsfile',
                    type=str,
                    help='Wordsfile input.',
                    required=True)

parser.add_argument('--incontext',
                    help='Externalize contexts with extra relation?',
                    action='store_true',
                    default=False)


def write_nt_to_stdout(cid, entities, words, incontext):
    words.append('"')
    if incontext:
        print('\t'.join(
            ['<context:' + cid + '>', '<text>', ' '.join(words), '.']))
        for e in entities:
            print('\t'.join([e, '<in-text>', '<context:' + cid + '>', '.']))
    else:
        context_lit = ' '.join(words)
        for e in entities:
            print('\t'.join([e, '<text>', context_lit, '.']))


def transform(wordsfile, incontext):
    current_c = '0'
    entities = []
    words = ['"']
    for line in open(wordsfile):
        (word, entityFlag, cid, score) = line.strip('\n').split('\t')
        if cid != current_c:
            write_nt_to_stdout(current_c, entities, words, incontext)
            current_c = cid
            entities = []
            words = ['"']
        if entityFlag == "1":
            entities.append(word)
        else:
            words.append(word.replace('"', '').replace("'", ""))
    write_nt_to_stdout(current_c, entities, words, incontext)


def main():
    args = vars(parser.parse_args())
    wordsfile = args['wordsfile']
    incontext = args['incontext']
    transform(wordsfile, incontext)


if __name__ == '__main__':
    main()
