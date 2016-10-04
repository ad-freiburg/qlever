import argparse
import seaborn as sns
import pandas as pd
import numpy as np

__author__ = 'buchholb'


parser = argparse.ArgumentParser()

parser.add_argument('--times-file',
                    type=str,
                    help='Output of the compare_performance.py script',
                    required=True)


def get_data_frame_from_input_file(in_file):
    data = {'type': {}, 'approach': {}, 'time in ms': {}, 'nof matches': {}}
    col_id_to_label = {}
    first_line = True
    id = 0
    for line in open(in_file):
        if first_line:
            labels = line.strip().split('\t')
            for i, l in enumerate(labels):
                col_id_to_label[i] = l
            first_line = False
        else:
            if line.startswith('--'):
                continue
            vals = line.strip().split('\t')
            type = vals[0].split('_')[0]
            time = ""
            approach = ""
            for i in range(2, len(vals)):
                if i % 2 == 0:
                    if vals[i] == '-':
                        time = np.nan
                    else:
                        time = vals[i].strip().rstrip('ms').rstrip()
                    approach = col_id_to_label[i]
                else:
                    nof_matches = vals[i]
                    data['type'][id] = type
                    data['approach'][id] = approach
                    data['time in ms'][id] = float(time)
                    data['nof matches'][id] = int(nof_matches)
                    id += 1
    return pd.DataFrame(data)


def main():
    args = vars(parser.parse_args())
    in_file = args['times_file']
    df = get_data_frame_from_input_file(in_file)
    print(df)
    ax = sns.boxplot(x='type', y='time in ms', hue='approach', data=df)
    ax.set(yscale="log")
    ax.figure.savefig(in_file + '.boxplot.png')


if __name__ == '__main__':
    main()
