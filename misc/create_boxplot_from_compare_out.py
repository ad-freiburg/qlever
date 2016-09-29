import argparse
import seaborn as sns
import pandas as pd

__author__ = 'buchholb'


parser = argparse.ArgumentParser()

parser.add_argument('--times-file',
                    type=str,
                    help='Output of the compare_performance.py script',
                    required=True)


def get_data_frame_from_input_file(in_file):
    label_to_id_val_dict = {}
    col_id_to_label = {}
    first_line = True
    for line in open(in_file):
        if first_line:
            labels = line.strip().split('\t')
            label_to_id_val_dict['type'] = {}
            for i, l in enumerate(labels):
                col_id_to_label[i] = l
                if i >= 2 and i % 2 == 0:
                    label_to_id_val_dict[l] = {}
            first_line = False
        else:
            if line.startswith('--'):
                continue
            vals = line.strip().split('\t')
            for i, v in enumerate(vals):
                type = vals[0].split('_')[0]
                label_to_id_val_dict['type'][vals[0]] = type
                if i >= 2 and i % 2 == 0 and v != '-':
                    l = col_id_to_label[i]
                    label_to_id_val_dict[l][vals[0]] = v.strip().rstrip('ms').rstrip()
    return pd.DataFrame(label_to_id_val_dict)


def main():
    args = vars(parser.parse_args())
    in_file = args['times_file']
    df = get_data_frame_from_input_file(in_file)
    print(df)
    ax = sns.boxplot(df, x=df.type)
    ax.set(yscale="log")
    ax.figure.savefig(in_file + '.boxplot.png')


if __name__ == '__main__':
    main()
