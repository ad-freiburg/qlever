import sys

q_to_time = {}

i = 0
for line in open(sys.argv[1]):
    try:
        line = line.strip()
        cols = line.split('\t')
        q_to_time[cols[0]] = [(int(cols[2].split(' ')[0]), i)]
        i += 1
    except ValueError:
        continue

i = 0
for line in open(sys.argv[2]):
    try:
        line = line.strip()
        cols = line.split('\t')
        q_to_time[cols[0]].append((int(cols[2].split(' ')[0]), i))
        i += 1
    except KeyError:
        continue
    except ValueError:
        continue

for k,v in q_to_time.items():
        if v[0][0] < v[1][0]:
            smaller = float(v[0][0])
            larger = float(v[1][0])
        else:
            smaller = float(v[1][0])
            larger = float(v[0][0])
        try:
            if (larger / smaller > 2):
                print('SIGNIFICANT DIFFERENCE: ' + k + ':  (' + str(v[0][0]) + ', ' +
                        str(v[0][1]) + ') vs (' + str(v[1][0]) + ', ' + str(v[1][1])
                        + ').')
                print(' -> FACTOR: ' + str(larger / smaller))
        except:
            print('problem with : ' + k + ' ' + str(larger) + ' ' + str(smaller))
