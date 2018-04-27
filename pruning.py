import sys
import pandas as pd
import util
import heapq

def main():
    if len(sys.argv) != 2:
        print("usage: naive.py <K-value>")
        return
    k = int(sys.argv[1])

    names = ['age', 'workclass', 'fnlwgt', 'education', 'education_num', 'marital_status',
             'occupation', 'relationship', 'race', 'sex', 'capital_gain', 'capital_loss',
             'hours_per_week', 'native_country', 'label']

    datadf = pd.read_csv('adult.data', names=names, delimiter=',')

    dimensions = ['age', 'workclass', 'education', 'education_num', 'marital_status',
                  'occupation', 'relationship', 'race', 'sex', 'native_country']
    measures = ['capital_gain', 'capital_loss', 'hours_per_week']

    functions = ['mean', 'sum', 'max', 'count', 'min']

    # Original question: Ref dataset is Unmarried people
    ref_dataset = datadf[datadf.marital_status == 'Never-married']
    # Original question: Target dataset is Married people
    target_dataset = datadf[datadf.marital_status == 'Married-civ-spouse']

    k_best = []
    heapq.heapify(k_best)
    for a in dimensions:
        for m in measures:
            for f in functions:
                Qt, Qr = util.create_view_query(a, m, ref_dataset, target_dataset, f)
                if Qt is None:
                    continue
                kl = util.kl_score(Qt, Qr)
                if kl < util.pruning_threshold():
                    continue;
                print(a, m, f, kl)
                k_best.append({
                    'a': a,
                    'm': m,
                    'f': f,
                    'utility': kl
                })
                if len(k_best) > k:
                    heapq.heappop(k_best)

    print('%d-best views:' % k)
    print(k_best)


if __name__ == '__main__':
    main()