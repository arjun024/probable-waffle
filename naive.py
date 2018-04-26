#!/usr/bin/env python

import pandas as pd
import pdb
import math
import sys

def kl_score(qt, qr):
	kl = 0
	for k, val in qt.iteritems():
		# TODO: is this right?
		# What to do when qr and qt have different sizes?
		# K-L dist = inf when either qr or qt has a 0
		if k not in qr or qr[k] == 0 or qt[k] == 0:
			continue
		kl += qr[k] * math.log(val / qr[k])
	return -1 * kl


def create_view_query(a, m, ref_dataset, target_dataset, f):
	result_ref = getattr(ref_dataset.groupby([a]), f)()[m].to_frame()
	result_target = getattr(target_dataset.groupby([a]), f)()[m].to_frame()

	# Normalize to a probability distribution (i.e. the values of f(m) sum to 1)
	# E.g.
	# result_ref
	# sex         capital_gain          
	# Female      0.441264
	# Male        0.558736
	#
	# result_target
	# sex         capital_gain             
	# Female      0.474307
	# Male        0.525693

	ref_sum = float(result_ref[m].sum())
	target_sum = float(result_target[m].sum())
	if not ref_sum or not target_sum:
		return None, None
	result_ref[m] = result_ref[m].apply(lambda x: x/ref_sum)
	result_target[m] = result_target[m].apply(lambda x: x/target_sum)
	return (result_target[m].to_frame()[m], result_ref[m].to_frame()[m])


	


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
	for a in dimensions:
		for m in measures:
			for f in functions:
				print(a, m, f)
				Qt, Qr = create_view_query(a, m, ref_dataset, target_dataset, f)
				if Qt is None:
					continue
				kl = kl_score(Qt, Qr)
				k_best.append({
					'a': a,
					'm': m,
					'f': f,
					'utility': kl
					})
				# make sure k-best is stored
				k_best = sorted(k_best, key=lambda x: x['utility'], reverse=True)
				if len(k_best) > k:
					k_best.pop()

	# pdb.set_trace()
	print('%d-best views:' % k)
	print(k_best)
	

if __name__ == '__main__':
	main()