#!/usr/bin/env python

import pandas as pd
import pdb
import math

def kl_score(qt, qr):
	kl = 0
	i = 0
	for val in qt:
		kl += qr[i] * math.log(val / qr[i])
		i += 1
	return -1 * kl


def create_view_query(a, m, ref_dataset, target_dataset, f='mean'):
	#datadf.groupby(['sex']).mean()['capital-gain']
	result_ref = ref_dataset.groupby([a]).mean()[m].to_frame()
	result_target = target_dataset.groupby([a]).mean()[m].to_frame()

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

	result_ref[m] = result_ref[m].apply(lambda x: x/result_ref[m].sum())
	result_target[m] = result_target[m].apply(lambda x: x/result_target[m].sum())

	return (result_target[m].to_frame()[m], result_ref[m].to_frame()[m])


	


def main():
	names = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status",
	"occupation", "relationship", "race", "sex", "capital_gain", "capital_loss",
	"hours_per_week", "native_country", "label"]

	datadf = pd.read_csv('adult.data', names=names, delimiter=", ")
	a = 'sex'
	m = 'capital_gain'
	f = 'mean'

	# Original question: Ref dataset is Unmarried people
	ref_dataset = datadf[datadf.marital_status == 'Never-married']
	# Original question: Target dataset is Married people
	target_dataset = datadf[datadf.marital_status == 'Married-civ-spouse']

	Qt, Qr = create_view_query(a, m, ref_dataset, target_dataset, f)
	kl = kl_score(Qt, Qr)
	pdb.set_trace()
	# print("kl score is " + str(kl))
	

if __name__ == '__main__':
	main()