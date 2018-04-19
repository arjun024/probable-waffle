#!/usr/bin/env python

import pandas as pd
import pdb
import math

def kl_score(Qt, Qr):
	qt = dict(Qt)
	qr = dict(Qr)
	kl = 0
	for key in qt:
		kl += qr[key] * math.log(qt[key] / qr[key])
	return -1*kl


def create_view_query(a, m, ref_dataset, target_dataset, f='mean'):
	#datadf.groupby(['sex']).mean()['capital-gain']
	result_ref = ref_dataset.groupby([a]).mean()[m].to_frame()
	result_target = target_dataset.groupby([a]).mean()[m].to_frame()

	# Normalize to a probability distribution (i.e. the values of f(m) sum to 1)
	result_ref[m] = result_ref[m].apply(lambda x: x/result_ref[m].sum())
	result_target[m] = result_target[m].apply(lambda x: x/result_target[m].sum())

	distance = kl_score(result_target, result_ref)
	pdb.set_trace()
	


def main():
	names = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status",
	"occupation", "relationship", "race", "sex", "capital_gain", "capital_loss",
	"hours_per_week", "native_country", "label"]

	datadf = pd.read_csv('adult.data', names=names, delimiter=', ')
	a = 'sex'
	m = 'capital_gain'
	f = 'mean'

	# Original question: Ref dataset is Unmarried people
	ref_dataset = datadf[datadf.marital_status == 'Never-married']
	# Original question: Target dataset is Married people
	target_dataset = datadf[datadf.marital_status == 'Married-civ-spouse']

	create_view_query(a, m, ref_dataset, target_dataset, f)
	

if __name__ == '__main__':
	main()