#!/usr/bin/env python

import pandas as pd
import pdb

def create_view_query(a, m, ref_dataset, target_dataset, f='mean'):
	#datadf.groupby(['sex']).mean()['capital-gain']
	result_ref = ref_dataset.groupby([a]).mean()[m]
	result_target = target_dataset.groupby([a]).mean()[m]
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