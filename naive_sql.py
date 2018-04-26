#!/usr/bin/env python

import pandas as pd
import pdb
import math
import sys
import psycopg2
import pandas.io.sql as sqlio

DB_NAME = 'seedb'
USER_NAME = 'arjun'
TABLE_NAME = 'adult'

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


def create_view_query(a, m, f):
	conn = psycopg2.connect('dbname=%s user=%s password=postgres' % (DB_NAME, USER_NAME))

	sql_ref = "select %s, %s(%s) from %s where marital_status = 'Never-married' group by %s" % (a, f, m, TABLE_NAME, a)
	sql_tar = "select %s, %s(%s) from %s where marital_status = 'Married-civ-spouse' group by %s" % (a, f, m, TABLE_NAME, a)

	data_ref = sqlio.read_sql_query(sql_ref, conn)
	data_tar = sqlio.read_sql_query(sql_tar, conn)

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

	ref_sum = float(data_ref[f].sum())
	target_sum = float(data_tar[f].sum())
	if not ref_sum or not target_sum:
		return None, None
	data_ref[f] = data_ref[f].apply(lambda x: x/ref_sum)
	data_tar[f] = data_tar[f].apply(lambda x: x/target_sum)
	return (data_tar.set_index(a)[f].to_dict(), data_ref.set_index(a)[f].to_dict())


	


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

	functions = ['avg', 'sum', 'max', 'count', 'min']

	# Original question: Ref dataset is Unmarried people
	#ref_dataset = datadf[datadf.marital_status == 'Never-married']
	# Original question: Target dataset is Married people
	#target_dataset = datadf[datadf.marital_status == 'Married-civ-spouse']

	k_best = []
	for a in dimensions:
		for m in measures:
			for f in functions:
				Qt, Qr = create_view_query(a, m, f)
				if Qt is None:
					continue
				kl = kl_score(Qt, Qr)
				print(a, m, f, kl)
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

	print('%d-best views:' % k)
	print(k_best)
	

if __name__ == '__main__':
	main()