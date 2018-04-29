import sys
import pdb
import psycopg2
import math
import pandas.io.sql as sqlio
import util
import heapq

#Input the name of the full dataset
#And a psycopg2 connection conn
#Will take the whole table and partition the data into n subsets by creating n views
#Output an array of view names
def partitioner(fullTable, conn, n):
	sql = "select count(*) from adult;"
	df = sqlio.read_sql_query(sql, conn)
	totalRows = df["count"].values[0]
	size = math.floor(totalRows/n)
	leftOver = totalRows%n
	index = []
	print("size", size)
	for i in range(n):
		print(leftOver, i)
		if leftOver > i:
			if i != 0:
				end = index[i-1][1]
				index.append((1+end, 1 + end+size))
			else: 
				index.append((1, size+1))
		else: 
			if i != 0:
				end = index[i-1][1]
				index.append((1+end, end+size))
			else:
				index.append((1, size))

	allViews = [] 
	for i in range(n):
		sql = "create view part_" + str(i) + " as select * from adult where id between " + str(index[i][0]) + " and " + str(index[i][1]) + ";" 
		# print(sql)
		# sqlio.read_sql_query(sql, conn)
		allViews.append("part_"+str(i))

	return allViews



#runs the naive algorithm with optimizations, but doesnt run on tuples given in the rejection_list
#does pruning and KL score to filter out unlikely candidates for next round
#will query using sharing optimizations
def optimized_runner(rejectSet, tableName):
	dimensions = ['age', 'workclass', 'education', 'education_num', 'marital_status',
		'occupation', 'relationship', 'race', 'sex', 'native_country']

	measures = ['capital_gain', 'capital_loss', 'hours_per_week']

	functions = ['avg', 'sum', 'max', 'count', 'min']

	k_best = []
	heapq.heapify(k_best)
	newRejections = set() #set of tuples : (a,m,f)
	selects = []
	for a in dimensions:

		for m in measures:
			for f in functions:
				if (a,m,f) not in rejectSet:
					selects.append("{}({})".format(f,m))

		select = ",".join(selects)
		pdb.set_trace()
		Qt, Qr = util.create_view_query_wst(a, selects, tableName)
		kl = kl_score(Qt, Qr)
		k_best.append({
		'a': a,
		'm': m,
		'f': f,
		'utility': kl
		})
		# make sure k-best is stored
		#TODO:  Do we want to be preforming a sort at every iteration???
		k_best = sorted(k_best, key=lambda x: x['utility'], reverse=True)
		if len(k_best) > k:
			heapq.heappop(k_best)
		#TODO : PRUNING CALCULATION - ADD TO newRejections



	# TODO: Add worst KL scores (by some metric of worst?) to newRejections
	return k_best, newRejections



#Performs the n phase transitions. 
#Using C.I., some aggregrate views will be discarded
#The KL score is calculated for the subset on each query pair
#The ones with "low(?)" utility will be removed from later iterations
#output: Top k query, at the end of the n phases. 
def phaser(fullTable, conn, k, allViews):

	rejectSet = set() #set of tuples : (a,m,f)

	for i in range(len(allViews)):
		tableName = "part_" + str(i)
		top_k, rL = optimized_runner(rejectSet, tableName)
		rejectSet = rejectSet.union(rL)

	return top_k






def main():
	conn = psycopg2.connect("dbname=adult user=ben password=postgres")
	k = int(sys.argv[1])
	n = int(sys.argv[2])
	allViews = partitioner("adult", conn, n)
	# print(allViews)
	tok_k = phaser("adult", conn, k, allViews)


if __name__ == '__main__':
	main()

