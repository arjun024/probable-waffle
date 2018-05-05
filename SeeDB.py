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
	cur = conn.cursor()
	totalRows = 32561
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
		sql = "create view part_{} as (select * from adult where id between {} and {});".format(str(i), str(index[i][0]), str(index[i][1]) ) 
		print(sql)
		# cur.execute(sql)
		allViews.append("part_"+str(i))

	conn.commit()
	cur.close()
	conn.close()

	return allViews , size, totalRows



#runs the naive algorithm with optimizations, but doesnt run on tuples given in the rejection_list
#does pruning and KL score to filter out unlikely candidates for next round
#will query using sharing optimizations
def optimized_runner(rejectSet, tableName, eb, k):
	dimensions = ['age', 'workclass', 'education', 'education_num', 'marital_status',
		'occupation', 'relationship', 'race', 'sex', 'native_country']

	measures = ['capital_gain', 'capital_loss', 'hours_per_week']

	functions = ['avg', 'sum', 'max', 'count', 'min']

	newRejections = set() #set of tuples : (a,m,f)
	listOfDics = []
	for a in dimensions:
		selects = []

		for m in measures:
			for f in functions:
				if (a,m,f) not in rejectSet:
					selects.append("{}({}) as {}${}".format(f,m,f,m))

		select = ",".join(selects)
		listOfDics.extend(util.create_view_query_wst(a, select, tableName))
	
	listOfDics = sorted(listOfDics, key=lambda x: x['utility'], reverse=True)
	#Find worst of best k
	worstOfBest = listOfDics[k-1]
	#Calculate error bound
	#called eb
	#Find threshold
	threshold = worstOfBest['utility']
	#Calculate all Upperbounds of each View Query 
	for d in listOfDics[k:]:
		if d['utility'] < threshold-2*eb:
			newRejections.add((d['a'], d['m'], d['f']))


	# TODO: Add worst KL scores (by some metric of worst?) to newRejections
	return listOfDics, newRejections



#Performs the n phase transitions. 
#Using C.I., some aggregrate views will be discarded
#The KL score is calculated for the subset on each query pair
#The ones with "low(?)" utility will be removed from later iterations
#output: Top k query, at the end of the n phases. 
def phaser(fullTable, conn, k, allViews, N, m):

	rejectSet = set() #set of tuples : (a,m,f)

	for i in range(len(allViews)):
		tableName = "part_" + str(i)
		eb = util.error_bound(m, N, 0.05)
		top_k, rL = optimized_runner(rejectSet, tableName, eb, k)
		rejectSet = rejectSet.union(rL)

	return top_k[:k]






def main():
	conn = psycopg2.connect("dbname=adult user=ben password=postgres")
	# conn = con.cursor()
	k = int(sys.argv[1])
	n = int(sys.argv[2])
	allViews, m, N= partitioner("adult", conn, n)
	print(allViews)
	top_k = phaser("adult", conn, k, allViews, N, m)
	print(top_k)

def main2():
	conn = psycopg2.connect("dbname=adult user=postgres password=benjamin")
	# conn = con.cursor()
	k = int(sys.argv[1])
	num_part = int(sys.argv[2])
	allRuns = []
	uu = 1
	for n in range(20, num_part):
		for alpha in [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1]:
			# times = 0
			# for i in range(3):	
			startTime = time.time()
			allViews, m, N= partitioner("adult", conn, n)
			# print(allViews)
			top_k = phaser("adult", conn, k, allViews, N, m, alpha)
			#Call Vizualizer function here: 
			#Todo: Vizualization. 
			#Remove each allViews
			cur = conn.cursor()
			for v in allViews:
				cur.execute("DROP VIEW %s;" % (v))
			conn.commit()
			# print(time.time() - startTime, alpha, n)
			# times += time.time() - startTime
			# allRuns.append((times/3, alpha, n))	
			allRuns.append((startTime-time.time(), alpha, n))
			# print(top_k)
			if uu % 5 == 0:
				print(uu/200)
			uu+=1

	# pdb.set_trace()
	allRuns = sorted(allRuns, key=lambda x: x[0])
	print(allRuns[:50])
	pdb.set_trace()
	conn.close()



if __name__ == '__main__':
	main()

