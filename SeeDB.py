import sys
import pdb
import psycopg2
import math
import pandas.io.sql as sqlio

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




#Performs the n phase transitions. 
#Using C.I., some aggregrate views will be discarded
#The KL score is calculated for the subset on each query pair
#The ones with "low(?)" utility will be removed from later iterations
#output: Top k query, at the end of the n phases. 
def phaser(fullTable, conn, k, allViews):
	




def main():
	conn = psycopg2.connect("dbname=adult user=ben password=postgres")
	k = int(sys.argv[1])
	n = int(sys.argv[2])
	allViews = partitioner("adult", conn, n)
	# print(allViews)
	tok_k = phaser("adult", conn, k, allViews)


if __name__ == '__main__':
	main()

