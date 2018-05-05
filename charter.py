import numpy as np
from matplotlib import pyplot as plt
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
import pdb
import math

def bar_chart(a, m, f):
	#Do SQL to query for group by a, select f(m) from D_married/D_unmarried
	sql_ref = "select %s, %s(%s) from %s where marital_status = ' Never-married' group by %s" % (a, f, m, "adult", a)
	sql_tar = "select %s, %s(%s) from %s where marital_status = ' Married-civ-spouse' group by %s" % (a, f, m, "adult", a)

	conn = psycopg2.connect('dbname=%s user=%s password=postgres' % ("adult", "ben"))
	data_ref = sqlio.read_sql_query(sql_ref, conn)
	data_tar = sqlio.read_sql_query(sql_tar, conn)

	# pdb.set_trace()
	# df2 = pd.concat([data_ref.T, data_tar.T])
	# pdb.set_trace()
	# plot = df2.plot.bar()
	# print(data_ref)
	# fig = plot.get_figure()
	# fig.savefig("gtgtg")

	x = data_ref[a]
	print(x)
	x2 = data_tar[a]
	print(x2)


	x = [ii for ii in set(data_ref[a].values.tolist()).union(set(data_tar[a].values.tolist()))]
	# xx = [iii for iii in range(len(x))]
	xx = np.arange(len(x))
	y_ref = []
	for x_v in x:
		if(x_v not in data_ref[a].values.tolist()):
			y_ref.append(0)
		else:
			# pdb.set_trace()
			y_ref.append(data_ref[data_ref[a]==x_v][f].values[0])

	y_tar = []
	for x_v in x:
		if(x_v not in data_tar[a].values.tolist()):
			y_tar.append(0)
		else:
			y_tar.append(data_tar[data_tar[a]==x_v][f].values[0])

	plt.figure()		
	ax = plt.subplot(111)
	ax.bar(xx-0.15, [math.log(i) if i!= 0 else i for i in y_ref], width=0.3, color='b', align='center')
	ax.bar(xx+0.15, [math.log(i) if i!=0 else i for i in y_tar], width=0.3,color='r', align='center')
	ax.set_xticklabels([" "] + x, rotation=40)
	plt.legend(["Unmarried", "Married"])
	plt.xlabel(a)
	plt.ylabel("log(%s(%s))"%(f,m))
	plt.title("%s(%s) vs %s" % (f,m,a))
	plt.tight_layout()
	plt.savefig("%s_%s_%s" % (a,m,f))



# if __name__ == '__main__':
# 	bar_chart("relationship", "capital_gain", "sum")
