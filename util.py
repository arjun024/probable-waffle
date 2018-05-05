import psycopg2,math
import pdb 
import pandas.io.sql as sqlio


def kl_score(qt, qr):

	kl = 0
	for k, val in qt.items():
		# TODO: is this right?
		# What to do when qr and qt have different sizes?
		# K-L dist = inf when either qr or qt has a 0
		if k not in qr or qr[k] == 0 or qt[k] == 0:
			continue
		kl += qr[k] * math.log(val / qr[k])
	return -1 * kl

def error_bound(m,N,delta):
	i = (1-(m-1)/N)*(2*math.log2(math.log2(m))+ math.log2(math.pi*math.pi/3/delta))
	return math.sqrt(i/2/m)

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


def create_view_query_wst(a, selects, tableName, mapOfPriors, phase_number): #wst = with specific table

	conn = psycopg2.connect('dbname=%s user=%s password=postgres' % ("adult", "ben"))

	sql_ref = "select %s, %s from %s where marital_status = ' Never-married' group by %s" % (a, selects, tableName, a)
	sql_tar = "select %s, %s from %s where marital_status = ' Married-civ-spouse' group by %s" % (a, selects, tableName, a)

	data_ref = sqlio.read_sql_query(sql_ref, conn)
	data_tar = sqlio.read_sql_query(sql_tar, conn)

	#Extracting the keys here from selects input: 
	
	listofDics = []

	for v in data_ref.columns[1:]:
		#create 2 df 
		dr = data_ref[[a, v]]
		dt = data_tar[[a, v]]
		#Normalize
		ref_sum = float(dr[v].sum())
		target_sum = float(dt[v].sum())
		if not ref_sum or not target_sum:
			continue
		dr[v] = dr[v].apply(lambda x: x/ref_sum)
		dt[v] = dt[v].apply(lambda x: x/target_sum)
		#run kl score
		dt = dt.set_index(a)[v].to_dict()
		dr = dr.set_index(a)[v].to_dict()
		kl = kl_score(dt, dr)
		# check the total for (a,m,f)
		# we add total += kl
		m = v.split("$")[1]
		f = v.split("$")[0]
		listofDics.append({
			'a': a,
			'm': m,
			'f': f,
			'utility': (kl+mapOfPriors[(a,m,f)])/phase_number #AVERAGE UTITILY
			})



	return listofDics, mapOfPriors

