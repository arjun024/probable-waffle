import psycopg2

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

def pruning_threshold():
    return 0;

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


def create_view_query_wst(a, selects, tableName): #wst = with specific table

	conn = psycopg2.connect('dbname=%s user=%s password=postgres' % (DB_NAME, USER_NAME))

	sql_ref = "select %s, %s from %s where marital_status = 'Never-married' group by %s" % (a, selects, tableName, a)
	sql_tar = "select %s, %s from %s where marital_status = 'Married-civ-spouse' group by %s" % (a, selects, tableName, a)

	data_ref = sqlio.read_sql_query(sql_ref, conn)
	data_tar = sqlio.read_sql_query(sql_tar, conn)

	#Extracting the keys here from selects input: 
	keys = []
	for k in selects.split(","):
		keys.append( (k.split("(")[0], k.split("(")[1][:-1]) ) 


	#Todo: Distribution calculation will need to be recalculated. 
	#Perhaps output is a map of key (f,m) :> (Qt,Qr) <distibutions>  
	ref_sum = float(data_ref[f].sum())
	target_sum = float(data_tar[f].sum())
	if not ref_sum or not target_sum:
		return None, None
	data_ref[f] = data_ref[f].apply(lambda x: x/ref_sum)
	data_tar[f] = data_tar[f].apply(lambda x: x/target_sum)
	return (data_tar.set_index(a)[f].to_dict(), data_ref.set_index(a)[f].to_dict())

