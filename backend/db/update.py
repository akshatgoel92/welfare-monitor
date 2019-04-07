#----------------------------------------#
# Check if a given table is empty
# MySQL uses an arbitrary index 
# Exists return 1 if the row exists else 0
# We take that and store it in not_empty	
#----------------------------------------#
def check_table_empty(conn, table):

	result = pd.read_sql('SELECT EXISTS ' + '(SELECT 1 FROM ' + table + ')', con = conn)

	return(1 - result.iloc[0, 0])


#-----------------------------------------------#
# Return only those entries in df_1 not in df_2 	
#-----------------------------------------------#
def anti_join(df_1, df_2, on):

	df = pd.merge(df_1, df_2, how = 'outer', on = on, indicator = True)
	df = df.loc[df['_merge'] == 'left_only']
	df.drop(['_merge'], inplace = True, axis = 1)

	return(df)

#-----------------------------------------------------------#
# This function will get data from the columns that you want
#-----------------------------------------------------------#
def select_data(engine, table, cols = ['*']):

	cols = '.'.join(cols)
	query = 'SELECT {} FROM {};'.format(cols, table)
	result = pd.read_sql(query, con = engine)

	return(result)

#---------------------------------------------------------------------# 
# Insert a new item into the SQL data-base
#---------------------------------------------------------------------# 	
def insert_data(item, keys, table, unique = 0):

	#---------------------------------------------------------------------# 
	# Only insert fields which are both in the item and the table
	#---------------------------------------------------------------------# 	
	keys = get_keys(table) & item.keys()
	fields = u','.join(keys)
	qm = u','.join([u'%s'] * len(keys))
	
	sql = "INSERT INTO " + table + " (%s) VALUES (%s)"
	sql_unique = "INSERT IGNORE INTO " + table + " (%s) VALUES (%s)"
	insert = sql if unique == 0 else sql_unique
	sql = insert % (fields, qm)
	data = [item[k] for k in keys]

	return(sql, data)

#---------------------------------------------------------------------# 
# Update FTO type
#---------------------------------------------------------------------# 
def update_fto_type(fto_no, fto_type, table):

	sql = "UPDATE " + table + " SET fto_type = %s WHERE fto_no = %s"
	data = [fto_type, fto_no]
	
	return(sql, data)