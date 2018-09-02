# Import packages
import os
import json 
import pymysql
from twisted.enterprise import adbapi
from sqlalchemy import *

# MySQLdb functionality
pymysql.install_as_MySQLdb()

# Connect to AWS RDB
def sql_connect():

	with open('./gma_secrets.json') as secrets:
		
		sql_access = json.load(secrets)['mysql']
		
	return(sql_access)
	
# Create a DB engine	
def create_engine():
	
	u, p, h, db = sql_connect().values()
		
	engine = create_engine("mysql+pymysql://" + u + ":" + p + "@" + h + "/" + db)
	
	return(engine)
	
# Create connection pool
def create_pool():

	u, p, h, db = sql_connect().values()
	
	db_pool = adbapi.ConnectionPool('pymysql', charset = 'utf8', 
															use_unicode = True, user = u, 
															password = p, host = h, db = db)
	
	return(db_pool)
	
	
   
   
   

    	