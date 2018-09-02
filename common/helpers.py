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
	
	user, password, host, db = sql_connect().values()
		
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	
	return(engine)
	

   
   
   

    	