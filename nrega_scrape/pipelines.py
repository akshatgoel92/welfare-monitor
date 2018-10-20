# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# Import packages
import os
import json

# MySQL driver 
import pymysql
# Install this as MySQLdb to ensure backward compatibality
pymysql.install_as_MySQLdb()

# Scrapy
from scrapy import signals
from scrapy.contrib.exporter import CsvItemExporter
from scrapy.exceptions import DropItem

# Project specific
from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from nrega_scrape.items import FTOItem
from common.helpers import sql_connect
from common.helpers import insert_data

# Twisted adbapi library for connection pools to SQL data-base
from twisted.enterprise import adbapi

	
# FTO number pipe-line		
class FTOSummaryPipeline(object):
	
	def __init__(self):
		
		# Get credentials to connect to the data-base
		user, password, host, db = sql_connect().values()
		# Create a connection to the data-base
		self.conn = pymysql.connect(host, user, password, db, charset="utf8", 
									use_unicode=True)
		
    # Item processing function
	def process_item(self, item, spider):
		
		# Check what instance type we have
		if isinstance(item, NREGAItem):
			
			title_fields = ['block_name']
			insert_sql = "INSERT INTO fto_summary (%s) VALUES (%s)"
		
		# Check what instance type we have
		elif isinstance(item, FTONo):
			
			title_fields = ['fto_stage']
			insert_sql = "INSERT INTO fto_numbers (%s) VALUES (%s)"
		
		if spider.name == "fto_stats":
			
			for field in item.keys():
				item[field] = item[field].strip() if type(item[field]) == str else item[field]
				if field in title_fields:
					item[field] = item[field].title()
			
			# Get the inputs we need to execute the	query
			sql, data = insert_data(item, insert_sql)
			# Execute query
			self.conn.cursor().execute(sql, data)
			# Commit to DB
			self.conn.commit()
		# Return statement
		return(item)
	
	# Execute this function when the spider closes		
	def close_spider(self, spider):
		
		# Close the data-base connection
		self.conn.close()
		# Delete the data-base connection
		del self.conn
		
class FTOContentPipeline(object):
    
    def __init__(self):
    	
    	# Get the connection credentials
    	user, password, host, db_name = sql_connect().values()
    	# Create the data-base connection pool using credentials
    	self.dbpool = adbapi.ConnectionPool('pymysql', 
    										db = db_name, 
    										host = host, 
    										user = user, 
    										passwd = password, 
    										cursorclass = pymysql.cursors.DictCursor, 
    										charset = 'utf8', 
    										use_unicode = True,
    										cp_max = 16)
		self.tables = ['fto_content']
		
	# Process item method
    def process_item(self, item, spider):
    
    	# Check if the current item is an FTO item instance
    	if isinstance(item, FTOItem):
    		title_fields = ['block_name', 'app_name', 'prmry_acc_holder_name', 
    						'status', 'rejection_reason']
    		if item['block_name'] is None:
    			raise(DropItem("Block name missing"))
    		else:
    			item = clean_item(item, self.title_fields)
    			for table in self.tables:
    				keys = get_keys(item, table)
    				sql, data = insert_sql(item, keys, table)
    				self.dbpool.runOperation(sql, data)
    	# Return the item
    	return(item)
	
	# Execute this function when the spider is closing
    def close_spider(self, spider):
    	# Shut down all the connections in the DB connection pool
        self.dbpool.close()
	

			
	


	
