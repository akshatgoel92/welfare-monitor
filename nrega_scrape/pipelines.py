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
from nrega_scrape.items import FTOOverviewItem

from common.helpers import sql_connect
from common.helpers import insert_data
from common.helpers import update_fto_type
from common.helpers import clean_item
from common.helpers import get_keys 


# Twisted adbapi library for connection pools to SQL data-base
from twisted.enterprise import adbapi

	
# FTO number pipe-line
# Get credentials to connect to the data-base
# Create a connection to the data-base
# Item processing function	
# Execute query
# Commit to DB	
class FTOSummaryPipeline(object):
	
	def __init__(self):
		
		
		user, password, host, db = sql_connect().values()
		self.conn = pymysql.connect(host, 
									user,
									password,
									db, charset="utf8", 
									use_unicode=True)
		
	def process_item(self, item, spider):
			
		# Check what instance type we have
		if isinstance(item, NREGAItem):
			tables = ['fto_summary']
			title_fields = ['block_name']
			
		# Check what instance type we have
		elif isinstance(item, FTONo):
			tables = ['fto_nos']
			title_fields = ['fto_stage']
		
		# Do this for the FTO stats spider
		if spider.name == "fto_stats":
			
			# Clean item
			# Process each table
			# Get the keys
			# Get the inputs for the query
			item = clean_item(item, title_fields)

			for table in tables:
				keys = get_keys(table)
				sql, data = insert_data(item,
										keys, 
										table)
			
			self.conn.cursor().execute(sql, data)
			self.conn.commit()
		
		return(item)
	
	# Execute this function when the spider closes
	# Close the data-base connection
	# Delete the data-base connection		
	def close_spider(self, spider):
		
		self.conn.close()
		del self.conn

# Get the connection credentials
# Create the data-base connection pool using credentials
# Process item method	
class FTOContentPipeline(object):

	def __init__(self):
		

		user, password, host, db_name = sql_connect().values()
		
		self.dbpool = adbapi.ConnectionPool('pymysql', 
											db = db_name, 
											host = host, 
											user = user, 
											passwd = password, 
											cursorclass = pymysql.cursors.DictCursor, 
											charset = 'utf8', 
											use_unicode = True,
											cp_max = 16)
		
	
	def process_item(self, item, spider):

		if isinstance(item, FTOOverviewItem):
			title_fields = ['state', 
							'district',
							'block', 
							'type',
							'pay_mode']
		
		# Check if the current item is an FTO item instance
		if isinstance(item, FTOItem):
			
			title_fields = ['block_name',
							'app_name', 
							'prmry_acc_holder_name', 
							'status', 
							'rejection_reason']
			
			tables = ['accounts', 
					  'banks', 
					  'transactions', 
					  'wage_lists']
			
			unique_tables = ['accounts', 
							'banks', 
							'wage_lists']

		if item['block_name'] is None:
			raise(DropItem("Block name missing"))

		elif isinstance(item, FTOItem):
				
			item = clean_item(item, title_fields)	
			for table in tables:
				
				unique = 1 if table in unique_tables else 0
				keys = get_keys(table)
				sql, data = insert_data(item,
										keys,
										table, 
										unique)
				self.dbpool.runOperation(sql, 
										data)
		
		elif isinstance(item, FTOOverviewItem):

				item = clean_item(item, title_fields)
				sql,  data = update_fto_type(item['fto_no'], 
											 item['fto_type'], 
											 str(spider.block))
				self.dbpool.runOperation(sql, 
										 data)
		
		return(item)
	
	# Execute this function when the spider is closing
	# Shut down all the connections in the DB connection pool
	def close_spider(self, spider):
		self.dbpool.close()