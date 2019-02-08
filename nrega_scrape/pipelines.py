# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# Import packages
import os
import json
import re

# MySQL driver 
# Install this as MySQLdb to ensure backward compatibality
import pymysql
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

# Import helper functions
from common.helpers import sql_connect
from common.helpers import insert_data
from common.helpers import update_fto_type
from common.helpers import clean_item
from common.helpers import get_keys 


# Twisted adbapi library for connection pools to SQL data-base
from twisted.enterprise import adbapi

# Process each item using this pipeline
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

		# Do this for over-view item spider
		if isinstance(item, FTOOverviewItem) and spider.name == 'fto_content':
			
			title_fields = ['state', 
							'district',
							'block', 
							'type',
							'pay_mode']
			
			item = clean_item(item, title_fields)
			
			sql,  data = update_fto_type(item['fto_no'], 
										item['fto_type'], 
										str(spider.block))
			
			self.dbpool.runOperation(sql, data)
		
		# Do this for FTO content spider
		if isinstance(item, FTOItem) and spider.name == 'fto_content':

			title_fields = ['block_name,' 
							'status', 
							'rejection_reason',
							'prmry_acc_holder_name',
							'app_name']
			
			tables = ['banks', 
					  'transactions',
					  'wage_lists',
					  'accounts']
			
			unique_tables = ['banks', 
							 'wage_lists',
							 'accounts']
			
			if item['block_name'] is None:
				raise(DropItem("Block name missing"))
			
			item = clean_item(item, title_fields)
			
			for table in tables:
				
				unique = 1 if table in unique_tables else 0
				keys = get_keys(table)
				sql, data = insert_data(item,
										keys,
										table, 
										unique)
				try:
					self.dbpool.runOperation(sql, 
											data)
				except Exception as e:
					self.logger.error('Error in the data-base upload: %s', str(e))

		# Do this for FTO branch spider
		if isinstance(item, FTOItem) and spider.name == 'fto_branch':
			
			title_fields = ['block_name',
							'app_name',
							'status', 
							'rejection_reason']
		
			tables = ['banks', 
					  'transactions', 
					  'wage_lists']
			
			unique_tables = ['banks', 
							'wage_lists']
			
			if item['block_name'] is None:
				raise(DropItem("Block name missing"))
		
			if item['ifsc_code'] == "Total":
				raise(DropItem("IFSC code missing"))
		
			if item['wage_list_no'] is None:
				raise(DropItem("Wage list no. missing"))
		
			if item['wage_list_no'] == '':
				raise(DropItem("Wage list no. missing"))
			
			# Drop transaction reference numbers that don't match the format
			if re.search('\d{10}NRG\d{17}', item['transact_ref_no']) is None:
				raise(DropItem("Transaction ref no does not fit format"))
				
			item = clean_item(item, title_fields)	
			
			for table in tables:
				
				unique = 1 if table in unique_tables else 0
				keys = get_keys(table)
				sql, data = insert_data(item,
										keys,
										table, 
										unique)
				try:
					self.dbpool.runOperation(sql, 
												data)
				except Exception as e:
					self.logger.error('Error in the data-base upload: %s', str(e))
					
		return(item)
	
	# Execute this function when the spider is closing
	# Shut down all the connections in the DB connection pool
	def close_spider(self, spider):
		self.dbpool.close()

class FTOMaterialPipeline(object):

	def open_spider(self, spider):
	
		if spider.name == 'fto_material':
		
			self.file = open('fto_content.csv', 'w+b')
			
			self.exporter = CsvItemExporter(self.file)
			
			self.exporter.start_exporting()

	def process_item(self, item, spider):
	
		if isinstance(item, FTOMaterialItem):
		
			self.exporter.export_item(item)
			
		return(item)
		
	def close_spider(self, spider):
	
		if spider_name == 'fto_material':
		
			self.exporter.finish_exporting()
			
			self.file.close()

