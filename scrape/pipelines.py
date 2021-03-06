# -*- coding: utf-8 -*-
import os
import json
import re
import pymysql
from scrapy import signals
from scrapy.contrib.exporter import CsvItemExporter
from scrapy.exceptions import DropItem
from scrape.items import NREGAItem
from scrape.items import FTONo
from scrape.items import FTOItem
from scrape.items import FTOOverviewItem
from scrape.items import FTOMaterialItem
from common.helpers import sql_connect
from common.helpers import clean_item
from common.helpers import  db_engine
from common.helpers import send_email
from db.update import insert_data
from db.update import delete_data
from db.update import update_fto_type
from db.update import get_keys 
from twisted.enterprise import adbapi

pymysql.install_as_MySQLdb()



class FTONoPipeline(object):
	'''This writes the output of FTO urls to a set of .csvs.'''


	def open_spider(self, spider):
	
		if spider.name == 'fto_urls':
			
			self.file = open('./output/' + spider.stage + '.csv', 'w+b')
			self.exporter = CsvItemExporter(self.file)
			self.exporter.start_exporting()


	def process_item(self, item, spider):
		
		if isinstance(item, FTONo) and spider.name == 'fto_urls': 
			
			if item['fto_no'] is None: raise(DropItem("Block name missing"))
			if item['fto_no'] == '': raise(DropItem("Block name missing"))
			if item['url'] is None: raise(DropItem('IFSC code missing'))
		
			self.exporter.export_item(item)

		return(item)


	def close_spider(self, spider):	

		if spider.name == 'fto_urls':
			self.exporter.finish_exporting()
			self.file.close()



class FTOBranchPipeline(object):
	'''This writes the output of FTO urls to a set of .csvs.'''


	def open_spider(self, spider):
	
		if spider.name == 'fto_branch':
			
			self.file = open('./output/transactions_alt' + '.csv', 'w+b')
			self.exporter = CsvItemExporter(self.file)
			self.exporter.start_exporting()


	def process_item(self, item, spider):
		
		if isinstance(item, FTOItem) and spider.name == 'fto_branch': 
			
			if item['block_name'] is None: raise(DropItem("Block name missing"))
			if item['ifsc_code'] == "Total": raise(DropItem("IFSC code missing"))
			if item['wage_list_no'] is None: raise(DropItem("Wage list no. missing"))
			if item['wage_list_no'] == '': raise(DropItem("Wage list no. missing"))
			if re.search('\d{10}NRG\d{17}', item['transact_ref_no']) is None: raise(DropItem("Transaction ref no does not fit format"))
		
			self.exporter.export_item(item)

		return(item)


	def close_spider(self, spider):
		
		if spider.name == 'fto_branch':
			
			self.exporter.finish_exporting()
			self.file.close()


		
class FTOContentPipeline(object):
	'''This takes in input from the content spiders and 
	writes information to the database.'''


	def __init__(self):
		
		user, password, host, db_name = sql_connect().values()
		self.dbpool = adbapi.ConnectionPool('pymysql', db = db_name, host = host, 
											user = user, passwd = password, 
											cursorclass = pymysql.cursors.DictCursor, 
											charset = 'utf8', use_unicode = True, cp_max = 16)
	
	def open_spider(selif, spider):
		
		if spider.name == 'fto_branch': 
			
			subject = 'GMA Update: The alternate transactions scrape is starting...'
			message = ''
			send_email(subject, message)
			
			engine = db_engine()
			tables = ['transactions_alt', 'wage_lists_alt', 'banks_alt']
			for table in tables: delete_data(engine, table)
		
		return	
	
	def process_item(self, item, spider):
		
		
		if isinstance(item, FTOOverviewItem) and spider.name == 'fto_content':
			
			title_fields = ['state', 'district', 'block', 'type', 'pay_mode']
			item = clean_item(item, title_fields)
			sql,  data = update_fto_type(item['fto_no'], item['fto_type'], str(spider.block))
			self.dbpool.runOperation(sql, data)
		
		
		if isinstance(item, FTOItem) and spider.name == 'fto_content':

			title_fields = ['block_name,' 'status', 'rejection_reason', 'prmry_acc_holder_name', 'app_name']
			tables = ['banks', 'transactions', 'wage_lists', 'accounts']
			unique_tables = ['banks', 'wage_lists', 'accounts']
			
			if item['block_name'] is None: raise(DropItem("Block name missing"))
			item = clean_item(item, title_fields)
			
			for table in tables:
				
				unique = 1 if table in unique_tables else 0
				keys = get_keys(table)
				sql, data = insert_data(item, keys, table, unique)
				
				try: self.dbpool.runOperation(sql, data)
				except Exception as e: self.logger.error('Error in the data-base upload: %s', str(e))
		
		if isinstance(item, FTOItem) and spider.name == 'fto_branch':
			
			title_fields = ['block_name', 'app_name', 'status', 'rejection_reason']
			tables = ['banks_alt', 'transactions_alt', 'wage_lists_alt']
			unique_tables = ['banks_alt', 'wage_lists_alt']
			
			if item['block_name'] is None: raise(DropItem("Block name missing"))
			if item['ifsc_code'] == "Total": raise(DropItem("IFSC code missing"))
			
			if item['wage_list_no'] is None: raise(DropItem("Wage list no. missing"))
			if item['wage_list_no'] == '': raise(DropItem("Wage list no. missing"))
			
			if re.search('\d{10}NRG\d{17}', item['transact_ref_no']) is None: raise(DropItem("Transaction ref no does not fit format"))
			item = clean_item(item, title_fields)	
			
			for table in tables:
				
				unique = 1 if table in unique_tables else 0
				keys = get_keys(table)
				sql, data = insert_data(item, keys, table, unique)
				try: self.dbpool.runOperation(sql, data)
				except Exception as e: self.logger.error('Error in the data-base upload: %s', str(e))
					
		return(item)
	
	
	def close_spider(self, spider):
		
		if spider.name == 'fto_branch':
			subject = 'GMA Update: The alternate transactions scrape is ending...'
			message = ''
			send_email(subject, message)
		
		self.dbpool.close()



class FTOMaterialPipeline(object):

	
	def open_spider(self, spider):
	
		if spider.name == 'fto_material':
			
			self.file = open('./output/fto_material.csv', 'w+b')
			self.exporter = CsvItemExporter(self.file)
			self.exporter.start_exporting()

	def process_item(self, item, spider):
		
		 
		# Check to see whether items are missing
		# There are rows on FTOs which don't contain transaction information
		# This sequence of statements removes these rows from the final scrape
		if isinstance(item, FTOMaterialItem) and spider.name == 'fto_material': 
			
			if item['block_name'] is None: raise(DropItem("Block name missing"))
			if item['block_name'] == '': raise(DropItem("Block name missing"))
			
			if item['ifsc_code'] is None: raise(DropItem('IFSC code missing'))
			if item['ifsc_code'] == "Total": raise(DropItem("IFSC code missing"))
			
			self.exporter.export_item(item)
			
		return(item)


	def close_spider(self, spider):	

		if spider.name == 'fto_material':
			
			self.exporter.finish_exporting()
			self.file.close()
