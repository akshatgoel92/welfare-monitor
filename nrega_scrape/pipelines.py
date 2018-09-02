# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# Import packages
import os
import json 
import pymysql

from scrapy.contrib.exporter import CsvItemExporter
from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from nrega_scrape.items import FTOItem
from common.helpers import create_pool

# MySQLdb functionality
pymysql.install_as_MySQLdb()
'''
 Table('fto_summary', metadata, 
							   		Column('block_name', String(100)), 
							   		Column('total_fto', Integer), 
							   		Column('first_sign', Integer),
							   		Column('first_sign_pending', Integer),
							   		Column('second_sign', Integer),
							   		Column('second_sign_pending', Integer),
							   		Column('fto_sent_bank', Integer),
							   		Column('transact_sent_bank', Integer),
							   		Column('fto_processed_bank', Integer),
							   		Column('transact_processed_bank', Integer),
							   		Column('fto_partial_bank', Integer),
							   		Column('transact_partial_bank', Integer),
							   		Column('fto_pending_bank', Integer),
							   		Column('transact_pending_bank', Integer),
							   		Column('transact_processed_bank_resp', Integer), 
							   		Column('invalid_accounts_bank_resp', Integer), 
							   		Column('transact_rejected_bank_resp', Integer), 
							   		Column('transact_total_bank_resp', Integer), 
							   		Column('url', String(100)),
							   		Column('spider', String(100)),
							   		Column('server', String(100)),
							   		Column('date', String(100)))'''

	
# FTO number pipe-line   	
class FTOSummaryPipeline(object):
	
	def __init__(self):
		
		self.pool = create_pool()
		
	def close_spider(self, spider):
	
		self.pool.close()
		
	def _insert_record(self, tx, item):
	
		fields = item.fields
		
		type = ["%s"]*len(fields)
		
		args = (
		
					item['block_name'],
    
    				item['total_fto'],
         
    				item['first_sign'],
    
    				item['first_sign_pending'],
    
    				item['second_sign'],
    
    				item['second_sign_pending'],
    
    				item['fto_sent_bank'],
    
    				item['transact_sent_bank'],
    
    				item['fto_processed_bank'],
    
    				item['transact_processed_bank'],
    
    				item['fto_partial_bank'],
    
    				item['transact_partial_bank'],
    
    				item['fto_pending_bank'],
    
    				item['transact_pending_bank'],
    
    				item['transact_processed_bank_resp'],
    
    				item['invalid_accounts_bank_resp'],
    
    				item['transact_rejected_bank_resp'],
    
    				item['transact_total_bank_resp'],
    	
    				item['url'],
    		
    				item['spider'],
    
    				item['server'],
    
    				item['date']
    				
    				)
		
		sql = """ INSERT INTO fto_summary (block_name, first_sign, first_sign_pending, fto_partial_bank, 
					 fto_pending_bank, fto_processed_bank, fto_sent_bank, invalid_accounts_bank_resp, second_sign, 
					 second_sign_pending, server, spider, total_fto, transact_partial_bank, transact_pending_bank, transact_processed_bank, 
					 transact_processed_bank_resp, transact_rejected_bank_resp, transact_sent_bank, transact_total_bank_resp, url, date)
					 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
					 
		tx.execute(sql, args)
		
	def process_item(self, item, spider):
	
		if isinstance(item, NREGAItem):
		
			query = self.pool.runInteraction(self._insert_record, item)
			
		return(item)
   		   		
# FTO number pipe-line   	
class FTONoPipeline(object):
	
	def open_spider(self, spider):
	
		if spider.name == 'fto_stats':
		
			self.file = open('fto_numbers.csv', 'w+b')
		
			self.exporter = CsvItemExporter(self.file)
		
			self.exporter.start_exporting()
	
	def close_spider(self, spider):
   		
   		if spider.name == 'fto_stats':
   		
   			self.exporter.finish_exporting()
   		
   			self.file.close()
   		
	def process_item(self, item, spider):
   		
   		if isinstance(item, FTONo):
   			
   			self.exporter.export_item(item)
   		
   		return(item)
   		
# FTO Content Processor		
class FTOContentPipeline(object):
	
	def open_spider(self, spider):
	
		if spider.name == 'fto_content':
		
			self.file = open('fto_content.csv', 'w+b')
			
			self.exporter = CsvItemExporter(self.file)
			
			self.exporter.start_exporting()
		
	def close_spider(self, spider):
	
		if spider.name == 'fto_content':
		
			self.exporter.finish_exporting()
			
			self.file.close()

	def process_item(self, item, spider):
	
		if isinstance(item, FTOItem):
		
			self.exporter.export_item(item)
			
		return(item)
		


	
