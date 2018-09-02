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
from common.helpers import sql_connect

# MySQLdb functionality
pymysql.install_as_MySQLdb()

	
# FTO number pipe-line   	
class FTOSummaryPipeline(object):
	
	def __init__(self):
		
		user, password, host, db = sql_connect().values()
		
		self.conn = pymysql.connect(host, user, password, db, charset="utf8", use_unicode=True)
		
		self.cursor = self.conn.cursor()
	
	def _insert_record(self, item):
		
		args = (
		
					item['block_name'].encode('utf-8'),
    
    				item['total_fto'].encode('utf-8'),
         
    				item['first_sign'].encode('utf-8'),
    
    				item['first_sign_pending'].encode('utf-8'),
    
    				item['second_sign'].encode('utf-8'),
    
    				item['second_sign_pending'].encode('utf-8'),
    
    				item['fto_sent_bank'].encode('utf-8'),
    
    				item['transact_sent_bank'].encode('utf-8'),
    
    				item['fto_processed_bank'].encode('utf-8'),
    
    				item['transact_processed_bank'].encode('utf-8'),
    
    				item['fto_partial_bank'].encode('utf-8'),
    
    				item['transact_partial_bank'].encode('utf-8'),
    
    				item['fto_pending_bank'].encode('utf-8'),
    
    				item['transact_pending_bank'].encode('utf-8'),
    
    				item['transact_processed_bank_resp'].encode('utf-8'),
    
    				item['invalid_accounts_bank_resp'].encode('utf-8'),
    
    				item['transact_rejected_bank_resp'].encode('utf-8'),
    
    				item['transact_total_bank_resp'].encode('utf-8'),
    	
    				item['url'].encode('utf-8'),
    		
    				item['spider'].encode('utf-8'),
    
    				item['server'].encode('utf-8'),
    
    				item['date'].encode('utf-8')
    				
    				)
		
		sql = """ INSERT INTO fto_summary (block_name, first_sign, first_sign_pending, fto_partial_bank, 
					 fto_pending_bank, fto_processed_bank, fto_sent_bank, invalid_accounts_bank_resp, second_sign, 
					 second_sign_pending, server, spider, total_fto, transact_partial_bank, transact_pending_bank, transact_processed_bank, 
					 transact_processed_bank_resp, transact_rejected_bank_resp, transact_sent_bank, transact_total_bank_resp, url, date)
					 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
					 
		self.cursor.execute(sql, args)
		
		self.conn.commit()
		
	def process_item(self, item, spider):
	
		if isinstance(item, NREGAItem):
		
			self._insert_record(item)
			
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
		


	
