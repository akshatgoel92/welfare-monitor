# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# Import packages
import os
import sys
import json 
import pymysql

from scrapy.contrib.exporter import CsvItemExporter
from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from nrega_scrape.items import FTOItem

# Do this to ensure pymysql has same functionality as MySQLdb
pymysql.install_as_MySQLdb()
	
# FTO number pipe-line   	
class FTOSummaryPipeline(object):
	
	def __init__(self):
	
		with open('./gma_secrets.json') as file:
		
			self.mysql = json.load(file)['mysql']
			
			self.host = self.mysql['host']
			
			self.user = self.mysql['username']
			
			self.password = self.mysql['password']
			
			self.db = self.mysql['db']
			
		self.conn = pymysql.connect(self.host, self. user, self.password, self.db)
		
		self.cursor = self.conn.cursor()
	
	def process_item(self, item, spider):
   		
   		if isinstance(item, NREGAItem):
   			
   			try:
   				
   				self.cursor.execute(" " "INSERT INTO fto_summary (block_name, jcn) VALUES ( %s, %s) " " ",  
   											  (item['block_name'].encode(' utf-8'), item['total_fto'].encode('utf-8')))
   											  
   				self.conn.commit()
   			
   			except Exception as e:
   				
   				print(e)
   				sys.exit()
   			
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
		

    	
	
