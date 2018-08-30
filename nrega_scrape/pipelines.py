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

# Do this to ensure pymysql has same functionality as MySQLdb
pymysql.install_as_MySQLdb()

class MySQLStorePipeline(object):
	
	# Store AWS credentials
	with open('./gma_secrets.json') as file:
		
		mysql = json.load(file)['mysql']
		
    	host = mysql['host']
    
    	user = mysql['username']
    
    	password = mysql['password']
    
    	db = mysql['db']


# FTO number pipe-line   	
class FTOSummaryPipeline(object):
	
	def __init__(self):
		
		self.connection = MySQLdb.connect(self.host, self. user, self.password, self.db)
		
		self.cursor = self.connection.cursor()
	
	def process_item(self, item, spider):
   		
   		if isinstance(item, NREGAItem):
   			
   			try:
   				
   				self.cursor.execute(
   			
   			except Exception as e:
   				
   				print("Error!")
   			
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
		

    	
	
