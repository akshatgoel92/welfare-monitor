# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


# Import packages
from scrapy.contrib.exporter import CsvItemExporter
from gma_scrape.items import NREGAItem
from gma_scrape.items import FTONo

# FTO number pipe-line   	
class FTOSummaryPipeline(object):
	
	def open_spider(self, spider):
		
		self.file = open('fto_summary.csv', 'w+b')
		
		self.exporter = CsvItemExporter(self.file)
		
		self.exporter.start_exporting()
	
	def process_item(self, item, spider):
   		
   		if isinstance(item, NREGAItem):
   		
   			self.exporter.export_item(item)
   			
   		return(item)
   	
	def close_spider(self, spider):
   		
   		self.exporter.finish_exporting()
   		
   		self.file.close()
   		   		
# FTO number pipe-line   	
class FTONoPipeline(object):
	
	def open_spider(self, spider):
		
		self.file = open('fto_numbers.csv', 'w+b')
		
		self.exporter = CsvItemExporter(self.file)
		
		self.exporter.start_exporting()
	
	def process_item(self, item, spider):
   		
   		if isinstance(item, FTONo):
   		
   			self.exporter.export_item(item)
   			
   		return(item)
   	
	def close_spider(self, spider):
   		
   		self.exporter.finish_exporting()
   		
   		self.file.close()
	
	
    	
	
