# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# Import packages
import os
from scrapy.contrib.exporter import CsvItemExporter
from gma_scrape.items import NREGAItem
from gma_scrape.items import FTONo
from gma_scrape.items import FTOItem

# FTO number pipe-line   	
class FTOSummaryPipeline(object):
	
	def open_spider(self, spider):
		
		if spider.name == 'fto_stats':
	
			self.file = open('fto_summary.csv', 'w+b')
		
			self.exporter = CsvItemExporter(self.file)
		
			self.exporter.start_exporting()
	
	def close_spider(self, spider):
	
		if spider.name == 'fto_stats':
   		
   			self.exporter.finish_exporting()
   		
   			self.file.close()

	def process_item(self, item, spider):
   		
   		if isinstance(item, NREGAItem):
   		
   			self.exporter.export_item(item)
   			
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
	
		if spider_name == 'fto_content':
		
			self.exporter.finish_exporting()
			
			self.file.close()

	def process_item(self, item, spider):
	
		if isinstance(item, FTOItem):
		
			self.exporter.export_item(item)
			
		return(item)
		

    	
	
