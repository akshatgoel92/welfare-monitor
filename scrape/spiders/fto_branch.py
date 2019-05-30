# -*- coding: utf-8 -*-
# Scraping and cleaning modules
import scrapy
import datetime
import time
import socket
import re
import os
import pandas as pd
import numpy as np

# Scrapy sub-modules
from scrapy.selector import Selector
from scrapy.loader.processors import MapCompose, Join
from scrapy.linkextractors import LinkExtractor
from scrapy.loader import ItemLoader
from scrapy.http import Request
from scrapy.spiders import CrawlSpider, Rule

# Date and time sub-modules 
from datetime import date, timedelta

# Twisted errors
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

# Item class
from scrape.items import FTOItem
from scrape.items import FTOOverviewItem
from common.helpers import *


# FTO scraper
class FtoBranchSpider(CrawlSpider):

	name = "fto_branch"
	conn, cursor = db_conn()
	start_urls = pd.read_sql("SELECT fto_no FROM fto_queue WHERE done = 0;", con = conn).values.tolist()
	cursor.close()
	conn.close()
	

	def parse(self, response):
		
		# Now go through each hyperlink on the table
		# Call the FTO list parser on each URL
		# Yield the result of the response to processing pipeline
		for url in self.start_urls:
				yield(response.follow(url, self.parse_fto_content))

	
	# This takes as input a Twisted failure object
	# It returns as output a representation of this object to the log file
	# This ensures that all errors are logged 
	def error_handling(self, failure):
		self.logger.error('Downloader error')

	
	def parse_fto_content(self, response):
		
		# Get all the tables on the web-page
		# Then select the correct one
		try:
			
			item = FTOItem()
			table = response.xpath('//table')[2]
			rows = table.xpath('//tr')
			
			# Process the item by iterating over rows
			# Log the item name to the log file
			for row in rows[4:]:
			
				item['block_name'] = row.xpath('td[2]//text()').extract_first()
				item['jcn'] = row.xpath('td[3]//text()').extract_first()
				item['transact_ref_no'] = row.xpath('td[4]//text()').extract_first()

				item['transact_date'] = row.xpath('td[5]//text()').extract_first()
				item['transact_date'] = str(datetime.datetime.strptime(item['transact_date'], '%d/%m/%Y').date())
				
				item['app_name'] = row.xpath('td[6]//text()').extract_first()
				item['wage_list_no'] = row.xpath('td[7]//text()').extract_first()

				item['prmry_acc_holder_name'] = row.xpath('td[8]//text()').extract_first()
				item['acc_no'] = ''
				item['bank_code'] = row.xpath('td[9]//text()').extract_first()

				item['ifsc_code'] = row.xpath('td[10]//text()').extract_first()
				item['credit_amt_due'] = row.xpath('td[11]//text()').extract_first()
				item['credit_amt_actual'] = row.xpath('td[12]//text()').extract_first()
			
				item['status'] = row.xpath('td[13]//text()').extract_first()
				item['processed_date'] = row.xpath('td[14]//text()').extract_first()
				item['utr_no'] = row.xpath('td[16]//text()').extract_first()
			
				item['rejection_reason'] = row.xpath('td[17]//text()').extract_first()
				item['server'] = socket.gethostname()
				item['fto_no'] = re.findall('fto_no=(.*FTO_\d+)&source', response.url)[0]
			
				item['scrape_date'] = str(datetime.datetime.now().date())
				item['scrape_time'] = str(datetime.datetime.now().time())
			
				self.logger.info('Completed: %s', item['fto_no'])
				yield(item)
			
		except Exception as e:
				
				print(e)
				self.logger.error('Parse error on transactions table: %s', response.url)