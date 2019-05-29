# -*- coding: utf-8 -*-

#------------------------------------------------------------------#
# Author: Akshat Goel
# Date: 8th August 2018
# Python version: 3.6.3
# Dependencies:

# [Only modules outside Python standard listed]
# 1) scrapy 
# 2) pandas 
# 3) numpy 
# 4) Selenium
#------------------------------------------------------------------#

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
from scrapy.contrib.spiders import CrawlSpider, Rule

# Date and time sub-modules 
from datetime import date, timedelta

# Twisted errors
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

# Item class
from nrega_scrape.items import FTOItem
from nrega_scrape.items import FTOOverviewItem
from nrega_scrape.items import FTOMaterialItem
from common.helpers import *


# FTO scraper
class FtoMaterialSpider(CrawlSpider):

	# Set globals
	name = "fto_material"
	state_name = "CHHATTISGARH"
	state_code = '33' 
	district_name = 'RAIPUR'
	district_code = '3316'
	block_name = 'ABHANPUR'
	block_code = district_code + '008'
	fin_year = '2018-2019'

	# Construct the URL
	basic = 'http://mnregaweb4.nic.in/netnrega/FTO/fto_sign_detail.aspx?lflag=&flg=M&page=b'
	state = '&state_name=' + state_name + '&state_code=' + state_code
	district = '&district_name=' + district_name + '&district_code=' + district_code
	block = '&block_name=' + block_name + '&block_code=' + block_code 
	fin_year = '&fin_year=' + fin_year 
	meta = '&typ=fst_sig&mode=b&source=national&Digest=8QNGgmbQHeXr43fWKEhzaw'

	# Store the start URL
	start_urls = [basic + state + district + block + fin_year + meta]
	urls = []

	# Parse function 
	def parse(self, response):
		
		# Store the last table of the response
		tables = response.xpath('//table')
		# Store the table
		table = response.xpath('//table')[-1]
		
		# Store new URL basic
		basic_1 = 'http://mnregaweb4.nic.in/netnrega/FTO/'

		# Store the URLs in the table
		# We will follow these in the next parse function
		urls = table.xpath('*//a//@href').extract()
		print(urls)
		# Prepend basic URL to the scraped href
		urls = [basic_1 + url for url in urls]
		print(urls)
		# Clean up further 
		# urls = [re.findall('(.+fto_no.+Digest.+)/.+referer', url) for url in urls]
		# print(urls)

		# Now go through each hyperlink on the table
		# Call the FTO list parser on each URL
		# Yield the result of the response to processing pipeline
		for url in urls:
			yield(response.follow(url, self.parse_fto_content))

	# This takes as input a Twisted failure object
	# It returns as output a representation of this object
	# to the log file
	# This ensures that all errors are logged in case we
	# want to do anything with them later
	def error_handling(self, failure):
		self.logger.error('Downloader error')

	def parse_fto_content(self, response):
		
		# Get all the tables on the web-page
		# Then select the correct one
		try:
			
			item = FTOMaterialItem()
			table = response.xpath('//table')[2]
			rows = table.xpath('//tr')
			
			# Process the item by iterating over rows
			# Log the item name to the log file
			for row in rows[4:]:
				
				item['sr_no'] = row.xpath('td[1]//text()').extract_first()
				item['block_name'] = row.xpath('td[2]//text()').extract_first()
				item['transact_ref_no'] = row.xpath('td[3]//text()').extract_first()

				item['transact_date'] = row.xpath('td[4]//text()').extract_first()
				item['vendor_name'] = row.xpath('td[5]//text()').extract_first()
				item['vendor_id'] = row.xpath('td[6]//text()').extract_first()
				item['voucher_no'] = row.xpath('td[7]//text()').extract_first()

				item['prmry_acc_holder_name'] = row.xpath('td[8]//text()').extract_first()
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
				# Log the exception first 
				# Then move on 
				self.logger.error('Parse error on transactions table: %s', response.url)