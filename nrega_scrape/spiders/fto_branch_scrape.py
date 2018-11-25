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

# Selenium sub-modules
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options

# Date and time sub-modules 
from datetime import date, timedelta

# Twisted errors
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

# Item class
from nrega_scrape.items import FTOItem
from nrega_scrape.items import FTOOverviewItem
from common.helpers import *


# FTO scraper
class FtoContentSpider(scrapy.Spider):

	# Set globals
	name = "fto_content"
	basic = "http://mnregaweb4.nic.in/netnrega/FTO/fto_trasction_dtl.aspx?page=b&"
	fin_year = "2018-2019"
	state_code = "33"
	output_dir = os.path.abspath(".")
	block = "arang"
	block_name = "ARANG"
	meta = "&source=national&Digest=fqadRSN0J5knFQHrZrZQNQ"
	# Get the target FTO nos.
	fto_nos = pd.read_csv('./fto_nos/arang.csv').values().to_list()
	
	# Store start URLs here	
	start_urls = []

	# Construct URL for each FTO no
	for fto_no in fto_nos:
		url = 	basic + "&state_code=" + state_code + "&state_name=CHHATTISGARH&district_code=3316&district_name=RAIPUR&block_code=3316015&block_name=" + block_name + "&flg=W&fin_year=" + fin_year + "&fto_no=" + fto_no + meta
		start_urls.append(url)

	def start_requests(self):

		for url in self.start_urls:
			yield(scrapy.Request(url, 
								callback = self.parse, 
								errback = self.error_handling, 
								dont_filter = True))

	# This takes as input a Twisted failure object
	# It returns as output a representation of this object
	# to the log file
	# This ensures that all errors are logged in case we
	# want to do anything with them later
	def error_handling(self, failure):
		self.logger.error('Downloader error')

	def parse(self, response):
		
		# Get all the tables on the web-page
		# Then select the correct one
		try:
			
			item = FTOItem()
			table = response.xpath('//table')[4]
			rows = table.xpath('//tr')
			
			# Process the item by iterating over rows
			# Log the item name to the log file
			for row in rows:
			
				item['block_name'] = row.xpath('td[2]//text()').extract_first().strip()
				item['jcn'] = row.xpath('td[3]//text()').extract_first().strip()
				item['transact_ref_no'] = row.xpath('td[4]//text()').extract_first().strip()

				item['transact_date'] = row.xpath('td[5]//text()').extract_first().strip()
				item['app_name'] = row.xpath('td[6]//text()').extract_first().strip().title()
				item['wage_list_no'] = row.xpath('td[7]//text()').extract_first().strip()

				item['prmry_acc_holder_name'] = row.xpath('td[8]//text()').extract_first().strip().title()
				item['acc_no'] = ''
				item['bank_code'] = row.xpath('td[9]//text()').extract_first().strip()

				item['ifsc_code'] = row.xpath('td[10]//text()').extract_first().strip()
				item['credit_amt_due'] = row.xpath('td[11]//text()').extract_first().strip()
				item['credit_amt_actual'] = row.xpath('td[12]//text()').extract_first().strip()
			
				item['status'] = row.xpath('td[13]//text()').extract_first().strip()
				item['processed_date'] = row.xpath('td[14]//text()').extract_first().strip()
				item['utr_no'] = row.xpath('td[16]//text()').extract_first().strip()
			
				item['rejection_reason'] = row.xpath('td[17]//text()').extract_first().strip()
				item['server'] = socket.gethostname()
				item['fto_no'] = re.findall('fto_no=(.*FTO_\d+)&fin_year', response.url)[0]
			
				item['scrape_date'] = str(datetime.datetime.now().date())
				item['scrape_time'] = str(datetime.datetime.now().time())
			
				self.logger.info('Completed: %s', item['fto_no'])
				yield(item)
			
		except Exception as e:
				
				# Log the exception first 
				# Then move on 
				self.logger.error('Parse error on transactions table: %s', response.url)