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
from common.helpers import *


# FTO scraper
class FtoContentSpider(scrapy.Spider):

	# Set globals
	name = "fto_content"
	basic = "http://mnregaweb4.nic.in/netnrega/fto/fto_status_dtl.aspx?"
	fin_year = "2016-2017"
	state_code = "17"
	output_dir = os.path.abspath(".")
	block = "gwalior"
	
	# Set Path to Chrome driver
	user = 'ec2-user'
	path = "./../software/chromedriver/" if user == 'local' else "/home/ec2-user/chromedriver/"
	path_to_chrome_driver = os.path.abspath(path)

	# Get the target FTO nos.
	conn, cursor = db_conn()
	fto_nos = pd.read_sql("SELECT fto_no FROM " + block + " WHERE done = 0;", con = conn).values.tolist()
	cursor.close()
	conn.close()

	# Store target FTO nos.
	fto_nos = [fto_no[0] for fto_no in fto_nos]
	
	# Store start URLs here	
	start_urls = []

	# Construct URL for each FTO no
	for fto_no in fto_nos:
		url = basic + "fto_no=" + fto_no + "&fin_year=" + fin_year + "&state_code=" + state_code
		start_urls.append(url)

	# Create options object for Chrome driver
	# Set option to run headless
	options = Options()
	options.add_argument('--headless')

	# Create driver object
	driver = webdriver.Chrome(path_to_chrome_driver, chrome_options = options)

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
		self.logger.error(failure.printBriefTraceback())

	# Get selector object for file
	def get_source(self, response, driver):
		
		# Get response URL
		# Sleep for 4 seconds
		driver.get(response.url)
		time.sleep(4)

		# Drop down selector object
		# Sleep for 3 seconds
		fto_drop_down = Select(driver.find_element_by_css_selector("#ctl00_ContentPlaceHolder1_Ddfto"))
		time.sleep(3)

		# Click the button
		# Sleep for 4 seconds 
		fto_drop_down.select_by_index(1)
		time.sleep(4)

		# Get the source code
		# Return as a Selector object
		# Return statement
		page_source = driver.page_source
		page_source = Selector(text = page_source)

		return(page_source)

	def parse(self, response):
		
		# Create FTO content item
		# Get the source code of the FTO page
		item = FTOItem()
		source = self.get_source(response, self.driver)
    try:
			# Get all the tables on the web-page
			# Then select the correct one
			tables = source.xpath('//table')
			table = tables[4]
			
	except Exception as e: 
		self.logger.error(response.url)

		# Store the rows so we can iterature over
		# them
		rows = table.xpath('*//tr')

		# Process the item by iterating over rows
		# Log the item name to the log file
		for row in rows:
			
			item['block_name'] = row.xpath('td[1]//text()').extract_first() 
			item['jcn'] = row.xpath('td[2]//text()').extract_first()
			item['transact_ref_no'] = row.xpath('td[3]//text()').extract_first()
			
			item['transact_date'] = row.xpath('td[4]//text()').extract_first()
			item['app_name'] = row.xpath('td[5]//text()').extract_first()
			item['prmry_acc_holder_name'] = row.xpath('td[6]//text()').extract_first()
			
			item['wage_list_no'] = row.xpath('td[7]//text()').extract_first()
			item['acc_no'] = row.xpath('td[8]//text()').extract_first()
			item['bank_code'] = row.xpath('td[9]//text()').extract_first()
			
			item['ifsc_code'] = row.xpath('td[10]//text()').extract_first()
			item['credit_amt_due'] = row.xpath('td[11]//text()').extract_first()
			item['credit_amt_actual'] = row.xpath('td[12]//text()').extract_first()
			
			item['status'] = row.xpath('td[13]//text()').extract_first()
			item['processed_date'] = row.xpath('td[14]//text()').extract_first()
			item['utr_no'] = row.xpath('td[15]//text()').extract_first()
			
			item['rejection_reason'] = row.xpath('td[16]//text()').extract_first()
			item['server'] = socket.gethostname()
			item['fto_no'] = re.findall('fto_no=(.*FTO_\d+)&fin_year', response.url)[0]
			
			item['scrape_date'] = str(datetime.datetime.now().date())
			item['scrape_time'] = str(datetime.datetime.now().time())
			
			self.logger.info(item['fto_no'])
			
			yield(item)