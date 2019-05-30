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
from scrape.items import FTOItem
from scrape.items import FTOOverviewItem
from common.helpers import *


# FTO scraper
class FtoContentSpider(scrapy.Spider):

	name = "fto_content"
	basic = "http://mnregaweb4.nic.in/netnrega/fto/fto_status_dtl.aspx?"
	fin_year = "2019-2020"
	state_code = "33"
	block = "fto_queue"
	output_dir = os.path.abspath(".")
	
	user = "ec2"
	path = "./../software/chromedriver/" if user == "local" else "/home/ec2-user/chromedriver/"
	path_to_chrome_driver = os.path.abspath(path)

	conn, cursor = db_conn()
	fto_nos = pd.read_sql("SELECT fto_no FROM fto_queue WHERE done = 0;", con = conn).values.tolist()
	cursor.close()
	conn.close()

	fto_nos = [fto_no[0] for fto_no in fto_nos]
	start_urls = []

	for fto_no in fto_nos:
		url = basic + "fto_no=" + fto_no + "&fin_year=" + fin_year + "&state_code=" + state_code
		start_urls.append(url)

	options = Options()
	options.add_argument('--headless')
	driver = webdriver.Chrome(path_to_chrome_driver, chrome_options = options)

	
	def start_requests(self):

		for url in self.start_urls:
			yield(scrapy.Request(url, callback = self.parse, 
								 errback = self.error_handling, dont_filter = True))

	
	def error_handling(self, failure):
		
		self.logger.error('Downloader error')

	
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
		overview_item = FTOOverviewItem()
		item = FTOItem()

		try: 
			source = self.get_source(response, self.driver)
		
		except Exception as e:
			self.logger.error('Get source error: %s', e)
			self.logger.error('This error happened on: %s', response.url)
			return

		try: 
			tables = source.xpath('//table')
			
		except Exception as e:
			self.logger.error('No table found error: %s', response.url)
			return
		
		# First we scrape the over-view table on the top
		try:
			
			table = tables[3]
			content = []
			for row in table.xpath('*//tr'):
				content.append(row.xpath('*//text()').extract())
			
			content = [item.strip() for row in content 
						for item in row if item.strip() != ''][1::2]

		except Exception as e:
			self.logger.error('Parse error on overview table: %s', response.url)
			return
		
		try:
			
			overview_item['state'] = content[0]
			overview_item['district'] = content[1]
			overview_item['block_name'] = content[2]
			
			overview_item['fto_type'] = content[3]
			overview_item['fto_no'] = content[4]
			overview_item['pay_mode'] = content[5]
			
			overview_item['acc_signed_dt'] = content[6]
			overview_item['po_signed_dt'] = content[7]
			overview_item['acc_signed_dt_p2w'] = content[8]
			
			overview_item['po_signed_dt_p2w'] = content[9]
			overview_item['cr_processed_dt'] = content[10]
			overview_item['cr_processed_dt_P'] = content[11]
			
			yield(overview_item)

		except Exception as e:
			
			print(e)
			self.logger.error('Item parse error on overview table: %s', 
								response.url)
			return
		
		# Go to the next function call
		if overview_item['fto_type'] == 'Material':
			return
		
		# Get all the tables on the web-page
		# Then select the correct one
		try:
			
			item = FTOItem()
			table = tables[4]
			rows = table.xpath('*//tr')
			
			# Process the item by iterating over rows
			# Log the item name to the log file
			for row in rows:
			
				item['block_name'] = row.xpath('td[1]//text()').extract_first() 
				item['jcn'] = row.xpath('td[2]//text()').extract_first()
				item['transact_ref_no'] = row.xpath('td[3]//text()').extract_first()

				item['transact_date'] = row.xpath('td[4]//text()').extract_first()
				item['transact_date'] = str(datetime.datetime.strptime(item['transact_date'], '%d/%m/%Y').date())
				
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
			
				self.logger.info('Completed: %s', item['fto_no'])
				yield(item)
			
		except Exception as e:
				 
				self.logger.error('Parse error on transactions table: %s', response.url)
				self.logger.error('The error is: %s', e)