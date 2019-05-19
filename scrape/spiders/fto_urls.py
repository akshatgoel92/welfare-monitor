# -*- coding: utf-8 -*-
# Import packages
import scrapy
import datetime
import time
import socket
import re
import os
import logging
import pandas as pd
import numpy as np
from itertools import chain

# Scrapy submodules
from scrapy.selector import Selector
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.http import Request
from scrapy import Spider
from scrapy.spiders import CrawlSpider
from scrapy.spiders.crawl import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.http import Request
from scrapy.utils.log import configure_logging
from dateutil.parser import parse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options

from scrape.items import NREGAItem
from scrape.items import FTONo


# FTO URLs
class FTOUrls(CrawlSpider):

	
	# Store command line arguments 
	# To override these from the command line use the following syntax: 
	# scrapy crawl fto_urls -a fin_year=2019-2020 -a stage=fst_sig
	name = 'fto_urls'
	basic = 'http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?page=d&mode=B&flg=W&'
	state_name = 'CHHATTISGARH'
	state_code = '33'
	district_name = 'RAIPUR'
	district_code = '3316'
	fin_year = '2018-2019'
	stage = 'sec_sig'
	
	# Store whether to parse the URLS from a block level page or not
	# Store whether we need materials FTOs or wage FTOs
	block = 1
	material = 0
	
	# Store the start URL here
	if block == 0: 
		start_urls = [("http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?"
				   	   "page=s&mode=B&flg=W&state_name=CHHATTISGARH&state_code" 
				       "=33&fin_year=2018-2019&dstyp=B&source=national&"
				       "Digest=UdEewHqde6mcn4hhpT93Qg")]
	
	if block == 1:
		start_urls = [("http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?page=d"
					   "&mode=B&lflag=&flg=W&state_name=CHHATTISGARH&state_code=33&" 
					   "district_name=RAIPUR&district_code=3316&fin_year=2019-2020&dstyp=B"
					   "&source=national&Digest=8EcA8mQek9YptIB2JguAaQ")] 


	# Parse the block page
	# Get the URLs from the block page		
	def parse_block(self, response):
		
		urls = response.xpath('*//a//@href').extract()
		
		for url in urls:
			if 'block_name' in url and self.stage + '&' in url:
				yield(response.follow(url, self.parse_fto_list))


	# Parse URLs the FTO list
	def parse_fto_list(self, response):

		# Get the URLs
		item = FTONo()
		urls = response.xpath('*//a//@href').extract()
		
		# Print the URLs
		urls = ['http://mnregaweb4.nic.in/netnrega/FTO/' + url for url in urls]
		urls = [url for url in urls if 'fto_no' in url]
		
		# Parse URLs the FTO list		
		for url in urls:
			
			item['url'] = url
			item['fto_no'] = re.findall('fto_no=(.+)&source', url)[0]
			item['state_code'] = re.findall('state_code=(.+)&state_name', url)[0]
			item['district_code'] = re.findall('district_code=(.+)&district_name', url)[0]
			item['block_code'] = re.findall('block_code=(.+)&block_name', url)[0]
			item['transact_date'] = re.findall('fto_no=CH\d{7}_(.{6})', url)[0]

			yield(item)


	# Select the check box
	def select_check_box(self, response):
		
		# Store the response URL
		user = 'ec2'
		path = "./../software/chromedriver/" if user == 'local' else "/home/ec2-user/chromedriver/"
		path_to_chrome_driver = os.path.abspath(path)

		# Store the response URL
		options = Options()
		options.add_argument('--headless')

		# Store the response URL
		driver = webdriver.Chrome(path_to_chrome_driver, chrome_options = options)

		# Store the response URL
		driver.get(response.url)
		time.sleep(4)

		# Click the check box
		driver.find_element_by_css_selector("#ctl00_ContentPlaceHolder1_RBtnLst_1").click()
		time.sleep(3)

		# Store the source and return it as a selector object
		page_source = driver.page_source
		page_source = Selector(text = page_source)

		return(page_source)


	def parse(self, response):

		if self.material == 1 and self.block == 0:

			page_source = self.select_check_box(response)
			district_urls = page_source.xpath('*//a//@href').extract()

		elif self.material == 0 and self.block == 0:

			district_urls = response.xpath('*//a//@href').extract()
		
		
		# Store which callback you want here
		# Then parse the district URLs
		# Then yield those to the processing function		
		elif self.block == 0:
			process_link = self.parse_fto_list		
		
		# Store the district URLs
		# Parse them to get the links to all FTOs which have been through the 
		# selected stage
		if self.block == 0:
			
			stage = self.stage + '&'
			
			for url in district_urls:
				if 'district_name' in url and stage in url and self.district_name in url:
					print('Following URL....:' + url)
					yield(response.follow(url, self.parse_fto_list))
			
		if self.block == 1:

			for url in self.start_urls:
				yield(scrapy.Request(url, 
									 callback = self.parse_block, 
									 dont_filter = True))

