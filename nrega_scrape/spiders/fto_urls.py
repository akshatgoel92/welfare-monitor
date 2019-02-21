# -*- coding: utf-8 -*-
#----------------------------------------------------------------------#
# Import packages
#----------------------------------------------------------------------#

#----------------------------------------------------------------------#
# Overall imports
#----------------------------------------------------------------------#
import scrapy
import datetime
import socket
import re
import os
import logging
import pandas as pd
import numpy as np

#----------------------------------------------------------------------#
# Python standard
#----------------------------------------------------------------------#
from itertools import chain
#----------------------------------------------------------------------#
# Scrapy submodules
#----------------------------------------------------------------------#
from scrapy.selector import Selector
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.http import Request
from scrapy import Spider
from scrapy.contrib.spiders import CrawlSpider
from scrapy.contrib.spiders.crawl import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.http import Request
from scrapy.utils.log import configure_logging
from dateutil.parser import parse
#----------------------------------------------------------------------#
# Selenium submodules
#----------------------------------------------------------------------#
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options

#----------------------------------------------------------------------#
# Items
#----------------------------------------------------------------------#
from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from nrega_scrape.items import URLItem

#----------------------------------------------------------------------#
# This class scrapes the FTO URLs
#----------------------------------------------------------------------#
class FTOUrls(CrawlSpider):

	#----------------------------------------------------------------------#
	# Store the target URL
	#----------------------------------------------------------------------#
	name = 'fto_urls'
	basic = 'http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?page=d&mode=B&flg=W&'
	state_name = 'CHHATTISGARH'
	state_code = '33'
	district_name = 'AGAR-MALWA'
	district_code = '3316'
	fin_year = '2018-2019'

	#----------------------------------------------------------------------#
	# Store the components of the state URL
	#----------------------------------------------------------------------#
	state = 'state_name=' + state_name +'&state_code=' + state_code
	district = '&district_name=' + district_name + '&district_code=' + district_code 
	fin_year_url = '&fin_year=' + fin_year
	meta = '&dstyp=B&source=national&Digest=tuhEXy+HR52YT8lJYijdtw'
	
	#----------------------------------------------------------------------#
	# Store whether to parse the URLS from a block level page or not
	#----------------------------------------------------------------------#
	block = 0
	material = 1
	#----------------------------------------------------------------------#
	# Store the start URL here
	#----------------------------------------------------------------------#

	start_urls = [("http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?"
				 "page=s&mode=B&flg=W&state_name=MADHYA+PRADESH&state_code" 
				 "=17&fin_year=2018-2019&dstyp=B&source=national&Digest="
				 "5DA1ZSxezvKXndS3loV25w")]

	#----------------------------------------------------------------------#
	# Parse the block page
	#----------------------------------------------------------------------#			
	def parse_block(self, response):
		
		# We will follow these in the next parse function
		urls = response.xpath('*//a//@href').extract()
		
		block_urls = pd.DataFrame(urls, columns = ['url'])
		block_urls.to_csv('block_urls.csv', index = False)

		for url in urls:
			if 'block_name' in url and 'Rejected' not in url:
				yield(response.follow(url, self.parse_fto_list))


	#----------------------------------------------------------------------#
	# Parse URLs the FTO list
	#----------------------------------------------------------------------#
	def parse_fto_list(self, response):

		item = URLItem()
		urls = response.xpath('*//a//@href').extract()
		urls = ['http://mnregaweb4.nic.in/netnrega/FTO/' + url for url in urls]
		
		fto_urls = pd.DataFrame(urls, columns = ['url'])
		fto_urls.to_csv('fto_urls.csv', index = False)

		for url in fto_urls:
			if 'block_name' in url and 'Rejected' not in url:
				item['link'] = url
				yield(item)

	#----------------------------------------------------------------------#
	# Select the check box
	#----------------------------------------------------------------------#
	def select_check_box(self, response):
		
		#----------------------------------------------------------------------#
		# Store the response URL
		#----------------------------------------------------------------------#
		user = 'local'
		path = "./../software/chromedriver/" if user == 'local' else "/home/ec2-user/chromedriver/"
		path_to_chrome_driver = os.path.abspath(path)

		#----------------------------------------------------------------------#
		# Store the response URL
		#----------------------------------------------------------------------#
		options = Options()
		options.add_argument('--headless')

		#----------------------------------------------------------------------#
		# Store the response URL
		#----------------------------------------------------------------------#
		driver = webdriver.Chrome(path_to_chrome_driver, chrome_options = options)

		#----------------------------------------------------------------------#
		# Store the response URL
		#----------------------------------------------------------------------#
		driver.get(response.url)
		time.sleep(4)

		#----------------------------------------------------------------------#
		# Click the check box
		#----------------------------------------------------------------------#
		driver.find_element_by_css_selector("#ctl00_ContentPlaceHolder1_RBtnLst_1").click()
		time.sleep(3)

		#----------------------------------------------------------------------#
		# Storge the source and return it as a selector object
		#----------------------------------------------------------------------#
		page_source = driver.page_source
		page_source = Selector(text = page_source)

		return(page_source)

	#----------------------------------------------------------------------#
	# This is the main parse function
	#----------------------------------------------------------------------#
	def parse(self, response):

		if material == 1:

			source = self.select_check_box(response)

		#----------------------------------------------------------------------#
		# Store which callback you want here
		# Then parse the district URLs
		# Then yield those to the processing function
		#----------------------------------------------------------------------#
		if self.block == 1:
			process_link = self.parse_block
		
		elif self.block == 0:
			process_link = self.parse_fto_list	
		
		#----------------------------------------------------------------------#
		# Store the district URLs
		# Parse them to get the links to all FTOs which have been through the 
		# first sign stage
		#----------------------------------------------------------------------#		
		district_urls = source.xpath('*//a//@href').extract()
		
		for url in district_urls:
			if 'district_name' in url and self.district_name in url and 'fst_sig' in url:
				yield(response.follow(url, process_link))

		#----------------------------------------------------------------------#
		# Store the district URLs
		#----------------------------------------------------------------------#
		district_urls = ['http://mnregaweb4.nic.in/netnrega/FTO/' + url for url in district_urls]
		district_urls = pd.DataFrame(district_urls, columns = ['url'])
		district_urls = district_urls.to_csv('district_urls.csv')

	
