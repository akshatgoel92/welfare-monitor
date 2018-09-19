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
from scrapy import signals

from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from nrega_scrape.items import FTOItem
from common.helpers import sql_connect
from common.helpers import send_file

# MySQLdb functionality
pymysql.install_as_MySQLdb()

	
# FTO number pipe-line		
class FTOSummaryPipeline(object):
	
	def __init__(self):
		
		user, password, host, db = sql_connect().values()
		
		self.conn = pymysql.connect(host, user, password, db, charset="utf8", use_unicode=True)
		
		self.cursor = self.conn.cursor()
	
	def _insert_record(self, item):
		
		args = (
		
					item['block_name'].encode('utf-8'),
	
					item['total_fto'].encode('utf-8'),
		 
					item['first_sign'].encode('utf-8'),
	
					item['first_sign_pending'].encode('utf-8'),
	
					item['second_sign'].encode('utf-8'),
	
					item['second_sign_pending'].encode('utf-8'),
	
					item['fto_sent_bank'].encode('utf-8'),
	
					item['transact_sent_bank'].encode('utf-8'),
	
					item['fto_processed_bank'].encode('utf-8'),
	
					item['transact_processed_bank'].encode('utf-8'),
	
					item['fto_partial_bank'].encode('utf-8'),
	
					item['transact_partial_bank'].encode('utf-8'),
	
					item['fto_pending_bank'].encode('utf-8'),
	
					item['transact_pending_bank'].encode('utf-8'),
	
					item['transact_processed_bank_resp'].encode('utf-8'),
	
					item['invalid_accounts_bank_resp'].encode('utf-8'),
	
					item['transact_rejected_bank_resp'].encode('utf-8'),
	
					item['transact_total_bank_resp'].encode('utf-8'),
		
					item['url'].encode('utf-8'),
			
					item['spider'].encode('utf-8'),
	
					item['server'].encode('utf-8'),
	
					item['date'].encode('utf-8')
					
					)
		
		sql = """ INSERT INTO fto_summary (block_name, first_sign, first_sign_pending, fto_partial_bank, 
					 fto_pending_bank, fto_processed_bank, fto_sent_bank, invalid_accounts_bank_resp, second_sign, 
					 second_sign_pending, server, spider, total_fto, transact_partial_bank, transact_pending_bank, transact_processed_bank, 
					 transact_processed_bank_resp, transact_rejected_bank_resp, transact_sent_bank, transact_total_bank_resp, url, date)
					 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
					 
		self.cursor.execute(sql, args)
		
		self.conn.commit()
		
	def process_item(self, item, spider):
	
		if isinstance(item, NREGAItem):
		
			self._insert_record(item)
			
		return(item)
	
	def close_spider(self, spider):
	
		with open('./backend/recipients.json') as recipients:
			
			error_recipients = json.load(recipients)
			
		send_file('./nrega_output/log.csv', 'Error log for NREGA Pull', error_recipients)	

class FTONoPipeline(object):
	
	def __init__(self):
		
		user, password, host, db = sql_connect().values()
		
		self.conn = pymysql.connect(host, user, password, db, charset = "utf8", use_unicode = True)
		
		self.cursor = self.conn.cursor()
		
	def _insert_record(self, item):
		
		args = (
					
					item['fto_no'].encode('utf-8'),
					
					item['state_code'].encode('utf-8'),
					
					item['district_code'].encode('utf-8'), 
					
					item['block_code'].encode('utf-8'), 
					
					item['process_date'],
					
					item['url'].encode('utf-8'),
					
					item['spider'].encode('utf-8'),
					
					item['server'].encode('utf-8'),
					
					item['date'].encode('utf-8'),
					
					item['fto_stage'].encode('utf-8')
					
					)
					
		sql = """INSERT INTO fto_numbers (fto_no, state_code, district_code, block_code, process_date,
					 url, spider, server, date, fto_stage) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
					 
		self.cursor.execute(sql, args)
		
		self.conn.commit()
		
	def process_item(self, item, spider):
		
		if isinstance(item, FTONo):
			
			self._insert_record(item)
			
		return(item)
	
	#def close_spider(self, spider):
	
	#	with open('./backend/recipients.json') as recipients:
			
	#		error_recipients = json.load(recipients)
			
	#	send_file('./nrega_output/log.csv', 'Error log for NREGA Pull', error_recipients)	
		
		
class FTOContentPipeline(object):
	
	def __init__(self):
		
		user, password, host, db = sql_connect().values()
		
		self.conn = pymysql.connect(host, user, password, db, charset="utf8", use_unicode=True)
		
		self.cursor = self.conn.cursor()
		
	def _insert_record(self, item):
	
		args = (
					
					item['block_name'].encode('utf-8'),
					
					item['jcn'].encode('utf-8'),
					
					item['transact_ref_no'].encode('utf-8'),
					
					item['transact_date'].encode('utf-8'),
					
					item['app_name'].encode('utf-8'),
					
					item['prmry_acc_holder_name'].encode('utf-8'),
					
					item['wage_list_no'].encode('utf-8'),
					
					item['acc_no'].encode('utf-8'),
					
					item['bank_code'].encode('utf-8'),
					
					item['ifsc_code'].encode('utf-8'),
					
					item['credit_amt_due'].encode('utf-8'),
					
					item['status'].encode('utf-8'),
					
					item['processed_date'].encode('utf-8'),
					
					item['utr_no'].encode('utf-8'),
					
					item['rejection_reason'].encode('utf-8'),
					
					item['server'].encode('utf-8'),
					
					item['scrape_date'].encode('utf-8'),
					
					item['time_taken'].encode('utf-8'),
					
					item['url'].encode('utf-8')
				
					)
					
		sql = """ INSERT INTO fto_content (block_name, jcn, transact_ref_no, transact_date, app_name,
					  prmry_acc_holder_name, wage_list_no, acc_no, bank_code, ifsc_code, credit_amt_due, 
					  status, processed_date, utr_no, rejection_reason, server, scrape_date, time_taken, url)
					  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
					  
	def process_item(self, item, spider):
		
		if isinstance(item, FTOItem):
			
			self._insert_record(self, item)
			
		return(item)
			
	


	
