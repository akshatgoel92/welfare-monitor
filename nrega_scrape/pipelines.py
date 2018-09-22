# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# Import packages
import os
import json 
import pymysql
pymysql.install_as_MySQLdb()

from scrapy import signals
from scrapy.contrib.exporter import CsvItemExporter
from scrapy.exceptions import DropItem

from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from nrega_scrape.items import FTOItem
from common.helpers import sql_connect
from common.helpers import send_file

# Twisted
from twisted.enterprise import adbapi

	
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
	
class FTONoPipeline(object):
	
    def __init__(self):
    	
    	user, password, host, db_name = sql_connect().values()
    	self.insert_sql = "INSERT INTO fto_numbers (%s) VALUES (%s)"
    	self.dbpool = adbapi.ConnectionPool('pymysql', db = db_name, host = host, user = user, passwd = password, cursorclass = pymysql.cursors.DictCursor, charset = 'utf8', use_unicode = True, cp_max = 2)

    def spider_closed(self):
        self.dbpool.close()

    def process_item(self, item, spider):
    	if isinstance(item, FTONo):
    		self.insert_data(item, self.insert_sql)
    	return(item)

    def insert_data(self, item, insert):
        keys = item.keys()
        fields = u','.join(keys)
        print(fields)
        qm = u','.join([u'%s'] * len(keys))
        print(qm)
        sql = insert % (fields, qm)
        print(sql)
        data = [item[k] for k in keys]
        return(self.dbpool.runOperation(sql, data))
		
		
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
					
					item['credit_amt_actual'].encode('utf-8'),
					
					item['status'].encode('utf-8'),
					
					item['processed_date'].encode('utf-8'),
					
					item['utr_no'].encode('utf-8'),
					
					item['rejection_reason'].encode('utf-8'),
					
					item['server'].encode('utf-8'),
					
					item['fto_no'].encode('utf-8'),
					
					item['scrape_date'].encode('utf-8'),
					
					item['time_taken'],
					
					item['url'].encode('utf-8')
				
					)
					
		sql = """ INSERT INTO fto_content (block_name, jcn, transact_ref_no, transact_date, app_name,
					  prmry_acc_holder_name, wage_list_no, acc_no, bank_code, ifsc_code, credit_amt_due, 
					  credit_amt_actual, status, processed_date, utr_no, rejection_reason, server, fto_no, scrape_date, 
					  time_taken, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
		
		self.cursor.execute(sql, args)
		
		self.conn.commit()
				
	def process_item(self, item, spider):
		
		if isinstance(item, FTOItem):
			
			if item['block_name'] is None:
				
				raise DropItem("Missing block name in %s" % item)
			
			else:
				
				self._insert_record(item)
				
				return(item)
			
	


	
