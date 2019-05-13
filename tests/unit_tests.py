# Overall imports
import functools
import os
import json
import sys
import dropbox
import pymysql
import smtplib
import boto3
import pandas as pd
import numpy as np
import queue

from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from common import errors as er
from common import helpers


def run_test(func):

	@functools.wraps(func)
	def test(*args, **kwargs):
		
		success = 1
		try: func(*args, **kwargs)
		except Exception as e: success = 0
		
		return(success)
	
	return(test)


@run_test
def test_sql_connect():

	helpers.sql_connect()


@run_test
def test_db_conn():

	helpers.db_conn()


@run_test
def test_db_engine():

	helpers.db_engine()


@run_test
def test_upload_dropbox():

	helpers.upload_dropbox()


@run_test
def test_download_file_s3():

	helpers.download_file_s3()


@run_test
def test_delete_files():

	helpers.delete_files()


@run_test
def test_send_email():

	helpers.send_email()


@run_test
def test_s3(test_file_source, test_file_dest):

	helpers.upload_file_s3(test_file_source, test_file_dest)


@run_test
def test_error_handling():

	er.handle_error(error_code ='29', data = {'sheet_name': 'Test'})


@run_test
def test_queue_add():

	queue.add.main()
	

@run_test
def test_queue_update():

	queue.update.main()


@run_test
def test_queue_make():

	queue.make.main()


@run_test
def test_queue_download():
	
	queue.download.main()


def main():

	results = []
	results.append(test_error_handling())

	return(results)


if __name__ == '__main__':

	results = main()