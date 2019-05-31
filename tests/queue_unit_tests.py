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
		
		try: 

			func(*args, **kwargs)
		
		except Exception as e: 

			success = 0
			er.handle_error(error_code ='1')

		return(success)
	
	return(test)


@run_test
def test_queue_make():

	queue.make.main()


@run_test
def test_queue_add():

	queue.add.main()
	

@run_test
def test_queue_update():

	queue.update.main()


@run_test
def test_queue_download():
	
	queue.download.main()