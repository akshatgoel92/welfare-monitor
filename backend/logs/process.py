import argparse
import os
from common import helpers
from datetime import datetime


# Upload Scrapy log files to Dropbox and S3 and delete after
def process_log():
	
	parser = argparse.ArgumentParser(description='Dropbox upload parser')
	parser.add_argument('file_from', type=str, help='Source file path')
	parser.add_argument('file_to', type=str, help='Destination file path')
	
	args = parser.parse_args()
	file_from = args.file_from
	file_to = args.file_to + '_' + str(datetime.today()) + '.csv'
	
	try: helpers.upload_dropbox(file_from, file_to)
	except Exception as e: er.handle_error(error_code ='12', data = {})
		
	try: helpers.upload_s3(file_from, file_to)
	except Exception as e: er.handle_error(error_code ='13', data = {})

	os.unlink(file_from)


if __name__ == '__main__':
	
	process_log()