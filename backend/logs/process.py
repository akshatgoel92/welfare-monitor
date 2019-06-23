import argparse
import os
from common import helpers
from common import errors as er
from datetime import datetime


# Upload Scrapy log files to Dropbox and S3 and delete after
def process_log():
	
	parser = argparse.ArgumentParser(description='Dropbox upload parser')
	parser.add_argument('file_from', type=str, help='Source file path')
	parser.add_argument('file_to', type=str, help='Destination file path')
	
	args = parser.parse_args()
	file_from = args.file_from
	file_to = args.file_to + '_' + str(datetime.today()) + '.csv'
	
	try:

		log_size = os.path.getsize(file_from)/1024
		subj = "GMA Update 3: Log size report"
		msg = "The size of the log is {} KB.".format(log_size) 
		helpers.send_email(subj, msg)

	except Exception as e:

		er.handle_error(error_code ='12', data = {})


	try: helpers.upload_dropbox(file_from, file_to)
	except Exception as e: er.handle_error(error_code ='13', data = {})
		
	try: helpers.upload_s3(file_from, file_to)
	except Exception as e: er.handle_error(error_code ='14', data = {})

	os.unlink(file_from)


if __name__ == '__main__':
	
	process_log()