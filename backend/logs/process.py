#--------------------------------------------------------#
# Author: Akshat Goel
# Purpose: Process and upload the log file
# Contact: akshat.goel@ifmr.ac.in
#--------------------------------------------------------#
import argparse
import os
from common import helpers
from datetime import datetime


# Upload Scrapy log files to Dropbox and S3
def process_log():
	
	# Create the parser and add the arguments
	parser = argparse.ArgumentParser(description='Dropbox upload parser')
	parser.add_argument('file_from', type=str, help='Source file path')
	parser.add_argument('file_to', type=str, help='Destination file path')
	
	# Parse arguments
	args = parser.parse_args()
	file_from = args.file_from
	file_to = args.file_to + '_' + str(datetime.today()) + '.csv'
	
	# Send to Dropbox and S3
	helpers.upload_dropbox(file_from, file_to)
	helpers.upload_s3(file_from, file_to)

	# Delete file
	os.unlink(log_file_from)


if __name__ == '__main__':
	
	process_log()