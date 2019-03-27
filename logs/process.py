# Import packages
import argparse
import os
from common import helpers
from datetime import datetime

# Uploads log file
def process_log(log_file_from, log_file_to):
	
	helpers.dropbox_upload(log_file_from, log_file_to)
	os.unlink(log_file_from)

# Execute the processing
if __name__ == '__main__':
	
	# Create the parser and add the arguments
	parser = argparse.ArgumentParser(description='Dropbox upload parser')
	parser.add_argument('file_from', type=str, help='Source file path')
	parser.add_argument('file_to', type=str, help='Destination file path')
	
	# Parse arguments
	args = parser.parse_args()
	file_from = args.file_from
	file_to = args.file_to + '_' + str(datetime.today()) + '.csv'

	# Call the function
	process_log(file_from, file_to )