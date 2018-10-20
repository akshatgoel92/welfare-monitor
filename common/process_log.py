# Import packages
import os
import datetime
import pandas as pd
import numpy as np
import json
import helpers


# Process the log
def process_log():
	
	# Store recipients
	with open('./backend/mail/recipients.json') as r:
		recipients = json.load(r)
	
	# Store date 
	now = str(datetime.datetime.now())
	
	# Upload log file and send notification
	helpers.dropbox_upload('./nrega_output/log.csv', '/Logs/log_' + now + '.csv')
	helpers.send_email('Done, check the log!', 'GMA NREGA Scrape: Error count', recipients)
	os.unlink('./nrega_output/log.csv')

if __name__ == '__main__':

	# Execute the processing	
	process_log()
	