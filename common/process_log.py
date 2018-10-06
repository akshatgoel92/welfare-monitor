# Import packages
import pandas as pd
import numpy as np
import json
import helpers

# Process the log
def process_log():
	
	# Store recipients
	with open('./backend/mail/recipients.json') as r:
		recipients = json.load(r)
	
	# Upload log file and send notification
	helpers.dropbox_upload('./nrega_output/log.csv', '/Logs/log.csv')
	helpers.send_email('Done, check the log!', 'GMA NREGA Scrape: Error count', recipients)

if __name__ == '__main__':

	# Execute the processing	
	process_log()
	