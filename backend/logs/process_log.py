# Import packages
import pandas as pd
import numpy as np
import json

from common.helpers import *

# Process the log
def process_log():
	
	# Store recipients
	with open('./backend/mail/recipients.json') as r:
		recipients = json.load(r)	
	
	# Send notifications
	dropbox_upload('./nrega_output/log.csv', '/Logs/log.csv')
	send_email('Done, check the log!', 'GMA NREGA Scrape: Error count', recipients)
	