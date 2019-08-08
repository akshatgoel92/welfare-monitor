import pandas as pd
import sys
from sqlalchemy import *
from datetime import datetime
from common import helpers
from common import errors as er
import numpy as np


def process_chunks(gens):
	# Initialize an empty list
	df_list = pd.concat([gen for gen in gens])	
	# Return statement
	return(df_list)


def ensure_jcn_format(old_jcn):
	# This function loops through the original JCN and appends to a new string
	# with the appropriate values - it then returns the new string
	# It proceeds as follows:
	# We only want to reformat old JCNs so filter out new JCNS
	# Replace whitespace in old JCN
	# Initialize the new JCN
	# Loop through each index and character of the old JCN
	# Check for alphabetic and numeric characters and then append them to the new JCN
	# If we have then just add the remaining part of the old JCN to the new JCN and then we are done
	# Else if we have found a '-' we know that this is not the end of the JCN
	# Difference between indices should be 4 for the region code -> if not, add zeros
	# Get the index of the next dash starting from the current index + 1
	# Calculate the index difference
	# -1 is returned if no more dashes exist -> so near "/"
	# Find the occurence of the '/'
	# Add the zeros
	# Fill up the zeros
	# Deal with the other case
	# Add in new zeros until the next index??
	# Return statement
	
	if not old_jcn: return None
	old_jcn.replace(" ", "")
	new_jcn = ''
	
	for index, char in enumerate(old_jcn):
		
		if char.isalpha() or char.isdigit(): 
			new_jcn += char
		
		elif char == '/':
			new_jcn += old_jcn[index:]
			break
		
		elif char == '-':
			new_jcn += char
			next_dash_index = old_jcn.find('-', index + 1)
			index_difference = next_dash_index - index
			
			if next_dash_index == -1: 
				index_dash_difference = old_jcn.find('/') - index
				
				if index_dash_difference != 4: 
					for iter in range(4 - index_dash_difference): 
						new_jcn += '0'
			
			elif index_difference != 4:
				for iter in range(4 - index_difference): new_jcn += '0'
	
	return(new_jcn)