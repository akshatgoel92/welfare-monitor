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

	# We only want to reformat old JCNs
	if not old_jcn: return None
	# Replace whitespace in old JCN
	old_jcn.replace(" ", "")
	# Initialize the new JCN
	new_jcn = ''
	# Loop through each index and character of the old JCN
	for index, char in enumerate(old_jcn):
		# Check for alphabetic and numeric characters
		if char.isalpha() or char.isdigit(): 
			# And then append them to the new JCN
			new_jcn += char
		# Check to see if we have reached '/'
		elif char == '/':
			# If we have then just add the remaining part of the old JCN to the new JCN
			new_jcn += old_jcn[index:]
			# We are done
			break
		# Else if we have found a '-' we know that this is not the end of the JCN
		elif char == '-':
			# Difference between indices should be 4 for the region code -> if not, add zeros
			new_jcn += char
			# Get the index of the next dash starting from the current index + 1
			next_dash_index = old_jcn.find('-', index + 1)
			# Calculate the index difference
			index_difference = next_dash_index - index
			# -1 is returned if no more dashes exist -> so near "/"			
			if next_dash_index == -1: 
				# Find the occurence of the '/'
				index_dash_difference = old_jcn.find('/') - index
				# Add the zeros
				if index_dash_difference != 4:
					# Fill up the zeros
					for iter in range(4 - index_dash_difference): new_jcn += '0'
			# Deal with the other case
			elif index_difference != 4:
				# Add in new zeros until the next index??
				for iter in range(4 - index_difference): new_jcn += '0'
	# Return statement
	return new_jcn