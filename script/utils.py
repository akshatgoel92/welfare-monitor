import pandas as pd
import numpy as np


def format_jcn(old_jcn):
	
	if not old_jcn: return None
	old_jcn.replace(" ", "")
	new_jcn = ''
	
	for index, char in enumerate(old_jcn):
		
		if char.isalpha() or char.isdigit(): new_jcn += char
		elif char == '/': new_jcn += old_jcn[index:]; break
		
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
				for iter in range(4 - index_difference): 
					new_jcn += '0'
	
	return(new_jcn)