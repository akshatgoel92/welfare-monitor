



def format_jcn_refactor(old_jcn):
	
	if not old_jcn: return ''
	old_jcn.replace(" ", "")
	new_jcn = ''
	
	for index, char in enumerate(old_jcn):
		
		new_jcn = add_alpha_numeric(char, new_jcn)
		new_jcn, break_flag = add_chars_after_slash(old_jcn, new_jcn, index, char)
		if break_flag == 1: break
		
		next_dash_index, index_difference = add_index_diff(old_jcn, index)

			

				

			
		elif index_difference != 4: add_zeros
	
	return(new_jcn)
	

def add_alpha_numeric(char, new_jcn):

	if char.isalpha() or char.isdigit(): new_jcn += char
	
	return(new_jcn)
	
	
def add_chars_after_slash(old_jcn, new_jcn, index, char):
	
	if char == '/': 
		new_jcn += old_jcn[index:]
		to_break = 1
	
	return(new_jcn, to_break)
	
	
def add_index_diff(old_jcn, index):
		
	next_dash_index = old_jcn.find('-', index + 1)
	index_difference = next_dash_index - index
	
	return(next_dash_index, index_difference)
			

def add_index_dash_diff(next_dash_index, old_jcn, index):
		
	to_add_zeros = 0
		
	if next_dash_index == -1: 
			
		index_dash_difference = old_jcn.find('/') - index
		if index_dash_difference != 4: to_add_zeros = 1 
		
	return(index_dash_difference, to_add_zeros)
	
	
def add_zeros(index_dash_difference, new_jcn):
		
	for iter in range(4 - index_dash_difference): new_jcn += '0'
		
	return(new_jcn)