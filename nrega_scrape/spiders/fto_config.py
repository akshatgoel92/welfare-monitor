# Import packages
import json


# Store stages 
def store_stages():
	
	stages =	{'pb': 'fto_processed_by_bank', 'sb': 'fto_sent_to_bank', 
				'sec_sig': 'fto_second_sign_done', 'fst_sig': 'fto_first_sign_done',
				'fst_sig_not': 'fto_first_sign_pending', 'sec_sig_not': 'fto_second_sign_pending',
				'pp': 'fto_partial_processed_by_bank', 'P': 'fto_pending_bank_processing'}

	return(stages)

# Store stage numbers from the URL
def store_stage_numbers():

	stages = {'pb': 8, 'sb': 5, 'sec_sig': 4, 'fst_sig': 2, 
				'fst_sig_not': 1, 'sec_sig_not': 3, 'pp': 7, 
				'P': 6}

	return(stages)

# Store whether the stage occurs at the block office or bank
def store_block_or_bank():

	block = ['sec_sig', 'fst_sig', 'fst_sig_not', 'sec_sig_not']
	bank = ['pb', 'sb', 'pp', 'P']

	return(block, bank)

# Store columns from the table
def store_table_columns():

	columns = ['fto_no', 'institution', 'sign_date', 
			   'total_transact_due', 'total_amt_due',
			   'total_transact_processed', 'total_amt_processed',
			   'total_transact_rejected', 'total_amt_rejected',
			   'total_credited_amt','total_invalid_account', 'block']

	return(columns)

# Put everything together
def main():

	stage_names = store_stages()
	
	stage_numbers = store_stage_numbers()
	
	stages_block, stages_bank = store_block_or_bank()
	
	stage_table_columns = store_table_columns()

	table_config = {'stage_names': stage_names, 'stage_numbers': stage_numbers, 
					'stages_block': stages_block, 'stages_bank': stages_bank, 
					'stage_table_columns': stage_table_columns}

	with open('./nrega_scrape/spiders/tables_config.json', 'w') as config:

		json.dump(table_config, config, indent = 4, sort_keys = True)

# Execute this
if __name__ == '__main__':

	main()