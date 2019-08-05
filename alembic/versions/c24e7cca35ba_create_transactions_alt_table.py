"""create transactions_alt table

Revision ID: c24e7cca35ba
Revises: 0264a2f16522
Create Date: 2019-08-05 21:52:54.668098

"""
from common import helpers
from alembic import op

import sqlalchemy as sa
import pandas as pd

# revision identifiers, used by Alembic.
revision = 'c24e7cca35ba'
down_revision = '0264a2f16522'
branch_labels = None
depends_on = None


def upgrade():
    
	# Store the columns that go in the transactions table
	transact_cols = ["block_name", "jcn", "transact_ref_no", "transact_date", "app_name", "wage_list_no", "acc_no", "ifsc_code", 
					 "credit_amt_due", "credit_amt_actual", "processed_date", "utr_no", "rejection_reason", "fto_no", 
					 "scrape_date", "scrape_time"]
	
	# Load the data
	transactions = pd.read_csv('./output/transactions_alt.csv')
	
	# Get the correct columns for the correct tables
	wage_lists_alt = transactions[['block_name', 'wage_list_no']]
	accounts_alt = transactions[['jcn', 'acc_no', 'ifsc_code', 'prmry_acc_holder_name']]
	banks_alt = transactions[['ifsc_code', 'bank_code']]
	transactions_alt = transactions[transact_cols]
	
	engine = helpers.db_engine()
	
	# Create the tables 
	transactions_alt.to_sql('transactions_alternate', if_exists = 'replace', index = False, con = engine, chunksize = 1000)
	banks_alt.to_sql('banks_alternate', if_exists = 'replace', index = False, con = engine, chunksize = 1000)
	accounts_alt.to_sql('accounts_alternate', if_exists = 'replace', index = False, con = engine, chunksize = 1000)
	wage_lists_alt.to_sql('wage_lists_alternate', if_exists = 'replace', index = False, con = engine, chunksize = 1000)

	return
	
def downgrade():
    
	# Drop tables
	op.drop_table('wage_lists_alternate')
	op.drop_table('accounts_alternate')
	op.drop_table('transactions_alternate')
	op.drop_table('banks_alternate')
	
