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
	
	op.execute('CREATE TABLE transactions_alt LIKE transactions')
	op.execute('CREATE TABLE wage_lists_alt LIKE wage_lists')
	op.execute('CREATE TABLE banks_alt LIKE banks')
	 
	return
	
def downgrade():
    
	# Drop tables
	op.drop_table('wage_lists_alt')
	op.drop_table('accounts_alt')
	op.drop_table('transactions_alt')
	op.drop_table('banks_alt')
	
