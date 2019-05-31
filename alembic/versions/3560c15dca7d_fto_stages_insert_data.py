"""FTO stages insert data

Revision ID: 3560c15dca7d
Revises: 017350a6411f
Create Date: 2019-05-30 15:21:47.422809

"""
from alembic import op
from sqlalchemy import MetaData
from common import helpers

import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '3560c15dca7d'
down_revision = '017350a6411f'
branch_labels = None
depends_on = None


def upgrade():

	conn = helpers.db_engine()
	metadata = MetaData(bind = conn, reflect = True)
	fto_stages = metadata.tables['fto_stages']
	
	data = [

		{"stage_id": 6, "stage":"P"},
    	{"stage_id": 2, "stage": "fst_sig"},
    
    	{"stage_id": 1, "stage": "fst_sig_not"},
    	{"stage_id": 8, "stage": "pb"},
    
    	{"stage_id": 7, "stage": "pp"},
    	{"stage_id": 5, "stage": "sb"},
    
    	{"stage_id": 4, "stage": "sec_sig"},
    	{"stage_id": 3, "stage": "sec_sig_not"}]
	
	op.bulk_insert(fto_stages, data)


def downgrade():
    
    op.execute("DELETE FROM fto_stages;")