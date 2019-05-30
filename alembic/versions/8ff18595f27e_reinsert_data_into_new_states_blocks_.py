"""Reinsert data into new states, blocks tables

Revision ID: 8ff18595f27e
Revises: 2d72fc4339ff
Create Date: 2019-05-30 19:04:32.584004

"""
from alembic import op
from common import helpers
from sqlalchemy import MetaData

import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8ff18595f27e'
down_revision = '2d72fc4339ff'
branch_labels = None
depends_on = None


def upgrade():
	
	conn = helpers.db_engine()
	metadata = MetaData(bind = conn, reflect = True)
	
	states = metadata.tables['states']
	op.bulk_insert(states, [{'state_code': 33, 'state_name': 'Chhattisgarh'}])

	data = [

		{"block_code": 15, "block_name": "Arang"},
    	{"block_code": 12, "block_name": "Dharsiwa"},
    
    	{"block_code": 8, "block_name": "Abhanpur"},
    	{"block_code": 7, "block_name": "Tilda"}]
	
	blocks = metadata.tables['blocks']
	op.bulk_insert(blocks, data)


def downgrade():
	
	op.execute("DELETE FROM states;")
	op.execute("DELETE FROM blocks;")
