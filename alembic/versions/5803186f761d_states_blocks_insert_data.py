"""States + blocks insert data

Revision ID: 5803186f761d
Revises: 3560c15dca7d
Create Date: 2019-05-30 17:02:01.281644

"""
from alembic import op
from common import helpers
from sqlalchemy import MetaData

import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5803186f761d'
down_revision = '3560c15dca7d'
branch_labels = None
depends_on = None


def upgrade():
	
	conn = helpers.db_engine()
	metadata = MetaData(bind = conn, reflect = True)
	
	states = metadata.tables['states']
	op.bulk_insert(states, [{'state_code': 33, 'state_name': 'Chhattisgarh'}])

	data = [

		{"block_code": 6, "block_name": "Arang"},
    	{"block_code": 2, "block_name": "Dharsiwa"},
    
    	{"block_code": 1, "block_name": "Abhanpur"},
    	{"block_code": 8, "block_name": "Tilda"}]
	
	blocks = metadata.tables['blocks']
	op.bulk_insert(blocks, data)


def downgrade():
	
	op.execute("DELETE FROM states;")
	op.execute("DELETE FROM blocks;")
