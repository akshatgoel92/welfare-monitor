"""States + blocks + panchayats remove auto-increment in primary key

Revision ID: 7e0da12e53a4
Revises: 5803186f761d
Create Date: 2019-05-30 17:27:08.379826

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7e0da12e53a4'
down_revision = '5803186f761d'
branch_labels = None
depends_on = None


def upgrade():

	op.drop_column('states', 'state_code')
	op.drop_column('blocks', 'block_code')
	op.drop_column('panchayats', 'panchayat_code')
	

def downgrade():
    
	pass

