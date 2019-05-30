"""States + blocks + panchayats add new non-incrementing primary key

Revision ID: 63a66b828aaf
Revises: 7e0da12e53a4
Create Date: 2019-05-30 18:20:11.541763

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '63a66b828aaf'
down_revision = '7e0da12e53a4'
branch_labels = None
depends_on = None


def upgrade():
   	
    op.drop_table('states')
    op.drop_table('blocks')
    op.drop_table('panchayats')

    op.create_table('states',
        sa.Column('state_code', sa.Integer, primary_key = True, autoincrement = False),
        sa.Column('state_name', sa.String(50), nullable = False))

    op.create_table('blocks',
        sa.Column('block_code', sa.Integer, primary_key = True, autoincrement = False),
        sa.Column('block_name', sa.String(50), nullable = False))

    op.create_table('panchayats',
        sa.Column('panchayat_code', sa.Integer, primary_key = True, autoincrement = False),
        sa.Column('panchayat_name', sa.String(50), nullable = False))


def downgrade():
    
 	op.drop_constraint('state_id', 'states', 'primary')
 	op.drop_constraint('block_id', 'blocks', 'primary')
 	op.drop_constraint('panchayats_id', 'panchayats', 'primary')

 	op.drop_column('states', 'state_code')
 	op.drop_column('blocks', 'block_code')
 	op.drop_column('panchayats', 'panchayat_code')
