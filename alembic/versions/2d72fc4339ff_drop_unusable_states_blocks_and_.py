"""Drop unusable states, blocks, and panchayats tables

Revision ID: 2d72fc4339ff
Revises: 63a66b828aaf
Create Date: 2019-05-30 18:43:23.164298

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2d72fc4339ff'
down_revision = '63a66b828aaf'
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
    
	pass
