"""Add primary key to FTO tracker

Revision ID: 31e2639d9349
Revises: df95ddec35e0
Create Date: 2019-05-31 15:11:34.811310

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '31e2639d9349'
down_revision = 'df95ddec35e0'
branch_labels = None
depends_on = None


def upgrade():
    
    op.create_primary_key('primary_fto_queue', 'fto_queue', ['fto_no'])


def downgrade():
    
    op.drop_constraint('primary_fto_queue', 'fto_queue', type_='primary')
