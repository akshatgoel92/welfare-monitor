"""Recreate fto-queue table

Revision ID: d818e9d78659
Revises: aaad15c0e11a
Create Date: 2019-06-09 01:09:03.585480

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd818e9d78659'
down_revision = 'aaad15c0e11a'
branch_labels = None
depends_on = None


def upgrade():
    	
    	op.create_table('fto_queue',
        				sa.Column('fto_no', sa.String(100), primary_key = True),
        				sa.Column('fto_type', sa.String(15)), 
        				sa.Column('done', sa.SmallInteger()),
        				sa.Column('current_stage', sa.String(15)))
   



def downgrade():
    
    op.drop_table('fto_queue')
