"""fix stage column name q history

Revision ID: b02939fefad8
Revises: 60d64adb387a
Create Date: 2019-07-21 15:24:15.661591

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b02939fefad8'
down_revision = '60d64adb387a'
branch_labels = None
depends_on = None


def upgrade():
    
	op.execute('ALTER TABLE fto_queue_history CHANGE current_stage stage VARCHAR(50)')


def downgrade():
	
    op.execute('ALTER TABLE fto_queue_history CHANGE stage current_stage TEXT')
