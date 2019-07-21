"""update_fto_no_dtype_history

Revision ID: 0264a2f16522
Revises: b02939fefad8
Create Date: 2019-07-21 15:30:04.133271

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0264a2f16522'
down_revision = 'b02939fefad8'
branch_labels = None
depends_on = None


def upgrade():
    
	op.execute('ALTER TABLE fto_queue_history MODIFY stage VARCHAR(100);')


def downgrade():
	
    op.execute('ALTER TABLE fto_queue_history MODIFY stage TEXT;')
