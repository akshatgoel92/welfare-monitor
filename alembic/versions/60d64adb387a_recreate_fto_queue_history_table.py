"""recreate fto_queue_history table

Revision ID: 60d64adb387a
Revises: d818e9d78659
Create Date: 2019-07-21 14:56:32.558793

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '60d64adb387a'
down_revision = 'd818e9d78659'
branch_labels = None
depends_on = None


def upgrade():
	
	op.execute('DELETE FROM fto_queue_history;')
	op.execute('ALTER TABLE fto_queue_history DROP COLUMN action;')
	
def downgrade():
    
	op.execute('ALTER TABLE fto_queue_history ADD COLUMN action VARCHAR(50)')