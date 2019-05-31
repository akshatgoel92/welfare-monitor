"""Change queue data types again

Revision ID: df95ddec35e0
Revises: 8ff18595f27e
Create Date: 2019-05-31 14:47:05.414505

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'df95ddec35e0'
down_revision = '8ff18595f27e'
branch_labels = None
depends_on = None


def upgrade():
    
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN fto_no varchar(60)')
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN fto_type varchar(30)')
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN current_stage varchar(10)')

def downgrade():
    
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN fto_no TEXT')
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN fto_type TEXT')
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN current_stage TEXT')
