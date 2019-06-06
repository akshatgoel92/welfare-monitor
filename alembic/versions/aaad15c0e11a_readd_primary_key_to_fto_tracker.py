"""readd primary key to FTO tracker

Revision ID: aaad15c0e11a
Revises: 2b201a9c2f6d
Create Date: 2019-06-06 22:09:03.516100

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aaad15c0e11a'
down_revision = '2b201a9c2f6d'
branch_labels = None
depends_on = None


def upgrade():
    
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN fto_no varchar(100)')
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN fto_type varchar(30)')
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN current_stage varchar(15)')
    op.create_primary_key('primary_fto_queue', 'fto_queue', ['fto_no'])

def downgrade():
    
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN fto_no TEXT')
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN fto_type TEXT')
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN current_stage TEXT')
    op.drop_constraint('primary_fto_queue', 'fto_queue', type_='primary')
