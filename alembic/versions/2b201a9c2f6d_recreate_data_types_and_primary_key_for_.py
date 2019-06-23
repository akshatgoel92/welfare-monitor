"""Recreate data types and primary key for FTO queue

Revision ID: 2b201a9c2f6d
Revises: 46e289f71cfc
Create Date: 2019-06-05 17:56:59.189197

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2b201a9c2f6d'
down_revision = '46e289f71cfc'
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
