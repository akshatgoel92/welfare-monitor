"""Change length of varchar in tracker

Revision ID: 46e289f71cfc
Revises: 31e2639d9349
Create Date: 2019-05-31 15:37:40.180243

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '46e289f71cfc'
down_revision = '31e2639d9349'
branch_labels = None
depends_on = None


def upgrade():
    
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN current_stage varchar(15)')


def downgrade():
    
    op.execute('ALTER TABLE fto_queue MODIFY COLUMN current_stage varchar(10)')
