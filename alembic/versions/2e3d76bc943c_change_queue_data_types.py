"""Change queue data-types

Revision ID: 2e3d76bc943c
Revises: 855a3112fba1
Create Date: 2019-05-29 19:16:01.026053

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2e3d76bc943c'
down_revision = '855a3112fba1'
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
