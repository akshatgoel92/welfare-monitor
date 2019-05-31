"""Create states

Revision ID: 0c98365f95bd
Revises: 2e3d76bc943c
Create Date: 2019-05-29 19:34:13.221409

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0c98365f95bd'
down_revision = '2e3d76bc943c'
branch_labels = None
depends_on = None


def upgrade():

	op.create_table('states',
        sa.Column('state_code', sa.Integer, primary_key = True),
        sa.Column('state_name', sa.String(50), nullable = False)
    )


def downgrade():
    
    op.drop_table('states')
