"""Create FTO stages mapping to numeric ID

Revision ID: 855a3112fba1
Revises: 
Create Date: 2019-05-29 17:53:28.062817

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import String, Column
from sqlalchemy.sql import table, column


# revision identifiers, used by Alembic.
revision = '855a3112fba1'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():

	op.create_table('fto_stages',
        sa.Column('stage_id', sa.Integer, primary_key = True),
        sa.Column('stage', sa.String(50), nullable = False)
    )


def downgrade():
    
    op.drop_table('fto_stages')