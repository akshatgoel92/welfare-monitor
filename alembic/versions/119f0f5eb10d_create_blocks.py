"""Create blocks

Revision ID: 119f0f5eb10d
Revises: 0c98365f95bd
Create Date: 2019-05-29 19:34:28.354837

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '119f0f5eb10d'
down_revision = '0c98365f95bd'
branch_labels = None
depends_on = None


def upgrade():

	op.create_table('blocks',
        sa.Column('block_code', sa.Integer, primary_key = True),
        sa.Column('block_name', sa.String(50), nullable = False)
    )


def downgrade():
    
    op.drop_table('blocks')
