"""Create panchayats

Revision ID: 017350a6411f
Revises: 119f0f5eb10d
Create Date: 2019-05-29 19:34:35.948970

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '017350a6411f'
down_revision = '119f0f5eb10d'
branch_labels = None
depends_on = None


def upgrade():

	op.create_table('panchayats',
        sa.Column('panchayat_code', sa.Integer, primary_key = True),
        sa.Column('panchayat_name', sa.String(50), nullable = False)
    )


def downgrade():
    
    op.drop_table('panchayats')
