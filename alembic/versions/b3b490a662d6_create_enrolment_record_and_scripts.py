"""Create enrolment record and scripts

Revision ID: b3b490a662d6
Revises: c24e7cca35ba
Create Date: 2019-09-04 14:07:10.522359

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b3b490a662d6'
down_revision = 'c24e7cca35ba'
branch_labels = None
depends_on = None


def upgrade():
	
	op.create_table('enrolment_record', 
				 sa.Column('id', sa.Integer, primary_key=True), 
				 sa.Column('phone', sa.String(50)), 
				 sa.Column('jcn', sa.String(50)), 
				 sa.Column('jc_status', sa.Integer),
				 sa.Column('time_pref', sa.String(50)),
				 sa.Column('time_pref_label', sa.String(50)),
				 sa.Column('file_name_s3', sa.String(50)),
				 sa.Column('file_upload_to_s3_date', sa.String(50)),
				 sa.Column('insert_date', sa.String(50)),
				 sa.Column('enrolment_date', sa.String(50)),
				 sa.Column('pilot', sa.Integer)
				 )
	
	op.create_table('scripts', 
				 sa.Column('id', sa.Integer, primary_key=True), 
				 sa.Column('phone', sa.String(50)), 
				 sa.Column('time_pref', sa.String(50)),
				 sa.Column('time_pref_label', sa.String(50)),
				 sa.Column('amount', sa.Integer),
				 sa.Column('transact_date', sa.String(50)),
				 sa.Column('rejection_reason', sa.String(50)),
				 sa.Column('day1', sa.String(50)),
				 sa.Column('file_name_s3', sa.String(50), primary_key=True),
				 sa.Column('file_upload_to_s3_date', sa.String(50), primary_key=True),
				 sa.Column('insert_date', sa.String(50))
				 )

def downgrade():
	
	op.drop_table('enrolment_record')
	op.drop_table('scripts')
