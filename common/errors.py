import json
from common import helpers


def output_error(subj, msg):
	
	helpers.send_email(subj, msg)

	return


def handle_error(error_code, data={}):

	
	with open('./common/error_messages.json') as data_file:
		error_data = json.load(data_file)

	subj = error_data[0][error_code]['subj'].format(**data)
	msg = error_data[0][error_code]['msg'].format(**data)
	output_error(subj, msg)

	return