# Import packages
import helpers

# Execute the processing
if __name__ == '__main__':

	now = str(datetime.datetime.now())
	file_from = './nrega_output/log.csv'
	file_to = '/Logs/fba/log_' + now + '.csv'
	helpers.process_log(file_from, file_to )