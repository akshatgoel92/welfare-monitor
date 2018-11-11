# Import packages
import helpers

# Execute the processing
if __name__ == '__main__':

	now = str(datetime.datetime.now())
	helpers.process_log('./nrega_output/log.csv', '/Logs/log_' + now + '.csv')
	helpers.update_fto_nos()
