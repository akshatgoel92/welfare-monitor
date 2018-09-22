# Import Cron API
from crontab import CronTab 

# Schedule job to run 730 am every weekday
my_cron = CronTab()
# Create a cron job    
job = my_cron.new(command='./nrega_etl.sh')
# Set the job schedule
job.setall('30 7 * 1-5 *')
# Write the job to the crontab 
my_cron.write()