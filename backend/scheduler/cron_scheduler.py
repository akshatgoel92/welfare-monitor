# Import Cron API
from crontab import CronTab 

# Schedule job to run 730 am every weekday
my_cron = CronTab()
    
job = my_cron.new(command='./nrega_etl.sh')

job.setall('30 7 * 1-5 *')

my_cron.write()