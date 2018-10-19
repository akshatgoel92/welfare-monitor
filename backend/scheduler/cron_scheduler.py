# Import Cron API
from crontab import CronTab 

# Schedule job to run 730 am every weekday
my_cron = CronTab()
# Create a cron job    
job = my_cron.new(command='scrapy crawl fto_content')
# Set the job schedule
job.setall('41 09-18 * * *')
# Write the job to the crontab 
my_cron.write()