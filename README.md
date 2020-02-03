# ETL for Gates Mobile Access

## Overview

This repository contains scripts for the __Gates Mobile Access Mor Awaaz__ push call ETL. Every week for 52 weeks, 7200 __Sanchaar Kranti Yojana (SKY)__ programme beneficiaries in Raipur district, Chattisgarh will recieve a push call from our IVRS system. An automated voice will **give these beneficiaries information about their household's pending NREGA payments.** This information will include the total amount due to the household, which payment stage the pending transaction has reached, and the contact information of the Gram Rozgar Sevak in their panchayat if the payment has been rejected. The information pushed will be updated throughout the project based on user response. 

The NREGA payments process captures these variables on documents called __Fund Transfer Orders (FTOs)__. FTOs are generated at the block office in each block and uploaded to the NREGA FTO tracking system. The scrape visits the FTO tracking system every week to scrape all the FTOs in Raipur district. The scraped data gets used to create calling scripts, which are then sent to our calling platform so that the push calls can be made.

## SKY programme

The SKY programme is a Chattisgarh Government initiative to **expand women's access to mobile phones** in the state. Under this programme, **50,00,000 women are going to receive free mobile phones** from MicroMax and phone connectivity from Reliance Jio. The beneficiaries will recieve phones at distribution camps run by government officials. 

For this project, the **survey team sampled women** from among these beneficiaries in Raipur district, Chattisgarh. As part of the intervention, our survey team is carrying out supplementary enrollment camps which train women to use the phones they have recieved and explain what to expect in the push calls.

## Main scripts 

The scrape is based on a collection of scrapy spiders which visit the web-pages on the NREGA MIS that have the data we need. The shell scripts in the shell folder deploy the spiders on an AWS EC2 instance. The data is stored by the scrape in an AWS MySQL database instance and on AWS S3. The main scripts in the repository are described below in order of priority. 

## NREGA FTO Tracking System

The NREGA payments process **tracks each transaction** that has to be made under NREGA. This **information is digitized** and stored in the **FTO status tracking system.** On the **topmost page** relevant to us, the **total no. of FTOs at each stage** of the NREGA payments process is given. Each total has **hyperlinks to the list of FTO nos.** that were added to make up that total. These are themselves **hyperlinked to the payments and transaction information on each FTO.** The **scrape follows these hyperlinks** from the top totals to the bottom transactions.

### scrape

#### fto_branch.py

This script is run from **fto_etl_branch.sh**. It starts at 1231 am every morning. This spider visits Table 8.1.1 directly and scrapes the transactions data from there. It is the main scrape which populates the transactions-alt and other associated tables. The information from this scrape goes into the dynamic script. The reason for this is that we needed to get updated information on the status of an FTO every cycle. This just gets all the transactions for the fiscal year every single day. It has the following steps:

* Construct the **FTO URLs** that need to be scraped
* Open an instance of **headless Chrome**
* Load the target URLs from the ./scrape/data/ftos.json file
* The ftos.json file has target URLS for all FTOs which have received the first sign
* This makes sense because the first sign is the first step where the FTO appears on Table 8.1.1
* Then scrape every single hyperlink in these target URL - each URL corresponds to one FTO
* Wait for **response to appear** and scrape **the response**
* Write scraped data to MySQL DB **via the pipeline** 


#### fto_content.py 

There are multiple places on the MIS where FTOs are tracked.  Table 8.1.1 does not display the last two digits of a worker's bank account number but the FTO tracking system which is visited by this scrape does. Apart from this, the fto-content.py scrape gets all the fields which are scraped by fto-brancg. This script is run from **fto_etl.sh**. It has the following steps:

* Take a list of FTO nos from the **fto_queue table** as input 
* Construct the **FTO URLs** that need to be scraped
* Open an instance of **headless Chrome**
* Navigate Chrome instance to each **FTO URL**
* Select the **financial year drop-down** 
* Select the **correct financial year**
* Select the **state drop-down**
* Select the target **state** 
* Fill in the form with the target **FTO No.** 
* Select the **FTO No. drop-down**
* Select the **correct FTO No.**
* Wait for **response to appear** 
* Scrape **the response**
* Write scraped data to MySQL DB **via the pipeline**



#### fto_material.py

NREGA payments to work materials vendors are also tracked by the FTO tracking system. This scrapes materials FTOs from Table 8.1.1. The pattern of this scrape is identical to the fto-branch.py scrape except that it does not write to the database. This can be easily changed in the future by making the relevant backend changes by creating the tables in the database.

#### fto_urls.py

This visits Table 8.1.1 and scrapes the URLs for the list of FTOs at each stage. It then parses this list and creates a .csv list of FTOs at each stage. These .csvs are then written to the tracking table fto-queue in the database by the code described below. It is used by the fto_stage.sh shell script described below.

#### settings.py

This is the **scrapy settings file**. The important settings we customize for this project are: 

* **CLOSESPIDER_ERRORCOUNT**: 20 
* **CLOSESPIDER_TIMEOUT**: 7200 
* **CONCURRENT_REQUESTS**:

#### items.py

This is the **scrapy items** file. It defines the **items** that are going to be scraped by the scrapy project as classes composed of **scrapy field** attributes. Our file defines **three items**.

* NREGAItem: This is a **block summary table row**
* FTONo: this is an **FTO number**
* FTOItem: this is a **transaction**

#### pipelines.py 

This script contains the **scrapy pipeline objects** which process each **item** that is scraped by the spiders. Each pipeline has the following steps: 

* Take as input list of tables to write the item information to 
* Take as input a list of fields in each item that have to be converted to title case 
* Create a connection to the data-base

### tracker

#### make.py

This is executed by **fto-stage.sh** which is described below. It processes the scraped list of FTO nos. at each stage returned by **fto_urls.py** and creates a pandas data-frame containing a queue of FTOs along with information about them. This is then inserted into the FTO queue table in the database. 

#### update.py

This script updates the FTO queue tracker every day with the progress of the scrape and sends an email out to the team with information on how many FTOs are remaining. 

#### download.py

This script is run at the end of each day to download transactions and other data onto S3/Cyberduck where it can be accessed by the team.


### shell

#### fto-stage.sh 

* This script is located in the shell folder. 
* It is triggered by a cron job on the EC2 instance at 930 pm every evening. 
* It scrapes the updated list of FTOs at each stage in each block using the fto_urls spider. 
* This spider stores the list of FTOs at each stage as a set of .csvs in the output folder on the EC2 instance. 
* fto_stage.sh then calls .queue/make.py to process these .csvs into the format of the MySQL database fto_queue table and insert them there.

#### fto-etl.sh 

* Every morning at 6:01 am, the ETL shell script nrega_etl.sh is triggered by a cron job on an __Amazon EC2 instance__. 
* This script can be seen in the __shell__ folder. 
* This shell script triggers the following scripts in the order given below.  

#### fto-match-field.sh

* This script is found in the shell directory.
* Its main purpose is to pull field data, merge it with transaction/fto data from the database, 
  and output a csv in the appropriate format for BTT.
* It will be run every day, and a time needs to be selected. 
* It reformats necessary data in order to be able to match the job card numbers.
* A message is emailed to the team if there are no matches.

### common

#### helpers.py 

This file has common functions which are used by the spiders and pipelines file to **perform housekeeping tasks.** The important functions are: 

* **send_email**: This function sends an e-mail notifying the recipients given in **recipients.json** that the scrape has closed. 
* **clean_data**: This function strips leading and trailing whitespace from scraped items and converts the relevant fields to title case. 

* **insert_data**:  This function prepares a string SQL statement to insert data into the MySQL database. The SQL statement is based on the value of the variable **unique** which is 1 if the table that the data is going to be written to has to satisfy a uniqueness constraint and 0 otherwise.

### errors.py

This is a generic error-handling script. It takes the error messages in **error_messages.json** and sends an email to the team with the relevant error message. It is called by other scripts in the repository whenever a particular error happens.

### backend

#### db/schema.py 

This is the original **sqlalchemy** data-base schema.

#### db/create.py

This is the script which creates an instance of the original schema.

#### db/update.py

This contains helper functions that are used to modify the database.

#### ec2

This folder contains requirements files for the repository (*.txt). setup.sh installs all the dependencies on an AWS EC2 instance.

### logs

#### process_log.py

This file is called as the last step in the ETL. It processes the log file that the scrape creates and uploads it to Dropbox. It takes following steps: 

* **Delete** any existing log 
* **Load** the current log 
* Use the **Dropbox API** to upload it to a target folder 

### alembic

* We have stopped doing database migrations manually and have started using **Alembic**. 
* This folder is the boiler-plate Alembic folder which has the migration environment and migration scripts.

### script

#### .utils/
This folder contains helper scripts that format and clean the camp and scraped data before creating scripts.

#### get-static-script.py
Thie file's main purpose is to generate the static script. It gets the updated camp data from the camps table, merges it with the health-schedule.csv for that week and then creates the static script. The script is then uploaded to S3/Cyberduck to the tests folder for the team to use.


#### get-dynamic-script.py
This file's main purpose is to pull field data, merge it with transaction/FTO data from the database, and output a csv in the appropriate format for BTT. It reformats necessary data in order to be able to match the job card numbers. A message is emailed to the team if there are no matches. It then uploads the dynamic script to S3/Cyberduck to the tests folder for the team to use.

#### put-script.py
This puts the script into the scripts table in the database.

#### put-camp.py
This goes to S3/Cyberduck and merges all the camp files there every day and writes them to the enrolment-record table in the database. It is a running record of enrolment that was especially useful when we were doing a rolling start during the camp implementation.
