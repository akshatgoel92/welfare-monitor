# ETL for Gates Mobile Access

## Overview

This repository contains scripts for the __Gates Mobile Access Mor Awaaz__ push call ETL. Every week for 52 weeks, 7200 __Sanchaar Kranti Yojana (SKY)__ programme beneficiaries in Raipur district, Chattisgarh will recieve a push call from our IVRS system. An automated voice will **give these beneficiaries information about their household's pending NREGA payments.** This information will include the total amount due to the household, which payment stage the pending transaction has reached, and the contact information of the Gram Rozgar Sevak in their panchayat if the payment has been rejected. 

The NREGA payments process captures these variables on documents called __Fund Transfer Orders (FTOs)__. FTOs are generated at the block office in each block and uploaded to the NREGA FTO tracking system. The scrape visits the FTO tracking system every week to scrape all the FTOs in Raipur district. The scraped data gets used to create calling scripts, which are then sent to our calling platform so that the push calls can be made.  

## SKY programme

The SKY programme is a Chattisgarh Government initiative to **expand women's access to mobile phones** in the state. Under this programme, **50,00,000 women are going to receive free mobile phones** from MicroMax and phone connectivity from Reliance Jio. The beneficiaries will recieve phones at distribution camps run by government officials. 

For this project, the **survey team sampled 7200 women** from among these beneficiaries in Raipur district, Chattisgarh. As part of the intevention, our survey team is carrying out supplementary enrollment camps which train women to use the phones they have recieved and explain what to expect in the push calls.

## NREGA FTO Tracking System

The NREGA payments process **tracks each transaction** that has to be made under NREGA. This **information is digitized** and stored in the **FTO status tracking system.** On the **topmost page** relevant to us, the **total no. of FTOs at each stage** of the NREGA payments process is given. Each total has **hyperlinks to the list of FTO nos.** that were added to make up that total. These are themselves **hyperlinked to the payments and transaction information on each FTO.** The **scrape follows these hyperlinks** from the top totals to the bottom transactions using __scrapy__ and __Selenium__ and stores the results in a __MySQL__ data-base hosted on __Amazon Web Services__.

## Main scripts 

#### nrega_etl.sh 

* Every morning at 730 am, the ETL shell script nrega_etl.sh is triggered by a cron job on an __Amazon EC2 instance__. 

* This script can be seen in the __shell__ folder. 

* To look at the cron job which triggers this script, log in to the Amazon EC2 instance and use the commond **crontab -e.** 

* This shell script triggers the following scripts in the order given below. 

#### fto_stats.py

This script is run from nrega_etl.sh as soon as the cron triggers the script. It takes the following steps: 

* **Visits the block level summary page** for Raipur district 
* **Scrapes the total number of FTOs** at each stage
* **Follows the links in each of the cel**l in the block-level summary page
* **Scrapes the list of FTO numbers** for each block for each stage from the hyperlinked pages 
* **Writes the scraped list to DB** via the pipeline

#### fto_filter.py

This script is run from nrega_etl.sh as soon as fto_stats.py finishes. It filters the scraped FTO numbers to get only those FTOs that are needed for the call in that week. It takes the following steps: 

* FTOs are listed for each stage they have been through so we keep only FTO under the furthest stage
* **Removes any FTOs** older than 7 days 
* Queries the DB for the FTOs that have already been scraped 
* Removes these from the list of FTOs to be scraped 
* Queries the DB for FTOs for whom information has already been pushed
* Removes any FTOs for whom information has already been pushed 
* Writes to SQL DB table with list of FTOs to be scraped 

#### fto_content.py 

This script is run from **nrega_etl.sh** as soon as **fto_filter.py** finishes. It has the following steps:

* Take a list of FTO nos from the **fto_queue table** as input 
* Construct the **FTO URLs** that need to be scraped
* Open an instance of **headless Chrome**
* Navigate Chrome instance to **FTO URL**
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

#### items.py

This is the **scrapy items** file. It defines the **items** that are going to be scraped by the scrapy project as classes composed of **scrapy field** attributes. Our file defines **three items**.

* NREGAItem: This is a **block summary table row**

* FTONo: this is an **FTO number**

* FTOItem: this is a **transaction**

#### pipelines.py 

This script contains two **scrapy pipeline objects** which process each **item** that is scraped by the two spiders. Each pipeline has the following steps: 

* Take as input list of tables to write the item information to 
* Take as input a list of fields in each item that have to be converted to title case 
* Create a connection to the data-base  

#### helpers.py 

This file has common functions which are used by the spiders and pipelines file to **perform housekeeping tasks.** The important functions are: 

* **send_email**: This function sends an e-mail notifying the recipients given in **recipients.json** that the scrape has closed. 
* **clean_data**: This function strips leading and trailing whitespace from scraped items and converts the relevant fields to title case. 

* **insert_data**:  This function prepares a string SQL statement to insert data into the MySQL database. The SQL statement is based on the value of the variable **unique** which is 1 if the table that the data is going to be written to has to satisfy a uniqueness constraint and 0 otherwise.

#### settings.py

This is the **scrapy settings file**. The important settings we customize for this project are: 

* **CLOSESPIDER_ERRORCOUNT**: 20 
* **CLOSESPIDER_TIMEOUT**: 7200 
* **CONCURRENT_REQUESTS**:

#### process_log.py

This file is called as the last step in the ETL. It processes the log file that the scrape creates and uploads it to Dropbox. It takes following steps: 

* **Delete** any existing log 
* **Load** the current log 
* Use the **Dropbox API** to upload it to a target folder 

#### db_schema.py 

This is the **sqlalchemy** data-base schema. The schema is normalized. It creates the following tables with the following keys: 

* **accounts**: id 
* **blocks**: block code 
* **banks**: ifsc code  
* **fto_summary**: id 
* **fto_nos**: id 
* **fto_queue**: fto_no 
* **transactions**: transaction reference no. 
* **wage_lists**: wage list no. 